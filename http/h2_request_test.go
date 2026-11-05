//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/hpack"
)

// TestH2HeaderKeyTables holds the lookup tables to what they stand in for:
// strings.ToLower of a key, and textproto.CanonicalMIMEHeaderKey of a name.
func TestH2HeaderKeyTables(t *testing.T) {
	for key, name := range h2LowerKeys {
		if want := strings.ToLower(key); name != want {
			t.Errorf("h2LowerKeys[%q] = %q, want %q", key, name, want)
		}
		if want := textproto.CanonicalMIMEHeaderKey(name); key != want {
			t.Errorf("%q is not canonical: CanonicalMIMEHeaderKey makes it %q", key, want)
		}
		if got := h2CanonicalKey(name); got != key {
			t.Errorf("h2CanonicalKey(%q) = %q, want %q", name, got, key)
		}
	}
	for _, name := range []string{"x-custom", "accept-patch", "a", "x_under", "x.dot"} {
		if got, want := h2CanonicalKey(name), textproto.CanonicalMIMEHeaderKey(name); got != want {
			t.Errorf("h2CanonicalKey(%q) = %q, want %q", name, got, want)
		}
		if got := h2LowerKey(textproto.CanonicalMIMEHeaderKey(name)); got != name {
			t.Errorf("h2LowerKey(%q) = %q, want %q", textproto.CanonicalMIMEHeaderKey(name), got, name)
		}
	}
}

// TestH2KnownFields holds the one lookup a known name or key costs to the
// checks it stands in for: that the name is valid and lowercase, its key
// canonical, and whether it is hop-by-hop.
func TestH2KnownFields(t *testing.T) {
	for name, known := range h2KnownNames {
		if known.name != name || !h2ValidHeaderName(name) || known.key != textproto.CanonicalMIMEHeaderKey(name) ||
			known.hop != h2HopField(name) || h2KnownKeys[known.key] != known {
			t.Errorf("h2KnownNames[%q] = %+v", name, known)
		}
	}
	if len(h2KnownKeys) != len(h2KnownNames) {
		t.Errorf("%d known keys, %d known names", len(h2KnownKeys), len(h2KnownNames))
	}
	for _, name := range []string{"connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade"} {
		if !h2HopField(name) || !h2KnownNames[name].hop {
			t.Errorf("%q is not hop-by-hop", name)
		}
	}
	for _, name := range []string{"", "te", "connectio", "keep-alivE", "upgrades", "Connection", "transfer-encodinG"} {
		if h2HopField(name) {
			t.Errorf("h2HopField(%q) = true", name)
		}
	}
	for _, key := range []string{"Content-Type", "Last-Modified", "Connection", "X-Mine", "x-lower", ":status"} {
		if err := h2CheckField(key, []string{"v"}); err != nil {
			t.Errorf("h2CheckField(%q): %v", key, err)
		}
	}
	for _, key := range []string{"", "Bad Name", "X:Y", "X\x00", "X\xc3\xa9"} {
		if h2CheckField(key, []string{"v"}) == nil {
			t.Errorf("h2CheckField(%q) passed", key)
		}
	}
	if h2CheckField("Content-Type", []string{"a\r\nb"}) == nil {
		t.Error("h2CheckField passed a value with CRLF")
	}
}

// TestH2ByteClasses holds each class of h2ByteClass to the test of a byte it
// stands in for.
func TestH2ByteClasses(t *testing.T) {
	for i := 0; i < 256; i++ {
		c := byte(i)
		s := string([]byte{'a', c})
		for _, tt := range []struct {
			name      string
			got, want bool
		}{
			{"token", h2ValidMethod(s), tokenByte[c]},
			{"scheme", h2ValidScheme(s), c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.'},
			{"host", h2ValidAuthority(s), c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("!$%&'()*+,-.:;=[]_~", c) >= 0},
			{"name", h2ValidHeaderName(s), !(c >= 'A' && c <= 'Z' || c <= ' ' || c >= 0x7f || c == ':')},
			{"value", h2ValidHeaderValue(s), c != 0 && c != '\r' && c != '\n' && c != ' ' && c != '\t'},
			{"path", h2ParsePath(&url.URL{}, "/"+s) == nil, c > ' ' && c != 0x7f && c != '%'},
		} {
			if tt.got != tt.want {
				t.Errorf("%s of %q: %v, want %v", tt.name, s, tt.got, tt.want)
			}
		}
	}
	if !h2ValidHeaderName(":path") || h2ValidHeaderName("::") {
		t.Error("a pseudo-header's leading colon is not told apart from another")
	}
	if h2ValidScheme("1a") || h2ValidScheme("+a") || !h2ValidScheme("a1+-.") {
		t.Error("a scheme's first byte is not held to a letter")
	}
}

func testH2Fields(path string, regular ...hpack.HeaderField) []hpack.HeaderField {
	return append([]hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: "example.com"}, {Name: ":path", Value: path},
	}, regular...)
}

// TestH2NewRequest checks the request a stream's fields become: its URL the
// one url.ParseRequestURI makes of the path, and its header's values, taken
// from the stream while they last, each its own.
func TestH2NewRequest(t *testing.T) {
	sc := &h2ServerConn{remoteAddr: "192.0.2.1:1234"}
	for _, path := range []string{"/", "/echo", "/a/b;c=d", "/q?x=1&y=2", "/q?", "/q?a?b", "/%41", "/sp%20ace",
		"/!x", "/a#frag", "*"} {
		st := &h2ServerStream{sc: sc}
		fields := testH2Fields(path)
		if path == "*" {
			// Only OPTIONS may ask for "*" (RFC 9113 section 8.3.1).
			fields[0].Value = stdhttp.MethodOptions
		}
		req, err := sc.newRequest(fields, st)
		if err != nil {
			t.Fatalf("%q: %v", path, err)
		}
		want := &url.URL{Path: "*"}
		if path != "*" {
			if want, err = url.ParseRequestURI(path); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(req.URL, want) {
			t.Errorf("%q: URL %#v, want %#v", path, req.URL, want)
		}
		if req.RequestURI != path || req.Host != "example.com" || req.RemoteAddr != sc.remoteAddr {
			t.Errorf("%q: RequestURI %q, Host %q, RemoteAddr %q", path, req.RequestURI, req.Host, req.RemoteAddr)
		}
		if req != &st.block.Request || req.URL != &st.block.URL {
			t.Errorf("%q: the request is not the stream's own", path)
		}
	}

	// More values than the stream has room for, and a key that repeats.
	var regular []hpack.HeaderField
	for i := 0; i < h2RequestValues+2; i++ {
		regular = append(regular, hpack.HeaderField{Name: "x-field-" + string(rune('a'+i)), Value: "v"})
	}
	regular = append(regular, hpack.HeaderField{Name: "x-field-a", Value: "again"},
		hpack.HeaderField{Name: "content-type", Value: "text/plain"})
	st := &h2ServerStream{sc: sc}
	req, err := sc.newRequest(testH2Fields("/", regular...), st)
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Header["X-Field-A"]; !reflect.DeepEqual(got, []string{"v", "again"}) {
		t.Errorf("X-Field-A = %q", got)
	}
	for i := 1; i < h2RequestValues+2; i++ {
		key := "X-Field-" + string(rune('A'+i))
		if got := req.Header[key]; !reflect.DeepEqual(got, []string{"v"}) {
			t.Errorf("%s = %q", key, got)
		}
	}
	if got := req.Header.Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q", got)
	}
	// A handler appending to one key's values does not write over the next.
	req.Header["X-Field-B"] = append(req.Header["X-Field-B"], "appended")
	if got := req.Header["X-Field-C"]; !reflect.DeepEqual(got, []string{"v"}) {
		t.Errorf("appending to X-Field-B changed X-Field-C to %q", got)
	}
}

// TestH2NewRequestRejectsMalformedPseudoHeaders checks that a pseudo-header's
// value is held to what its field is (RFC 9113 section 8.3): a method that is
// a token, a path in origin form, or "*" for OPTIONS, and an authority, or a
// Host standing in for it, that holds only what a host and port may. Each of
// these once reached the handler with its CR, LF or NUL.
func TestH2NewRequestRejectsMalformedPseudoHeaders(t *testing.T) {
	sc := &h2ServerConn{remoteAddr: "192.0.2.1:1234"}
	set := func(name, value string) []hpack.HeaderField {
		fields := testH2Fields("/")
		for i := range fields {
			if fields[i].Name == name {
				fields[i].Value = value
			}
		}
		return fields
	}
	for _, tt := range []struct {
		name   string
		fields []hpack.HeaderField
	}{
		{"method with a request line", set(":method", "GET /x HTTP/1.1\r\nX-Evil: 1\r\n")},
		{"method with a space", set(":method", "GET /")},
		{"method with a separator", set(":method", "GE(T")},
		{"authority with CRLF", set(":authority", "a\r\nInjected: 1\x00")},
		{"authority with a space", set(":authority", "a b")},
		{"authority with userinfo", set(":authority", "user@host")},
		{"authority with a slash", set(":authority", "host/x")},
		{"path with a space", set(":path", "/a b")},
		{"path with a tab", set(":path", "/a\tb")},
		{"path with DEL", set(":path", "/a\x7f")},
		{"path in absolute form", set(":path", "http://h/")},
		{"asterisk for GET", set(":path", "*")},
		{"scheme with a space", set(":scheme", "ht tp")},
		{"scheme starting with a digit", set(":scheme", "1http")},
		{"Host with a space", []hpack.HeaderField{{Name: ":method", Value: "GET"}, {Name: ":scheme", Value: "http"},
			{Name: ":path", Value: "/"}, {Name: "host", Value: "a b"}}},
		{"protocol with a space", []hpack.HeaderField{{Name: ":method", Value: "CONNECT"}, {Name: ":protocol", Value: "web socket"},
			{Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "a"}, {Name: ":path", Value: "/"}}},
		// A path simpleTarget does not take is checked before it is parsed.
		{"path with a space in its query", set(":path", "/a?b c")},
		{"escaped path with a space", set(":path", "/%41 b")},
		{"path with a CR past a byte beyond ASCII", set(":path", "/\xc3\xa9\r")},
		{"extended CONNECT path with a space", []hpack.HeaderField{{Name: ":method", Value: "CONNECT"}, {Name: ":protocol", Value: "websocket"},
			{Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "a"}, {Name: ":path", Value: "/a b"}}},
		{"extended CONNECT path of an asterisk", []hpack.HeaderField{{Name: ":method", Value: "CONNECT"}, {Name: ":protocol", Value: "websocket"},
			{Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "a"}, {Name: ":path", Value: "*"}}},
		{"empty method", set(":method", "")},
		{"empty protocol", []hpack.HeaderField{{Name: ":method", Value: "CONNECT"}, {Name: ":protocol", Value: ""},
			{Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "a"}, {Name: ":path", Value: "/"}}},
		// A regular field's name, known or not.
		{"hop-by-hop field", testH2Fields("/", hpack.HeaderField{Name: "connection", Value: "close"})},
		{"hop-by-hop upgrade", testH2Fields("/", hpack.HeaderField{Name: "upgrade", Value: "h2c"})},
		{"known field with a CR", testH2Fields("/", hpack.HeaderField{Name: "accept", Value: "a\rb"})},
		{"unknown field in upper case", testH2Fields("/", hpack.HeaderField{Name: "X-Mine", Value: "1"})},
		{"unknown field with a colon", testH2Fields("/", hpack.HeaderField{Name: "x:mine", Value: "1"})},
		{"empty field name", testH2Fields("/", hpack.HeaderField{Name: "", Value: "1"})},
	} {
		if req, err := sc.newRequest(tt.fields, &h2ServerStream{sc: sc}); err == nil {
			t.Errorf("%s: accepted, Method %q Host %q RequestURI %q", tt.name, req.Method, req.Host, req.RequestURI)
		}
	}
	// What they may hold still passes.
	for _, tt := range []struct {
		name   string
		fields []hpack.HeaderField
	}{
		{"extension method", set(":method", "PROPFIND")},
		{"IPv6 authority with a port", set(":authority", "[::1]:8080")},
		{"authority with a zone", set(":authority", "[fe80::1%25en0]")},
		{"scheme with marks", set(":scheme", "svn+ssh")},
		{"path with a query", set(":path", "/a?b=c%20d&e=/f")},
		{"asterisk for OPTIONS", append(set(":path", "*")[1:], hpack.HeaderField{Name: ":method", Value: "OPTIONS"})},
		{"path with an escape and a byte beyond ASCII", set(":path", "/%41/\xc3\xa9?q=\xc3\xa9")},
		{"scheme in upper case", set(":scheme", "HTTPS")},
		{"known and unknown fields", testH2Fields("/", hpack.HeaderField{Name: "accept", Value: "*/*"},
			hpack.HeaderField{Name: "x-mine", Value: "1"}, hpack.HeaderField{Name: "keep-alive-ish", Value: "1"})},
	} {
		if _, err := sc.newRequest(tt.fields, &h2ServerStream{sc: sc}); err != nil {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
}

// h2HeaderSink moves the map the baseline makes to the heap, as the request's
// header is.
var h2HeaderSink map[string][]string

// TestH2NewRequestAllocations guards what building an ordinary request
// allocates once its stream exists: the header map and nothing else.
func TestH2NewRequestAllocations(t *testing.T) {
	sc := &h2ServerConn{remoteAddr: "192.0.2.1:1234"}
	fields := testH2Fields("/echo?x=1",
		hpack.HeaderField{Name: "content-length", Value: "1024"},
		hpack.HeaderField{Name: "content-type", Value: "application/octet-stream"},
		hpack.HeaderField{Name: "user-agent", Value: "test"},
		hpack.HeaderField{Name: "accept-encoding", Value: "gzip"})
	st := &h2ServerStream{sc: sc}
	baseline := testing.AllocsPerRun(100, func() {
		h2HeaderSink = make(map[string][]string, len(fields))
		h2HeaderSink["Content-Length"] = nil
	})
	allocs := testing.AllocsPerRun(100, func() {
		if _, err := sc.newRequest(fields, st); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > baseline {
		t.Fatalf("newRequest took %v allocations, the header map alone %v", allocs, baseline)
	}
}

// TestH2RetainedBodyOutlivesHandler checks that a request's body, whose
// buffer is the pool's, stays the request's until its last Release: read
// after the handler has returned, while other streams take buffers from the
// pool and give them back, it is still what the client sent; read after the
// Release, it reports ErrBodyReleased. That last read keeps the request past
// its response, which only a server that does not recycle its requests
// allows, so this one does not.
//
// The handler's return is a hold too, so the Release is the last one only
// once the handler has returned. On a loaded machine the handler's goroutine
// may not have recorded its return by the time the other one's sleep is over;
// its return then gives the body back, on its own goroutine, while the read
// goes on, which is a race and reads EOF, as macOS CI found. So the release
// waits for the handler to have returned first.
func TestH2RetainedBodyOutlivesHandler(t *testing.T) {
	released := make(chan error, 64)
	config := DefaultConfig()
	setReuse(&config, false)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		c.Retain()
		go func() {
			time.Sleep(10 * time.Millisecond)
			for c.word.Load()&ctxServed != 0 {
				time.Sleep(time.Millisecond)
			}
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				body = []byte(err.Error())
			}
			_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
			c.Release()
			_, err = c.Request.Body.Read(make([]byte, 1))
			released <- err
		}()
	})))
	var protocols stdhttp.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{Protocols: &protocols}}
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			body := bytes.Repeat([]byte{byte('a' + i%26)}, 512+i*97)
			resp, err := client.Post(fmt.Sprintf("http://%s/%d", addr, i), "application/octet-stream", bytes.NewReader(body))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			got, _ := io.ReadAll(resp.Body)
			if resp.ProtoMajor != 2 || !bytes.Equal(got, body) {
				t.Errorf("request %d: %s, %d bytes back of %d", i, resp.Proto, len(got), len(body))
			}
		})
	}
	wg.Wait()
	for i := 0; i < 64; i++ {
		if err := <-released; !errors.Is(err, ErrBodyReleased) {
			t.Fatalf("a read after the last Release returned %v, want ErrBodyReleased", err)
		}
	}
}
