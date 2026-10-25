//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/http/httptrace"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/lesismal/fib/bufferpool"
)

// stdResponseHead is what ReadResponse makes of head, as parseHead used it
// before the client parsed heads itself.
func stdResponseHead(head []byte, req *stdhttp.Request) (*stdhttp.Response, error) {
	resp, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(head)), req)
	if err == nil {
		_ = resp.Body.Close()
	}
	return resp, err
}

// sameResponseHead reports how got, from parseResponseHead, differs from
// want, from ReadResponse, in what a caller of the client sees, or "".
func sameResponseHead(got, want *stdhttp.Response) string {
	switch {
	case got.Status != want.Status:
		return "Status " + got.Status + " / " + want.Status
	case got.StatusCode != want.StatusCode, got.Proto != want.Proto, got.ProtoMajor != want.ProtoMajor,
		got.ProtoMinor != want.ProtoMinor:
		return "status line"
	case !reflect.DeepEqual(got.Header, want.Header):
		return "Header"
	case got.ContentLength != want.ContentLength:
		return "ContentLength"
	case !reflect.DeepEqual(got.TransferEncoding, want.TransferEncoding):
		return "TransferEncoding"
	case got.Close != want.Close:
		return "Close"
	case !reflect.DeepEqual(got.Trailer, want.Trailer):
		return "Trailer"
	case got.Uncompressed != want.Uncompressed, got.Request != want.Request, got.TLS != want.TLS:
		return "other fields"
	}
	return ""
}

var responseHeads = []string{
	"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\n",
	"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nDate: Tue, 20 Oct 2026 10:00:00 GMT\r\nContent-Length: 12\r\n\r\n",
	"HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n",
	"HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n",
	"HTTP/1.1 404 Not Found\r\n\r\n",
	"HTTP/1.0 200 OK\r\nContent-Length: 3\r\n\r\n",
	"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 3\r\n\r\n",
	"HTTP/1.0 200 OK\r\nConnection: Keep-Alive, close\r\n\r\n",
	"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 3\r\n\r\n",
	"HTTP/1.1 200 OK\r\nConnection: upgrade, CLOSE\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTransfer-Encoding: Chunked\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunKed\r\n\r\n", // a Kelvin sign
	"HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Sum\r\n\r\n",
	"HTTP/1.1 200 OK\r\nTrailer: X-Sum\r\nContent-Length: 1\r\n\r\n",
	"HTTP/1.1 204 No Content\r\nContent-Length: 10\r\n\r\n",
	"HTTP/1.1 304 Not Modified\r\nEtag: \"x\"\r\n\r\n",
	"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
	"HTTP/1.1 100 Continue\r\n\r\n",
	"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n",
	"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
	"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",
	"HTTP/1.1 200 OK\r\nContent-Length: \r\n\r\n",
	"HTTP/1.1 200 OK\r\nContent-Length:  7 \r\n\r\n",
	"HTTP/1.1 200 OK\r\ncontent-length: 7\r\nSET-COOKIE: a=1\r\nset-cookie: b=2\r\nx-custom-thing: v\r\n\r\n",
	"HTTP/1.1 200 OK\r\nPragma: no-cache\r\n\r\n",
	"HTTP/1.1 200 OK\r\nPragma: no-cache\r\nCache-Control: max-age=1\r\n\r\n",
	"HTTP/1.1 200 OK\r\nX-Empty:\r\nX-Tab:\tv\t\r\n\r\n",
	"HTTP/1.1 200 OK\r\nX-Folded: a\r\n b\r\n\r\n",
	"HTTP/1.1 200 OK\r\nX-A : 1\r\n\r\n",
	"HTTP/1.1 200 OK\r\nBad\x01: 1\r\n\r\n",
	"HTTP/1.1 200 OK\r\nX-A: 1\x00\r\n\r\n",
	"HTTP/1.1 200 OK\nContent-Length: 1\n\n",
	"HTTP/1.1 200 OK\r\nX-A: 1\nX-B: 2\r\n\r\n",
	"HTTP/1.1  200 OK\r\n\r\n",
	"HTTP/1.1 2000 OK\r\n\r\n",
	"HTTP/1.1 20x OK\r\n\r\n",
	"HTTP/2.0 200 OK\r\n\r\n",
	"HTTP/1.1 999 Odd\r\n\r\n",
	"ICY 200 OK\r\n\r\n",
	"HTTP/1.1 200 OK\r\n: no name\r\n\r\n",
	"HTTP/1.1 200 OK\r\nNo-Colon\r\n\r\n",
	"HTTP/1.1 200 OK\r\nX-Unicode: caf\xc3\xa9\r\n\r\n",
}

// TestParseResponseHead holds parseResponseHead to ReadResponse: whatever it
// takes, it reads as ReadResponse does, and it takes the heads most
// responses have.
func TestParseResponseHead(t *testing.T) {
	fast := map[string]bool{responseHeads[0]: true, responseHeads[1]: true, responseHeads[4]: true,
		responseHeads[8]: true, responseHeads[10]: true, responseHeads[27]: true}
	for _, method := range []string{stdhttp.MethodGet, stdhttp.MethodHead, stdhttp.MethodPost} {
		for _, head := range responseHeads {
			req := &stdhttp.Request{Method: method}
			got, ok := parseResponseHead([]byte(head), req)
			want, err := stdResponseHead([]byte(head), req)
			if !ok {
				if fast[head] {
					t.Errorf("%s %q: left to net/http", method, head)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s %q: taken, but net/http refuses it: %v", method, head, err)
				continue
			}
			if d := sameResponseHead(got, want); d != "" {
				t.Errorf("%s %q: %s differs:\n got %+v\nwant %+v", method, head, d, got, want)
			}
			if got.Body != stdhttp.NoBody {
				t.Errorf("%s %q: Body %v", method, head, got.Body)
			}
		}
	}
}

func FuzzParseResponseHead(f *testing.F) {
	for _, head := range responseHeads {
		f.Add(head, false)
	}
	f.Fuzz(func(t *testing.T, head string, isHead bool) {
		if !strings.HasSuffix(head, "\r\n\r\n") || strings.Contains(head[:len(head)-4], "\r\n\r\n") {
			return // parseHead hands over exactly one head, up to its blank line
		}
		req := &stdhttp.Request{Method: stdhttp.MethodGet}
		if isHead {
			req.Method = stdhttp.MethodHead
		}
		got, ok := parseResponseHead([]byte(head), req)
		if !ok {
			return
		}
		want, err := stdResponseHead([]byte(head), req)
		if err != nil {
			t.Fatalf("%q: taken, but net/http refuses it: %v", head, err)
		}
		if d := sameResponseHead(got, want); d != "" {
			t.Fatalf("%q: %s differs:\n got %+v\nwant %+v", head, d, got, want)
		}
	})
}

// stdMarshalRequest is marshalRequest as it was, through Request.Write alone.
func stdMarshalRequest(req *stdhttp.Request, body []byte) ([]byte, error) {
	http10 := req.ProtoMajor == 1 && req.ProtoMinor == 0
	out := *req
	if http10 {
		out.ContentLength = int64(len(body))
		out.TransferEncoding = nil
		out.Trailer = nil
		if len(body) == 0 {
			out.Body = nil
		}
	}
	if out.Body != nil && out.Body != stdhttp.NoBody {
		out.Body = io.NopCloser(bytes.NewReader(body))
	}
	var buf bytes.Buffer
	if err := out.Write(&buf); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	if http10 {
		lineEnd := bytes.Index(data, []byte("\r\n"))
		if lineEnd < 0 || !bytes.HasSuffix(data[:lineEnd], []byte(" HTTP/1.1")) {
			return nil, fmt.Errorf("http: cannot write request line %q as HTTP/1.0", data[:max(lineEnd, 0)])
		}
		data[lineEnd-1] = '0'
	}
	return data, nil
}

// stdStreamedStart is what marshalStreamedHead took from Request.Write
// before its own framing: the head without Content-Length: 0 and without the
// blank line.
func stdStreamedStart(req *stdhttp.Request) ([]byte, error) {
	clone := *req
	clone.Body, clone.GetBody = nil, nil
	clone.ContentLength = 0
	clone.TransferEncoding = nil
	clone.Trailer = nil
	var buf bytes.Buffer
	if err := clone.Write(&buf); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	if i := bytes.Index(data, []byte("\r\nContent-Length: 0\r\n")); i >= 0 {
		data = append(data[:i+2], data[i+len("\r\nContent-Length: 0\r\n"):]...)
	}
	return data[:len(data)-2], nil
}

type requestCase struct {
	name     string
	method   string
	target   string
	host     string
	header   stdhttp.Header
	body     string
	declared int64 // ContentLength, -2 for len(body)
	http10   bool
	close    bool
	fast     bool // the fast path must take it
}

func (c requestCase) request() (*stdhttp.Request, []byte) {
	u, err := url.Parse(c.target)
	if err != nil {
		panic(err)
	}
	req := &stdhttp.Request{Method: c.method, URL: u, Host: c.host, Header: c.header, Close: c.close,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1}
	if c.http10 {
		req.Proto, req.ProtoMinor = "HTTP/1.0", 0
	}
	if req.Header == nil {
		req.Header = stdhttp.Header{}
	}
	var body []byte
	if c.body != "" || c.declared != 0 {
		body = []byte(c.body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = c.declared
		if c.declared == -2 {
			req.ContentLength = int64(len(body))
		}
	}
	return req, body
}

var proxied = stdhttp.Header{"User-Agent": {}, "X-Forwarded-For": {"127.0.0.1"}, "X-Forwarded-Proto": {"http"},
	"X-Forwarded-Host": {"127.0.0.1:8080"}, "Forwarded": {`for=127.0.0.1;host="127.0.0.1:8080";proto=http`}}

var requestCases = []requestCase{
	{name: "proxied", method: "GET", target: "/proxy", host: "127.0.0.1:8080", header: proxied, fast: true},
	{name: "default agent", method: "GET", target: "http://example.com/a?b=c", fast: true},
	{name: "no method", target: "/x", host: "h", fast: true},
	{name: "post", method: "POST", target: "/p", host: "h", body: "hello", declared: -2,
		header: stdhttp.Header{"Content-Type": {"text/plain"}}, fast: true},
	{name: "post empty", method: "POST", target: "/p", host: "h", fast: true},
	{name: "put empty body", method: "PUT", target: "/p", host: "h", declared: 0, body: ""},
	{name: "delete", method: "DELETE", target: "/d", host: "h", fast: true},
	{name: "unknown length", method: "POST", target: "/p", host: "h", body: "abc", declared: 0},
	{name: "chunked", method: "POST", target: "/p", host: "h", body: "abc", declared: -1},
	{name: "http/1.0", method: "POST", target: "/p", host: "h", body: "abc", declared: 0, http10: true, fast: true},
	{name: "http/1.0 get", method: "GET", target: "/p", host: "h", http10: true, fast: true},
	{name: "close", method: "GET", target: "/c", host: "h", close: true},
	{name: "many values", method: "GET", target: "/v", host: "h", fast: true, header: stdhttp.Header{
		"Accept": {"a", " b ", "c\r\nInjected: 1"}, "Cookie": {"x=1; y=2"}, "User-Agent": {" agent\n "},
		"Content-Length": {"99"}, "Transfer-Encoding": {"chunked"}, "Trailer": {"X"}, "Host": {"other"},
		"Bad Name": {"dropped"}, "X-Empty": {}, "X-Blank": {""}}},
	{name: "empty agent", method: "GET", target: "/", host: "h", header: stdhttp.Header{"User-Agent": {""}}, fast: true},
	{name: "unicode host", method: "GET", target: "/", host: "bücher.example"},
	{name: "zone", method: "GET", target: "/", host: "[fe80::1%25en0]:80"},
	{name: "bad host", method: "GET", target: "/", host: "a b"},
	{name: "escaped path", method: "GET", target: "/a%20b/c?d=%2F", host: "h", fast: true},
	{name: "bad method", method: "GE T", target: "/", host: "h"},
	{name: "connect", method: "CONNECT", target: "example.com:443", host: "example.com:443"},
	{name: "many fields", method: "GET", target: "/", host: "h", fast: true, header: func() stdhttp.Header {
		h := stdhttp.Header{}
		for _, k := range strings.Fields("A B C D E F G H I J K L M N O P Q R S T U V W X Y Z") {
			h["X-"+k] = []string{k}
		}
		return h
	}()},
}

// TestFastRequest holds fastRequest and fastStreamedHead to Request.Write's
// output: whatever they take, they write byte for byte as it does.
func TestFastRequest(t *testing.T) {
	for _, c := range requestCases {
		req, body := c.request()
		want, wantErr := stdMarshalRequest(req, body)
		req, body = c.request()
		got, ok := fastRequest(req, body, c.http10)
		switch {
		case !ok && c.fast:
			t.Errorf("%s: left to net/http", c.name)
		case ok && wantErr != nil:
			t.Errorf("%s: taken, but net/http refuses it: %v", c.name, wantErr)
		case ok && !bytes.Equal(got, want):
			t.Errorf("%s:\n got %q\nwant %q", c.name, got, want)
		}
		// marshalRequest itself, whichever way it goes.
		req, body = c.request()
		if got, _, err := marshalRequest(req, body); (err != nil) != (wantErr != nil) || err == nil && !bytes.Equal(got, want) {
			t.Errorf("%s: marshalRequest %q, %v; want %q, %v", c.name, got, err, want, wantErr)
		}

		if c.http10 {
			continue
		}
		req, _ = c.request()
		wantStart, wantErr := stdStreamedStart(req)
		req, _ = c.request()
		gotStart, ok := fastStreamedHead(req)
		switch {
		case !ok && c.fast:
			t.Errorf("%s: streamed head left to net/http", c.name)
		case ok && wantErr != nil:
			t.Errorf("%s: streamed head taken, but net/http refuses it: %v", c.name, wantErr)
		case ok && !bytes.Equal(gotStart, wantStart):
			t.Errorf("%s: streamed head:\n got %q\nwant %q", c.name, gotStart, wantStart)
		}
	}

	// A trace on the request hears of each field as net/http writes it, so
	// such a request is left to net/http.
	req, body := requestCases[0].request()
	req = req.WithContext(httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{}))
	if _, ok := fastRequest(req, body, false); ok {
		t.Error("a traced request was taken")
	}
}

func FuzzFastRequest(f *testing.F) {
	f.Add("GET", "/proxy?a=b", "127.0.0.1:8080", "X-Forwarded-For", "127.0.0.1", "", false, false)
	f.Add("POST", "/p", "h", "Content-Type", "text/plain", "hello", true, false)
	f.Add("PUT", "/", "h", "User-Agent", " x\r\n", "", false, true)
	f.Fuzz(func(t *testing.T, method, target, host, key, value, body string, declare, http10 bool) {
		u, err := url.Parse(target)
		if err != nil {
			return
		}
		build := func() (*stdhttp.Request, []byte) {
			req := &stdhttp.Request{Method: method, URL: u, Host: host, Header: stdhttp.Header{key: {value}},
				Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1}
			if http10 {
				req.ProtoMinor = 0
			}
			var b []byte
			if body != "" {
				b = []byte(body)
				req.Body = io.NopCloser(bytes.NewReader(b))
				if declare {
					req.ContentLength = int64(len(b))
				}
			}
			return req, b
		}
		req, b := build()
		got, ok := fastRequest(req, b, http10)
		if !ok {
			return
		}
		req, b = build()
		want, err := stdMarshalRequest(req, b)
		if err != nil {
			t.Fatalf("taken, but net/http refuses it: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("\n got %q\nwant %q", got, want)
		}
	})
}

// TestClientCodecAllocs bounds what writing a proxied request and reading a
// plain response head allocate: nothing for the one, whose buffer is the
// pool's and goes back to it, and for the other the Response, its Header (a
// map and its table), the head's string and the values' backing.
func TestClientCodecAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops buffers under the race detector")
	}
	req, _ := requestCases[0].request()
	if n := testing.AllocsPerRun(100, func() {
		data, pooled, _ := marshalRequest(req, nil)
		if !pooled {
			t.Fatal("marshalRequest: the fast path's buffer is not the pool's")
		}
		bufferpool.Put(data)
	}); n > 0 {
		t.Errorf("marshalRequest: %v allocations, want 0", n)
	}
	head := []byte(responseHeads[1])
	get := &stdhttp.Request{Method: stdhttp.MethodGet}
	if n := testing.AllocsPerRun(100, func() { _, _ = parseResponseHead(head, get) }); n > 5 {
		t.Errorf("parseResponseHead: %v allocations, want 5", n)
	}
}

func BenchmarkMarshalRequest(b *testing.B) {
	req, _ := requestCases[0].request()
	b.Run("fib", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _, _ = marshalRequest(req, nil)
		}
	})
	b.Run("net-http", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = stdMarshalRequest(req, nil)
		}
	})
}

func BenchmarkParseResponseHead(b *testing.B) {
	head := []byte(responseHeads[1])
	req := &stdhttp.Request{Method: stdhttp.MethodGet}
	b.Run("fib", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = parseResponseHead(head, req)
		}
	})
	b.Run("net-http", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = stdResponseHead(head, req)
		}
	})
}

// FuzzDecodeChunks holds decodeChunks to net/http's chunked reader: a body it
// decodes, net/http decodes the same, with no trailer.
func FuzzDecodeChunks(f *testing.F) {
	for _, body := range []string{"5\r\nhello\r\n0\r\n\r\n", "1;ext=1\r\na\r\n0\r\n\r\n", " 1\r\nb\r\n0\r\n\r\n",
		"1 \t\r\nb\r\n0\r\n\r\n", "0\r\nX: 1\r\n\r\n", "1\nb\r\n0\r\n\r\n", "00000000000000001\r\nb\r\n0\r\n\r\n"} {
		f.Add(body)
	}
	f.Fuzz(func(t *testing.T, chunks string) {
		got, ok := decodeChunks(nil, []byte(chunks))
		if !ok {
			return
		}
		head := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
		resp, err := stdhttp.ReadResponse(bufio.NewReader(strings.NewReader(head+chunks)), &stdhttp.Request{Method: "GET"})
		if err != nil {
			t.Fatal(err)
		}
		want, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("%q: decoded, but net/http refuses it: %v", chunks, err)
		}
		if !bytes.Equal(got, want) || len(resp.Trailer) != 0 {
			t.Fatalf("%q: %q, net/http %q with trailer %v", chunks, got, want, resp.Trailer)
		}
	})
}

// TestReadWhole checks that a body read into a buffer of its declared length
// comes out as io.ReadAll reads it, whether it is as long as declared,
// shorter or longer, and that a read error is reported.
func TestReadWhole(t *testing.T) {
	for _, tc := range []struct {
		body     string
		declared int64
	}{
		{"hello", 5}, {"hello", 3}, {"hello", 9}, {"hello", 0}, {"hello", -1}, {"", 4},
		{strings.Repeat("x", 4096), 4096}, {strings.Repeat("x", 4097), 4096},
		// A declared length far past the body is the peer's word, not a
		// buffer to take before any of it has arrived.
		{"hello", 1 << 40}, {"hello", 1<<40 + 1}, {strings.Repeat("x", 1<<20+1), 1 << 40},
	} {
		got, err := readWhole(iotest.OneByteReader(strings.NewReader(tc.body)), tc.declared)
		if err != nil || string(got) != tc.body {
			t.Errorf("readWhole(%d bytes, %d) = %d bytes, %v; want the body", len(tc.body), tc.declared, len(got), err)
		}
	}
	fail := errors.New("broken")
	for _, declared := range []int64{3, 5, 8} {
		r := io.MultiReader(strings.NewReader("hello"), iotest.ErrReader(fail))
		if _, err := readWhole(r, declared); !errors.Is(err, fail) {
			t.Errorf("readWhole(broken after 5 bytes, %d): %v, want %v", declared, err, fail)
		}
	}
}
