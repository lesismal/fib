//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"errors"
	stdhttp "net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
)

// White-box checks of what each side refuses in the fields, frames and
// control streams a peer sends, and in what a caller hands it to send:
// every branch here is a malformed message, which the protocol tests reach
// end to end for a few, and which these pin down one by one.

// connErrCode is the code err ends a connection with, or zero for an error
// that does not.
func connErrCode(err error) ErrorCode {
	var ce *connError
	if errors.As(err, &ce) {
		return ce.code
	}
	return 0
}

// A request whose fields RFC 9114 section 4.3.1 calls malformed is refused
// before it reaches a handler.
func TestNewRequestRefusesMalformedFields(t *testing.T) {
	f := func(name, value string) qpack.HeaderField { return qpack.HeaderField{Name: name, Value: value} }
	get := []qpack.HeaderField{f(":method", "GET"), f(":scheme", "https"), f(":authority", "localhost"), f(":path", "/")}
	with := func(extra ...qpack.HeaderField) []qpack.HeaderField {
		return append(append([]qpack.HeaderField(nil), get...), extra...)
	}
	cases := map[string][]qpack.HeaderField{
		"pseudo-header after a field":  {f(":method", "GET"), f("x-a", "b"), f(":path", "/")},
		"unknown pseudo-header":        with(f(":status", "200")),
		"duplicate pseudo-header":      with(f(":path", "/again")),
		"empty name":                   with(f("", "v")),
		"uppercase name":               with(f("X-Upper", "v")),
		"NUL in a value":               with(f("x-a", "a\x00b")),
		"connection-specific field":    with(f("connection", "close")),
		"TE other than trailers":       with(f("te", "gzip")),
		"missing method":               {f(":scheme", "https"), f(":authority", "localhost"), f(":path", "/")},
		"extended CONNECT, no path":    {f(":method", "CONNECT"), f(":protocol", "websocket"), f(":scheme", "https"), f(":authority", "localhost")},
		"extended CONNECT, bad target": {f(":method", "CONNECT"), f(":protocol", "websocket"), f(":scheme", "https"), f(":authority", "localhost"), f(":path", "ws")},
		"CONNECT with a path":          {f(":method", "CONNECT"), f(":authority", "localhost:443"), f(":path", "/")},
		"relative target":              {f(":method", "GET"), f(":scheme", "https"), f(":authority", "localhost"), f(":path", "index.html")},
		"two Host fields":              with(f("host", "localhost"), f("host", "localhost")),
	}
	for name, fields := range cases {
		if req, err := newRequest(fields, nil, nil, nil); err == nil {
			t.Errorf("%s: accepted as %s %v", name, req.Method, req.URL)
		}
	}
	// The asterisk form names the server itself, for OPTIONS.
	req, err := newRequest([]qpack.HeaderField{f(":method", "OPTIONS"), f(":scheme", "https"), f(":authority", "localhost"), f(":path", "*")}, nil, nil, nil)
	if err != nil || req.URL.Path != "*" || req.RequestURI != "*" {
		t.Fatalf("OPTIONS *: %v, %v", req, err)
	}
}

// A response's fields are checked as a request's are, and an interim one
// comes back as nil, except 101, which HTTP/3 has no use for.
func TestNewResponseRefusesMalformedFields(t *testing.T) {
	f := func(name, value string) qpack.HeaderField { return qpack.HeaderField{Name: name, Value: value} }
	req := &stdhttp.Request{Method: stdhttp.MethodGet}
	cases := map[string][]qpack.HeaderField{
		"status after a field":  {f("x-a", "b"), f(":status", "200")},
		"two statuses":          {f(":status", "200"), f(":status", "204")},
		"request pseudo-header": {f(":path", "/")},
		"uppercase name":        {f(":status", "200"), f("X-Upper", "v")},
		"connection field":      {f(":status", "200"), f("keep-alive", "1")},
		"no status":             {f("x-a", "b")},
		"status not a number":   {f(":status", "2xx")},
		"status of four digits": {f(":status", "2000")},
		"status below 100":      {f(":status", "099")},
		"101":                   {f(":status", "101")},
		"negative length":       {f(":status", "200"), f("content-length", "-1")},
		"length not a number":   {f(":status", "200"), f("content-length", "ten")},
	}
	for name, fields := range cases {
		if resp, err := newResponse(fields, req); err == nil {
			t.Errorf("%s: accepted as %v", name, resp)
		}
	}
	if resp, err := newResponse([]qpack.HeaderField{f(":status", "103"), f("link", "</a>")}, req); resp != nil || err != nil {
		t.Fatalf("103: %v, %v, want an interim nil", resp, err)
	}
	if _, err := trailerFrom([]qpack.HeaderField{f(":status", "200")}); err == nil {
		t.Fatal("a trailer with a pseudo-header was accepted")
	}
}

// What a handler or a caller asks to send is checked before anything is
// encoded, and a trailer leaves out what may not trail a message.
func TestOutgoingFieldChecks(t *testing.T) {
	bad := []stdhttp.Header{
		{"": {"v"}},
		{"Bad Name": {"v"}},
		{"X-A": {"a\nb"}},
	}
	for _, header := range bad {
		if err := checkHeader(header); err == nil {
			t.Errorf("%q passed", header)
		}
	}
	if err := checkHeader(stdhttp.Header{"X-A": {"a\tb"}}); err != nil {
		t.Fatalf("a tab in a value: %v", err)
	}
	for name, values := range map[string][]string{
		"connection": {"close"},
		"x-bad":      {"a\rb"},
		"Upper":      {"v"},
		"":           {"v"},
		"set-cookie": {"a=1"},
	} {
		if validTrailer(name, values) {
			t.Errorf("trailer %q %q passed", name, values)
		}
	}
	if !validTrailer("x-sum", []string{"42"}) {
		t.Fatal("x-sum refused as a trailer")
	}
	// A trailer with nothing sendable adds no frame at all.
	if out := appendTrailer(nil, stdhttp.Header{"Content-Length": {"1"}}); len(out) != 0 {
		t.Fatalf("an empty trailer section was sent: % x", out)
	}
}

// A request's fields are built and checked before it goes anywhere, so a
// request no server could take fails at once, with nothing sent.
func TestRequestFields(t *testing.T) {
	u, _ := url.Parse("https://example.com:8443/p?q=1")
	req := &stdhttp.Request{URL: u, Header: stdhttp.Header{
		"Connection": {"keep-alive"}, "Te": {"trailers"}, "Content-Length": {"9"}, "X-A": {"b"},
	}}
	fields, _, err := requestFields(req, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{":method": "GET", ":authority": "example.com:8443", ":path": "/p?q=1", "x-a": "b", "user-agent": "Go-http-client/3"}
	got := map[string]string{}
	for _, f := range fields {
		got[f.Name] = f.Value
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s is %q, want %q", name, got[name], value)
		}
	}
	for _, name := range []string{"connection", "te", "content-length"} {
		if _, ok := got[name]; ok {
			t.Errorf("%s was sent", name)
		}
	}

	connect := &stdhttp.Request{Method: stdhttp.MethodConnect, Host: "proxy.example:443", URL: &url.URL{}}
	fields, _, err = requestFields(connect, -1)
	if err != nil || len(fields) < 2 || fields[1] != (qpack.HeaderField{Name: ":authority", Value: "proxy.example:443"}) {
		t.Fatalf("CONNECT: %v, %v", fields, err)
	}
	for _, f := range fields {
		if f.Name == ":path" || f.Name == ":scheme" {
			t.Fatalf("CONNECT sent %s", f.Name)
		}
	}

	bad := map[string]*stdhttp.Request{
		"method with a space":  {Method: "GE T", URL: u},
		"method with a slash":  {Method: "GET/", URL: u},
		"header name":          {URL: u, Header: stdhttp.Header{"Bad Name": {"v"}}},
		"header value":         {URL: u, Header: stdhttp.Header{"X-A": {"a\nb"}}},
		"trailer name":         {Method: "POST", URL: u, Trailer: stdhttp.Header{"Bad Name": {"v"}}},
		"pseudo trailer":       {Method: "POST", URL: u, Trailer: stdhttp.Header{":path": {"/"}}},
		"connection trailer":   {Method: "POST", URL: u, Trailer: stdhttp.Header{"Connection": {"close"}}},
		"trailer value":        {Method: "POST", URL: u, Trailer: stdhttp.Header{"X-Sum": {"a\x00"}}},
		"method of a NUL byte": {Method: "\x00", URL: u},
	}
	for name, req := range bad {
		if _, _, err := requestFields(req, 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A URL without a port goes to 443, and the certificate is checked against
// the bare host name, brackets and all taken off.
func TestConnKey(t *testing.T) {
	for host, want := range map[string][2]string{
		"example.com":      {"example.com:443", "example.com"},
		"example.com:8443": {"example.com:8443", "example.com"},
		"[::1]":            {"[::1]:443", "::1"},
		"[::1]:8443":       {"[::1]:8443", "::1"},
	} {
		addr, name := connKey(&stdhttp.Request{URL: &url.URL{Host: host}})
		if addr != want[0] || name != want[1] {
			t.Errorf("%s: %s %s, want %s %s", host, addr, name, want[0], want[1])
		}
	}
}

// SETTINGS that are cut short, repeat an identifier, or carry one HTTP/2
// reserves are connection errors (RFC 9114 section 7.2.4).
func TestParseSettingsErrors(t *testing.T) {
	cases := map[string]struct {
		payload []byte
		code    ErrorCode
	}{
		"truncated identifier": {[]byte{0x40}, ErrCodeFrameError},
		"truncated value":      {[]byte{0x06, 0x40}, ErrCodeFrameError},
		"duplicate":            {[]byte{0x06, 0x01, 0x06, 0x02}, ErrCodeSettingsError},
		"HTTP/2 setting":       {[]byte{0x02, 0x00}, ErrCodeSettingsError},
	}
	for name, c := range cases {
		if _, _, err := parseSettings(c.payload); connErrCode(err) != c.code {
			t.Errorf("%s: %v, want %v", name, err, c.code)
		}
	}
	// An unknown setting is ignored (RFC 9114 section 7.2.4.1).
	if max, limited, err := parseSettings([]byte{0x21, 0x05, 0x06, 0x40, 0x80}); err != nil || max != 128 || !limited {
		t.Fatalf("got %d, %v", max, err)
	}
}

// The errors name their codes, and say which side reset a stream.
func TestErrorStrings(t *testing.T) {
	if got := ErrorCode(0x1f21).String(); got != "H3 error 0x1f21" {
		t.Errorf("unknown code: %q", got)
	}
	if got := connErr(ErrCodeFrameError, "bad").Error(); got != "http3: H3_FRAME_ERROR: bad" {
		t.Errorf("connError: %q", got)
	}
	if got := (&StreamError{Code: ErrCodeRequestCancelled}).Error(); got != "http3: stream reset: H3_REQUEST_CANCELLED" {
		t.Errorf("local reset: %q", got)
	}
	if got := (&StreamError{Code: ErrCodeRequestRejected, Remote: true}).Error(); !strings.Contains(got, "by peer") {
		t.Errorf("remote reset: %q", got)
	}
}

// A frame parser that had to hold a large frame's start lets the big
// buffer go once what it holds is small again, rather than keeping 64KB
// per stream for a frame header.
func TestFrameParserShrinksItsBuffer(t *testing.T) {
	p := frameParser{maxFrame: 1 << 20}
	payload := bytes.Repeat([]byte{'x'}, 100<<10)
	frame := append(appendFrameHeader(nil, 0x21, len(payload)), payload...)
	frames := 0
	onFrame := func(typ uint64, got []byte) error {
		if typ != 0x21 || len(got) != len(payload) {
			t.Fatalf("frame %#x of %d bytes", typ, len(got))
		}
		frames++
		return nil
	}
	noData := func([]byte) error { t.Fatal("DATA"); return nil }
	// All but the last byte, which leaves the parser holding nearly 100KB.
	if err := p.feed(frame[:len(frame)-1], noData, onFrame); err != nil || frames != 0 {
		t.Fatalf("%v, %d frames", err, frames)
	}
	// The last byte, and the start of the next frame's header.
	if err := p.feed([]byte{frame[len(frame)-1], 0x40}, noData, onFrame); err != nil || frames != 1 {
		t.Fatalf("%v, %d frames", err, frames)
	}
	if !p.midFrame() || cap(p.buf) > 64<<10 {
		t.Fatalf("holding %d bytes in a buffer of %d", len(p.buf), cap(p.buf))
	}
}

// What the peer's unidirectional streams may carry: each critical stream
// once, no push stream (none was allowed), and on the control stream only
// well-formed frames that belong there.
func TestUniStreamRules(t *testing.T) {
	newPeer := func(isClient bool) *peerStreams { return &peerStreams{isClient: isClient} }
	open := func(p *peerStreams, typ byte) error {
		return newUniStream(p, nil, nil).handle([]byte{typ}, false)
	}
	p := newPeer(false)
	for _, typ := range []byte{streamQPACKEncoder, streamQPACKDecoder} {
		if err := open(p, typ); err != nil {
			t.Fatalf("stream type %d: %v", typ, err)
		}
		if err := open(p, typ); connErrCode(err) != ErrCodeStreamCreationError {
			t.Fatalf("second stream of type %d: %v", typ, err)
		}
	}
	if err := open(p, streamPush); connErrCode(err) != ErrCodeIDError {
		t.Fatalf("push stream: %v", err)
	}
	// A type cut short waits for the rest of it.
	u := newUniStream(newPeer(false), nil, nil)
	if err := u.handle([]byte{0x40}, false); err != nil || u.typ >= 0 {
		t.Fatalf("partial type: %v, type %d", err, u.typ)
	}
	// What arrives on the decoder stream is ignored; its end is not.
	u = newUniStream(newPeer(false), nil, nil)
	if err := u.handle([]byte{streamQPACKDecoder, 0x01}, false); err != nil {
		t.Fatal(err)
	}
	if err := u.handle(nil, true); connErrCode(err) != ErrCodeClosedCriticalStream {
		t.Fatalf("decoder stream ended: %v", err)
	}
	// A stream of a type nothing reads has whatever follows dropped.
	u = newUniStream(newPeer(false), nil, nil)
	u.typ = 0x21
	if err := u.handle([]byte("anything"), true); err != nil {
		t.Fatalf("unknown stream type: %v", err)
	}

	control := func(isClient bool) *uniStream {
		u := newUniStream(newPeer(isClient), nil, nil)
		if err := u.handle(appendSettings([]byte{streamControl}), false); err != nil {
			t.Fatal(err)
		}
		return u
	}
	varint := func(v uint64) []byte { return quic.AppendVarint(nil, v) }
	cases := map[string]struct {
		isClient bool
		typ      uint64
		payload  []byte
		code     ErrorCode
	}{
		"second SETTINGS":        {false, frameSettings, nil, ErrCodeFrameUnexpected},
		"empty GOAWAY":           {false, frameGoAway, nil, ErrCodeFrameError},
		"GOAWAY with extra":      {true, frameGoAway, append(varint(4), 0), ErrCodeFrameError},
		"empty MAX_PUSH_ID":      {false, frameMaxPushID, nil, ErrCodeFrameError},
		"MAX_PUSH_ID with extra": {false, frameMaxPushID, append(varint(4), 0), ErrCodeFrameError},
		"empty CANCEL_PUSH":      {false, frameCancelPush, nil, ErrCodeFrameError},
		"DATA":                   {false, frameData, []byte("x"), ErrCodeFrameUnexpected},
		"HEADERS":                {true, frameHeaders, []byte{0, 0}, ErrCodeFrameUnexpected},
		"PUSH_PROMISE":           {true, framePushPromise, varint(0), ErrCodeFrameUnexpected},
		"HTTP/2 PING":            {false, 0x06, make([]byte, 8), ErrCodeFrameUnexpected},
		"HTTP/2 WINDOW_UPDATE":   {true, 0x08, make([]byte, 4), ErrCodeFrameUnexpected},
	}
	for name, c := range cases {
		u := control(c.isClient)
		frame := append(appendFrameHeader(nil, c.typ, len(c.payload)), c.payload...)
		if err := u.handle(frame, false); connErrCode(err) != c.code {
			t.Errorf("%s: %v, want %v", name, err, c.code)
		}
	}
	// A frame of an unknown type is skipped (RFC 9114 section 9).
	u = control(false)
	if err := u.handle(append(appendFrameHeader(nil, 0x21, 3), "abc"...), false); err != nil {
		t.Fatalf("unknown frame: %v", err)
	}
	if err := u.handle(nil, true); connErrCode(err) != ErrCodeClosedCriticalStream {
		t.Fatalf("control stream ended: %v", err)
	}
}

// The encoder stream may set the dynamic table's capacity to zero, as often
// as it likes and however the instruction is split, and nothing else.
func TestEncoderStreamInstructions(t *testing.T) {
	u := newUniStream(&peerStreams{}, nil, nil)
	if err := u.handle([]byte{streamQPACKEncoder, 0x20, 0x20}, false); err != nil || len(u.head) != 0 {
		t.Fatalf("zero capacity twice: %v, %d bytes held", err, len(u.head))
	}
	// A capacity whose prefix is full waits for the bytes that follow it.
	if err := u.handle([]byte{0x3f}, false); err != nil || len(u.head) != 1 {
		t.Fatalf("partial capacity: %v, %d bytes held", err, len(u.head))
	}
	if err := u.handle([]byte{0x00}, false); connErrCode(err) != ErrCodeQPACKEncoderStreamError {
		t.Fatalf("capacity 31: %v", err)
	}
}

// readPrefixedInt reads RFC 7541 integers, reporting one cut short as not
// all there and one too long to hold as there but useless.
func TestReadPrefixedInt(t *testing.T) {
	cases := []struct {
		in       []byte
		v        uint64
		n        int
		complete bool
	}{
		{nil, 0, 0, false},
		{[]byte{0x0a}, 10, 1, true},
		{[]byte{0x1f, 0x9a, 0x0a}, 1337, 3, true},
		{[]byte{0x1f, 0x80}, 0, 0, false},
		{[]byte{0x1f, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, 0, 0, true},
	}
	for _, c := range cases {
		v, n, complete := readPrefixedInt(c.in, 5)
		if v != c.v || n != c.n || complete != c.complete {
			t.Errorf("% x: %d %d %v, want %d %d %v", c.in, v, n, complete, c.v, c.n, c.complete)
		}
	}
}
