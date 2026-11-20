//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bytes"
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
)

// fakeTransport records what a Connection sends through it, or fails every
// send with err.
type fakeTransport struct {
	err            error
	frames         [][]byte
	closed         int
	closeAfterSend int
}

func (f *fakeTransport) send(parts ...[]byte) error {
	if f.err != nil {
		return f.err
	}
	var frame []byte
	for _, part := range parts {
		frame = append(frame, part...)
	}
	f.frames = append(f.frames, frame)
	return nil
}

func (f *fakeTransport) SendParts(first, second []byte) error { return f.send(first, second) }
func (f *fakeTransport) SendOwned(data []byte) error          { return f.send(data) }
func (f *fakeTransport) SendShared(data []byte) error         { return f.send(data) }
func (f *fakeTransport) CloseAfterSend()                      { f.closeAfterSend++ }
func (f *fakeTransport) Close() error                         { f.closed++; return nil }

// plainHandler implements Handler and nothing else, so that a ping gets the
// default reply and a data message goes to OnMessage.
type plainHandler struct{ messages []string }

func (*plainHandler) OnOpen(*Connection, *stdhttp.Request) {}
func (h *plainHandler) OnMessage(_ *Connection, _ Opcode, payload []byte) {
	h.messages = append(h.messages, string(payload))
}
func (*plainHandler) OnClose(*Connection, uint16, string, error) {}

// framesOnly takes data messages frame by frame through an OnFrame of its own.
type framesOnly struct{ plainHandler }

func (*framesOnly) OnFrame(*Connection, Opcode, bool, []byte) {}

func TestFrameHandlerSelection(t *testing.T) {
	frame := func(*Connection, Opcode, bool, []byte) {}
	for _, tc := range []struct {
		name    string
		handler Handler
		want    bool
	}{
		{"nil *HandlerFuncs", (*HandlerFuncs)(nil), false},
		{"*HandlerFuncs without Frame", &HandlerFuncs{}, false},
		{"*HandlerFuncs with Frame", &HandlerFuncs{Frame: frame}, true},
		{"FrameHandler", &framesOnly{}, true},
		{"Handler alone", &plainHandler{}, false},
	} {
		if got := frameHandler(tc.handler) != nil; got != tc.want {
			t.Errorf("%s: frame handler %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestNewHandlerWithConfigDefaults(t *testing.T) {
	h := NewHandlerWithConfig(Config{}, nil)
	defaults := DefaultConfig()
	if h.config.MaxMessageBytes != defaults.MaxMessageBytes ||
		h.config.HTTP.MaxHeaderBytes != defaults.HTTP.MaxHeaderBytes ||
		h.config.HTTP.MaxBodyBytes != defaults.HTTP.MaxBodyBytes {
		t.Fatalf("limits %d/%d/%d, want the defaults", h.config.MaxMessageBytes,
			h.config.HTTP.MaxHeaderBytes, h.config.HTTP.MaxBodyBytes)
	}
	if _, ok := h.handler.(HandlerFuncs); !ok || h.needsRequest {
		t.Fatalf("nil handler became %T, needsRequest %v", h.handler, h.needsRequest)
	}
	if p := h.handshakeParsers.Get().(*handshakeParser); p.maxHeaderBytes != defaults.HTTP.MaxHeaderBytes {
		t.Fatalf("pooled parser bounds headers at %d", p.maxHeaderBytes)
	}

	open := func(*Connection, *stdhttp.Request) {}
	for _, tc := range []struct {
		name    string
		handler Handler
		want    bool
	}{
		{"*HandlerFuncs without Open", &HandlerFuncs{}, false},
		{"*HandlerFuncs with Open", &HandlerFuncs{Open: open}, true},
		{"nil *HandlerFuncs", (*HandlerFuncs)(nil), true},
		{"other Handler", &plainHandler{}, true},
	} {
		if got := NewHandlerWithConfig(DefaultConfig(), tc.handler).needsRequest; got != tc.want {
			t.Errorf("%s: needsRequest %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestConnectionRejectsWhatItCannotSend(t *testing.T) {
	transport := &fakeTransport{}
	server := &Connection{conn: transport}
	client := &Connection{conn: transport, client: true}
	long := make([]byte, 126)
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"control opcode as message", server.WriteMessage(Ping, nil), errDataOpcode},
		{"invalid UTF-8 text", server.WriteMessage(Text, []byte{0xff}), ErrInvalidPayload},
		{"long server ping", server.Ping(long), ErrProtocol},
		{"long client ping", client.Ping(long), ErrProtocol},
		{"reserved close code", server.Close(1005, ""), ErrInvalidPayload},
		{"invalid UTF-8 reason", server.Close(CloseNormal, "\xff"), ErrInvalidPayload},
	} {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, tc.err, tc.want)
		}
	}
	if err := server.Close(CloseNormal, strings.Repeat("r", 124)); err == nil {
		t.Error("a close reason past 123 bytes was accepted")
	}
	if len(transport.frames) != 0 {
		t.Fatalf("sent %d frames", len(transport.frames))
	}
}

func TestConnectionCloseSentOnce(t *testing.T) {
	transport := &fakeTransport{}
	c := &Connection{conn: transport}
	if err := c.Close(CloseNormal, "bye"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(CloseGoingAway, ""); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if len(transport.frames) != 1 || transport.closeAfterSend != 1 {
		t.Fatalf("sent %d frames, closed after send %d times", len(transport.frames), transport.closeAfterSend)
	}
	if want := []byte{0x88, 5, 0x03, 0xe8, 'b', 'y', 'e'}; !bytes.Equal(transport.frames[0], want) {
		t.Fatalf("close frame %x, want %x", transport.frames[0], want)
	}
	if code, reason, _ := c.closeStatus(nil); code != CloseNormal || reason != "bye" {
		t.Fatalf("close status %d %q", code, reason)
	}
	if err := c.WriteText("late"); !errors.Is(err, errCloseSent) {
		t.Fatalf("write after close: %v", err)
	}
	prepared, err := NewPreparedMessage(Text, []byte("late"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WritePreparedMessage(prepared); !errors.Is(err, errCloseSent) {
		t.Fatalf("prepared write after close: %v", err)
	}
}

func TestConnectionSendFailures(t *testing.T) {
	broken := errors.New("broken")
	transport := &fakeTransport{err: broken}
	c := &Connection{conn: transport}
	if c.replyPing([]byte("p")) || transport.closed != 1 {
		t.Fatalf("a pong that failed to go left the connection open (closed %d)", transport.closed)
	}
	if err := c.Close(CloseNormal, ""); !errors.Is(err, broken) {
		t.Fatalf("close: %v", err)
	}
	if transport.closeAfterSend != 0 {
		t.Fatal("a close frame that failed to go closed the connection after it")
	}
}

// A ping the default handling cannot answer ends the connection, and nothing
// after it in the same read is delivered.
func TestServeFramesStopsWhenPongFails(t *testing.T) {
	transport := &fakeTransport{err: errors.New("broken")}
	ws := &Connection{conn: transport}
	handler := &plainHandler{}
	parser := &Parser{maxMessageBytes: 1 << 20}
	data := append(clientFrame(Ping, true, []byte("p")), clientFrame(Text, true, []byte("after"))...)
	serveFrames(handler, nil, ws, parser, data)
	if transport.closed != 1 || len(handler.messages) != 0 {
		t.Fatalf("closed %d, messages %q", transport.closed, handler.messages)
	}
	if parser.buffer != nil || parser.borrowedBuffer {
		t.Fatal("the parser kept the read buffer it borrowed")
	}
}

// A close frame with no payload is answered with one carrying 1000.
func TestServeFramesEchoesEmptyClose(t *testing.T) {
	transport := &fakeTransport{}
	ws := &Connection{conn: transport}
	serveFrames(&plainHandler{}, nil, ws, &Parser{maxMessageBytes: 1 << 20}, clientFrame(Close, true, nil))
	if len(transport.frames) != 1 || !bytes.Equal(transport.frames[0], []byte{0x88, 0}) {
		t.Fatalf("frames %x", transport.frames)
	}
	if code, reason, _ := ws.closeStatus(nil); code != CloseNormal || reason != "" {
		t.Fatalf("close status %d %q", code, reason)
	}
}

func TestCloseParserErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code uint16
	}{
		{errors.New("other"), CloseProtocolError},
		{ErrMessageTooBig, CloseMessageTooBig},
		{ErrInvalidPayload, CloseInvalidPayload},
	} {
		transport := &fakeTransport{}
		closeParserError(&Connection{conn: transport}, tc.err)
		if want := []byte{0x88, 2, byte(tc.code >> 8), byte(tc.code)}; len(transport.frames) != 1 ||
			!bytes.Equal(transport.frames[0], want) {
			t.Errorf("%v: frames %x, want %x", tc.err, transport.frames, want)
		}
	}
}

func TestHeaderTokenHelpers(t *testing.T) {
	header := stdhttp.Header{"Connection": {"keep-alive, close", ""}}
	if headerHasToken(header, "Connection", "upgrade") {
		t.Error("found upgrade in keep-alive, close")
	}
	if !headerHasToken(header, "Connection", "Close") {
		t.Error("missed close")
	}
	if token, rest := nextHeaderToken(" a , b"); token != "a" || rest != " b" {
		t.Errorf("nextHeaderToken: %q %q", token, rest)
	}
	if byteHeaderHasToken([]byte("keep-alive, close"), []byte("upgrade")) {
		t.Error("found upgrade in keep-alive, close")
	}
	if got := trimOWS([]byte(" \tvalue\t ")); string(got) != "value" {
		t.Errorf("trimOWS: %q", got)
	}
	if code, reason := closePayload([]byte{0x03}); code != CloseNormal || reason != "" {
		t.Errorf("closePayload of one byte: %d %q", code, reason)
	}
}

func TestValidMinimalRequestURI(t *testing.T) {
	for uri, want := range map[string]bool{
		"":           false,
		"chat":       false,
		"/a b":       false,
		"/a\x7f":     false,
		"/a#frag":    false,
		"/%2":        false,
		"/%zz":       false,
		"/%g0":       false,
		"/%0g":       false,
		"/%2F%af%B9": true,
		"/chat?x=1":  true,
	} {
		if got := validMinimalRequestURI([]byte(uri)); got != want {
			t.Errorf("%q: %v, want %v", uri, got, want)
		}
	}
}

const minimalHandshake = "GET /chat HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
	"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\n"

func TestValidateMinimalHandshake(t *testing.T) {
	config := DefaultConfig()
	config.Subprotocols = []string{"bad token", "chat"}
	config.EnableCompression = true
	h := NewHandlerWithConfig(config, nil)

	key, result, remainder, complete, err := h.validateMinimalHandshake([]byte(minimalHandshake +
		"Sec-WebSocket-Protocol: , chat\r\nSec-WebSocket-Protocol: other\r\nContent-Length:\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate\r\n\r\nrest"))
	if err != nil || !complete || string(key) != "MDEyMzQ1Njc4OWFiY2RlZg==" || string(remainder) != "rest" {
		t.Fatalf("key %q remainder %q complete %v err %v", key, remainder, complete, err)
	}
	if result.subprotocol != "chat" || result.extensions == "" {
		t.Fatalf("result %+v", result)
	}

	if _, _, _, complete, err := h.validateMinimalHandshake([]byte("GET /chat HTTP/1.1\r\n")); complete || err != nil {
		t.Fatalf("partial handshake: complete %v err %v", complete, err)
	}

	small := NewHandlerWithConfig(Config{HTTP: config.HTTP}, nil)
	small.config.HTTP.MaxHeaderBytes = 32
	for _, data := range []string{strings.Repeat("x", 33), minimalHandshake + "\r\n"} {
		if _, _, _, _, err := small.validateMinimalHandshake([]byte(data)); !errors.Is(err, errHandshakeHeaderTooLarge) {
			t.Errorf("%d bytes: %v", len(data), err)
		}
	}

	for _, tc := range []struct {
		name, data string
		want       error
	}{
		{"method", "POST /chat HTTP/1.1\r\nHost: test\r\n\r\n", errMalformedHandshake},
		{"request URI", "GET chat HTTP/1.1\r\nHost: test\r\n\r\n", errMalformedHandshake},
		{"no colon", minimalHandshake + "Origin\r\n\r\n", errMalformedHandshake},
		{"folded line", minimalHandshake + " Origin: x\r\n\r\n", errMalformedHandshake},
		{"bad name", minimalHandshake + "Ori gin: x\r\n\r\n", errMalformedHandshake},
		{"control in value", minimalHandshake + "Origin: a\x01b\r\n\r\n", errMalformedHandshake},
		{"second host", minimalHandshake + "Host: other\r\n\r\n", errMalformedHandshake},
		{"empty host", strings.Replace(minimalHandshake, "Host: test", "Host:", 1) + "\r\n", errMalformedHandshake},
		{"body", minimalHandshake + "Content-Length: 5\r\n\r\n", errMalformedHandshake},
		{"chunked", minimalHandshake + "Transfer-Encoding: chunked\r\n\r\n", errMalformedHandshake},
		{"bad subprotocol", minimalHandshake + "Sec-WebSocket-Protocol: b@d, chat\r\n\r\n", ErrProtocol},
		{"no upgrade token", strings.Replace(minimalHandshake, "Connection: Upgrade", "Connection: keep-alive", 1) + "\r\n", ErrProtocol},
		{"short key", strings.Replace(minimalHandshake, "MDEyMzQ1Njc4OWFiY2RlZg==", "MDEy", 1) + "\r\n", ErrProtocol},
	} {
		if _, _, _, _, err := h.validateMinimalHandshake([]byte(tc.data)); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestSelectSubprotocol(t *testing.T) {
	config := DefaultConfig()
	config.Subprotocols = []string{"bad token", "chat"}
	h := NewHandlerWithConfig(config, nil)
	request := func(values ...string) *stdhttp.Request {
		return &stdhttp.Request{Header: stdhttp.Header{"Sec-Websocket-Protocol": values}}
	}
	if got, err := h.selectSubprotocol(request("", " , other", "chat")); err != nil || got != "chat" {
		t.Fatalf("selected %q, %v", got, err)
	}
	if got, err := h.selectSubprotocol(request("other")); err != nil || got != "" {
		t.Fatalf("selected %q, %v", got, err)
	}
	if _, err := h.selectSubprotocol(request("b@d")); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid token: %v", err)
	}
	if _, err := h.validateHandshakeRequest(&stdhttp.Request{Method: stdhttp.MethodPost, ProtoMajor: 1,
		ProtoMinor: 1}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("POST: %v", err)
	}
}

// Only a validator's own step sees a byte with no sequence in progress that
// is ASCII; write hands it ASCII in bulk.
func TestUTF8ValidatorStepASCII(t *testing.T) {
	var v utf8Validator
	if !v.step('a') || !v.complete() {
		t.Fatal("ASCII rejected")
	}
}
