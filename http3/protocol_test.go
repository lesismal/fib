//go:build linux || darwin || windows

package http3

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/http3/internal/qpack"
	"github.com/lesismal/fib/http3/internal/quic"
	"github.com/lesismal/fib/internal/tlstest"
)

// rawConn is a bare QUIC connection to the server, for sending what a
// well-behaved client never would.
type rawConn struct {
	mu        sync.Mutex
	qc        *quic.Conn
	handshake chan struct{}
	closed    chan error
	resets    chan uint64
}

func (r *rawConn) OnOpen(*fib.Connection)                 {}
func (r *rawConn) OnPriorityData(*fib.Connection, []byte) {}
func (r *rawConn) OnData(_ *fib.Connection, data []byte) {
	r.mu.Lock()
	qc := r.qc
	r.mu.Unlock()
	if qc != nil {
		qc.HandleDatagram(data)
	}
}
func (r *rawConn) OnClose(*fib.Connection, error) {}

type rawHandler struct{ r *rawConn }

func (h rawHandler) OnHandshake(*quic.Conn)                  { close(h.r.handshake) }
func (h rawHandler) OnStreamData(*quic.Stream, []byte, bool) {}
func (h rawHandler) OnStreamReset(_ *quic.Stream, code uint64) {
	h.r.resets <- code
}
func (h rawHandler) OnStopSending(*quic.Stream, uint64) {}
func (h rawHandler) OnStreamsAvailable(*quic.Conn)      {}
func (h rawHandler) OnClose(_ *quic.Conn, err error)    { h.r.closed <- err }

func dialRaw(t *testing.T, base string) *rawConn {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	r := &rawConn{handshake: make(chan struct{}), closed: make(chan error, 1), resets: make(chan uint64, 16)}
	dialed := make(chan error, 1)
	err = engine.DialWithHandler("udp", "127.0.0.1:"+u.Port(), time.Second, r, func(fc *fib.Connection, err error) {
		if err != nil {
			dialed <- err
			return
		}
		r.mu.Lock()
		r.qc, err = quic.Dial(fc, fc.RemoteAddr(), quic.Config{TLSConfig: clientTLS}, rawHandler{r})
		r.mu.Unlock()
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.handshake:
	case <-time.After(5 * time.Second):
		t.Fatal("no handshake")
	}
	return r
}

// expectClose waits for the server to close the connection with code.
func (r *rawConn) expectClose(t *testing.T, code ErrorCode) {
	t.Helper()
	select {
	case err := <-r.closed:
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || !appErr.Remote || ErrorCode(appErr.Code) != code {
			t.Fatalf("closed with %v, want %v", err, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("not closed, want %v", code)
	}
}

func headersFrame(fields ...qpack.HeaderField) []byte {
	block := append([]byte(nil), qpack.Prefix...)
	for _, f := range fields {
		block = qpack.AppendField(block, f.Name, f.Value, false)
	}
	return appendHeadersFrame(nil, block)
}

func TestDataBeforeHeaders(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(append(appendFrameHeader(nil, frameData, 2), 'h', 'i'), true)
	r.expectClose(t, ErrCodeFrameUnexpected)
}

func TestControlStreamWithoutSettings(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := quic.AppendVarint(nil, 0)
	frame := append(appendFrameHeader(nil, frameGoAway, len(payload)), payload...)
	_ = s.Write(append([]byte{streamControl}, frame...), false)
	r.expectClose(t, ErrCodeMissingSettings)
}

func TestSecondControlStream(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	for i := 0; i < 2; i++ {
		s, err := r.qc.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write(appendSettings([]byte{streamControl}), false)
	}
	r.expectClose(t, ErrCodeStreamCreationError)
}

func TestMalformedRequestIsReset(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// No :path.
	_ = s.Write(headersFrame(
		qpack.HeaderField{Name: ":method", Value: "GET"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
	), true)
	select {
	case code := <-r.resets:
		if ErrorCode(code) != ErrCodeMessageError {
			t.Fatalf("reset with %v", ErrorCode(code))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not reset")
	}
}

func TestDynamicTableReferenceFails(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// A Required Insert Count of one refers to a table the server never
	// allowed.
	block := []byte{0x02, 0x00, 0x80}
	_ = s.Write(appendHeadersFrame(nil, block), true)
	r.expectClose(t, ErrCodeQPACKDecompressionFailed)
}

// rawServer is an HTTP/3 server written frame by frame, for the things a
// well-behaved peer never sends. It answers every request with a short
// response, and setup writes whatever the case needs on the control stream.
type rawServer struct {
	mu      sync.Mutex
	control *quic.Stream
	closed  chan error
	// setup writes the control stream's frames after SETTINGS.
	setup func(control *quic.Stream)
	// respond writes a response on a request stream.
	respond func(s *quic.Stream)
}

func (r *rawServer) OnHandshake(qc *quic.Conn) {
	control, err := qc.OpenUniStream()
	if err != nil {
		return
	}
	r.mu.Lock()
	r.control = control
	r.mu.Unlock()
	_ = control.Write(appendSettings([]byte{streamControl}), false)
	if r.setup != nil {
		r.setup(control)
	}
}

func (r *rawServer) OnStreamData(s *quic.Stream, _ []byte, fin bool) {
	if !fin || !s.Bidirectional() {
		return
	}
	if r.respond != nil {
		r.respond(s)
		return
	}
	block := append([]byte(nil), qpack.Prefix...)
	block = qpack.AppendField(block, ":status", "200", false)
	_ = s.Write(appendHeadersFrame(nil, block), true)
}

func (r *rawServer) OnStreamReset(*quic.Stream, uint64) {}
func (r *rawServer) OnStopSending(*quic.Stream, uint64) {}
func (r *rawServer) OnStreamsAvailable(*quic.Conn)      {}
func (r *rawServer) OnClose(_ *quic.Conn, err error)    { r.closed <- err }

// rawServerHandler feeds the engine's datagrams to one QUIC connection.
type rawServerHandler struct {
	t      *testing.T
	server *rawServer
	config quic.Config
}

func (h *rawServerHandler) OnOpen(*fib.Connection)                 {}
func (h *rawServerHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *rawServerHandler) OnData(c *fib.Connection, data []byte) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.HandleDatagram(data)
		return
	}
	if !quic.IsInitial(data) {
		return
	}
	if qc := quic.Accept(c, c.RemoteAddr(), h.config, h.server, data); qc != nil {
		c.SetAttachment(qc)
	}
}

func (h *rawServerHandler) OnClose(c *fib.Connection, err error) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.Abort(err)
	}
	c.SetAttachment(nil)
}

// startRawServer serves HTTP/3 by hand and returns the server and its URL.
func startRawServer(t *testing.T, setup func(control *quic.Stream)) (*rawServer, string) {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	server := &rawServer{closed: make(chan error, 4), setup: setup}
	config := fib.DefaultConfig()
	config.Network = "udp"
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, &rawServerHandler{t: t, server: server,
		config: quic.Config{TLSConfig: ConfigureTLS(serverTLS)}})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	return server, fmt.Sprintf("https://localhost:%d", addr.Port)
}

// expectClientCloses runs a request against a raw server and waits for the
// client to end the connection with code.
func expectClientCloses(t *testing.T, server *rawServer, url string, code ErrorCode) {
	t.Helper()
	client := newClient(t, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait()
	}()
	select {
	case err := <-server.closed:
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || !appErr.Remote || ErrorCode(appErr.Code) != code {
			t.Fatalf("the client closed with %v, want %v", err, code)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the client did not close the connection with %v", code)
	}
	<-done
}

func controlFrame(typ uint64, payload []byte) []byte {
	return append(appendFrameHeader(nil, typ, len(payload)), payload...)
}

// A server may not send MAX_PUSH_ID (RFC 9114 section 7.2.7).
func TestClientRejectsMaxPushID(t *testing.T) {
	server, url := startRawServer(t, func(control *quic.Stream) {
		_ = control.Write(controlFrame(frameMaxPushID, quic.AppendVarint(nil, 10)), false)
	})
	expectClientCloses(t, server, url, ErrCodeFrameUnexpected)
}

// Nothing was ever promised, so there is no push to cancel (RFC 9114
// section 7.2.3).
func TestClientRejectsCancelPush(t *testing.T) {
	server, url := startRawServer(t, func(control *quic.Stream) {
		_ = control.Write(controlFrame(frameCancelPush, quic.AppendVarint(nil, 0)), false)
	})
	expectClientCloses(t, server, url, ErrCodeIDError)
}

// A server's GOAWAY names a request stream, which a unidirectional stream
// identifier is not (RFC 9114 section 5.2).
func TestClientRejectsGoAwayOnAnotherStreamType(t *testing.T) {
	server, url := startRawServer(t, func(control *quic.Stream) {
		_ = control.Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 3)), false)
	})
	expectClientCloses(t, server, url, ErrCodeIDError)
}

// GOAWAY may only lower the identifier it names (RFC 9114 section 5.2).
func TestClientRejectsRaisedGoAway(t *testing.T) {
	var server *rawServer
	server, url := startRawServer(t, func(control *quic.Stream) {
		// Stream 0, the request this test sends, is below this one and so
		// is served; the connection therefore stays open.
		_ = control.Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 4)), false)
	})
	// The second GOAWAY goes out once the request is there, and raises what
	// the first one named. The request itself is left unanswered.
	server.respond = func(*quic.Stream) {
		server.mu.Lock()
		control := server.control
		server.mu.Unlock()
		_ = control.Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 8)), false)
	}
	expectClientCloses(t, server, url, ErrCodeIDError)
}

// This side allows no dynamic table, so the peer may not fill one (RFC 9204
// section 4.3).
func TestClientRejectsQPACKInsertion(t *testing.T) {
	server, url := startRawServer(t, func(control *quic.Stream) {
		qc := control.Conn()
		encoder, err := qc.OpenUniStream()
		if err != nil {
			return
		}
		// An Insert With Literal Name instruction, which needs a table.
		_ = encoder.Write([]byte{streamQPACKEncoder, 0x40 | 0x03, 'a', 'b', 'c', 0x00}, false)
	})
	expectClientCloses(t, server, url, ErrCodeQPACKEncoderStreamError)
}

// A response whose field section refers to the dynamic table cannot be
// decoded (RFC 9204 section 2.2).
func TestClientRejectsDynamicTableReference(t *testing.T) {
	server, url := startRawServer(t, nil)
	server.respond = func(s *quic.Stream) {
		// A Required Insert Count of one refers to a table that is empty.
		_ = s.Write(appendHeadersFrame(nil, []byte{0x02, 0x00, 0x80}), true)
	}
	expectClientCloses(t, server, url, ErrCodeQPACKDecompressionFailed)
}

// rawControl opens the client's control stream and sends its SETTINGS.
func (r *rawConn) rawControl(t *testing.T) *quic.Stream {
	t.Helper()
	s, err := r.qc.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(appendSettings([]byte{streamControl}), false); err != nil {
		t.Fatal(err)
	}
	return s
}

// A client that never received a promise has no push to cancel (RFC 9114
// section 7.2.3).
func TestServerRejectsCancelPush(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	control := r.rawControl(t)
	_ = control.Write(controlFrame(frameCancelPush, quic.AppendVarint(nil, 0)), false)
	r.expectClose(t, ErrCodeIDError)
}

// MAX_PUSH_ID may only raise the limit it grants (RFC 9114 section 7.2.7).
func TestServerRejectsLoweredMaxPushID(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	control := r.rawControl(t)
	_ = control.Write(controlFrame(frameMaxPushID, quic.AppendVarint(nil, 10)), false)
	_ = control.Write(controlFrame(frameMaxPushID, quic.AppendVarint(nil, 4)), false)
	r.expectClose(t, ErrCodeIDError)
}

// A client's GOAWAY may only lower the push ID it names (RFC 9114 section
// 5.2).
func TestServerRejectsRaisedGoAway(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	control := r.rawControl(t)
	_ = control.Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 0)), false)
	_ = control.Write(controlFrame(frameGoAway, quic.AppendVarint(nil, 8)), false)
	r.expectClose(t, ErrCodeIDError)
}

// The server allows no dynamic table, so a client may not fill one (RFC
// 9204 section 4.3).
func TestServerRejectsQPACKInsertion(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	encoder, err := r.qc.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	// Set Dynamic Table Capacity to something the server did not allow.
	_ = encoder.Write([]byte{streamQPACKEncoder, 0x20 | 0x10}, false)
	r.expectClose(t, ErrCodeQPACKEncoderStreamError)
}

// A request whose :authority and Host disagree is malformed (RFC 9114
// section 4.3.1).
func TestMalformedAuthorityIsReset(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(headersFrame(
		qpack.HeaderField{Name: ":method", Value: "GET"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
		qpack.HeaderField{Name: ":path", Value: "/"},
		qpack.HeaderField{Name: "host", Value: "elsewhere"},
	), true)
	select {
	case code := <-r.resets:
		if ErrorCode(code) != ErrCodeMessageError {
			t.Fatalf("reset with %v", ErrorCode(code))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the request was served")
	}
}

// A request for http or https has to name an authority one way or another
// (RFC 9114 section 4.3.1).
func TestMissingAuthorityIsReset(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(headersFrame(
		qpack.HeaderField{Name: ":method", Value: "GET"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":path", Value: "/"},
	), true)
	select {
	case code := <-r.resets:
		if ErrorCode(code) != ErrCodeMessageError {
			t.Fatalf("reset with %v", ErrorCode(code))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the request was served")
	}
}
