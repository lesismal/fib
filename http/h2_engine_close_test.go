//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdtls "crypto/tls"
	"encoding/binary"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// startEngine runs engine and returns its address and the result of Run.
// The caller closes the engine and waits on the result.
func startEngine(t *testing.T, engine *fib.Engine) (string, <-chan error) {
	t.Helper()
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	return addr.String(), runDone
}

// checkEngineCloseGoAway reads the server's GOAWAY after Engine.Close, and
// checks that it says the server is going (NO_ERROR) and names last as the
// last stream it accepted.
func checkEngineCloseGoAway(t *testing.T, tc *h2TestConn, last uint32) {
	t.Helper()
	f := tc.readUntil(h2FrameGoAway)
	if got := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); got != H2NoError {
		t.Fatalf("GOAWAY %v, want NO_ERROR", got)
	}
	if got := binary.BigEndian.Uint32(f.payload) & 0x7fffffff; got != last {
		t.Fatalf("GOAWAY last stream %d, want %d", got, last)
	}
}

// TestEngineCloseSendsGoAway checks that Engine.Close tells an HTTP/2 client
// that the server is going, with GOAWAY before the connection is cut, rather
// than leaving it to find a reset. Engine.Stop leaves the connection open, as
// before; Close is what ends it.
//
// Two requests go before the close so that the server has read every frame
// the client sent, its SETTINGS acknowledgement and WINDOW_UPDATEs included:
// a socket closed with unread input sends a reset, and the kernel may drop
// what the client has not read yet, GOAWAY among it.
func TestEngineCloseSendsGoAway(t *testing.T) {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:0", Handler: NewHandler(echoHandler())})
	if err != nil {
		t.Fatal(err)
	}
	addr, runDone := startEngine(t, engine)
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/one")...)
	tc.response(1)
	tc.headers(3, true, get("/two")...)
	tc.response(3)

	engine.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	checkEngineCloseGoAway(t, tc, 3)
}

// TestEngineCloseSendsGoAwayOverTLS checks the same of a connection whose
// frames the TLS layer seals: the engine's close reaches the handler through
// the layer, and what the handler writes there is encrypted as anything else
// it sends is.
func TestEngineCloseSendsGoAwayOverTLS(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:0",
		Handler: fibtls.NewServer(ConfigureTLS(serverTLS), NewHandler(echoHandler())),
	})
	if err != nil {
		t.Fatal(err)
	}
	addr, runDone := startEngine(t, engine)
	clientTLS.NextProtos = []string{"h2"}
	conn, err := stdtls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tc := newH2TestConn(t, conn)
	tc.write(append([]byte(h2Preface), h2AppendSettings(nil)...))
	tc.headers(1, true, get("/one")...)
	tc.response(1)
	tc.headers(3, true, get("/two")...)
	tc.response(3)

	engine.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	checkEngineCloseGoAway(t, tc, 3)
}

// TestH2EngineCloseTakesNoConnectionLock checks that the GOAWAY Engine.Close
// writes takes no lock of the HTTP/2 connection: a handler may hold one while
// Close must not wait. The test holds the connection's lock, as a handler
// writing a response would, then writes the GOAWAY and reads it.
func TestH2EngineCloseTakesNoConnectionLock(t *testing.T) {
	conns := make(chan *fib.Connection, 1)
	handler := NewHandler(HandlerFunc(func(c *Context) {
		select {
		case conns <- c.Conn:
		default:
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}))
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, runDone := startEngine(t, engine)
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	tc.response(1)
	sc, ok := (<-conns).Attachment().(*h2ServerConn)
	if !ok {
		t.Fatal("the connection is not HTTP/2")
	}
	sc.mu.Lock()
	done := make(chan struct{})
	go func() {
		handler.OnEngineClose(sc.conn)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		sc.mu.Unlock()
		t.Fatal("OnEngineClose waited for the connection's lock")
	}
	checkEngineCloseGoAway(t, tc, 1)
	sc.mu.Unlock()
}
