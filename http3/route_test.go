//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"fmt"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/tlstest"
)

// The server routes the connection IDs it issues, so that a client sending
// to one from an address the server does not know finds its connection, and
// forgets them all with the connection.
func TestServerRoutesIssuedConnectionIDs(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlerWithConfig(Config{TLSConfig: serverTLS}, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}))
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("https://localhost:%d", addr.Port)
	client := newClient(t, nil)
	if _, err := client.Go(mustRequest(t, "GET", url+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	routes := func() int {
		h.routesMu.RLock()
		defer h.routesMu.RUnlock()
		return len(h.routes)
	}
	// The client allows two connection IDs, the handshake's and one more.
	for deadline := time.Now().Add(5 * time.Second); routes() != 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d routes", routes())
		}
	}
	h.routesMu.RLock()
	for cid, qc := range h.routes {
		if !qc.Owns([]byte(cid)) {
			t.Errorf("route %x to a connection that does not own it", cid)
		}
	}
	h.routesMu.RUnlock()
	client.Close()
	for deadline := time.Now().Add(5 * time.Second); routes() != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d routes left after the connection closed", routes())
		}
	}
}
