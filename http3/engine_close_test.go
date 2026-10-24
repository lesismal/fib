//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// TestEngineCloseTellsClients checks that a server whose engine closes tells
// its clients so, with H3_NO_ERROR, rather than leaving them to time out.
func TestEngineCloseTellsClients(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.TLSConfig = serverTLS
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: NewHandlerWithConfig(config, fibhttp.HandlerFunc(echo))})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	w := dialH3Wire(t, nil, fmt.Sprintf("https://localhost:%d", addr.Port))
	id := w.send(t, h3Request{path: "/before"}, false)
	if status, _ := w.response(id); status != 200 {
		t.Fatalf("status %d", status)
	}
	engine.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-w.closed:
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || !appErr.Remote || appErr.Code != uint64(ErrCodeNoError) {
			t.Fatalf("closed with %v, want the server's H3_NO_ERROR", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the client was not told")
	}
}

// TestEngineCloseTellsServers checks that a client whose engine closes
// tells the server so: the handler of a request still open hears of it at
// once, rather than once the connection idles out.
func TestEngineCloseTellsServers(t *testing.T) {
	started, ended := make(chan struct{}), make(chan error, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		close(started)
		<-c.Context().Done()
		ended <- c.Context().Err()
	})
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	clientConfig := DefaultClientConfig()
	clientConfig.TLSConfig = clientTLS
	clientConfig.TLSConfig.ServerName = "localhost"
	client := NewClient(engine, clientConfig)
	go func() { _, _ = client.Go(mustRequest(t, stdhttp.MethodGet, base+"/held", nil)).Wait() }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not arrive")
	}
	engine.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the server did not hear of the close")
	}
}
