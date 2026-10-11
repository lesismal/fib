//go:build linux || darwin || windows

package http

import (
	"context"
	stdtls "crypto/tls"
	"crypto/x509"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

func benchTLSServer(b *testing.B) (*httptest.Server, *stdtls.Config) {
	b.Helper()
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	b.Cleanup(ts.Close)
	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	return ts, &stdtls.Config{RootCAs: pool}
}

func benchEngine(b *testing.B) *fib.Engine {
	b.Helper()
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	b.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	return engine
}

var handshakeModes = []struct {
	name     string
	blocking bool
}{{"state machine", false}, {"crypto/tls", true}}

var handshakeProtocols = []struct {
	name string
	h1   bool
}{{"HTTP/2", false}, {"HTTP/1.1", true}}

// BenchmarkClientNewConnection measures a request on a connection of its own:
// a TLS handshake, then the request, then the connection is dropped. The same
// client would reuse its connection, so each request has a client of its own.
// It compares the handshake that takes no goroutine while it waits with the one
// that runs in crypto/tls on a pool worker. The server is net/http's, in this
// process, so the figures are relative.
func BenchmarkClientNewConnection(b *testing.B) {
	ts, tlsConfig := benchTLSServer(b)
	engine := benchEngine(b)
	for _, protocol := range handshakeProtocols {
		for _, mode := range handshakeModes {
			b.Run(protocol.name+"/"+mode.name, func(b *testing.B) {
				config := DefaultClientConfig()
				config.TLSConfig = tlsConfig
				config.DisableHTTP2 = protocol.h1
				config.BlockingTLSHandshake = mode.blocking
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						client := NewClient(engine, config)
						req, _ := stdhttp.NewRequest(stdhttp.MethodGet, ts.URL, nil)
						resp, err := client.Go(req).Wait()
						if err != nil {
							b.Error(err)
							client.Close()
							return
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						client.Close()
					}
				})
			})
		}
	}
}

// BenchmarkClientStalledHandshakes measures what handshakes cost while their
// peer does not answer: a server that accepts connections and says nothing,
// and a few hundred clients, each with a request to it. The metric is the
// goroutines the process has more while they wait, per handshake.
func BenchmarkClientStalledHandshakes(b *testing.B) {
	const handshakes = 400
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	b.Cleanup(func() {
		listener.Close()
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	})
	engine := benchEngine(b)
	for _, protocol := range handshakeProtocols {
		for _, mode := range handshakeModes {
			b.Run(protocol.name+"/"+mode.name, func(b *testing.B) {
				config := DefaultClientConfig()
				config.TLSConfig = &stdtls.Config{InsecureSkipVerify: true}
				config.DisableHTTP2 = protocol.h1
				config.BlockingTLSHandshake = mode.blocking
				config.MaxConnsPerHost = 0
				config.Timeout = 0
				var most int
				for i := 0; i < b.N; i++ {
					// A client apiece: an HTTP/2 client would put the requests
					// on the one connection it is dialing.
					clients := make([]*Client, handshakes)
					ctx, cancel := context.WithCancel(context.Background())
					time.Sleep(200 * time.Millisecond) // let the last round's goroutines go
					before := runtime.NumGoroutine()
					for j := range clients {
						clients[j] = NewClient(engine, config)
						req, _ := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "https://"+listener.Addr().String(), nil)
						clients[j].Do(req, func(*ClientResponse, error) {})
					}
					time.Sleep(400 * time.Millisecond)
					most = max(most, runtime.NumGoroutine()-before)
					cancel()
					for _, client := range clients {
						client.Close()
					}
					time.Sleep(100 * time.Millisecond)
				}
				b.ReportMetric(float64(most)/handshakes, "goroutines/handshake")
			})
		}
	}
}
