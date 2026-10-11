//go:build linux || darwin || windows

package http3

import (
	"io"
	"net"
	stdhttp "net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/tlstest"
)

var handshakeModes = []struct {
	name     string
	blocking bool
}{{"state machine", false}, {"crypto/tls", true}}

// BenchmarkClientNewConnection measures a request on a connection of its own:
// a QUIC handshake, then the request, then the connection is dropped. The
// handshake is either the state machines of package tls, on both ends, or
// crypto/tls's QUICConn, which runs it on a goroutine of its own that waits
// for the peer. The server is this package's, in this process, so the figures
// are relative.
func BenchmarkClientNewConnection(b *testing.B) {
	for _, mode := range handshakeModes {
		b.Run(mode.name, func(b *testing.B) {
			url := startServer(b, Config{BlockingTLSHandshake: mode.blocking}, func(c *fibhttp.Context) {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
			})
			// By address, as the TCP clients' benchmark is: a name would be
			// resolved on a pool, which is not what is measured.
			url = strings.Replace(url, "localhost", "127.0.0.1", 1)
			_, clientTLS, err := tlstest.Configs()
			if err != nil {
				b.Fatal(err)
			}
			clientTLS.ServerName = "localhost"
			engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
			if err != nil {
				b.Fatal(err)
			}
			runEngine(b, engine)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					config := DefaultClientConfig()
					config.TLSConfig = clientTLS
					config.BlockingTLSHandshake = mode.blocking
					client := NewClient(engine, config)
					req, _ := stdhttp.NewRequest(stdhttp.MethodGet, url, nil)
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

// BenchmarkClientStalledHandshakes measures what handshakes cost while their
// peer does not answer: a UDP socket that swallows what it is sent, and a few
// hundred clients, each with a request to it. The metric is the goroutines the
// process has more while they wait, per handshake: none for the state machine,
// and one for crypto/tls's QUICConn, parked in quicWaitForSignal.
func BenchmarkClientStalledHandshakes(b *testing.B) {
	const handshakes = 400
	sink, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sink.Close() })
	go func() {
		buf := make([]byte, 2048)
		for {
			if _, _, err := sink.ReadFrom(buf); err != nil {
				return
			}
		}
	}()
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		b.Fatal(err)
	}
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		b.Fatal(err)
	}
	runEngine(b, engine)
	_, port, _ := net.SplitHostPort(sink.LocalAddr().String())
	url := "https://127.0.0.1:" + port + "/"

	for _, mode := range handshakeModes {
		b.Run(mode.name, func(b *testing.B) {
			var most int
			for i := 0; i < b.N; i++ {
				time.Sleep(200 * time.Millisecond) // let the last round's goroutines go
				before := runtime.NumGoroutine()
				clients := make([]*Client, handshakes)
				for j := range clients {
					config := DefaultClientConfig()
					config.TLSConfig = clientTLS
					config.HandshakeTimeout = 30 * time.Second
					config.BlockingTLSHandshake = mode.blocking
					clients[j] = NewClient(engine, config)
					req, _ := stdhttp.NewRequest(stdhttp.MethodGet, url, nil)
					clients[j].Do(req, func(*fibhttp.ClientResponse, error) {})
				}
				time.Sleep(400 * time.Millisecond)
				most = max(most, runtime.NumGoroutine()-before)
				for _, client := range clients {
					client.Close()
				}
				time.Sleep(100 * time.Millisecond)
			}
			b.ReportMetric(float64(most)/handshakes, "goroutines/handshake")
		})
	}
}
