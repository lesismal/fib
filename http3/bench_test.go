//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
)

// udpRelay forwards the datagrams of one client to target and back,
// counting their bytes both ways, and returns the address to dial.
func udpRelay(b *testing.B, target string) (string, *atomic.Int64) {
	b.Helper()
	front, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	back, err := net.Dial("udp", target)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = front.Close()
		_ = back.Close()
	})
	var counted atomic.Int64
	var client atomic.Pointer[net.Addr]
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, from, err := front.ReadFrom(buf)
			if err != nil {
				return
			}
			client.Store(&from)
			counted.Add(int64(n))
			_, _ = back.Write(buf[:n])
		}
	}()
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := back.Read(buf)
			if err != nil {
				return
			}
			counted.Add(int64(n))
			if to := client.Load(); to != nil {
				_, _ = front.WriteTo(buf[:n], *to)
			}
		}
	}()
	return front.LocalAddr().String(), &counted
}

// BenchmarkHTTP3LargeHeaders measures the requests of an RPC client whose
// large fields repeat, a cookie and a bearer token among them, and their
// responses, with QPACK's dynamic table off, the default, and on, and
// reports the bytes the requests take on the wire, both ways, through a
// relay.
func BenchmarkHTTP3LargeHeaders(b *testing.B) {
	for _, capacity := range []int{0, 4096} {
		name := "static"
		if capacity > 0 {
			name = "dynamic4k"
		}
		b.Run(name, func(b *testing.B) {
			config := Config{QPACKTableCapacity: capacity, QPACKBlockedStreams: 16}
			url := startServer(b, config, func(c *fibhttp.Context) {
				h := c.Header()
				h.Set("Content-Type", "application/grpc")
				h.Set("X-Tenant-Config", strings.Repeat("feature-flag;", 16))
				h.Set("Server-Timing", "app;dur="+strconv.Itoa(len(c.Request.Header.Get("X-Request-Id"))))
				_, _ = c.Write([]byte("ok"))
			})
			addr, counted := udpRelay(b, strings.TrimPrefix(url, "https://localhost"))
			_, port, _ := net.SplitHostPort(addr)
			url = "https://localhost:" + port
			client := newClient(b, withQPACK(capacity, 16))
			cookie := "session=" + strings.Repeat("c0ffee", 100)
			token := "Bearer " + strings.Repeat("eyJhbGciOi", 20)
			paths := [...]string{"List", "Get", "Put", "Watch"}
			do := func(i int) {
				req, err := stdhttp.NewRequest(stdhttp.MethodPost, url+"/inventory.v1.Items/"+paths[i%4], nil)
				if err != nil {
					b.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/grpc")
				req.Header.Set("User-Agent", "grpc-go/1.70.0")
				req.Header.Set("Cookie", cookie)
				req.Header.Set("Authorization", token)
				req.Header.Set("X-Tenant", "acme-corp-production")
				req.Header.Set("X-Request-Id", strconv.Itoa(1000000+i))
				resp, err := client.Go(req).Wait()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					b.Fatal(err)
				}
				_ = resp.Body.Close()
			}
			// The handshake, and the table filling, are left out.
			for i := 0; i < 8; i++ {
				do(i)
			}
			start := counted.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				do(i)
			}
			b.StopTimer()
			b.ReportMetric(float64(counted.Load()-start)/float64(b.N), "wire-B/op")
		})
	}
}

// The benchmarks in this file run the HTTP/3 client and server over a UDP
// socket of the loopback interface, so they include what package quic's
// benchmarks leave out: the engine, the socket and the HTTP/3 framing.

// BenchmarkHTTP3Transfer measures a response body of a declared length over
// one connection, one request after another: the bulk download a single
// connection gives, and a small one that a whole response is kept for too.
func BenchmarkHTTP3Transfer(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i)
			}
			url := startServer(b, Config{}, func(c *fibhttp.Context) {
				_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", payload)
			})
			client := newClient(b, nil)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req, err := stdhttp.NewRequest(stdhttp.MethodGet, url+"/payload", nil)
				if err != nil {
					b.Fatal(err)
				}
				resp, err := client.Go(req).Wait()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					b.Fatal(err)
				}
				_ = resp.Body.Close()
			}
		})
	}
}

// BenchmarkHTTP3Requests measures a small request and its response over one
// connection, one at a time, and what each allocates.
func BenchmarkHTTP3Requests(b *testing.B) {
	url := startServer(b, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(b, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodGet, url+"/", nil)
		if err != nil {
			b.Fatal(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

// BenchmarkHTTP3ConcurrentRequests measures requests in flight at once on
// one connection, from as many goroutines as there are processors.
func BenchmarkHTTP3ConcurrentRequests(b *testing.B) {
	url := startServer(b, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(b, nil)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := stdhttp.NewRequest(stdhttp.MethodGet, url+"/", nil)
			if err != nil {
				b.Error(err)
				return
			}
			resp, err := client.Go(req).Wait()
			if err != nil {
				b.Error(err)
				return
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				b.Error(err)
				return
			}
			_ = resp.Body.Close()
		}
	})
}

// BenchmarkHTTP3Upload measures a request body of a declared length: the
// direction where the server sends the acknowledgements.
func BenchmarkHTTP3Upload(b *testing.B) {
	const size = 256 << 10
	body := make([]byte, size)
	url := startServer(b, Config{}, func(c *fibhttp.Context) {
		if _, err := io.Copy(io.Discard, c.Request.Body); err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", nil)
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(b, nil)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodPost, url+"/upload", bytes.NewReader(body))
		if err != nil {
			b.Fatal(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
