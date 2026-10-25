//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
)

// The benchmarks in this file run the HTTP/3 client and server over a UDP
// socket of the loopback interface, so they include what package quic's
// benchmarks leave out: the engine, the socket and the HTTP/3 framing.

// BenchmarkHTTP3Transfer measures a response body of a declared length over
// one connection, one request after another: the bulk download a single
// connection gives.
func BenchmarkHTTP3Transfer(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
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
