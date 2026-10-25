//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"crypto/tls"
	stdhttp "net/http"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// ClientConfig.TLS13CipherSuites is what the client's ClientHello offers, so a
// list of ChaCha20-Poly1305 alone settles the connection on it even where the
// CPU prefers AES-GCM, and the server, which follows its client's preference,
// settles on it too.
func TestClientTLS13CipherSuites(t *testing.T) {
	serverSuites := make(chan uint16, 1)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		select {
		case serverSuites <- c.Request.TLS.CipherSuite:
		default:
		}
		_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK,
			Header: stdhttp.Header{"Content-Type": {"text/plain"}}, Body: []byte("chacha20")})
	})
	client := newClient(t, func(config *ClientConfig) {
		config.TLS13CipherSuites = []uint16{tls.TLS_CHACHA20_POLY1305_SHA256}
	})
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/chacha20", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "chacha20" {
		t.Fatalf("body %q", body)
	}
	if resp.TLS == nil || resp.TLS.CipherSuite != tls.TLS_CHACHA20_POLY1305_SHA256 {
		t.Fatalf("the client settled on %+v", resp.TLS)
	}
	select {
	case suite := <-serverSuites:
		if suite != tls.TLS_CHACHA20_POLY1305_SHA256 {
			t.Fatalf("the server settled on suite %#04x", suite)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not run")
	}
}
