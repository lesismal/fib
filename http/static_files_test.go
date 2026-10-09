//go:build linux || darwin || windows

package http_test

import (
	stdtls "crypto/tls"
	"io"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/internal/statictest"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// serveStatic runs an engine serving handler, over TLS when secure, and
// returns its address.
func serveStatic(t *testing.T, secure bool, handler fibhttp.HandlerFunc) string {
	t.Helper()
	var h fib.Handler = fibhttp.NewHandler(handler)
	if secure {
		serverConfig, _, err := tlstest.Configs()
		if err != nil {
			t.Fatal(err)
		}
		h = fibtls.NewServer(fibhttp.ConfigureTLS(serverConfig), h)
	}
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, h)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
		_ = engine.Close()
	})
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	return addr.String()
}

// TestStaticFiles serves files over HTTP/1.1 and HTTP/2, in cleartext and
// over TLS, to net/http's client: whole and ranged, small and big, from the
// file cache, from net/http's helpers and with io.Copy from a file (the
// sendfile path on a cleartext HTTP/1 connection).
func TestStaticFiles(t *testing.T) {
	dir, files := statictest.Setup(t)
	for _, tt := range []struct {
		name          string
		secure, http2 bool
	}{
		{"http1", false, false},
		{"http1-tls", true, false},
		{"h2c", false, true},
		{"h2", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			addr := serveStatic(t, tt.secure, statictest.Handler(t, dir))
			transport := &stdhttp.Transport{DisableCompression: true}
			switch {
			case tt.secure:
				_, clientConfig, err := tlstest.Configs()
				if err != nil {
					t.Fatal(err)
				}
				transport.TLSClientConfig = clientConfig
				transport.ForceAttemptHTTP2 = tt.http2
				if !tt.http2 {
					// A non-nil, empty map keeps net/http off HTTP/2.
					transport.TLSNextProto = map[string]func(string, *stdtls.Conn) stdhttp.RoundTripper{}
				}
			case tt.http2:
				transport.Protocols = new(stdhttp.Protocols)
				transport.Protocols.SetUnencryptedHTTP2(true)
			}
			t.Cleanup(transport.CloseIdleConnections)
			client := &stdhttp.Client{Transport: transport, Timeout: 30 * time.Second}
			base := "http://"
			if tt.secure {
				base = "https://"
			}
			wantMajor := 1
			if tt.http2 {
				wantMajor = 2
			}
			statictest.Run(t, func(method, path string, header map[string]string) (int, stdhttp.Header, []byte) {
				req, err := stdhttp.NewRequest(method, base+addr+path, nil)
				if err != nil {
					t.Errorf("%s %s: %v", method, path, err)
					return 0, nil, nil
				}
				for k, v := range header {
					req.Header.Set(k, v)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Errorf("%s %s: %v", method, path, err)
					return 0, nil, nil
				}
				defer resp.Body.Close()
				if resp.ProtoMajor != wantMajor {
					t.Errorf("%s %s: served over %s, want HTTP/%d", method, path, resp.Proto, wantMajor)
				}
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Errorf("%s %s: reading the body: %v", method, path, err)
					return 0, nil, nil
				}
				return resp.StatusCode, resp.Header, body
			}, files)
		})
	}
}
