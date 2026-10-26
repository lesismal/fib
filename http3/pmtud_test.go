//go:build linux || darwin

package http3

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// waitDatagramSize waits for qc to send datagrams of want bytes.
func waitDatagramSize(t *testing.T, who string, qc *quic.Conn, want int) {
	t.Helper()
	got := 0
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if got = qc.DatagramSize(); got == want {
			return
		}
	}
	t.Fatalf("%s sends datagrams of %d bytes, want %d", who, got, want)
}

// Over loopback, whose MTU is larger than any limit, the engine's sockets
// stop fragmenting and both ends discover the limit, unless told not to.
func TestPathMTUDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		disable bool
		limit   int
		want    int
	}{
		// Loopback here is IPv4, which carries 1472 bytes over an Ethernet
		// MTU of 1500; an IPv6 path would discover 1452.
		{"default", false, 0, 1472},
		{"limit", false, 1300, 1300},
		{"disabled", true, 0, 1200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("h3 pmtud "), 8<<10)
			var conn atomic.Pointer[fib.Connection]
			url := startServer(t, Config{DisablePathMTUDiscovery: tc.disable, MaxProbeDatagramSize: tc.limit},
				func(c *fibhttp.Context) {
					conn.Store(c.Conn)
					_, _ = c.Write(payload)
				})
			client := newClient(t, func(c *ClientConfig) {
				c.DisablePathMTUDiscovery, c.MaxProbeDatagramSize = tc.disable, tc.limit
			})
			get := func() {
				resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url, nil)).Wait()
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				if err != nil || !bytes.Equal(body, payload) {
					t.Fatalf("body of %d bytes, %v", len(body), err)
				}
			}
			get()
			cc := ccvConn(t, client)
			cc.mu.Lock()
			qc := cc.qc
			cc.mu.Unlock()
			waitDatagramSize(t, "client", qc, tc.want)
			waitDatagramSize(t, "server", csServerConn(t, conn.Load()), tc.want)
			get()
		})
	}
}
