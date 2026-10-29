//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// Records sealed just past a size class of the pool go in a buffer of the
// class below and a small one for the rest, rather than in one of the class
// above, half of which would wait in the queue empty; other sizes are sealed
// into one buffer of their own size.
func TestSealBufferSize(t *testing.T) {
	for _, tc := range []struct{ size, want int }{
		{sealedSize(64<<10+36, 22), 64 << 10},
		{sealedSize(1<<20+36, 22), 1 << 20},
		{64 << 10, 64 << 10},
		{3, 3},
		{40000, 40000},
		{200000, 200000},
	} {
		got := sealBufferSize(tc.size)
		if got != tc.want {
			t.Errorf("sealBufferSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
		if got < tc.size && bufferpool.Align(got) != got {
			t.Errorf("sealBufferSize(%d) = %d, not a class of the pool", tc.size, got)
		}
	}
}

// Sends of every size around the classes reach the client whole and in
// order, whether sealed on a corked read round or by a goroutine.
func TestSealSplitRoundTrip(t *testing.T) {
	sizes := []int{1, maxPlaintext, 64<<10 - 30, 64<<10 + 36, 3*maxPlaintext + 5, 128<<10 + 100, 1<<20 + 36}
	payload := func(i, n int) []byte { return bytes.Repeat([]byte{byte('a' + i)}, n) }
	send := func(c *fib.Connection) {
		for i, n := range sizes {
			p := payload(i, n)
			_ = c.SendParts(p[:n/2], p[n/2:])
		}
	}
	serverConfig, clientConfig := tlsConfigs(t)
	serverConfig.MinVersion = stdtls.VersionTLS13
	handler := fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		for _, cmd := range b {
			if cmd == 'r' {
				send(c)
			} else {
				go send(c)
			}
		}
	}}
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, handler))
	conn, _ := dialCounted(t, addr, clientConfig)
	for _, cmd := range []byte("rg") {
		if _, err := conn.Write([]byte{cmd}); err != nil {
			t.Fatal(err)
		}
		for i, n := range sizes {
			expectRead(t, conn, payload(i, n))
		}
	}
}
