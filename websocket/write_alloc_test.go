//go:build (linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows) && !race

package websocket

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"testing"

	fib "github.com/lesismal/fib"
)

// A server frame's header is built on sendFrame's stack and written beside the
// payload, so WriteMessage allocates nothing. Every platform's two-part write
// has to keep it there: on Darwin and OpenBSD, a writev the compiler could not
// see into put the header and the iovec array on the heap, two allocations
// for every message to every connection, and on Windows sys.WSABufs did the
// same. The race detector's hooks in the syscall package let it escape anyway,
// so this does not run under it.
func TestServerWriteMessageAllocs(t *testing.T) {
	opened := make(chan *Connection, 1)
	config := fib.DefaultConfig()
	server, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: NewHandler(HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) { opened <- c },
	})})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	}()

	conn, reader := dialUpgraded(t, addr.String())
	defer conn.Close()
	go func() { _, _ = io.Copy(io.Discard, reader) }()
	c := <-opened

	// Small enough that the socket never pushes back, which would queue the
	// unsent rest and count a pool's allocations instead.
	payload := make([]byte, 16)
	if err := c.WriteMessage(Binary, payload); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := c.WriteMessage(Binary, payload); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("WriteMessage allocated %v times a message", allocs)
	}
}

// dialDiscardingServer dials a server that reads and drops every message,
// and returns the client's connection.
func dialDiscardingServer(tb testing.TB) *Connection {
	tb.Helper()
	url := startWebSocketServer(tb, DefaultConfig(), HandlerFuncs{})
	engine := startClientEngine(tb)
	type result struct {
		c   *Connection
		err error
	}
	done := make(chan result, 1)
	NewDialer(engine, DefaultDialerConfig()).Dial(url, nil, nil, func(c *Connection, _ *stdhttp.Response, err error) {
		done <- result{c, err}
	})
	r := <-done
	if r.err != nil {
		tb.Fatal(r.err)
	}
	tb.Cleanup(func() { _ = r.c.Close(CloseNormal, "") })
	return r.c
}

// A client's frame is masked into a buffer from the pool, which the
// connection gives back once it has been written, so a client's
// WriteMessage allocates nothing either.
func TestClientWriteMessageAllocs(t *testing.T) {
	c := dialDiscardingServer(t)
	payload := make([]byte, 16)
	if err := c.WriteMessage(Binary, payload); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := c.WriteMessage(Binary, payload); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("a client's WriteMessage allocated %v times a message", allocs)
	}
}

func BenchmarkClientWriteMessage(b *testing.B) {
	for _, size := range []int{16, 1 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			c := dialDiscardingServer(b)
			payload := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				if err := c.WriteMessage(Binary, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
