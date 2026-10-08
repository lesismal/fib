//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/taskpool"
)

func TestConcurrentBackpressuredEcho(t *testing.T) {
	for _, useWritev := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "writev"}[useWritev], func(t *testing.T) {
			config := DefaultConfig()
			config.Addr = "127.0.0.1:0"
			config.UseWritev = useWritev
			server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
				if err := c.Send(b); err != nil {
					c.Close()
				}
			}})
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
				if err := <-runDone; err != nil {
					t.Error(err)
				}
				if err := server.Close(); err != nil {
					t.Error(err)
				}
			}()
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(value byte) {
					defer wg.Done()
					payload := bytes.Repeat([]byte{value}, 1024*1024)
					conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
					if _, err = conn.Write(payload); err != nil {
						t.Error(err)
						return
					}
					received := make([]byte, len(payload))
					if _, err = io.ReadFull(conn, received); err != nil {
						t.Error(err)
						return
					}
					if !bytes.Equal(received, payload) {
						t.Error("echo mismatch")
					}
				}(byte(i))
			}
			wg.Wait()
		})
	}
}

func TestOnCloseReportsPeerEOF(t *testing.T) {
	closed := make(chan error, 1)
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != io.EOF {
			t.Fatalf("OnClose error = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnClose")
	}
	server.Stop()
	if err := <-runDone; err != nil {
		t.Error(err)
	}
	if err := server.Close(); err != nil {
		t.Error(err)
	}
}

// TestReadsDeferredWhileOutputQueued covers the flush-before-read ordering in
// process: without a watermark, while Send output is still queued the worker
// must not read, and the deferred readiness must survive until the queue
// drains so the socket does not turn into a zombie with unread bytes and no
// further edge.
func TestReadsDeferredWhileOutputQueued(t *testing.T) {
	for _, halfClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "halfclose"}[halfClose], func(t *testing.T) {
			payload := bytes.Repeat([]byte{0xAB}, 32*1024*1024)
			second := make(chan []byte, 1)
			closed := make(chan error, 1)
			config := DefaultConfig()
			config.Addr = "127.0.0.1:0"
			// Disable the watermark so epoll keeps EPOLLIN registered and only
			// the process ordering stands between queued output and a read.
			config.WriteBufferHighWatermark = -1
			var first sync.Once
			var sender atomic.Pointer[Connection]
			server, err := Bind(config, HandlerFuncs{
				Data: func(c *Connection, data []byte) {
					started := false
					first.Do(func() {
						started = true
						sender.Store(c)
						// Send in chunks: Windows accepts one send of any size
						// while its send backlog is below SO_SNDBUF, so a single
						// call could leave nothing queued. Later chunks queue
						// once the backlog is full, on every platform.
						for offset := 0; offset < len(payload); offset += 1 << 20 {
							if err := c.SendOwned(payload[offset : offset+1<<20]); err != nil {
								t.Error(err)
								return
							}
						}
					})
					if !started {
						second <- append([]byte(nil), data...)
					}
				},
				Close: func(_ *Connection, err error) { closed <- err },
			})
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
				if err := <-runDone; err != nil {
					t.Error(err)
				}
				if err := server.Close(); err != nil {
					t.Error(err)
				}
			}()
			conn, err := net.DialTCP("tcp4", nil, addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			if _, err := conn.Write([]byte("start")); err != nil {
				t.Fatal(err)
			}
			// Let the server block on the peer's full receive window before
			// the second message arrives.
			time.Sleep(300 * time.Millisecond)
			if c := sender.Load(); c == nil || !c.hasQueuedOutput() {
				t.Skip("the kernel accepted the whole payload; no output is queued to defer reads behind")
			}
			if _, err := conn.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			if halfClose {
				if err := conn.CloseWrite(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case data := <-second:
				t.Fatalf("read %q while output was still queued", data)
			case <-time.After(300 * time.Millisecond):
			}
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, received); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(received, payload) {
				t.Fatal("payload mismatch")
			}
			select {
			case data := <-second:
				if !bytes.Equal(data, []byte("ping")) {
					t.Fatalf("second message = %q", data)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("deferred read never resumed after the queue drained")
			}
			if halfClose {
				select {
				case err := <-closed:
					if err != io.EOF {
						t.Fatalf("OnClose error = %v, want io.EOF", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for OnClose after half-close")
				}
			}
		})
	}
}

// TestReadsContinueBelowWatermark covers the other side of that ordering: with
// a watermark, output queued below it does not stop the worker from reading,
// so a peer that sends while its replies are still queued is not left waiting
// on a full receive queue.
func TestReadsContinueBelowWatermark(t *testing.T) {
	payload := bytes.Repeat([]byte{0xCD}, 8*1024*1024)
	second := make(chan []byte, 1)
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	config.WriteBufferHighWatermark = 4 * len(payload)
	var first sync.Once
	var sender atomic.Pointer[Connection]
	server, err := Bind(config, HandlerFuncs{
		Data: func(c *Connection, data []byte) {
			started := false
			first.Do(func() {
				started = true
				sender.Store(c)
				for offset := 0; offset < len(payload); offset += 1 << 20 {
					if err := c.SendOwned(payload[offset : offset+1<<20]); err != nil {
						t.Error(err)
						return
					}
				}
			})
			if !started {
				second <- append([]byte(nil), data...)
			}
		},
	})
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
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()
	conn, err := net.DialTCP("tcp4", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := conn.Write([]byte("start")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if c := sender.Load(); c == nil || !c.hasQueuedOutput() {
		t.Skip("the kernel accepted the whole payload; no output is queued")
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-second:
		if !bytes.Equal(data, []byte("ping")) {
			t.Fatalf("second message = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read deferred behind output queued below the watermark")
	}
	if c := sender.Load(); !c.hasQueuedOutput() {
		t.Log("the queue drained before the second read; the test proved less than it meant to")
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, received); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("payload mismatch")
	}
}

// TestConcurrentAcceptBurst covers the accept path when many connections arrive
// at once: every one must be accepted, tracked, and able to carry data.
func TestConcurrentAcceptBurst(t *testing.T) {
	const burst = 256
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
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
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp4", addr.String(), 20*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
			payload := []byte{value, value, value, value}
			if _, err = conn.Write(payload); err != nil {
				t.Error(err)
				return
			}
			echoed := make([]byte, len(payload))
			if _, err = io.ReadFull(conn, echoed); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(echoed, payload) {
				t.Error("echo mismatch")
			}
		}(byte(i))
	}
	wg.Wait()
}

// An engine never runs a connection's round on its loop, so it refuses an
// inline pool, whether it would build it or is handed one.
func TestEngineRefusesInlinePool(t *testing.T) {
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	config.SetTaskPoolMode(taskpool.ModeInline)
	if server, err := Bind(config, nil); !errors.Is(err, errInlinePool) {
		if server != nil {
			_ = server.Close()
		}
		t.Fatalf("Bind with ModeInline: %v, want errInlinePool", err)
	}
	config = DefaultConfig()
	config.Addr = "127.0.0.1:0"
	pool := taskpool.NewInline("refused-inline")
	defer pool.Stop()
	config.SetTaskPool(pool)
	if server, err := Bind(config, nil); !errors.Is(err, errInlinePool) {
		if server != nil {
			_ = server.Close()
		}
		t.Fatalf("Bind with an inline TaskPool: %v, want errInlinePool", err)
	}
}

// TestMultipleListenersShareOneEngine covers a server carrying several
// listeners: every port must accept, and the connections from all of them must
// land in the one descriptor table, event loop and worker pool rather than
// needing a server each.
func TestMultipleListenersShareOneEngine(t *testing.T) {
	const listeners = 4
	config := DefaultConfig()
	config.Addr = "127.0.0.1:9999" // ignored once Addrs is set
	config.Addrs = make([]string, listeners)
	for i := range config.Addrs {
		config.Addrs[i] = "127.0.0.1:0"
	}
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.LocalAddrs()
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != listeners {
		t.Fatalf("LocalAddrs returned %d addresses, want %d", len(addrs), listeners)
	}
	seen := make(map[int]bool, listeners)
	for _, addr := range addrs {
		if addr.Port == 0 || addr.Port == 9999 || seen[addr.Port] {
			t.Fatalf("listener ports are not distinct ephemeral ports: %v", addrs)
		}
		seen[addr.Port] = true
	}
	first, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	if first.Port != addrs[0].Port {
		t.Fatalf("LocalAddr port = %d, want the first listener %d", first.Port, addrs[0].Port)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()

	var wg sync.WaitGroup
	for i, addr := range addrs {
		for round := 0; round < 4; round++ {
			wg.Add(1)
			go func(addr string, value byte) {
				defer wg.Done()
				conn, err := net.DialTimeout("tcp4", addr, 10*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				payload := bytes.Repeat([]byte{value}, 64)
				if _, err = conn.Write(payload); err != nil {
					t.Error(err)
					return
				}
				echoed := make([]byte, len(payload))
				if _, err = io.ReadFull(conn, echoed); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(echoed, payload) {
					t.Errorf("echo mismatch on listener %s", addr)
				}
			}(addr.String(), byte(i))
		}
	}
	wg.Wait()
}

// Network and Addr are read the way net.Listen reads them, so the same strings
// that name a listener there name one here.
func TestListenAddressFormsMatchNetListen(t *testing.T) {
	for _, tc := range []struct {
		name    string
		network string
		addr    string
		wantIP  func(net.IP) bool
	}{
		{"tcp4 literal", "tcp4", "127.0.0.1:0", func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) }},
		{"tcp literal", "tcp", "127.0.0.1:0", func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) }},
		{"tcp6 literal", "tcp6", "[::1]:0", func(ip net.IP) bool { return ip.Equal(net.IPv6loopback) }},
		{"tcp wildcard", "tcp", ":0", func(ip net.IP) bool { return ip.IsUnspecified() }},
		{"empty network defaults to tcp", "", "127.0.0.1:0", func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Whatever this library does with the arguments, net.Listen has to
			// accept them too, or they are not the standard forms.
			reference, err := net.Listen(map[bool]string{true: "tcp", false: tc.network}[tc.network == ""], tc.addr)
			if err != nil {
				t.Skipf("net.Listen(%q, %q): %v", tc.network, tc.addr, err)
			}
			reference.Close()

			config := DefaultConfig()
			config.Network = tc.network
			config.Addr = tc.addr
			server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
				if err := c.Send(b); err != nil {
					c.Close()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- server.Run() }()
			defer func() {
				server.Stop()
				if err := <-runDone; err != nil {
					t.Errorf("Run: %v", err)
				}
				if err := server.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}()

			addr, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			if addr.Port == 0 {
				t.Fatal("LocalAddr reports port 0, want the port the kernel chose")
			}
			if !tc.wantIP(addr.IP) {
				t.Fatalf("LocalAddr IP = %v, not the address asked for", addr.IP)
			}

			// A wildcard listener is reachable over loopback; a literal one is
			// reachable at itself. Dialing the reported address covers both.
			dialAddr := addr.String()
			if addr.IP.IsUnspecified() {
				dialAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(addr.Port))
			}
			conn, err := net.DialTimeout("tcp", dialAddr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			payload := []byte("listen")
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
			reply := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, reply); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reply, payload) {
				t.Fatalf("echo returned %q, want %q", reply, payload)
			}
		})
	}
}

func TestListenRejectsUnknownNetwork(t *testing.T) {
	config := DefaultConfig()
	config.Network = "unixgram"
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{})
	if err == nil {
		server.Close()
		t.Fatal("Bind accepted network \"unixgram\", want an error")
	}
	var unknown net.UnknownNetworkError
	if !errors.As(err, &unknown) {
		t.Fatalf("Bind error = %v, want net.UnknownNetworkError", err)
	}
}

// countingPool runs tasks on a real pool and counts what it was handed, so a
// test can tell the engine used it.
type countingPool struct {
	*taskpool.TaskPool
	tasks atomic.Int64
}

func (p *countingPool) GoTask(task taskpool.Task) bool {
	p.tasks.Add(1)
	return p.TaskPool.GoTask(task)
}

func (p *countingPool) GoTasks(tasks []taskpool.Task) int {
	p.tasks.Add(int64(len(tasks)))
	return p.TaskPool.GoTasks(tasks)
}

// A pool the caller supplies has to carry the connections, make the built-in
// pool's settings irrelevant, and survive the engine: the caller owns it and
// may be sharing it.
func TestCustomTaskPoolRunsConnectionsAndOutlivesEngine(t *testing.T) {
	pool := &countingPool{TaskPool: taskpool.NewWithMode("test", taskpool.ModeElastic, 4, 64)}
	defer pool.Stop()
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	// Invalid for the built-in pool, and ignored once a pool is supplied.
	config.WorkerCount = 0
	config.SetTaskPool(pool)
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	payload := []byte("custom pool")
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reply, payload) {
		t.Fatalf("echo = %q, want %q", reply, payload)
	}
	_ = conn.Close()
	server.Stop()
	if err := <-runDone; err != nil {
		t.Error(err)
	}
	if err := server.Close(); err != nil {
		t.Error(err)
	}
	if pool.tasks.Load() == 0 {
		t.Fatal("the engine never handed a task to the supplied pool")
	}
	done := make(chan struct{})
	if !pool.Go(func() { close(done) }) {
		t.Fatal("closing the engine stopped a pool it does not own")
	}
	<-done
}

// A close the loop carries out while a round is still running on a worker is
// ordered after that round, as an event would be: OnClose waits for the
// OnData in progress to return, and the descriptor stays the connection's
// until then, so the round never reads one that has been closed or handed on.
func TestOnCloseFollowsTheRunningRound(t *testing.T) {
	entered := make(chan *Connection, 1)
	release := make(chan struct{})
	var dataDone atomic.Bool
	closed := make(chan bool, 1)
	_, addr := startEchoServer(t, DefaultConfig(), HandlerFuncs{
		Data: func(c *Connection, _ []byte) {
			entered <- c
			<-release
			dataDone.Store(true)
		},
		Close: func(*Connection, error) { closed <- dataDone.Load() },
	})
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	c := <-entered
	fd := c.FD()
	c.Close()
	select {
	case <-closed:
		close(release)
		t.Fatal("OnClose ran while OnData was still running")
	case <-time.After(100 * time.Millisecond):
	}
	if c.FD() != fd {
		close(release)
		t.Fatalf("descriptor went from %d to %d under a running round", fd, c.FD())
	}
	close(release)
	select {
	case afterData := <-closed:
		if !afterData {
			t.Fatal("OnClose ran before OnData returned")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose never ran")
	}
	for deadline := time.Now().Add(5 * time.Second); c.FD() >= 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the descriptor was never released")
		}
	}
}

// unixSocketPath returns a path for a Unix socket in a directory of its own.
// t.TempDir can run past the 104 bytes macOS allows a socket path, so the
// directory is made directly under the system temporary directory.
func unixSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "fib")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func startUnixServer(t *testing.T, path string, handler Handler) *Engine {
	t.Helper()
	config := DefaultConfig()
	config.Network = "unix"
	config.Addr = path
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = server.Close()
	})
	return server
}

// A Unix socket serves like TCP: large echoes run through the same queueing
// and backpressure, the peer's address is a *net.UnixAddr, and the socket file
// is gone once the engine closes.
func TestUnixSocketEcho(t *testing.T) {
	path := unixSocketPath(t)
	var remote sync.Map
	config := DefaultConfig()
	config.Network = "unix"
	config.Addr = path
	server, err := Bind(config, HandlerFuncs{
		Open: func(c *Connection) { remote.Store(c, c.RemoteAddr()) },
		Data: func(c *Connection, b []byte) {
			if c.Send(b) != nil {
				c.Close()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()

	addrs, err := server.ListenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 || addrs[0].Network() != "unix" || addrs[0].String() != path {
		t.Fatalf("ListenAddrs = %v, want unix %s", addrs, path)
	}
	if _, err := server.LocalAddr(); err == nil {
		t.Error("LocalAddr reported a TCP address for a Unix listener")
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			conn, err := net.DialTimeout("unix", path, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
			payload := bytes.Repeat([]byte{value}, 1<<20)
			go func() { _, _ = conn.Write(payload) }()
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, received); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(received, payload) {
				t.Error("echo mismatch")
			}
		}(byte(i))
	}
	wg.Wait()
	remote.Range(func(_, addr any) bool {
		if _, ok := addr.(*net.UnixAddr); !ok {
			t.Errorf("RemoteAddr = %T, want *net.UnixAddr", addr)
		}
		return true
	})

	server.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket file left behind: %v", err)
	}
}

// Binding a path that already exists fails, as it does for net.Listen, rather
// than taking the path from whatever owns it.
func TestUnixSocketPathInUse(t *testing.T) {
	path := unixSocketPath(t)
	startUnixServer(t, path, nil)
	config := DefaultConfig()
	config.Network = "unix"
	config.Addr = path
	if second, err := Bind(config, nil); err == nil {
		second.Close()
		t.Fatal("a second engine bound a path already in use")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the failed Bind removed the first engine's socket: %v", err)
	}
}

// An engine dials a Unix socket like a TCP address, and a path nothing
// listens on fails the dial.
func TestDialUnix(t *testing.T) {
	path := unixSocketPath(t)
	startUnixServer(t, path, echoHandler())
	client, _ := startEchoServer(t, DefaultConfig(), nil)

	received := make(chan []byte, 16)
	handler := HandlerFuncs{Data: func(_ *Connection, b []byte) { received <- append([]byte(nil), b...) }}
	dialed := make(chan dialResult, 1)
	err := client.DialWithHandler("unix", path, 5*time.Second, handler, func(c *Connection, err error) {
		dialed <- dialResult{c, err}
	})
	if err != nil {
		t.Fatal(err)
	}
	r := <-dialed
	if r.err != nil {
		t.Fatal(r.err)
	}
	for i := 0; i < 3; i++ {
		msg := "unix " + strconv.Itoa(i)
		if err := r.c.Send([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		var got []byte
		for len(got) < len(msg) {
			select {
			case b := <-received:
				got = append(got, b...)
			case <-time.After(5 * time.Second):
				t.Fatal("no echo")
			}
		}
		if string(got) != msg {
			t.Fatalf("echo %q, want %q", got, msg)
		}
	}
	r.c.Close()

	failed := make(chan error, 1)
	missing := filepath.Join(filepath.Dir(path), "missing.sock")
	err = client.Dial("unix", missing, time.Second, func(c *Connection, err error) { failed <- err })
	if err == nil {
		err = <-failed
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Addr == nil || opErr.Addr.String() != missing {
		t.Fatalf("dial to a missing socket: %v, want a *net.OpError naming it", err)
	}
}

// On Linux a leading '@' names an abstract socket, which has no file.
func TestUnixAbstractSocket(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract Unix sockets are Linux-only")
	}
	name := "@fib-test-" + strconv.Itoa(os.Getpid())
	startUnixServer(t, name, echoHandler())
	conn, err := net.DialTimeout("unix", name, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("abstract")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "abstract" {
		t.Fatalf("echo %q, %v", buf, err)
	}
}

// dialResult is what one Dial's done callback reported.
type dialResult struct {
	c   *Connection
	err error
}

func echoHandler() Handler {
	return HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}}
}

// A dialed connection joins the engine the way an accepted one does: OnOpen
// runs before the dialer hears of it, and its replies arrive through the same
// handler and workers.
func TestDialedConnectionsEcho(t *testing.T) {
	_, serverAddr := startEchoServer(t, DefaultConfig(), echoHandler())

	const conns = 64
	payload := []byte("dialed through the event loop")
	var mu sync.Mutex
	opened := map[*Connection]bool{}
	received := map[*Connection][]byte{}
	echoed := make(chan *Connection, conns)
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			opened[c] = true
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			mu.Lock()
			received[c] = append(received[c], b...)
			done := len(received[c]) == len(payload)
			mu.Unlock()
			if done {
				echoed <- c
			}
		},
	})

	results := make(chan dialResult, conns)
	for i := 0; i < conns; i++ {
		err := client.Dial("tcp4", serverAddr, 5*time.Second, func(c *Connection, err error) {
			if err == nil {
				mu.Lock()
				if !opened[c] {
					t.Error("done ran before OnOpen")
				}
				mu.Unlock()
				err = c.Send(payload)
			}
			results <- dialResult{c, err}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < conns; i++ {
		if r := <-results; r.err != nil {
			t.Fatalf("dial %d: %v", i, r.err)
		}
	}
	timeout := time.After(10 * time.Second)
	for i := 0; i < conns; i++ {
		select {
		case c := <-echoed:
			mu.Lock()
			got := received[c]
			mu.Unlock()
			if !bytes.Equal(got, payload) {
				t.Fatalf("echo mismatch: %q", got)
			}
		case <-timeout:
			t.Fatalf("only %d of %d connections were echoed", i, conns)
		}
	}
}

// A host name goes through the resolver on a goroutine of its own, and must
// still end up on the event loop.
func TestDialResolvesHostName(t *testing.T) {
	_, serverAddr := startEchoServer(t, DefaultConfig(), echoHandler())
	_, port, _ := net.SplitHostPort(serverAddr)
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	results := make(chan dialResult, 1)
	if err := client.Dial("tcp4", net.JoinHostPort("localhost", port), 5*time.Second, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if r.err != nil {
			t.Fatal(r.err)
		}
		r.c.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("dial never reported")
	}
}

// A refused connect reaches the dialer as an error, and the handler never hears
// of the connection.
func TestDialRefusedReportsError(t *testing.T) {
	// A port that was just listened on and closed has nobody behind it.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()

	var handlerCalls int
	var mu sync.Mutex
	count := func(*Connection) { mu.Lock(); handlerCalls++; mu.Unlock() }
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{
		Open:  count,
		Close: func(c *Connection, _ error) { count(c) },
	})
	results := make(chan dialResult, 1)
	if err := client.Dial("tcp4", addr, 5*time.Second, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	r := <-results
	if r.err == nil || r.c != nil {
		t.Fatalf("dial to a closed port: connection %v, error %v", r.c, r.err)
	}
	var opErr *net.OpError
	if !errors.As(r.err, &opErr) || opErr.Op != "dial" {
		t.Fatalf("error is not a dial *net.OpError: %#v", r.err)
	}
	mu.Lock()
	defer mu.Unlock()
	if handlerCalls != 0 {
		t.Fatalf("handler called %d times for a refused dial", handlerCalls)
	}
}

// blackholeAddr names an address from TEST-NET-1, which is never routed, so a
// connect to it hangs until something gives up. Where a network refuses it
// outright instead, the tests that need a hanging connect are skipped.
const blackholeAddr = "192.0.2.1:81"

func TestDialTimeout(t *testing.T) {
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	results := make(chan dialResult, 1)
	start := time.Now()
	if err := client.Dial("tcp4", blackholeAddr, 200*time.Millisecond, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	r := <-results
	if !errors.Is(r.err, os.ErrDeadlineExceeded) {
		t.Skipf("connect to %s did not hang: %v", blackholeAddr, r.err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	var netErr net.Error
	if !errors.As(r.err, &netErr) || !netErr.Timeout() {
		t.Fatalf("timeout does not report Timeout(): %v", r.err)
	}
}

// Closing the engine settles a connect still in flight rather than leaving
// its dialer waiting forever.
func TestCloseFailsPendingDial(t *testing.T) {
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	client, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run() }()
	results := make(chan dialResult, 1)
	if err := client.Dial("tcp4", blackholeAddr, 0, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	// Give the loop time to start the connect.
	time.Sleep(100 * time.Millisecond)
	select {
	case r := <-results:
		t.Skipf("connect to %s did not hang: %v", blackholeAddr, r.err)
	default:
	}
	client.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if !errors.Is(r.err, net.ErrClosed) {
			t.Fatalf("pending dial reported %v, want net.ErrClosed", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the engine never settled the pending dial")
	}
}

func TestDialRejectsWhatCannotStart(t *testing.T) {
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	never := func(*Connection, error) { t.Error("done called for a dial that could not start") }
	for _, tc := range []struct{ network, addr string }{
		{"unixgram", "127.0.0.1:80"},
		{"unixgram", "localhost:80"},
		{"tcp", "127.0.0.1"},
		{"tcp", "127.0.0.1:99999"},
		{"tcp4", "[::1]:80"},
	} {
		if err := client.Dial(tc.network, tc.addr, 0, never); err == nil {
			t.Errorf("Dial(%q, %q) started", tc.network, tc.addr)
		}
	}

	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	closed, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed.Stop()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.Dial("tcp4", "127.0.0.1:"+strconv.Itoa(80), 0, never); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Dial on a closed engine: %v, want net.ErrClosed", err)
	}
}

// A dialed connection with a handler of its own gets every callback there, and
// the engine's handler hears nothing of it. An engine made by NewEngine has no
// listener at all.
func TestDialWithHandlerOnListenerlessEngine(t *testing.T) {
	_, addr := startEchoServer(t, DefaultConfig(), echoHandler())
	var engineCalls atomic.Int64
	client, err := NewEngine(DefaultConfig(), HandlerFuncs{
		Open: func(*Connection) { engineCalls.Add(1) },
		Data: func(*Connection, []byte) { engineCalls.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.LocalAddr(); err == nil {
		t.Fatal("LocalAddr on an engine without listeners reported an address")
	}
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run() }()
	defer func() {
		client.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = client.Close()
	}()

	opened := make(chan struct{})
	echoed := make(chan []byte, 1)
	closed := make(chan error, 1)
	own := HandlerFuncs{
		Open:  func(*Connection) { close(opened) },
		Data:  func(_ *Connection, b []byte) { echoed <- append([]byte(nil), b...) },
		Close: func(_ *Connection, err error) { closed <- err },
	}
	err = client.DialWithHandler("tcp", addr, 5*time.Second, own, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		_ = c.Send([]byte("own handler"))
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-echoed:
		if string(got) != "own handler" {
			t.Fatalf("echo = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no echo reached the dial's own handler")
	}
	<-opened
	if n := engineCalls.Load(); n != 0 {
		t.Fatalf("the engine's handler was called %d times for a connection dialed with its own", n)
	}
}

// closeWatcher reports the error each connection closed with.
type closeWatcher struct {
	HandlerFuncs
	closed chan error
}

func newCloseWatcher(data func(*Connection, []byte)) *closeWatcher {
	w := &closeWatcher{closed: make(chan error, 8)}
	w.HandlerFuncs = HandlerFuncs{
		Data:  data,
		Close: func(_ *Connection, err error) { w.closed <- err },
	}
	return w
}

func (w *closeWatcher) await(t *testing.T, within time.Duration) error {
	t.Helper()
	select {
	case err := <-w.closed:
		return err
	case <-time.After(within):
		t.Fatal("the connection did not close")
		return nil
	}
}

func (w *closeWatcher) quiet(t *testing.T, for_ time.Duration) {
	t.Helper()
	select {
	case err := <-w.closed:
		t.Fatalf("the connection closed with %v", err)
	case <-time.After(for_):
	}
}

func TestReadDeadlineClosesTheConnection(t *testing.T) {
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v, want os.ErrDeadlineExceeded", err)
	}
	// The peer sees the close too.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the peer's connection stayed open")
	}
}

// TestReadDeadlineMovedBeforeItFires checks the generation guard: a deadline
// pushed back must not be closed by the timer it outlived.
func TestReadDeadlineMovedBeforeItFires(t *testing.T) {
	var reads atomic.Int64
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		reads.Add(1)
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Keep pushing the deadline back for well past its length.
	for i := 0; i < 10; i++ {
		if _, err = conn.Write([]byte("tick")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(80 * time.Millisecond)
	}
	select {
	case err := <-watcher.closed:
		t.Fatalf("a deadline that kept moving closed the connection with %v", err)
	default:
	}
	if reads.Load() == 0 {
		t.Fatal("nothing was delivered")
	}
	// Stop moving it and it fires.
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestZeroDeadlineRemovesIt(t *testing.T) {
	watcher := newCloseWatcher(func(c *Connection, data []byte) {
		if string(data) == "arm" {
			_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
			return
		}
		_ = c.SetReadDeadline(time.Time{})
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("arm")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err = conn.Write([]byte("off")); err != nil {
		t.Fatal(err)
	}
	watcher.quiet(t, 500*time.Millisecond)
}

func TestWriteDeadlineIgnoresADrainedConnection(t *testing.T) {
	watcher := newCloseWatcher(func(c *Connection, data []byte) {
		_ = c.SetWriteDeadline(time.Now().Add(150 * time.Millisecond))
		_ = c.Send(data)
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("echo")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.ReadFull(conn, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	// The reply went out, so the write deadline has nothing to be about.
	watcher.quiet(t, 500*time.Millisecond)
}

// backlogOnceSettled reports how much output a connection is still holding,
// once the count has stopped moving. A reply is queued before the round that
// sent it hands it to the socket, so the count taken straight after a Send
// says what was offered rather than what the kernel would not take.
func backlogOnceSettled(t *testing.T, c *Connection) int64 {
	t.Helper()
	last := int64(-1)
	for stop := time.Now().Add(10 * time.Second); time.Now().Before(stop); {
		time.Sleep(200 * time.Millisecond)
		queued := c.pendingBytes.Load()
		if queued == last {
			return queued
		}
		last = queued
	}
	t.Fatal("the connection's backlog never settled")
	return 0
}

func TestWriteDeadlineClosesAStalledConnection(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 16<<20)
	replied := make(chan *Connection, 1)
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		_ = c.Send(payload)
		select {
		case replied <- c:
		default:
		}
	})
	config := DefaultConfig()
	// Keep the backlog off the watermarks, so the connection is closed by its
	// deadline rather than paused by its own backpressure.
	config.WriteBufferHighWatermark = 64 << 20
	config.MaxPendingBytes = 128 << 20
	_, addr := startEchoServer(t, config, watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		// Pin this side's receive buffer before anything is sent. Left alone,
		// a receive window that auto-tunes as far as the payload — which is
		// what Windows does — would take the whole reply into the kernel even
		// though nothing here ever reads it.
		if err = tcp.SetReadBuffer(16 << 10); err != nil {
			t.Fatal(err)
		}
	}
	// Never read, so nothing the server sends can drain.
	if _, err = conn.Write([]byte("go")); err != nil {
		t.Fatal(err)
	}
	var stalled *Connection
	select {
	case stalled = <-replied:
	case <-time.After(5 * time.Second):
		t.Fatal("the reply was never sent")
	}
	// A write deadline is only about output the connection still owes, so
	// there has to be some before one is armed. The first reply can leave
	// none: Windows accepts one send of any size while its own send backlog
	// is below the socket's send buffer, so the kernel takes all sixteen
	// megabytes however little of it the peer has room for. It does that once
	// — the backlog stands above the buffer afterwards, and a peer that never
	// reads does not bring it back down — so send again until the engine is
	// left holding something.
	queued := backlogOnceSettled(t, stalled)
	for attempt := 0; queued == 0; attempt++ {
		if attempt == 2 {
			t.Fatal("every reply went into the kernel; a write deadline would have nothing to be about")
		}
		if err = stalled.Send(payload); err != nil {
			t.Fatalf("Send: %v", err)
		}
		queued = backlogOnceSettled(t, stalled)
	}
	if err = stalled.SetWriteDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err = watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestReadTakesBytesOffTheSocket(t *testing.T) {
	got := make(chan string, 1)
	handler := HandlerFuncs{Data: func(c *Connection, data []byte) {
		// The read loop delivered the first byte; take the rest by hand.
		rest := make([]byte, 64)
		deadline := time.Now().Add(2 * time.Second)
		read := append([]byte(nil), data...)
		for len(read) < 5 && time.Now().Before(deadline) {
			n, err := c.Read(rest)
			if n > 0 {
				read = append(read, rest[:n]...)
				continue
			}
			if !errors.Is(err, ErrWouldBlock) {
				t.Errorf("Read error = %v", err)
				break
			}
			time.Sleep(time.Millisecond)
		}
		select {
		case got <- string(read):
		default:
		}
	}}
	_, addr := startEchoServer(t, DefaultConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, b := range []string{"h", "e", "l", "l", "o"} {
		if _, err = io.WriteString(conn, b); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case s := <-got:
		if s != "hello" {
			t.Fatalf("read %q, want %q", s, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never assembled the message")
	}
}

func TestReadAndWriteReportWhyTheConnectionClosed(t *testing.T) {
	reported := make(chan error, 1)
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		_ = c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		go func() {
			time.Sleep(300 * time.Millisecond)
			_, err := c.Read(make([]byte, 1))
			reported <- err
		}()
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v", err)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read after the deadline closed it = %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read never reported")
	}
}

func TestConnectionAddresses(t *testing.T) {
	addrs := make(chan [2]string, 1)
	handler := HandlerFuncs{Data: func(c *Connection, _ []byte) {
		local, remote := c.LocalAddr(), c.RemoteAddr()
		if local == nil || remote == nil {
			t.Errorf("LocalAddr=%v RemoteAddr=%v", local, remote)
			return
		}
		select {
		case addrs <- [2]string{local.String(), remote.String()}:
		default:
		}
	}}
	_, addr := startEchoServer(t, DefaultConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-addrs:
		if got[0] != addr {
			t.Fatalf("LocalAddr = %q, want the listener's %q", got[0], addr)
		}
		if got[1] != conn.LocalAddr().String() {
			t.Fatalf("RemoteAddr = %q, want the client's %q", got[1], conn.LocalAddr())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}
}

// TestRemoteAddrPort checks that an accepted connection reports the address
// its peer dialed from, the same as RemoteAddr's, on an IPv4 listener, an
// IPv6 one, and one listening on both, whose IPv4 peers arrive v4-mapped,
// with the engine's loop accepting and with pollers accepting.
func TestRemoteAddrPort(t *testing.T) {
	for _, tc := range []struct{ name, listen, dial string }{
		{"ipv4", "127.0.0.1:0", "127.0.0.1"},
		{"ipv6", "[::1]:0", "::1"},
		{"dualstack", ":0", "127.0.0.1"},
	} {
		for _, pollers := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pollers=%v", tc.name, pollers), func(t *testing.T) {
				got := make(chan [2]string, 1)
				config := DefaultConfig()
				config.Addr = tc.listen
				config.IOPollers, config.IOPollerCount = pollers, 2
				server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, _ []byte) {
					remote := ""
					if addr := c.RemoteAddr(); addr != nil {
						remote = addr.String()
					}
					select {
					case got <- [2]string{c.RemoteAddrPort().String(), remote}:
					default:
					}
				}})
				if err != nil {
					t.Skipf("listen %s: %v", tc.listen, err)
				}
				addr, err := server.LocalAddr()
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- server.Run() }()
				t.Cleanup(func() {
					server.Stop()
					<-done
					_ = server.Close()
				})
				_, port, _ := net.SplitHostPort(addr.String())
				conn, err := net.DialTimeout("tcp", net.JoinHostPort(tc.dial, port), 5*time.Second)
				if err != nil {
					t.Skipf("dial %s: %v", tc.dial, err)
				}
				defer conn.Close()
				if _, err = conn.Write([]byte("hi")); err != nil {
					t.Fatal(err)
				}
				select {
				case addrs := <-got:
					want := conn.LocalAddr().String()
					if addrs[0] != want || addrs[1] != want {
						t.Fatalf("RemoteAddrPort %q, RemoteAddr %q; the peer dialed from %q", addrs[0], addrs[1], want)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("no data reached the server")
				}
			})
		}
	}
}
