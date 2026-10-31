//go:build linux

package fib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// With EngineLoop the engine's own loop waits on every listener, TCP, Unix
// and UDP alike, with ReusePort or without it, and hands each connection it
// accepts to a poller, which listens on nothing.
func TestEngineLoopListensForPollers(t *testing.T) {
	for _, reusePort := range []bool{false, true} {
		t.Run(fmt.Sprintf("ReusePort=%v", reusePort), func(t *testing.T) {
			config := pollerConfig("engine-loop", 3)
			config.EngineLoop = true
			config.ReusePort = reusePort
			var mu sync.Mutex
			served := map[*Engine]bool{}
			tcp := HandlerFuncs{
				Open: func(c *Connection) {
					mu.Lock()
					served[c.engine] = true
					mu.Unlock()
				},
				Data: prefixEcho("t:").(HandlerFuncs).Data,
			}
			server, err := New(config,
				Binding{Addr: "127.0.0.1:0", Handler: tcp},
				Binding{Network: "unix", Addr: unixSocketPath(t), Handler: prefixEcho("x:")},
				Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: prefixEcho("u:")},
			)
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, server)
			if server.noLoop || server.pollersListen || server.udpSpread || server.udpHanded {
				t.Fatalf("noLoop=%v pollersListen=%v udpSpread=%v udpHanded=%v",
					server.noLoop, server.pollersListen, server.udpSpread, server.udpHanded)
			}
			for i, l := range server.listeners {
				if l.spread || l.shared || !acceptsConnections(t, l.fd) {
					t.Fatalf("the engine's listener %d is %+v", i, l)
				}
			}
			if len(server.udpListeners) != 1 {
				t.Fatalf("the engine reads %d UDP sockets, want 1", len(server.udpListeners))
			}
			for i, p := range server.pollers {
				if len(p.listeners) != 0 || len(p.udpListeners) != 0 {
					t.Fatalf("poller %d has %d listeners and %d UDP sockets", i, len(p.listeners), len(p.udpListeners))
				}
			}
			for i, prefix := range []string{"t:", "x:", "u:"} {
				addrs, err := server.BindingAddrs(i)
				if err != nil || len(addrs) != 1 {
					t.Fatalf("BindingAddrs(%d) = %v, %v", i, addrs, err)
				}
				for round := 0; round < 6; round++ {
					if got := exchange(t, addrs[0], "ping"); got != prefix+"ping" {
						t.Fatalf("binding %d answered %q", i, got)
					}
				}
			}
			mu.Lock()
			defer mu.Unlock()
			for e := range served {
				if e.parent != server {
					t.Fatalf("a TCP connection is served by %p, want one of the engine's pollers", e)
				}
			}
		})
	}
}

// Without EngineLoop the engine has no loop and no backend of its own, and
// every poller waits on its one Unix socket and serves the connections it
// accepts there itself.
func TestNoEngineLoopSharesUnixListener(t *testing.T) {
	const pollers, conns = 3, 60
	config := pollerConfig("no-engine-loop-unix", pollers)
	config.Network = "unix"
	var mu sync.Mutex
	served := map[*Engine]int{}
	server, err := New(config, Binding{Addr: unixSocketPath(t), Handler: HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			served[c.engine]++
			mu.Unlock()
		},
		Data: prefixEcho("x:").(HandlerFuncs).Data,
	}})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	if !server.noLoop || server.epollFD != 0 || server.wakeFD != 0 {
		t.Fatalf("noLoop=%v epollFD=%d wakeFD=%d, want no backend", server.noLoop, server.epollFD, server.wakeFD)
	}
	if len(server.listeners) != 1 || !server.listeners[0].shared || !acceptsConnections(t, server.listeners[0].fd) {
		t.Fatalf("the engine's listeners are %+v", server.listeners)
	}
	for i, p := range server.pollers {
		if len(p.listeners) != 1 || !p.listeners[0].shared || p.listeners[0].fd != server.listeners[0].fd {
			t.Fatalf("poller %d listens on %+v, want the engine's socket", i, p.listeners)
		}
	}
	addrs, err := server.ListenAddrs()
	if err != nil || len(addrs) != 1 {
		t.Fatalf("ListenAddrs() = %v, %v", addrs, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := exchange(t, addrs[0], "ping"); got != "x:ping" {
				t.Errorf("answered %q", got)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	total := 0
	for e, n := range served {
		if e.parent != server {
			t.Fatalf("a connection is served by %p, want one of the engine's pollers", e)
		}
		total += n
	}
	if total != conns {
		t.Fatalf("%d connections opened, want %d", total, conns)
	}
}

// Without a loop of its own the engine still shuts down, its stop reaching
// every poller, and a dial that fails before it has a socket is reported
// from a poller.
func TestNoEngineLoopShutdownAndDialFailure(t *testing.T) {
	config := pollerConfig("no-engine-loop-shutdown", 2)
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("")})
	if err != nil {
		t.Fatal(err)
	}
	if !server.noLoop {
		t.Fatal("the engine has a loop of its own")
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer server.Close()
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	if got := exchange(t, addr, "ping"); got != "ping" {
		t.Fatalf("answered %q", got)
	}

	dialed := make(chan error, 1)
	if err := server.Dial("tcp", "fib-no-such-host.invalid:80", 5*time.Second, func(_ *Connection, err error) {
		dialed <- err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-dialed:
		if err == nil {
			t.Fatal("a dial to a name that does not resolve succeeded")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a failed dial was never reported")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
	if _, err := net.DialTimeout("tcp4", addr.String(), time.Second); err == nil {
		t.Fatal("the engine still accepts after Shutdown")
	}
}

// Whichever poller accepts a connection on a socket they share, the poller
// its descriptor picks serves it.
func TestNoEngineLoopPlacesAcceptedByDescriptor(t *testing.T) {
	const pollers, conns = 3, 30
	config := pollerConfig("no-engine-loop-fd", pollers)
	config.Network = "unix"
	opened := make(chan *Connection, conns)
	server, err := New(config, Binding{Addr: unixSocketPath(t), Handler: HandlerFuncs{
		Open: func(c *Connection) { opened <- c },
		Data: prefixEcho("").(HandlerFuncs).Data,
	}})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	addrs, err := server.ListenAddrs()
	if err != nil || len(addrs) != 1 {
		t.Fatalf("ListenAddrs() = %v, %v", addrs, err)
	}
	for i := 0; i < conns; i++ {
		if got := exchange(t, addrs[0], "ping"); got != "ping" {
			t.Fatalf("answered %q", got)
		}
		c := <-opened
		if fd := c.FD(); fd >= 0 && c.engine != server.pollers[fd%pollers] {
			t.Fatalf("descriptor %d is served by %p, want poller %p", fd, c.engine, server.pollers[fd%pollers])
		}
	}
}

// Under AcceptKernelHash a poller that accepts on a TCP socket of its own
// serves the connection itself rather than handing it to the poller its
// descriptor picks.
func TestPollersKeepOwnAccepts(t *testing.T) {
	const pollers, conns = 3, 30
	config := pollerConfig("pollers-keep-own", pollers)
	config.AcceptPlacement = AcceptKernelHash
	accepted := make(chan *Connection, conns)
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
		Open: func(c *Connection) { accepted <- c },
		Data: prefixEcho("").(HandlerFuncs).Data,
	}})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	moved := 0
	for i := 0; i < conns; i++ {
		// Each connection is held open, so that each has a descriptor of
		// its own: closed, the next takes the same one, and should one
		// poller accept them all, as it may while the others are still to
		// listen, every one sits where that descriptor picks a third of the
		// time, which Linux CI met.
		conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if got, err := io.ReadAll(io.LimitReader(conn, 4)); err != nil || string(got) != "ping" {
			t.Fatalf("answered %q, %v", got, err)
		}
		c := <-accepted
		if c.engine.parent != server {
			t.Fatalf("a connection is served by %p, want one of the engine's pollers", c.engine)
		}
		if fd := c.FD(); fd >= 0 && c.engine != server.pollers[fd%pollers] {
			moved++
		}
	}
	// Placed by descriptor every connection would sit on pollers[fd%3]; kept
	// by its acceptor, some sit elsewhere, the kernel's hash being blind to
	// descriptors.
	if moved == 0 {
		t.Fatalf("all %d connections sit on the poller their descriptor picks", conns)
	}
}

// Under AcceptFDModulo, the default, a poller hands every connection it
// accepts on a socket of its own to the poller its descriptor picks.
func TestAcceptFDModuloOnOwnSockets(t *testing.T) {
	const pollers, conns = 3, 30
	config := pollerConfig("accept-fd-modulo", pollers)
	config.AcceptPlacement = AcceptFDModulo
	accepted := make(chan *Connection, conns)
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
		Open: func(c *Connection) { accepted <- c },
		Data: prefixEcho("").(HandlerFuncs).Data,
	}})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	if !server.listeners[0].spread {
		t.Fatal("the pollers do not listen on sockets of their own")
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < conns; i++ {
		if got := exchange(t, addr, "ping"); got != "ping" {
			t.Fatalf("answered %q", got)
		}
		c := <-accepted
		if fd := c.FD(); fd >= 0 && c.engine != server.pollers[fd%pollers] {
			t.Fatalf("descriptor %d is served by %p, want poller %p", fd, c.engine, server.pollers[fd%pollers])
		}
	}
}

func TestAcceptPlacementInvalid(t *testing.T) {
	config := pollerConfig("accept-invalid", 2)
	config.AcceptPlacement = AcceptKernelHash + 1
	if server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}}); err == nil {
		server.Close()
		t.Fatal("an unknown AcceptPlacement was accepted")
	}
}

// lockedBuffer is a bytes.Buffer slog may write to from the engine's
// goroutine while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The engine logs which placement each listener takes when it starts, and
// warns, LogStatus or not, when AcceptPlacement is set where it applies to
// no listener.
func TestAcceptPlacementLogged(t *testing.T) {
	for _, tc := range []struct {
		name       string
		engineLoop bool
		placement  AcceptPlacement
		logStatus  bool
		want       []string
		warn       bool
	}{
		{"default", false, AcceptFDModulo, true, []string{"=pollers,fd-modulo", "=pollers-shared-socket,fd-modulo"}, false},
		{"kernel-hash", false, AcceptKernelHash, true, []string{"=pollers,kernel-hash"}, false},
		{"engine-loop", true, AcceptFDModulo, true, []string{"=engine-loop,fd-modulo"}, false},
		{"engine-loop-warns", true, AcceptKernelHash, false, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out lockedBuffer
			defer slog.SetDefault(slog.Default())
			slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
			config := pollerConfig("accept-log-"+tc.name, 2)
			config.EngineLoop = tc.engineLoop
			config.AcceptPlacement = tc.placement
			config.LogStatus = tc.logStatus
			server, err := New(config,
				Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("")},
				Binding{Network: "unix", Addr: unixSocketPath(t), Handler: prefixEcho("")},
			)
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, server)
			deadline := time.Now().Add(5 * time.Second)
			for {
				got := out.String()
				done := !tc.logStatus || strings.Contains(got, "engine started")
				if tc.warn {
					done = strings.Contains(got, "applies to no listener")
				}
				if done {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("no startup line in %q", got)
				}
				time.Sleep(10 * time.Millisecond)
			}
			got := out.String()
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("log %q lacks %q", got, w)
				}
			}
			if warned := strings.Contains(got, "level=WARN"); warned != tc.warn {
				t.Errorf("warned=%v, want %v: %q", warned, tc.warn, got)
			}
		})
	}
}
