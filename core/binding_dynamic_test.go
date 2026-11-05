package core

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dynamicConfigs are the engine shapes bindings are added and removed under:
// one loop; pollers that listen, with SO_REUSEPORT spreading TCP and UDP
// over sockets of their own where it can; the same without ReusePort, which
// leaves UDP to the first poller; and pollers beside a loop of the engine's
// own, which listens for them. Where there are no pollers, the last three
// are one loop too.
func dynamicConfigs() map[string]Config {
	single := DefaultConfig()
	single.IOPollers = false
	pollers := DefaultConfig()
	pollers.IOPollers, pollers.IOPollerCount = true, 3
	noReuse := pollers
	noReuse.ReusePort = false
	engineLoop := pollers
	engineLoop.EngineLoop = true
	return map[string]Config{"single-loop": single, "pollers": pollers, "pollers-no-reuseport": noReuse,
		"engine-loop": engineLoop}
}

// closeLog records the errors connections close with, by binding prefix.
type closeLog struct {
	mu     sync.Mutex
	errs   map[string][]error
	opened atomic.Int64
}

func newCloseLog() *closeLog { return &closeLog{errs: make(map[string][]error)} }

func (l *closeLog) add(prefix string, err error) {
	l.mu.Lock()
	l.errs[prefix] = append(l.errs[prefix], err)
	l.mu.Unlock()
}

func (l *closeLog) of(prefix string) []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.errs[prefix]...)
}

// waitCloses waits until n connections of prefix have closed.
func (l *closeLog) waitCloses(t *testing.T, prefix string, n int) []error {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		errs := l.of(prefix)
		if len(errs) >= n {
			return errs
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections of %q closed, want %d", len(errs), prefix, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// taggedEcho answers every message with prefix and the message, and logs
// its connections' closes to log.
func taggedEcho(prefix string, log *closeLog) HandlerFuncs {
	return HandlerFuncs{
		Open: func(*Connection) { log.opened.Add(1) },
		Data: func(c *Connection, b []byte) {
			if c.Send(append([]byte(prefix), b...)) != nil {
				c.Close()
			}
		},
		Close: func(_ *Connection, err error) { log.add(prefix, err) },
	}
}

// startDynamic creates an engine of config listening on bindings, and runs
// it until the test ends.
func startDynamic(t *testing.T, name string, config Config, bindings ...Binding) *Engine {
	t.Helper()
	config.Name = name
	server, err := New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	runDynamic(t, server)
	return server
}

func runDynamic(t *testing.T, server *Engine) {
	t.Helper()
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = server.Close()
	})
}

// dynamicSocketPath returns a path for a Unix socket in a directory of its
// own, short enough for macOS; see unixSocketPath.
func dynamicSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "fib")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// unixSupported says the platform's engine can listen on a Unix socket.
func unixSupported() bool {
	return runtime.GOOS != "plan9" && runtime.GOOS != "js" && runtime.GOOS != "wasip1"
}

// dialMessage dials addr, sends message and returns the connection and its
// reply.
func dialMessage(t *testing.T, addr net.Addr, message string) (net.Conn, string) {
	t.Helper()
	conn, err := net.DialTimeout(addr.Network(), addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := roundTrip(conn, message)
	if err != nil {
		conn.Close()
		t.Fatalf("exchanging %q with %s: %v", message, addr, err)
	}
	return conn, reply
}

// roundTrip sends message on conn and reads its reply.
func roundTrip(conn net.Conn, message string) (string, error) {
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(message)); err != nil {
		return "", err
	}
	buf := make([]byte, 256)
	got := ""
	for {
		n, err := conn.Read(buf)
		got += string(buf[:n])
		if err != nil {
			return got, err
		}
		if conn.LocalAddr().Network() == "udp" || strings.HasSuffix(got, message) {
			return got, nil
		}
	}
}

// expectAnswer checks that addr answers message with prefix.
func expectAnswer(t *testing.T, addr net.Addr, prefix, message string) {
	t.Helper()
	conn, got := dialMessage(t, addr, message)
	conn.Close()
	if got != prefix+message {
		t.Fatalf("%s %s answered %q, want %q", addr.Network(), addr, got, prefix+message)
	}
}

// expectRefused checks that nothing at addr takes a TCP or Unix connection
// and answers on it.
func expectRefused(t *testing.T, addr net.Addr) {
	t.Helper()
	conn, err := net.DialTimeout(addr.Network(), addr.String(), time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	// Some systems complete a connection to a socket that has just stopped
	// listening and reset it; what matters is that nothing serves it.
	if got, err := roundTrip(conn, "x"); err == nil {
		t.Fatalf("%s %s still answers: %q", addr.Network(), addr, got)
	}
}

func bindingAddrs(t *testing.T, server *Engine, i, want int) []net.Addr {
	t.Helper()
	addrs, err := server.BindingAddrs(i)
	if err != nil || len(addrs) != want {
		t.Fatalf("BindingAddrs(%d) = %v, %v, want %d addresses", i, addrs, err, want)
	}
	return addrs
}

// AddBinding has a running engine listen on TCP, Unix and UDP addresses
// more, each served by its binding's handler.
func TestAddBindingWhileRunning(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "add-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("a:", log)})
			expectAnswer(t, bindingAddrs(t, server, 0, 1)[0], "a:", "ping")

			added := []struct {
				b       Binding
				prefix  string
				addrs   int
				network string
			}{
				{Binding{Network: "tcp4", Addrs: []string{"127.0.0.1:0", "127.0.0.1:0"}, Handler: taggedEcho("b:", log)},
					"b:", 2, "tcp"},
				{Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: taggedEcho("u:", log)}, "u:", 1, "udp"},
			}
			if unixSupported() {
				added = append(added, struct {
					b       Binding
					prefix  string
					addrs   int
					network string
				}{Binding{Network: "unix", Addr: dynamicSocketPath(t), Handler: taggedEcho("x:", log)}, "x:", 1, "unix"})
			}
			total := 1
			for k, a := range added {
				i, err := server.AddBinding(a.b)
				if err != nil {
					t.Fatalf("AddBinding(%+v): %v", a.b, err)
				}
				if i != k+1 {
					t.Fatalf("AddBinding returned index %d, want %d", i, k+1)
				}
				total += a.addrs
				for _, addr := range bindingAddrs(t, server, i, a.addrs) {
					if addr.Network() != a.network {
						t.Fatalf("binding %d listens on %s, want %s", i, addr.Network(), a.network)
					}
					// Several rounds, so that with pollers the connections land
					// on more than one of them.
					for round := 0; round < 6; round++ {
						expectAnswer(t, addr, a.prefix, "ping")
					}
				}
			}
			if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != total {
				t.Fatalf("ListenAddrs = %v, %v, want %d addresses", addrs, err, total)
			}
			if addrs, err := server.LocalUDPAddrs(); err != nil || len(addrs) != 1 {
				t.Fatalf("LocalUDPAddrs = %v, %v, want 1", addrs, err)
			}
		})
	}
}

// A binding added before Run is served once Run starts, as one New was
// given is.
func TestAddBindingBeforeRun(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			config.Name = "add-before-run-" + name
			server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("d:", log)})
			if err != nil {
				t.Fatal(err)
			}
			tcp, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("t:", log)})
			if err != nil {
				t.Fatal(err)
			}
			udp, err := server.AddBinding(Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: taggedEcho("u:", log)})
			if err != nil {
				t.Fatal(err)
			}
			gone, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("g:", log)})
			if err != nil {
				t.Fatal(err)
			}
			goneAddr := bindingAddrs(t, server, gone, 1)[0]
			if err := server.RemoveBinding(gone); err != nil {
				t.Fatal(err)
			}
			runDynamic(t, server)
			expectAnswer(t, bindingAddrs(t, server, tcp, 1)[0], "t:", "ping")
			expectAnswer(t, bindingAddrs(t, server, udp, 1)[0], "u:", "ping")
			expectAnswer(t, bindingAddrs(t, server, 0, 1)[0], "d:", "ping")
			expectRefused(t, goneAddr)
		})
	}
}

// RemoveBinding closes a binding's listeners, which take no connection from
// then on, and leaves the connections they took open. Its index names no
// binding afterwards, and the engine's other bindings are left alone.
func TestRemoveBindingKeepsConnections(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "remove-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("a:", log)})
			b := Binding{Network: "tcp", Addrs: []string{"127.0.0.1:0", "127.0.0.1:0"}, Handler: taggedEcho("b:", log)}
			var path string
			if unixSupported() {
				path = dynamicSocketPath(t)
			}
			i, err := server.AddBinding(b)
			if err != nil {
				t.Fatal(err)
			}
			addrs := bindingAddrs(t, server, i, 2)
			var conns []net.Conn
			for _, addr := range addrs {
				for k := 0; k < 4; k++ {
					conn, got := dialMessage(t, addr, "hi")
					if got != "b:hi" {
						t.Fatalf("answered %q", got)
					}
					conns = append(conns, conn)
				}
			}
			unix := -1
			if path != "" {
				if unix, err = server.AddBinding(Binding{Network: "unix", Addr: path, Handler: taggedEcho("x:", log)}); err != nil {
					t.Fatal(err)
				}
				conn, got := dialMessage(t, bindingAddrs(t, server, unix, 1)[0], "hi")
				if got != "x:hi" {
					t.Fatalf("answered %q", got)
				}
				conns = append(conns, conn)
			}
			defer func() {
				for _, conn := range conns {
					conn.Close()
				}
			}()

			if err := server.RemoveBinding(i); err != nil {
				t.Fatalf("RemoveBinding(%d): %v", i, err)
			}
			if unix >= 0 {
				if err := server.RemoveBinding(unix); err != nil {
					t.Fatalf("RemoveBinding(%d): %v", unix, err)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("the Unix socket file is still there: %v", err)
				}
			}
			for _, addr := range addrs {
				expectRefused(t, addr)
			}
			for k, conn := range conns {
				if got, err := roundTrip(conn, "again"); err != nil || !strings.HasSuffix(got, ":again") {
					t.Fatalf("connection %d after RemoveBinding: %q, %v", k, got, err)
				}
			}
			if got := log.of("b:"); len(got) != 0 {
				t.Fatalf("RemoveBinding closed connections: %v", got)
			}
			if addrs, err := server.BindingAddrs(i); err == nil {
				t.Fatalf("BindingAddrs of a removed binding = %v", addrs)
			}
			if err := server.RemoveBinding(i); err == nil {
				t.Fatal("removing a binding twice did not fail")
			}
			for _, bad := range []int{-1, 99} {
				if err := server.RemoveBinding(bad); err == nil {
					t.Fatalf("RemoveBinding(%d) did not fail", bad)
				}
			}
			if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != 1 {
				t.Fatalf("ListenAddrs = %v, %v, want binding 0's alone", addrs, err)
			}
			expectAnswer(t, bindingAddrs(t, server, 0, 1)[0], "a:", "still")
			// Indices are not reused.
			next, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("n:", log)})
			if err != nil {
				t.Fatal(err)
			}
			if next <= i || next <= unix {
				t.Fatalf("AddBinding after a removal returned index %d", next)
			}
		})
	}
}

// A removed binding's address is free again: it can be bound anew, here by
// another binding of the same engine, without SO_REUSEPORT to share it.
func TestRemoveBindingFreesAddress(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			config.ReusePort = false
			server := startDynamic(t, "free-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("d:", log)})
			for _, network := range []string{"tcp4", "udp4"} {
				i, err := server.AddBinding(Binding{Network: network, Addr: "127.0.0.1:0", Handler: taggedEcho("1:", log)})
				if err != nil {
					t.Fatal(err)
				}
				addr := bindingAddrs(t, server, i, 1)[0]
				expectAnswer(t, addr, "1:", network)
				// Without ReusePort a TCP address is the engine's alone. A UDP
				// one is not: its sockets set SO_REUSEADDR, which on Linux
				// lets another that sets it too bind beside them.
				if network == "tcp4" {
					if _, err := server.AddBinding(Binding{Network: network, Addr: addr.String(), Handler: taggedEcho("1:", log)}); err == nil {
						t.Fatalf("a second %s binding on %s was let in", network, addr)
					}
				}
				if err := server.RemoveBinding(i); err != nil {
					t.Fatal(err)
				}
				j, err := server.AddBinding(Binding{Network: network, Addr: addr.String(), Handler: taggedEcho("2:", log)})
				if err != nil {
					t.Fatalf("binding %s again after RemoveBinding: %v", addr, err)
				}
				// A failed AddBinding takes no index.
				if j != i+1 {
					t.Fatalf("AddBinding returned %d, want %d", j, i+1)
				}
				expectAnswer(t, bindingAddrs(t, server, j, 1)[0], "2:", network)
			}
		})
	}
}

// A binding that cannot listen on every address listens on none.
func TestAddBindingFailureOpensNothing(t *testing.T) {
	busy, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	free, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	freeAddr := free.Addr().String()
	free.Close()
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			config.ReusePort = false
			server := startDynamic(t, "add-fail-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("d:", newCloseLog())})
			if _, err := server.AddBinding(Binding{Network: "tcp4", Addrs: []string{freeAddr, busy.Addr().String()}, Handler: HandlerFuncs{}}); err == nil {
				t.Fatal("AddBinding on a busy address did not fail")
			}
			if _, err := server.AddBinding(Binding{Network: "bogus", Handler: HandlerFuncs{}}); err == nil {
				t.Fatal("AddBinding on an unknown network did not fail")
			}
			if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != 1 {
				t.Fatalf("ListenAddrs = %v, %v, want binding 0's alone", addrs, err)
			}
			// The address the failed binding opened first is free again.
			l, err := net.Listen("tcp4", freeAddr)
			if err != nil {
				t.Fatalf("the failed binding kept %s: %v", freeAddr, err)
			}
			l.Close()
			if i, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}}); err != nil || i != 1 {
				t.Fatalf("AddBinding after failures = %d, %v, want 1", i, err)
			}
		})
	}
}

// Removing a UDP binding closes its socket, and with it the connections of
// the peers it served, which hear net.ErrClosed.
func TestRemoveUDPBindingClosesPeers(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "remove-udp-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("d:", log)})
			i, err := server.AddBinding(Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: taggedEcho("u:", log)})
			if err != nil {
				t.Fatal(err)
			}
			addr := bindingAddrs(t, server, i, 1)[0]
			const peers = 4
			// Each peer keeps its socket until the binding is gone: a closed
			// one's port may be handed to the next dial, which the server
			// would take for the same peer.
			for k := 0; k < peers; k++ {
				conn, got := dialMessage(t, addr, "hi")
				defer conn.Close()
				if got != "u:hi" {
					t.Fatalf("answered %q", got)
				}
			}
			if opened := log.opened.Load(); opened != peers {
				t.Fatalf("%d peers opened, want %d", opened, peers)
			}
			if err := server.RemoveBinding(i); err != nil {
				t.Fatal(err)
			}
			for _, err := range log.waitCloses(t, "u:", peers) {
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("a peer closed with %v, want net.ErrClosed", err)
				}
			}
			if addrs, err := server.LocalUDPAddrs(); err != nil || len(addrs) != 0 {
				t.Fatalf("LocalUDPAddrs = %v, %v, want none", addrs, err)
			}
		})
	}
}

// drainHandler is an echo that, told its connection is to finish, closes it
// unless hold says to keep it, and records what ShuttingDown said.
type drainHandler struct {
	HandlerFuncs
	hold     atomic.Bool
	told     atomic.Int64
	shutting atomic.Int64
}

func (h *drainHandler) OnShutdown(c *Connection) {
	h.told.Add(1)
	if c.ShuttingDown() {
		h.shutting.Add(1)
	}
	if !h.hold.Load() {
		c.Close()
	}
}

// ShutdownBinding closes a binding's listeners and tells its connections to
// finish, and returns once they have; the engine's other connections are
// neither told nor shut down.
func TestShutdownBindingDrains(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			other := &drainHandler{HandlerFuncs: taggedEcho("a:", log)}
			server := startDynamic(t, "shutdown-binding-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: other})
			drained := &drainHandler{HandlerFuncs: taggedEcho("b:", log)}
			i, err := server.AddBinding(Binding{Addrs: []string{"127.0.0.1:0", "127.0.0.1:0"}, Handler: drained})
			if err != nil {
				t.Fatal(err)
			}
			addrs := bindingAddrs(t, server, i, 2)
			var conns []net.Conn
			for _, addr := range addrs {
				for k := 0; k < 3; k++ {
					conn, _ := dialMessage(t, addr, "hi")
					conns = append(conns, conn)
				}
			}
			keep, _ := dialMessage(t, bindingAddrs(t, server, 0, 1)[0], "hi")
			defer keep.Close()
			defer func() {
				for _, conn := range conns {
					conn.Close()
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.ShutdownBinding(ctx, i); err != nil {
				t.Fatalf("ShutdownBinding: %v", err)
			}
			if got := len(log.of("b:")); got != len(conns) {
				t.Fatalf("%d of the binding's %d connections closed", got, len(conns))
			}
			if told, shutting := drained.told.Load(), drained.shutting.Load(); told < int64(len(conns)) || shutting != told {
				t.Fatalf("OnShutdown ran %d times, ShuttingDown true in %d", told, shutting)
			}
			if other.told.Load() != 0 || len(log.of("a:")) != 0 {
				t.Fatal("ShutdownBinding reached another binding's connection")
			}
			if server.ShuttingDown() {
				t.Fatal("the engine reports itself shutting down")
			}
			for _, addr := range addrs {
				expectRefused(t, addr)
			}
			if got, err := roundTrip(keep, "on"); err != nil || got != "a:on" {
				t.Fatalf("the other binding's connection: %q, %v", got, err)
			}
			if _, err := server.BindingAddrs(i); err == nil {
				t.Fatal("the binding outlived ShutdownBinding")
			}
			if err := server.ShutdownBinding(ctx, i); err == nil {
				t.Fatal("ShutdownBinding of a removed binding did not fail")
			}
		})
	}
}

// ShutdownBinding returns only once its connections' OnClose has run, a
// slow one included: the last round of a close takes it on before it calls
// OnClose, and the connection is not finished until OnClose returns.
func TestShutdownBindingWaitsForOnClose(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "shutdown-onclose-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("a:", log)})
			slow := taggedEcho("s:", log)
			logClose := slow.Close
			slow.Close = func(c *Connection, err error) {
				time.Sleep(200 * time.Millisecond)
				logClose(c, err)
			}
			i, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: &drainHandler{HandlerFuncs: slow}})
			if err != nil {
				t.Fatal(err)
			}
			conn, _ := dialMessage(t, bindingAddrs(t, server, i, 1)[0], "hi")
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.ShutdownBinding(ctx, i); err != nil {
				t.Fatalf("ShutdownBinding: %v", err)
			}
			if got := len(log.of("s:")); got != 1 {
				t.Fatalf("ShutdownBinding returned with %d of 1 OnClose run", got)
			}
		})
	}
}

// Connections that do not finish by the time ShutdownBinding's ctx is done
// are closed, with net.ErrClosed, and ShutdownBinding reports ctx's error.
func TestShutdownBindingDeadline(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "shutdown-deadline-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("d:", log)})
			held := &drainHandler{HandlerFuncs: taggedEcho("h:", log)}
			held.hold.Store(true)
			i, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: held})
			if err != nil {
				t.Fatal(err)
			}
			conn, _ := dialMessage(t, bindingAddrs(t, server, i, 1)[0], "hi")
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := server.ShutdownBinding(ctx, i); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("ShutdownBinding = %v, want context.DeadlineExceeded", err)
			}
			for _, err := range log.waitCloses(t, "h:", 1) {
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("closed with %v, want net.ErrClosed", err)
				}
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if n, err := conn.Read(make([]byte, 16)); err == nil {
				t.Fatalf("the connection still reads, %d bytes", n)
			}
		})
	}
}

// While ShutdownBinding drains a UDP binding, the peers it knows are still
// served and a new one is not; once they have all finished, the socket
// closes.
func TestShutdownUDPBinding(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "shutdown-udp-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("d:", log)})
			drained := &drainHandler{HandlerFuncs: taggedEcho("u:", log)}
			drained.hold.Store(true)
			i, err := server.AddBinding(Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: drained})
			if err != nil {
				t.Fatal(err)
			}
			addr := bindingAddrs(t, server, i, 1)[0]
			known, got := dialMessage(t, addr, "hi")
			defer known.Close()
			if got != "u:hi" {
				t.Fatalf("answered %q", got)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- server.ShutdownBinding(ctx, i) }()
			for deadline := time.Now().Add(5 * time.Second); drained.told.Load() == 0; time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the peer was never told to finish")
				}
			}
			if got, err := roundTrip(known, "more"); err != nil || got != "u:more" {
				t.Fatalf("the known peer while draining: %q, %v", got, err)
			}
			stranger, err := net.Dial("udp", addr.String())
			if err != nil {
				t.Fatal(err)
			}
			defer stranger.Close()
			_ = stranger.SetDeadline(time.Now().Add(200 * time.Millisecond))
			if _, err := stranger.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			if n, err := stranger.Read(make([]byte, 64)); err == nil {
				t.Fatalf("a new peer was served while draining, %d bytes", n)
			}
			drained.hold.Store(false)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("ShutdownBinding: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("ShutdownBinding did not return")
			}
			if addrs, err := server.LocalUDPAddrs(); err != nil || len(addrs) != 0 {
				t.Fatalf("LocalUDPAddrs = %v, %v, want none", addrs, err)
			}
			if got := log.of("u:"); len(got) != 1 {
				t.Fatalf("closes = %v, want the known peer's", got)
			}
		})
	}
}

// Bindings cannot be added once the engine has stopped, and the ones it has
// are closed with it.
func TestAddBindingAfterStop(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "add-after-stop-" + name
			server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
			if err != nil {
				t.Fatal(err)
			}
			i, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
			if err != nil {
				t.Fatal(err)
			}
			addr := bindingAddrs(t, server, i, 1)[0]
			runDone := make(chan error, 1)
			go func() { runDone <- server.Run() }()
			server.Stop()
			if err := <-runDone; err != nil {
				t.Fatal(err)
			}
			if _, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}}); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("AddBinding after Stop = %v, want net.ErrClosed", err)
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			l, err := net.Listen("tcp", addr.String())
			if err != nil {
				t.Fatalf("Close left %s bound: %v", addr, err)
			}
			l.Close()
		})
	}
}

// Bindings come and go while clients connect to them and to a binding that
// stays, which goes on serving every one of its connections.
func TestBindingsChurn(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			server := startDynamic(t, "churn-"+name, config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("s:", log)})
			stay := bindingAddrs(t, server, 0, 1)[0]
			stop := make(chan struct{})
			var wg sync.WaitGroup
			var failures atomic.Int64
			for k := 0; k < 4; k++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					// A connection is kept for a while rather than dialed for
					// every exchange, which would run the client out of ports.
					var conn net.Conn
					defer func() {
						if conn != nil {
							conn.Close()
						}
					}()
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						if n%50 == 0 {
							if conn != nil {
								conn.Close()
							}
							var err error
							if conn, err = net.DialTimeout("tcp", stay.String(), 5*time.Second); err != nil {
								failures.Add(1)
								return
							}
						}
						if got, err := roundTrip(conn, "x"); err != nil || got != "s:x" {
							failures.Add(1)
							return
						}
					}
				}()
			}
			rounds := 30
			if testing.Short() {
				rounds = 10
			}
			for round := 0; round < rounds; round++ {
				network := "tcp"
				if round%3 == 2 {
					network = "udp"
				}
				i, err := server.AddBinding(Binding{Network: network, Addr: "127.0.0.1:0", Handler: taggedEcho("c:", log)})
				if err != nil {
					t.Fatal(err)
				}
				addr := bindingAddrs(t, server, i, 1)[0]
				conn, got := dialMessage(t, addr, "y")
				if got != "c:y" {
					t.Fatalf("round %d answered %q", round, got)
				}
				if round%2 == 0 {
					err = server.RemoveBinding(i)
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					err = server.ShutdownBinding(ctx, i)
					cancel()
					if errors.Is(err, context.DeadlineExceeded) {
						err = nil
					}
				}
				conn.Close()
				if err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
			}
			close(stop)
			wg.Wait()
			if n := failures.Load(); n != 0 {
				t.Fatalf("%d exchanges with the binding that stayed failed", n)
			}
			if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != 1 {
				t.Fatalf("ListenAddrs = %v, %v, want binding 0's alone", addrs, err)
			}
		})
	}
}

// New listens on the bindings it is given, each served by its own handler,
// and indexed by its place among them.
func TestNewServesBindings(t *testing.T) {
	for name, config := range dynamicConfigs() {
		t.Run(name, func(t *testing.T) {
			log := newCloseLog()
			config.Name = "new-" + name
			server, err := New(config,
				Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("a:", log)},
				Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: taggedEcho("u:", log)},
				Binding{Network: "tcp4", Addrs: []string{"127.0.0.1:0", "127.0.0.1:0"}, Handler: taggedEcho("b:", log)},
			)
			if err != nil {
				t.Fatal(err)
			}
			runDynamic(t, server)
			expectAnswer(t, bindingAddrs(t, server, 0, 1)[0], "a:", "ping")
			expectAnswer(t, bindingAddrs(t, server, 1, 1)[0], "u:", "ping")
			for _, addr := range bindingAddrs(t, server, 2, 2) {
				expectAnswer(t, addr, "b:", "ping")
			}
			// An engine has no handler to give a binding that names none.
			if _, err := server.AddBinding(Binding{Addr: "127.0.0.1:0"}); err == nil {
				t.Fatal("AddBinding without a Handler was let in")
			}
			i, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("c:", log)})
			if err != nil || i != 3 {
				t.Fatalf("AddBinding = %d, %v, want 3", i, err)
			}
			expectAnswer(t, bindingAddrs(t, server, i, 1)[0], "c:", "ping")
		})
	}
}

// Every binding New is given names its handler, and one that does not fails
// New before anything listens.
func TestNewRefusesBindingWithoutHandler(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	free := probe.Addr().String()
	probe.Close()
	config := DefaultConfig()
	config.Name = "new-no-handler"
	if _, err := New(config, Binding{Addr: free, Handler: HandlerFuncs{}}, Binding{Addr: "127.0.0.1:0"}); err == nil {
		t.Fatal("New took a binding without a Handler")
	}
	l, err := net.Listen("tcp4", free)
	if err != nil {
		t.Fatalf("the refused New kept %s: %v", free, err)
	}
	l.Close()
}

// Without bindings New makes a client engine, which listens nowhere.
func TestNewWithoutBindings(t *testing.T) {
	config := DefaultConfig()
	config.Name = "new-client"
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	runDynamic(t, client)
	if addrs, err := client.ListenAddrs(); err != nil || len(addrs) != 0 {
		t.Fatalf("a client engine listens on %v, %v", addrs, err)
	}

	log := newCloseLog()
	config.Name = "new-server"
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: taggedEcho("k:", log)})
	if err != nil {
		t.Fatal(err)
	}
	runDynamic(t, server)
	addr := bindingAddrs(t, server, 0, 1)[0]
	expectAnswer(t, addr, "k:", "ping")

	// The client dials with a handler of its own.
	replies := make(chan string, 1)
	done := make(chan error, 1)
	err = client.DialWithHandler("tcp", addr.String(), 5*time.Second, HandlerFuncs{
		Data: func(_ *Connection, b []byte) { replies <- string(b) },
	}, func(c *Connection, err error) {
		if err == nil {
			err = c.Send([]byte("dial"))
		}
		done <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-replies:
		if got != "k:dial" {
			t.Fatalf("the dialed connection read %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reply on the dialed connection")
	}
}
