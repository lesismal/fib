//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A UDP address another socket holds fails New, which closes what it had
// already bound for the bindings before it.
func TestNewRefusesAUDPAddressInUse(t *testing.T) {
	held, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	e, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}},
		Binding{Network: "udp", Addr: held.LocalAddr().String(), Handler: HandlerFuncs{}})
	if err == nil {
		e.Close()
		t.Fatalf("bound %s, which a socket holds", held.LocalAddr())
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("New = %v, want EADDRINUSE", err)
	}
}

// A prefork child listens with SO_REUSEPORT whatever its Config says, so
// that its siblings can share its addresses.
func TestPreforkChildListensWithReusePort(t *testing.T) {
	preforkChild = true
	defer func() { preforkChild = false }()
	config := DefaultConfig()
	config.ReusePort = false
	e, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !e.bindConfig.ReusePort {
		t.Fatal("a prefork child's engine did not listen with ReusePort")
	}
}

// A binding added while one of the pollers has stopped fails, TCP or UDP,
// and leaves the engine serving its other bindings: the loops that would
// have waited on its sockets after the stopped one never take them. One
// whose UDP address another socket holds fails before any loop is asked.
func TestAddBindingWhileAPollerHasStopped(t *testing.T) {
	config := pollerConfig("add-stopped", 3)
	config.ReusePort = true
	server, addr := startEchoServer(t, config, echoHandler())
	held, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := server.AddBinding(Binding{Network: "udp", Addr: held.LocalAddr().String(), Handler: echoHandler()}); !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("AddBinding of a held UDP address = %v, want EADDRINUSE", err)
	}
	server.pollers[0].Stop()
	for _, network := range []string{"tcp", "udp"} {
		if i, err := server.AddBinding(Binding{Network: network, Addr: "127.0.0.1:0", Handler: echoHandler()}); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("AddBinding(%s) with a stopped poller = %d, %v; want net.ErrClosed", network, i, err)
		}
	}
	if _, err := server.BindingAddrs(1); err == nil {
		t.Error("the failed binding still has addresses")
	}
	// A connection the stopped poller would serve is left unserved, so only
	// the listener itself is checked: it still accepts.
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

// A loop whose listeners have stopped takes no more sockets, and closes the
// ones it is handed.
func TestAttachSocketsAfterListenersStopped(t *testing.T) {
	server := newOfflineServer(t)
	server.Stop()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = server.attachSockets([]placement{{loop: server, stream: streamListener{fd: fd}}})
	if !errors.Is(err, net.ErrClosed) {
		syscall.Close(fd)
		t.Fatalf("attachSockets after Stop = %v, want net.ErrClosed", err)
	}
	if fdOpenUnix(fd) {
		syscall.Close(fd)
		t.Fatal("the socket handed over was left open")
	}
}

// fdOpenUnix reports whether fd is an open descriptor.
func fdOpenUnix(fd int) bool {
	var st syscall.Stat_t
	return syscall.Fstat(fd, &st) != syscall.EBADF
}

// addrKey names an address by its family and port, and falls back to a
// fixed name for a socket that is not an IP one, or for no socket at all.
func TestAddrKey(t *testing.T) {
	for _, network := range []string{"tcp4", "tcp6"} {
		host := "127.0.0.1"
		if network == "tcp6" {
			host = "[::1]"
		}
		ln, err := net.Listen(network, host+":0")
		if err != nil {
			t.Logf("no %s: %v", network, err)
			continue
		}
		f, err := ln.(*net.TCPListener).File()
		ln.Close()
		if err != nil {
			t.Fatal(err)
		}
		key := addrKey(int(f.Fd()))
		f.Close()
		port := ln.Addr().(*net.TCPAddr).Port
		if strings.Contains(key, ".") || !strings.HasSuffix(key, "_"+strconv.Itoa(port)) {
			t.Errorf("addrKey of %s = %q", ln.Addr(), key)
		}
	}
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])
	if key := addrKey(fds[0]); key != "addr" {
		t.Errorf("addrKey of a Unix socket = %q", key)
	}
	if key := addrKey(-1); key != "addr" {
		t.Errorf("addrKey of no socket = %q", key)
	}
	// Either way the fallback is only logged.
	server := newOfflineServer(t)
	server.logBalanceFallback(errors.New("a selector that failed for another reason"))
	server.logBalanceFallback(errReusePortBalanceUnsupported)
	// A group of no socket, or of one outside a prefork child, is not
	// balanced at all.
	server.attachBalancer(0, nil)
	server.attachBalancer(0, fds[:1])
	if len(server.balancers) != 0 {
		t.Errorf("%d selectors attached to groups too small to balance", len(server.balancers))
	}
}

// A connection accepted for a loop that has since stopped is closed rather
// than served, and so is one whose socket the loop cannot wait on; either
// way it no longer counts against MaxConns.
func TestAcceptedConnectionsALoopCannotServe(t *testing.T) {
	server := newOfflineServer(t)
	server.acceptConnections(-1) // no listener there: nothing to accept

	open := server.accepted.open.Load()
	server.accepted.take()
	c := newOfflineConnection(server)
	c.handler = HandlerFuncs{Open: func(*Connection) { t.Error("a connection the loop cannot wait on opened") }}
	server.admit(c, 1<<20-1) // no such descriptor
	if c.FD() != -1 || server.accepted.open.Load() != open {
		t.Errorf("a failed registration left fd %d, %d open", c.FD(), server.accepted.open.Load())
	}

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[1])
	server.Stop()
	server.accepted.take()
	c = newOfflineConnection(server)
	c.fd.Store(int32(fds[0]))
	server.admitAccepted(c)
	if c.FD() != -1 || server.accepted.open.Load() != open {
		t.Errorf("admitting after Stop left fd %d, %d open", c.FD(), server.accepted.open.Load())
	}
	if n, err := syscall.Read(fds[1], make([]byte, 1)); n != 0 || err != nil {
		t.Errorf("the peer read %d, %v; want the close", n, err)
	}
}

// socketError reports the socket's pending error, a reset when it has none,
// and the failure to ask when there is no socket.
func TestSocketError(t *testing.T) {
	server := newOfflineServer(t)
	c, _ := offlineSocketConnection(t, server)
	if err := c.socketError(); err != syscall.ECONNRESET {
		t.Errorf("socketError with none pending = %v, want ECONNRESET", err)
	}
	if err := newOfflineConnection(server).socketError(); err == nil {
		t.Error("socketError without a socket = nil")
	}
	if server.noteEvent(c, 0) != nil {
		t.Error("an event of nothing scheduled the connection")
	}
}

// finishDial waits on an event that does not settle the connect, and fails
// the dial on one that ends it without saying why, with the error the
// backend reported, or when the socket cannot even be asked.
func TestFinishDialWithoutAPeer(t *testing.T) {
	server := newOfflineServer(t)
	dial := func(fd int) (*Connection, chan error) {
		done := make(chan error, 1)
		c := &Connection{engine: server, handler: HandlerFuncs{}, dialed: true,
			dialing: &dialRequest{network: "tcp", done: func(_ *Connection, err error) { done <- err }}}
		c.fd.Store(int32(fd))
		return c, done
	}
	unconnected := func() int {
		fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { syscall.Close(fd) })
		return fd
	}

	c, done := dial(unconnected())
	if server.finishDial(c, evOut, nil) != nil || len(done) != 0 {
		t.Fatal("a write edge without a peer settled the dial")
	}
	if server.finishDial(c, evOut|evErr, nil) != nil {
		t.Fatal("a failed dial needs a worker")
	}
	if err := <-done; !errors.Is(err, syscall.ECONNREFUSED) {
		t.Errorf("dial ended with no error = %v, want ECONNREFUSED", err)
	}

	c, done = dial(unconnected())
	server.finishDial(c, evOut|evHup, io.ErrUnexpectedEOF)
	if err := <-done; !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("dial with a reported error = %v", err)
	}

	c, done = dial(-1)
	server.finishDial(c, evOut, nil)
	if err := <-done; err == nil {
		t.Error("a dial without a socket succeeded")
	}
}

// A UDP connection with no socket cannot be told not to fragment, and one
// dialed without waiters for room has none to be woken by.
func TestUDPWithoutASocket(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.udp = &udpState{}
	if err := c.SetDontFragment(); err != syscall.EBADF {
		t.Errorf("SetDontFragment without a socket = %v, want EBADF", err)
	}
	if c.NotifyWritable(func() {}) {
		t.Error("NotifyWritable without waiters said it would call back")
	}
}

// The settings spell a zero size plainly, a placement they do not know by
// its number, and a step of nothing leaves a value as it is.
func TestSettingsSpellings(t *testing.T) {
	if got := byteSize(0); got != "0" {
		t.Errorf("byteSize(0) = %q", got)
	}
	if got := AcceptPlacement(7).String(); got != "AcceptPlacement(7)" {
		t.Errorf("AcceptPlacement(7) = %q", got)
	}
	if got := floorInt64(1234, 0); got != 1234 {
		t.Errorf("floorInt64(1234, 0) = %d", got)
	}
}
