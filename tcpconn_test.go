//go:build linux || darwin

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// acceptOne starts a server on network and returns the connection it accepts
// from a plain net.Conn dialed to it, along with that client.
func acceptOne(t *testing.T, network string, handler HandlerFuncs) (*Connection, net.Conn) {
	t.Helper()
	opened := make(chan *Connection, 1)
	open := handler.Open
	handler.Open = func(c *Connection) {
		if open != nil {
			open(c)
		}
		opened <- c
	}
	config := DefaultConfig()
	config.Network = network
	config.Addr = "127.0.0.1:0"
	if network == "unix" {
		config.Addr = filepath.Join(t.TempDir(), "s")
	}
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.ListenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		server.Close()
	})
	client, err := net.Dial(network, addrs[0].String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	select {
	case c := <-opened:
		return c, client
	case <-time.After(5 * time.Second):
		t.Fatal("no connection opened")
		return nil, nil
	}
}

func sockoptInt(t *testing.T, c *Connection, level, opt int) int {
	t.Helper()
	v, err := syscall.GetsockoptInt(c.FD(), level, opt)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// The TCP options land on the socket, whichever way they point.
func TestTCPOptions(t *testing.T) {
	c, _ := acceptOne(t, "tcp", HandlerFuncs{})
	checkProtocol(t, c, ProtocolTCP)
	checkDialed(t, c, false)
	if err := c.SetNoDelay(false); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, syscall.TCP_NODELAY); v != 0 {
		t.Fatalf("TCP_NODELAY = %d after SetNoDelay(false)", v)
	}
	if err := c.SetNoDelay(true); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, syscall.TCP_NODELAY); v == 0 {
		t.Fatal("TCP_NODELAY off after SetNoDelay(true)")
	}

	if err := c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: 30 * time.Second,
		Interval: 4500 * time.Millisecond, Count: 4}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name       string
		level, opt int
		value      int
	}{
		{"SO_KEEPALIVE", syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1},
		{"idle", syscall.IPPROTO_TCP, tcpKeepIdle, 30},
		{"interval", syscall.IPPROTO_TCP, tcpKeepInterval, 5},
		{"count", syscall.IPPROTO_TCP, tcpKeepCount, 4},
	} {
		if v := sockoptInt(t, c, want.level, want.opt); (v != 0) != (want.value != 0) || want.value > 1 && v != want.value {
			t.Errorf("%s = %d, want %d", want.name, v, want.value)
		}
	}
	// A negative time leaves the setting alone, and zero takes the default.
	if err := c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: -1, Interval: 0, Count: -1}); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepIdle); v != 30 {
		t.Errorf("idle = %d after a negative Idle, want 30", v)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepInterval); v != 15 {
		t.Errorf("interval = %d after a zero Interval, want 15", v)
	}
	if err := c.SetKeepAlivePeriod(time.Minute); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepIdle); v != 60 {
		t.Errorf("idle = %d after SetKeepAlivePeriod(time.Minute)", v)
	}
	if err := c.SetKeepAlive(false); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE); v != 0 {
		t.Error("SO_KEEPALIVE on after SetKeepAlive(false)")
	}

	for _, sec := range []int{-1, 0, 5} {
		if err := c.SetLinger(sec); err != nil {
			t.Fatalf("SetLinger(%d): %v", sec, err)
		}
	}
	if err := c.SetReadBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_RCVBUF); v < 64<<10 {
		t.Errorf("SO_RCVBUF = %d", v)
	}
	if err := c.SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_SNDBUF); v < 64<<10 {
		t.Errorf("SO_SNDBUF = %d", v)
	}
	if mptcp, err := c.MultipathTCP(); err != nil || mptcp {
		t.Errorf("MultipathTCP() = %v, %v", mptcp, err)
	}
}

// File is a duplicate: closing it leaves the connection working.
func TestTCPFileAndSyscallConn(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	f, err := c.File()
	if err != nil {
		t.Fatal(err)
	}
	if int(f.Fd()) == c.FD() {
		t.Fatal("File returned the connection's own descriptor")
	}
	f.Close()

	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd uintptr
	if err := raw.Control(func(s uintptr) { fd = s }); err != nil || int(fd) != c.FD() {
		t.Fatalf("Control saw %d, %v; want %d", fd, err, c.FD())
	}
	if err := raw.Write(func(s uintptr) bool {
		_, err := syscall.Write(int(s), []byte("raw"))
		return err == nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("sent")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 7)
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil || string(got) != "rawsent" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

// TCP's own options do nothing on a Unix socket, which still takes the
// options every socket has.
func TestTCPOptionsOnUnixSocket(t *testing.T) {
	c, _ := acceptOne(t, "unix", HandlerFuncs{})
	checkProtocol(t, c, ProtocolUnix)
	checkDialed(t, c, false)
	for name, err := range map[string]error{
		"SetNoDelay":         c.SetNoDelay(false),
		"SetKeepAlive":       c.SetKeepAlive(true),
		"SetKeepAlivePeriod": c.SetKeepAlivePeriod(time.Second),
		"SetKeepAliveConfig": c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true}),
		"SetLinger":          c.SetLinger(0),
		"SetReadBuffer":      c.SetReadBuffer(32 << 10),
		"SetWriteBuffer":     c.SetWriteBuffer(32 << 10),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if mptcp, err := c.MultipathTCP(); err != nil || mptcp {
		t.Errorf("MultipathTCP() = %v, %v", mptcp, err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_SNDBUF); v < 32<<10 {
		t.Errorf("SO_SNDBUF = %d", v)
	}
}

// A UDP listener's peer has no socket of its own: the options and half-closes
// do nothing, and the socket cannot be handed out.
func TestTCPOptionsOnUDPPeer(t *testing.T) {
	opened := make(chan *Connection, 1)
	_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{Open: func(c *Connection) { opened <- c }})
	client, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	var c *Connection
	select {
	case c = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("no peer opened")
	}
	checkProtocol(t, c, ProtocolUDP)
	checkDialed(t, c, false)
	for name, err := range map[string]error{
		"SetNoDelay":     c.SetNoDelay(false),
		"SetKeepAlive":   c.SetKeepAlive(true),
		"SetLinger":      c.SetLinger(0),
		"SetReadBuffer":  c.SetReadBuffer(1 << 20),
		"SetWriteBuffer": c.SetWriteBuffer(1 << 20),
		"CloseRead":      c.CloseRead(),
		"CloseWrite":     c.CloseWrite(),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := c.File(); err == nil {
		t.Error("File on a UDP peer succeeded")
	}
	if _, err := c.SyscallConn(); err == nil {
		t.Error("SyscallConn on a UDP peer succeeded")
	}
}

func TestTCPOptionsOnClosedConnection(t *testing.T) {
	closed := make(chan struct{})
	c, _ := acceptOne(t, "tcp", HandlerFuncs{Close: func(*Connection, error) { close(closed) }})
	c.Close()
	<-closed
	if err := c.SetNoDelay(true); !errors.Is(err, net.ErrClosed) {
		t.Errorf("SetNoDelay on a closed connection: %v", err)
	}
	if err := c.CloseWrite(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("CloseWrite on a closed connection: %v", err)
	}
	if _, err := c.File(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("File on a closed connection: %v", err)
	}
}

// CloseWrite waits for the queue: the peer reads everything sent before it,
// then the end of the stream, while the connection goes on reading.
func TestCloseWriteAfterQueuedOutput(t *testing.T) {
	received := make(chan []byte, 16)
	c, client := acceptOne(t, "tcp", HandlerFuncs{Data: func(_ *Connection, b []byte) {
		received <- append([]byte(nil), b...)
	}})
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4MB, more than the socket takes
	if err := c.Send(payload); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("late")); !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("Send after CloseWrite: %v", err)
	}
	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("client read %d bytes, want %d", len(got), len(payload))
	}
	if _, err := client.Write([]byte("still reading")); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		if string(b) != "still reading" {
			t.Fatalf("server read %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server stopped reading after CloseWrite")
	}
}

// CloseRead stops OnData, and the end of input that follows, from the peer
// half-closing, does not close the connection, which goes on sending.
func TestCloseRead(t *testing.T) {
	received := make(chan []byte, 16)
	closed := make(chan error, 1)
	c, client := acceptOne(t, "tcp", HandlerFuncs{
		Data:  func(_ *Connection, b []byte) { received <- append([]byte(nil), b...) },
		Close: func(_ *Connection, err error) { closed <- err },
	})
	if err := c.CloseRead(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" {
		// Darwin resets a connection that is sent data after shutting its
		// reading side, as it would a net.TCPConn's.
		if _, err := client.Write([]byte("ignored")); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		t.Fatalf("OnData after CloseRead: %q", b)
	case err := <-closed:
		t.Fatalf("the connection closed after CloseRead: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := c.Send([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil || string(got) != "reply" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

// A CloseWrite made in OnData, behind a reply that is still corked, shuts the
// side only once the round has flushed the reply.
func TestCloseWriteFromOnData(t *testing.T) {
	_, client := acceptOne(t, "tcp", HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(append([]byte("echo:"), b...)); err != nil {
			t.Error(err)
		}
		if err := c.CloseWrite(); err != nil {
			t.Error(err)
		}
	}})
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil || string(got) != "echo:ping" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

// checkProtocol checks that c reports p, and that exactly the matching Is
// method agrees.
func checkProtocol(t *testing.T, c *Connection, p Protocol) {
	t.Helper()
	if got := c.Protocol(); got != p {
		t.Errorf("Protocol() = %v, want %v", got, p)
	}
	if c.IsTCP() != (p == ProtocolTCP) || c.IsUDP() != (p == ProtocolUDP) || c.IsUnix() != (p == ProtocolUnix) {
		t.Errorf("%v connection: IsTCP=%v IsUDP=%v IsUnix=%v", p, c.IsTCP(), c.IsUDP(), c.IsUnix())
	}
}

// checkDialed checks that c reports whether it was dialed or accepted.
func checkDialed(t *testing.T, c *Connection, dialed bool) {
	t.Helper()
	if c.IsDialed() != dialed || c.IsAccepted() == dialed {
		t.Errorf("%v connection: IsDialed=%v IsAccepted=%v, want dialed=%v",
			c.Protocol(), c.IsDialed(), c.IsAccepted(), dialed)
	}
}

// Dialed connections report their protocol, and that they were dialed, and
// keep both once closed.
func TestDialedConnectionProtocol(t *testing.T) {
	_, tcpAddr := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	_, udpAddr := startUDPServer(t, DefaultConfig(), HandlerFuncs{})
	unixPath := filepath.Join(t.TempDir(), "s")
	unixConfig := DefaultConfig()
	unixConfig.Network = "unix"
	unixConfig.Addr = unixPath
	unixServer, err := Bind(unixConfig, HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- unixServer.Run() }()
	t.Cleanup(func() {
		unixServer.Stop()
		<-runDone
		unixServer.Close()
	})
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	for _, d := range []struct {
		network, addr string
		want          Protocol
	}{
		{"tcp4", tcpAddr, ProtocolTCP},
		{"udp4", udpAddr.String(), ProtocolUDP},
		{"unix", unixPath, ProtocolUnix},
	} {
		dialed := make(chan *Connection, 1)
		if err := client.Dial(d.network, d.addr, 5*time.Second, func(c *Connection, err error) {
			if err != nil {
				t.Errorf("dial %s: %v", d.network, err)
			}
			dialed <- c
		}); err != nil {
			t.Fatal(err)
		}
		c := <-dialed
		if c == nil {
			continue
		}
		checkProtocol(t, c, d.want)
		checkDialed(t, c, true)
		c.Close()
		checkProtocol(t, c, d.want)
		checkDialed(t, c, true)
	}
}
