//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"net"
	"runtime"
	"testing"
	"time"
)

// udpPeerCatcher hands on the connection of each UDP peer that sends, with
// what it sent.
type udpPeerCatcher struct {
	peers chan *Connection
	data  chan string
}

func newUDPPeerCatcher() *udpPeerCatcher {
	return &udpPeerCatcher{peers: make(chan *Connection, 16), data: make(chan string, 16)}
}

func (h *udpPeerCatcher) OnOpen(c *Connection)               { h.peers <- c }
func (h *udpPeerCatcher) OnPriorityData(*Connection, []byte) {}
func (h *udpPeerCatcher) OnData(_ *Connection, b []byte)     { h.data <- string(b) }
func (h *udpPeerCatcher) OnClose(*Connection, error)         {}

// awaitWritable waits for NotifyWritable's fn on c to run, which on a
// socket with room it does at once, and reports whether NotifyWritable said
// it would.
func awaitWritable(t *testing.T, c *Connection) bool {
	t.Helper()
	ran := make(chan struct{})
	if !c.NotifyWritable(func() { close(ran) }) {
		return false
	}
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("NotifyWritable's fn never ran on a socket with room")
	}
	return true
}

// notifyWritableSupported says the backend tells when a UDP socket has room.
var notifyWritableSupported = runtime.GOOS != "windows"

// startWritableUDPServer runs an engine with a UDP listener and returns the
// connection of a peer that sent it a datagram, and the peer's socket.
func startWritableUDPServer(t *testing.T) (*Engine, *udpPeerCatcher, *Connection, *net.UDPConn) {
	t.Helper()
	handler := newUDPPeerCatcher()
	config := DefaultConfig()
	config.Network = "udp"
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	var peer *Connection
	select {
	case peer = <-handler.peers:
	case <-time.After(10 * time.Second):
		t.Fatal("no peer")
	}
	if got := <-handler.data; got != "hi" {
		t.Fatalf("read %q", got)
	}
	return server, handler, peer, conn
}

// TestNotifyWritableUDPPeer checks that NotifyWritable on a UDP listener's
// peer runs fn once its socket has room, every time it is asked, and that
// the loop stops watching the socket for room once it has told: it goes on
// reading datagrams, and does not keep finding room.
func TestNotifyWritableUDPPeer(t *testing.T) {
	_, handler, peer, conn := startWritableUDPServer(t)
	for i := 0; i < 50; i++ {
		if got := awaitWritable(t, peer); got != notifyWritableSupported {
			t.Fatalf("NotifyWritable reported %v", got)
		}
	}
	if !notifyWritableSupported {
		return
	}
	// Several waiting at once all go on.
	const waiters = 8
	ran := make(chan struct{}, waiters)
	for i := 0; i < waiters; i++ {
		if !peer.NotifyWritable(func() { ran <- struct{}{} }) {
			t.Fatal("NotifyWritable reported false")
		}
	}
	for i := 0; i < waiters; i++ {
		select {
		case <-ran:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d waiters ran", i, waiters)
		}
	}
	before := udpListenerWoken(peer.udp.listener)
	time.Sleep(50 * time.Millisecond)
	if after := udpListenerWoken(peer.udp.listener); after != before {
		t.Fatalf("the loop found room %d more times with nothing waiting", after-before)
	}
	if _, err := conn.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-handler.data:
		if got != "again" {
			t.Fatalf("read %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no datagram after waiting for room")
	}
}

// TestNotifyWritableDialedUDP checks NotifyWritable on a UDP connection the
// engine dialed, which waits on a socket of its own.
func TestNotifyWritableDialedUDP(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	engine, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	}()
	handler := newUDPPeerCatcher()
	dialed := make(chan *Connection, 1)
	if err := engine.DialWithHandler("udp", peer.LocalAddr().String(), time.Second, handler, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
		}
		dialed <- c
	}); err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if c == nil {
		t.FailNow()
	}
	for i := 0; i < 50; i++ {
		if got := awaitWritable(t, c); got != notifyWritableSupported {
			t.Fatalf("NotifyWritable reported %v", got)
		}
	}
	// It goes on reading what the peer sends.
	if err := c.Send([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	_ = peer.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, from, err := peer.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.WriteToUDP(buf[:n], from); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-handler.data:
		if got != "ping" {
			t.Fatalf("read %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no datagram after waiting for room")
	}
	c.Close()
	deadline := time.Now().Add(10 * time.Second)
	for c.NotifyWritable(func() {}) {
		if time.Now().After(deadline) {
			t.Fatal("NotifyWritable still waits on a closed connection")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestNotifyWritableClosedPeer checks that NotifyWritable reports false on a
// closed UDP peer, and on a connection that is not UDP.
func TestNotifyWritableClosedPeer(t *testing.T) {
	_, _, peer, _ := startWritableUDPServer(t)
	peer.Close()
	deadline := time.Now().Add(10 * time.Second)
	for peer.NotifyWritable(func() {}) {
		if time.Now().After(deadline) {
			t.Fatal("NotifyWritable still waits on a closed peer")
		}
		time.Sleep(time.Millisecond)
	}
	if (&Connection{}).NotifyWritable(func() {}) {
		t.Fatal("NotifyWritable reported true on a connection that is not UDP")
	}
}
