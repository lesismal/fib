//go:build linux

package fib

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestNotifyWritableFullSocket fills a dialed UDP socket until it refuses a
// send with EAGAIN, and checks that NotifyWritable's fn runs once the socket
// has drained enough to take a datagram again, and not before it has.
//
// Loopback hands a datagram to its receiver as it is sent, so a socket on it
// never fills: the test needs loopback shaped so that datagrams wait in a
// queue first, charged to the socket that sent them, which only the
// environment can do, as root, before it runs, say with
//
//	tc qdisc add dev lo root tbf rate 8mbit burst 16kb latency 400ms
//
// and FIB_TEST_SHAPED_LO=1. Without it the test is skipped.
func TestNotifyWritableFullSocket(t *testing.T) {
	if os.Getenv("FIB_TEST_SHAPED_LO") == "" {
		t.Skip("set FIB_TEST_SHAPED_LO=1 with loopback shaped to queue datagrams; see the test")
	}
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			if _, err := peer.Read(buf); err != nil {
				return
			}
		}
	}()
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
	dialed := make(chan *Connection, 1)
	if err := engine.DialWithHandler("udp", peer.LocalAddr().String(), time.Second, newUDPPeerCatcher(), func(c *Connection, err error) {
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
	fillAndAwaitRoom(t, c, c.FD())
}

// TestNotifyWritableFullListener is TestNotifyWritableFullSocket for the
// peer of a UDP listener, which waits on the listener's socket.
func TestNotifyWritableFullListener(t *testing.T) {
	if os.Getenv("FIB_TEST_SHAPED_LO") == "" {
		t.Skip("set FIB_TEST_SHAPED_LO=1 with loopback shaped to queue datagrams; see TestNotifyWritableFullSocket")
	}
	_, _, peer, conn := startWritableUDPServer(t)
	go func() {
		buf := make([]byte, 2048)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
	fillAndAwaitRoom(t, peer, peer.udp.listener.fd)
}

// fillAndAwaitRoom sends on c, whose socket is fd, until the socket refuses
// a send with EAGAIN, and checks that NotifyWritable's fn runs once it has
// room again, and not at once.
func fillAndAwaitRoom(t *testing.T, c *Connection, fd int) {
	t.Helper()
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 16<<10); err != nil {
		t.Fatal(err)
	}
	datagram := make([]byte, 1200)
	batch := [][]byte{datagram, datagram, datagram, datagram}
	for rounds := 0; ; rounds++ {
		if rounds == 10000 {
			t.Skip("the socket never filled: loopback is not shaped")
		}
		_, err := c.SendDatagrams(batch)
		if errors.Is(err, syscall.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	full := time.Now()
	ran := make(chan time.Time, 1)
	if !c.NotifyWritable(func() { ran <- time.Now() }) {
		t.Fatal("NotifyWritable reported false on a full socket")
	}
	var at time.Time
	select {
	case at = <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("NotifyWritable's fn never ran")
	}
	// At 8 Mbit/s, half of a send buffer of 32KB, which is what the
	// kernel makes of 16KB, takes 16ms to drain.
	if waited := at.Sub(full); waited < time.Millisecond {
		t.Fatalf("fn ran %v after the socket filled, before it could drain", waited)
	}
	if err := c.Send(datagram); err != nil {
		t.Fatalf("the socket had no room once fn ran: %v", err)
	}
}
