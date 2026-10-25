//go:build linux

package fib

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/udpbatch"
)

// groCatcher keeps what each datagram of a UDP connection holds, whole, so
// that a read the kernel coalesced several into is seen split again.
type groCatcher struct {
	data chan []byte
}

func (h *groCatcher) OnOpen(*Connection)                 {}
func (h *groCatcher) OnPriorityData(*Connection, []byte) {}
func (h *groCatcher) OnData(_ *Connection, b []byte) {
	h.data <- append([]byte(nil), b...)
}
func (h *groCatcher) OnClose(*Connection, error) {}

// TestUDPGROListener checks that a run of datagrams the kernel coalesced into
// one read with GRO is delivered to the handler a datagram at a time, whole
// and in order, rather than as one datagram of the run's length.
func TestUDPGROListener(t *testing.T) {
	handler := &groCatcher{data: make(chan []byte, 32)}
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
	// A raw socket sends the run as one message, since the datagrams are all
	// of a size, which the listener's socket has the kernel coalesce.
	ip := addr.IP.To4()
	if ip == nil {
		t.Fatalf("listener address %v", addr)
	}
	sfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(sfd)
	sa := &syscall.SockaddrInet4{Port: addr.Port}
	copy(sa.Addr[:], ip)
	if err := syscall.Connect(sfd, sa); err != nil {
		t.Fatal(err)
	}
	const count = 16
	datagrams := make([][]byte, count)
	for i := range datagrams {
		d := make([]byte, 1200)
		d[0], d[1] = byte(i), byte(i>>8)
		for j := 2; j < len(d); j++ {
			d[j] = byte(j)
		}
		datagrams[i] = d
	}
	if n, err := udpbatch.Send(sfd, nil, datagrams); err != nil || n != count {
		t.Fatalf("sent %d of %d datagrams: %v", n, count, err)
	}
	for i := 0; i < count; i++ {
		select {
		case got := <-handler.data:
			if !bytes.Equal(got, datagrams[i]) {
				t.Fatalf("datagram %d is %d bytes, want the one sent", i, len(got))
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d datagrams arrived", i, count)
		}
	}
	select {
	case got := <-handler.data:
		t.Fatalf("a datagram of %d bytes came after the run", len(got))
	case <-time.After(50 * time.Millisecond):
	}
}
