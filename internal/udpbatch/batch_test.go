//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package udpbatch

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// A datagram sent as parts arrives as one, in order, and the platforms that
// take a message of several buffers send it without joining them.
func TestUDPSendParts(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	to := &syscall.SockaddrInet4{Port: sink.LocalAddr().(*net.UDPAddr).Port, Addr: [4]byte{127, 0, 0, 1}}
	parts := [][]byte{[]byte("one"), []byte("two")}
	if err := SendPartsSys(fd, to, parts); err == syscall.ENOSYS {
		t.Skip("the kernel takes no message of several buffers")
	} else if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_ = sink.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, _, err := sink.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "onetwo" {
		t.Fatalf("a datagram of two parts arrived as %q", buf[:n])
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = SendPartsSys(fd, to, parts) }); allocs != 0 {
		t.Fatalf("a sent datagram of parts allocated %v times", allocs)
	}
}

// A batched send describes its datagrams to the kernel without allocating.
func TestUDPSendBatchAllocs(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	to := &syscall.SockaddrInet4{Port: sink.LocalAddr().(*net.UDPAddr).Port, Addr: [4]byte{127, 0, 0, 1}}
	batch := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	if _, err := Send(fd, to, batch); err != nil {
		t.Fatal(err)
	}
	if SingleSends() {
		t.Skip("the kernel sends one datagram at a time")
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = SendSys(fd, to, batch) }); allocs != 0 {
		t.Fatalf("a batched send allocated %v times", allocs)
	}
}
