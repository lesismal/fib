//go:build linux || darwin

package udpbatch

import (
	"net"
	"syscall"
	"testing"
)

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
