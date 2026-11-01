//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package udpbatch

import (
	"syscall"
	"testing"
	"unsafe"
)

// udpPair is a non-blocking UDP socket bound to loopback, its address, and a
// socket to send to it from.
func udpPair(t *testing.T) (rfd int, to *syscall.SockaddrInet4, sfd int) {
	t.Helper()
	rfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Close(rfd) })
	if err := syscall.SetNonblock(rfd, true); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(rfd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	sa, err := syscall.Getsockname(rfd)
	if err != nil {
		t.Fatal(err)
	}
	sfd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Close(sfd) })
	return rfd, sa.(*syscall.SockaddrInet4), sfd
}

// recvAll reads n datagrams that were sent before it is called.
func recvAll(t *testing.T, b *Batch, fd, n int) []string {
	t.Helper()
	var got []string
	for len(got) < n {
		k, _, err := b.Recv(fd, Size, len(got) > 0)
		if err != nil {
			t.Fatalf("Recv after %d datagrams: %v", len(got), err)
		}
		for i := 0; i < k; i++ {
			got = append(got, string(b.Datagram(i)))
		}
	}
	return got
}

// A batch set to read one datagram at a time reads each with its sender's
// address, and a send set to go one at a time sends only the first datagram,
// to an address or on a connected socket.
func TestSingleDatagrams(t *testing.T) {
	rfd, to, sfd := udpPair(t)
	prev := SetSingleSends(true)
	t.Cleanup(func() { SetSingleSends(prev) })
	if !SingleSends() {
		t.Fatal("SingleSends after SetSingleSends(true)")
	}
	if n, err := Send(sfd, to, [][]byte{[]byte("one"), []byte("two")}); n != 1 || err != nil {
		t.Fatalf("Send to an address = %d, %v", n, err)
	}
	if err := syscall.Connect(sfd, to); err != nil {
		t.Fatal(err)
	}
	if n, err := Send(sfd, nil, [][]byte{[]byte("three")}); n != 1 || err != nil {
		t.Fatalf("Send on a connected socket = %d, %v", n, err)
	}

	b := New()
	b.SetSingle(true)
	got := recvAll(t, b, rfd, 2)
	if got[0] != "one" || got[1] != "three" {
		t.Fatalf("read %q", got)
	}
	from, err := syscall.Getsockname(sfd)
	if err != nil {
		t.Fatal(err)
	}
	if addr := b.Addr(0); addr.Addr.Family != syscall.AF_INET ||
		(*syscall.RawSockaddrInet4)(unsafe.Pointer(addr)).Addr != from.(*syscall.SockaddrInet4).Addr {
		t.Fatalf("Addr(0) = %+v, want %v", addr, from)
	}
	if n, _, err := b.Recv(rfd, Size, false); n != 0 || err == nil {
		t.Fatalf("Recv of an empty socket = %d, %v", n, err)
	}
}

// Recv reports a failed receive, batched or not, with nothing read.
func TestRecvReportsErrors(t *testing.T) {
	b := New()
	if n, _, err := b.Recv(-1, Size, false); n != 0 || err != syscall.EBADF {
		t.Fatalf("batched Recv(-1) = %d, %v", n, err)
	}
	b.SetSingle(true)
	if n, _, err := b.Recv(-1, Size, true); n != 0 || err != syscall.EBADF {
		t.Fatalf("single Recv(-1) = %d, %v", n, err)
	}
	if batchRefused(syscall.EBADF) || !batchRefused(syscall.ENOSYS) {
		t.Fatal("batchRefused")
	}
}

// A batched send and receive carry a run of datagrams in one call each.
func TestBatchRoundTrip(t *testing.T) {
	rfd, to, sfd := udpPair(t)
	if n, err := Send(sfd, to, [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}); n != 3 || err != nil {
		t.Fatalf("Send = %d, %v", n, err)
	}
	b := New()
	got := recvAll(t, b, rfd, 3)
	if got[0] != "a" || got[1] != "bb" || got[2] != "ccc" {
		t.Fatalf("read %q", got)
	}
	// Asked for one receive, the batch takes one though more wait.
	if n, err := Send(sfd, to, [][]byte{[]byte("d"), []byte("e")}); n != 2 || err != nil {
		t.Fatalf("Send = %d, %v", n, err)
	}
	if n, _, err := b.Recv(rfd, Size, true); n != 1 || err != nil || string(b.Datagram(0)) != "d" {
		t.Fatalf("Recv of one = %d, %q, %v", n, b.Datagram(0), err)
	}
}
