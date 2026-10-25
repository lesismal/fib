//go:build linux

package udpbatch

import (
	"bytes"
	"syscall"
	"testing"
	"unsafe"
)

// A control message that gives the size of the datagrams of a coalesced read
// splits it by that size, the last datagram as long as the read leaves; one
// that gives none, or a size the read does not reach, leaves one datagram.
func TestGROSegments(t *testing.T) {
	b := New()
	pattern := b.buf[:4*1200]
	for i := range pattern {
		pattern[i] = byte(i / 1200)
	}
	setGRO := func(size int) {
		b.hdrs[0].hdr.Control = &b.ctl[0][0]
		h := (*syscall.Cmsghdr)(unsafe.Pointer(&b.ctl[0][0]))
		h.Level, h.Type = solUDP, udpGRO
		h.SetLen(syscall.CmsgLen(4))
		*(*uint32)(unsafe.Pointer(&b.ctl[0][syscall.CmsgLen(0)])) = uint32(size)
		b.hdrs[0].hdr.SetControllen(syscall.CmsgSpace(4))
	}
	for _, tc := range []struct {
		name  string
		read  int
		size  int
		segs  int
		lasts int
	}{
		{"three whole datagrams", 3600, 1200, 3, 1200},
		{"a short last datagram", 3000, 1200, 3, 600},
		{"a size the read does not reach", 1200, 1200, 1, 1200},
		{"no size", 3600, 0, 1, 3600},
	} {
		b.lens[0] = tc.read
		b.hdrs[0].hdr.SetControllen(0)
		if tc.size > 0 {
			setGRO(tc.size)
		}
		b.groSegments(0)
		if got := b.Segments(0); got != tc.segs {
			t.Fatalf("%s: %d segments, want %d", tc.name, got, tc.segs)
		}
		off := 0
		for j := 0; j < tc.segs; j++ {
			want := tc.size
			if j == tc.segs-1 {
				want = tc.lasts
			}
			seg := b.Segment(0, j)
			if len(seg) != want {
				t.Fatalf("%s: segment %d is %d bytes, want %d", tc.name, j, len(seg), want)
			}
			if !bytes.Equal(seg, pattern[off:off+want]) {
				t.Fatalf("%s: segment %d is not the bytes the read holds there", tc.name, j)
			}
			off += want
		}
	}
	// A control message that says nothing must not keep the walk going.
	b.hdrs[0].hdr.SetControllen(syscall.CmsgSpace(4))
	(*syscall.Cmsghdr)(unsafe.Pointer(&b.ctl[0][0])).SetLen(0)
	b.lens[0] = 3600
	b.groSegments(0)
	if got := b.Segments(0); got != 1 {
		t.Fatalf("a malformed control message made %d segments", got)
	}
}

// A run of datagrams sent to a socket with UDP_GRO arrives as one read, whose
// control message the batch splits it by, and every datagram comes back as
// it was sent.
func TestGRORead(t *testing.T) {
	rfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(rfd)
	if !EnableGRO(rfd) {
		t.Skip("the kernel does not take UDP_GRO")
	}
	if err := syscall.Bind(rfd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	sa, err := syscall.Getsockname(rfd)
	if err != nil {
		t.Fatal(err)
	}
	sfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(sfd)
	if err := syscall.Connect(sfd, sa); err != nil {
		t.Fatal(err)
	}
	const count = 16
	datagrams := make([][]byte, count)
	for i := range datagrams {
		d := make([]byte, 1200)
		for j := range d {
			d[j] = byte(i)
		}
		datagrams[i] = d
	}
	if n, err := Send(sfd, nil, datagrams); err != nil || n != count {
		t.Fatalf("sent %d of %d datagrams: %v", n, count, err)
	}
	b := New()
	got, coalesced := 0, false
	for got < count {
		n, _, err := b.Recv(rfd, Size, false)
		if err != nil {
			t.Fatalf("read after %d datagrams: %v", got, err)
		}
		if n == 0 {
			t.Fatalf("%d of %d datagrams arrived", got, count)
		}
		for i := 0; i < n; i++ {
			segs := b.Segments(i)
			coalesced = coalesced || segs > 1
			for j := 0; j < segs; j++ {
				seg := b.Segment(i, j)
				if got < count && !bytes.Equal(seg, datagrams[got]) {
					t.Fatalf("datagram %d is %d bytes, not the one sent", got, len(seg))
				}
				got++
			}
		}
	}
	if !coalesced {
		t.Skip("the kernel did not coalesce the run on loopback")
	}
	if got != count {
		t.Fatalf("%d datagrams came of %d", got, count)
	}
}
