//go:build linux

package udpbatch

import (
	"syscall"
	"testing"
	"unsafe"
)

// The walk over a read's control messages steps past one that is not
// UDP_GRO's, and with no UDP_GRO message after it the read is one datagram.
func TestGROSegmentsSkipsOtherControlMessages(t *testing.T) {
	b := New()
	b.lens[0] = 3600
	b.gso[0] = 1200
	b.hdrs[0].hdr.Control = &b.ctl[0][0]
	other := (*syscall.Cmsghdr)(unsafe.Pointer(&b.ctl[0][0]))
	other.Level, other.Type = syscall.SOL_SOCKET, syscall.SCM_RIGHTS
	other.SetLen(syscall.CmsgLen(0))
	b.hdrs[0].hdr.SetControllen(syscall.CmsgSpace(0))
	b.groSegments(0)
	if got := b.Segments(0); got != 1 {
		t.Fatalf("%d segments from another control message, want 1", got)
	}
}

// rawSendmsg sends a message already built, and reports a failed call's
// errno.
func TestRawSendmsg(t *testing.T) {
	_, to, sfd := udpPair(t)
	if err := syscall.Connect(sfd, to); err != nil {
		t.Fatal(err)
	}
	data := []byte("msg")
	iov := syscall.Iovec{Base: &data[0]}
	iov.SetLen(len(data))
	var h syscall.Msghdr
	h.Iov, h.Iovlen = &iov, 1
	if n, _, errno := rawSendmsg(sfd, unsafe.Pointer(&h)); errno != 0 || n != uintptr(len(data)) {
		t.Fatalf("rawSendmsg = %d, %v", n, errno)
	}
	if _, _, errno := rawSendmsg(-1, unsafe.Pointer(&h)); errno != syscall.EBADF {
		t.Fatalf("rawSendmsg(-1) = %v", errno)
	}
}

// A kernel that refuses a segmented send, as it does with EINVAL on a socket
// that sends without UDP checksums, has Send stop segmenting and send the
// datagrams a message each, all of them still in one call.
func TestSendFallsBackWithoutGSO(t *testing.T) {
	rfd, to, sfd := udpPair(t)
	const soNoCheck = 11
	if err := syscall.SetsockoptInt(sfd, syscall.SOL_SOCKET, soNoCheck, 1); err != nil {
		t.Skipf("SO_NO_CHECK: %v", err)
	}
	prev := SetGSOOff(false)
	t.Cleanup(func() { SetGSOOff(prev) })
	run := make([][]byte, 3)
	for i := range run {
		run[i] = make([]byte, 1200)
		run[i][0] = byte(i)
	}
	if n, err := Send(sfd, to, run); n != len(run) || err != nil {
		t.Fatalf("Send = %d, %v", n, err)
	}
	if !GSOOff() {
		t.Skip("the kernel took the segmented send")
	}
	got := recvAll(t, New(), rfd, len(run))
	for i, d := range got {
		if len(d) != 1200 || d[0] != byte(i) {
			t.Fatalf("datagram %d is %d bytes starting %d", i, len(d), d[0])
		}
	}
}
