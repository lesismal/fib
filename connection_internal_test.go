//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
)

// offlineSocketConnection builds a connection of e's that the event loop does
// not know of, on one end of a Unix socket pair, and returns it with the other
// end. The descriptors are the test's: it closes whichever the engine has not.
func offlineSocketConnection(t *testing.T, e *Engine) (*Connection, int) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.SetNonblock(fds[0], true); err != nil {
		t.Fatal(err)
	}
	c := newOfflineConnection(e)
	c.handler = HandlerFuncs{}
	c.fd.Store(int32(fds[0]))
	peer := fds[1]
	t.Cleanup(func() {
		if fd := c.fd.Swap(-1); fd >= 0 {
			_ = syscall.Close(int(fd))
		}
		_ = syscall.Close(peer)
	})
	return c, peer
}

// A write the socket fails, rather than one it is only too full for, closes
// the connection and reports the error, whichever way the bytes were handed
// over.
func TestDirectWriteFailureClosesTheConnection(t *testing.T) {
	server := newOfflineServer(t)
	path, _ := writeTestFile(t, 4096)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sends := map[string]func(c *Connection) error{
		"Send":          func(c *Connection) error { return c.Send([]byte("data")) },
		"SendRawPooled": func(c *Connection) error { return c.SendRawPooled(bufferpool.Get(64)) },
		"SendParts":     func(c *Connection) error { return c.SendParts([]byte("head"), []byte("body")) },
		"SendFile":      func(c *Connection) error { return c.SendFile(f, 0, 4096) },
	}
	for name, send := range sends {
		c, peer := offlineSocketConnection(t, server)
		_ = syscall.Close(peer)
		if err := send(c); err == nil {
			t.Errorf("%s to a socket whose peer is gone succeeded", name)
		}
		c.mu.Lock()
		closing := c.closing
		c.mu.Unlock()
		if !closing {
			t.Errorf("%s: a failed write left the connection open", name)
		}
	}
}

// Shutting the write side of a socket that never connected fails, and the
// failure comes back wrapped as net wraps it; a lingering close that cannot
// shut the write side closes at once instead.
func TestShutdownFailuresOnAnUnconnectedSocket(t *testing.T) {
	server := newOfflineServer(t)
	for _, linger := range []bool{false, true} {
		fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		c := newOfflineConnection(server)
		c.handler = HandlerFuncs{}
		c.fd.Store(int32(fd))
		t.Cleanup(func() {
			if fd := c.fd.Swap(-1); fd >= 0 {
				_ = syscall.Close(int(fd))
			}
		})
		if !linger {
			var sysErr *os.SyscallError
			if err := c.CloseWrite(); !errors.As(err, &sysErr) {
				t.Errorf("CloseWrite = %v, want a *os.SyscallError", err)
			}
			continue
		}
		c.CloseAfterSendLinger(time.Minute)
		c.mu.Lock()
		closing, lingering := c.closing, c.flags.has(flagLingering)
		c.mu.Unlock()
		if !closing || lingering {
			t.Errorf("a linger that could not shut the write side left closing %v, lingering %v", closing, lingering)
		}
	}
}

// A packed address of no kind it knows unpacks to the zero AddrPort.
func TestPackedAddrPortOfUnknownKind(t *testing.T) {
	p := packAddrPort(netip.MustParseAddrPort("10.0.0.1:80"))
	if got := p.unpack(); got.String() != "10.0.0.1:80" {
		t.Fatalf("unpack = %v", got)
	}
	p.kind = 9
	if got := p.unpack(); got.IsValid() {
		t.Fatalf("unpack of kind 9 = %v", got)
	}
}

// Attachment never returns a half-written attachment: while a SetAttachment
// is between its two words, it waits for it to finish.
func TestAttachmentWaitsOutAWrite(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.SetAttachment("before")
	// What SetAttachment does first, as if it were preempted there.
	c.flags.bits.Add(attachSeq)
	done := make(chan any, 1)
	go func() { done <- c.Attachment() }()
	select {
	case v := <-done:
		t.Fatalf("Attachment returned %v while a write was in progress", v)
	case <-time.After(20 * time.Millisecond):
	}
	c.flags.bits.Add(attachSeq)
	select {
	case v := <-done:
		if v != "before" {
			t.Fatalf("Attachment = %v", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Attachment never returned")
	}
}

// The backlog's bookkeeping ignores moves of nothing, and never goes below
// zero, whatever a caller subtracts.
func TestPendingBytesBookkeepingLimits(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addPending(0)
	c.subPending(0)
	c.addPending(10)
	c.subPending(25)
	if got := c.pendingBytes.Load(); got != 0 {
		t.Fatalf("pendingBytes = %d after taking more than was pending", got)
	}
	// A connection on its way out has nothing for the loop to settle.
	c.closing = true
	if c.settlePauseLocked(true) {
		t.Fatal("a closing connection asked the loop to settle its pause")
	}
}

// A UDP connection has no stream to cork or hold, and nothing queued to pause
// reads for.
func TestStreamControlsDoNothingOnUDP(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.udp = &udpState{}
	c.Cork()
	c.HoldReads(true)
	if c.corked || c.ReadsHeld() {
		t.Fatalf("corked %v, reads held %v on a UDP connection", c.corked, c.ReadsHeld())
	}
	if pause, _ := c.pauseDecision(true); pause {
		t.Fatal("a UDP connection paused its reads")
	}
}

// Read on a descriptor already gone reports the connection closed, and any
// other failure as it is.
func TestReadReportsSocketFailures(t *testing.T) {
	server := newOfflineServer(t)
	c := newOfflineConnection(server)
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read on no descriptor = %v, want net.ErrClosed", err)
	}
	if err := c.closedErrorOr(io.ErrNoProgress); err != io.ErrNoProgress {
		t.Fatalf("closedErrorOr on an open connection = %v, want the fallback", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	raw, err := ln.(*net.TCPListener).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	c.fd.Store(int32(fdOf(t, raw)))
	defer c.fd.Store(-1)
	// A listening socket has nothing to read and says so with an errno that
	// is neither EAGAIN nor EBADF.
	if _, err := c.Read(make([]byte, 8)); err == nil || err == ErrWouldBlock || errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read on a listening socket = %v", err)
	}
}

// A deadline timer that fires after its deadline moved, or after the
// connection closed, does nothing.
func TestStaleDeadlineDoesNotClose(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.expireDeadline(&deadline{}, false)
	if c.closing {
		t.Fatal("a removed deadline closed the connection")
	}
	// One still ahead, as a timer that fired for a deadline since moved
	// later finds it, is set again for the rest instead.
	d := &deadline{at: time.Now().Add(time.Hour)}
	d.timer = time.AfterFunc(time.Hour, func() {})
	defer d.timer.Stop()
	c.expireDeadline(d, false)
	if c.closing {
		t.Fatal("a deadline still ahead closed the connection")
	}
}

// A deadline that moves, or is removed and set again, keeps the timer it
// was first given.
func TestDeadlineReusesItsTimer(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	defer func() {
		c.mu.Lock()
		c.stopDeadlinesLocked()
		c.mu.Unlock()
	}()
	if err := c.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	timer := c.deadlines.read.timer
	for _, at := range []time.Time{time.Now().Add(2 * time.Hour), {}, time.Now().Add(time.Hour)} {
		if err := c.SetReadDeadline(at); err != nil {
			t.Fatal(err)
		}
		if c.deadlines.read.timer != timer {
			t.Fatalf("moving the deadline to %v made another timer", at)
		}
	}
}

// The descriptor table answers nil for a descriptor past its pages or on a
// page never allocated.
func TestConnectionTableLookupOutOfRange(t *testing.T) {
	server := newOfflineServer(t)
	if c := server.connectionAt(1 << 30); c != nil {
		t.Fatal("a descriptor past the table found a connection")
	}
	saved := server.connections
	defer func() { server.connections = saved }()
	server.connections = make([][]*Connection, 2)
	if c := server.connectionAt(1 << connPageShift); c != nil {
		t.Fatal("a descriptor on a missing page found a connection")
	}
}

// A gathered write skips empty buffers, writes nothing for none, and takes
// at most maxWritevItems buffers in one call.
func TestWritevBounds(t *testing.T) {
	c, peer := offlineSocketConnection(t, newOfflineServer(t))
	if n, err := c.sysWritev([][]byte{nil, {}}); n != 0 || err != nil {
		t.Fatalf("sysWritev of empty buffers = %d, %v", n, err)
	}
	buffers := make([][]byte, maxWritevItems+6)
	for i := range buffers {
		buffers[i] = []byte{byte(i)}
	}
	n, err := c.sysWritev(buffers)
	if err != nil || n != maxWritevItems {
		t.Fatalf("sysWritev of %d buffers = %d, %v; want %d", len(buffers), n, err, maxWritevItems)
	}
	if n, err := c.sysWrite2(nil, []byte("tail")); n != 4 || err != nil {
		t.Fatalf("sysWrite2 with no first part = %d, %v", n, err)
	}
	got := make([]byte, 128)
	m, _ := syscall.Read(peer, got)
	if m != maxWritevItems+4 || got[maxWritevItems-1] != maxWritevItems-1 || string(got[maxWritevItems:m]) != "tail" {
		t.Fatalf("the peer read %v", got[:m])
	}
}

// errSendFailed is what a failing send reports.
var errSendFailed = errors.New("send failed")

// shortReader hands out a byte at a time, then reports io.EOF at its end.
type shortReader struct {
	data []byte
}

func (r shortReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	p[0] = r.data[off]
	return 1, nil
}

func (shortReader) SyscallConn() (syscall.RawConn, error) { return nil, errNoDescriptor }

// The copying SendFile, which a layer uses, takes short reads in its stride,
// hands a failed send's error back, and closes the connection when the file
// ends short after some of it went, since the peer was promised the rest;
// a file that ends before anything went leaves the connection as it was.
func TestSendFileCopyFailures(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	var sent bytes.Buffer
	collect := func(b []byte) error { sent.Write(b); return nil }
	if err := sendFileCopy(c, shortReader{[]byte("abc")}, 0, 3, collect); err != nil || sent.String() != "abc" {
		t.Fatalf("short reads: %v, sent %q", err, sent.String())
	}
	if err := sendFileCopy(c, shortReader{[]byte("abc")}, 0, 3, func([]byte) error { return errSendFailed }); err != errSendFailed {
		t.Fatalf("a failing send = %v", err)
	}
	if err := sendFileCopy(c, shortReader{nil}, 0, 3, collect); err != io.ErrUnexpectedEOF {
		t.Fatalf("an empty file = %v, want io.ErrUnexpectedEOF", err)
	}
	if c.closing {
		t.Fatal("a file that ended before anything was sent closed the connection")
	}
	if err := sendFileCopy(c, shortReader{[]byte("ab")}, 0, 3, collect); err != io.ErrUnexpectedEOF {
		t.Fatalf("a short file = %v, want io.ErrUnexpectedEOF", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadAll(client); err != nil {
		t.Fatalf("the connection was not closed: %v", err)
	}
}

// A connection whose socket is gone has no remote address, and a Unix one has
// none an AddrPort can hold.
func TestRemoteAddrWithoutAPeer(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	if ap := c.RemoteAddrPort(); ap.IsValid() {
		t.Errorf("RemoteAddrPort with no socket = %v", ap)
	}
	if addr := c.RemoteAddr(); addr != nil {
		t.Errorf("RemoteAddr with no socket = %v", addr)
	}
	unix, _ := acceptOne(t, "unix", HandlerFuncs{})
	if ap := unix.RemoteAddrPort(); ap.IsValid() {
		t.Errorf("RemoteAddrPort of a Unix connection = %v", ap)
	}
}

// A datagram from an address family other than IP opens no peer, and neither
// does one from a new peer once the engine is stopping; a socket address that
// is not IP has no UDP address.
func TestUDPPeerRefusals(t *testing.T) {
	server := newOfflineServer(t)
	l := &udpListener{peers: map[netip.AddrPort]*Connection{}}
	var from syscall.RawSockaddrAny
	from.Addr.Family = syscall.AF_UNIX
	if c := server.udpPeer(l, &from); c != nil {
		t.Fatal("a datagram from a Unix address opened a peer")
	}
	v4 := (*syscall.RawSockaddrInet4)(unsafe.Pointer(&from))
	v4.Family = syscall.AF_INET
	v4.Addr = [4]byte{127, 0, 0, 1}
	server.Stop()
	if c := server.udpPeer(l, &from); c != nil {
		t.Fatal("a stopping engine opened a peer")
	}
	if addr := sockaddrToUDPAddr(&syscall.SockaddrUnix{Name: "x"}); addr != nil {
		t.Fatalf("sockaddrToUDPAddr(unix) = %v", addr)
	}
}

// A dial address naming its zone is still a literal, which needs no DNS, and
// a failed UDP dial names its peer as a UDP address.
func TestDialAddressHelpers(t *testing.T) {
	if !isLiteralAddr("[fe80::1%eth0]:80") {
		t.Error("a zoned IPv6 literal was taken for a host name")
	}
	d := &dialRequest{network: "udp", raddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53}}
	var opErr *net.OpError
	if err := d.opError(syscall.ECONNREFUSED); !errors.As(err, &opErr) {
		t.Fatalf("opError = %v", err)
	} else if _, ok := opErr.Addr.(*net.UDPAddr); !ok {
		t.Fatalf("a UDP dial's error names %T", opErr.Addr)
	}
}

// A dial reaching a loop that will never run it fails, closing the socket
// opened for it; one the resolver failed, or that reaches a loop after Stop,
// fails there.
func TestDialRequestFailures(t *testing.T) {
	server := newOfflineServer(t)
	failed := make(chan error, 4)
	done := func(c *Connection, err error) { failed <- err }

	d := &dialRequest{network: "tcp", done: done, err: errors.New("no such host")}
	server.startDial(d)
	if err := <-failed; err == nil {
		t.Fatal("a dial the resolver failed succeeded")
	}

	server.Stop()
	server.startDial(&dialRequest{network: "tcp", done: done})
	if err := <-failed; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("a dial after Stop = %v, want net.ErrClosed", err)
	}

	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	server.requestDial(&dialRequest{network: "tcp", done: done, socket: fd, hasSocket: true})
	if err := <-failed; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("a dial on a closed engine = %v, want net.ErrClosed", err)
	}
}

// The size of a round's next UDP receive follows what the poller counted as
// waiting: a batch where it counts several datagrams of the last one's size,
// an ordinary receive of one where it counts fewer or has nothing to go by.
func TestNextReadFollowsTheCount(t *testing.T) {
	cases := []struct {
		read, left, last int
		want             int
		one              bool
	}{
		{0, 100, 0, 1, true},
		{5, 1000, 100, 10, false},
		{5, 100, 100, 1, true},
	}
	for _, tc := range cases {
		if want, one := nextRead(tc.read, tc.left, tc.last); want != tc.want || one != tc.one {
			t.Errorf("nextRead(%d, %d, %d) = %d, %v; want %d, %v", tc.read, tc.left, tc.last, want, one, tc.want, tc.one)
		}
	}
	if l := newOfflineServer(t).udpListenerAt(1 << 20); l != nil {
		t.Error("a TCP engine found a UDP listener")
	}
}

// recordingLayer records what it is asked to send.
type recordingLayer struct{ sent [][]byte }

func (l *recordingLayer) Send(first, second []byte) error {
	l.sent = append(l.sent, append(append([]byte(nil), first...), second...))
	return nil
}

func (l *recordingLayer) CloseAfterSend() {}

// SendShared, like every send, goes through the connection's layer, which
// transforms what it is given and so has to copy it anyway.
func TestSendSharedGoesThroughTheLayer(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	l := &recordingLayer{}
	c.SetLayer(l)
	if err := c.SendShared([]byte("shared")); err != nil {
		t.Fatal(err)
	}
	if len(l.sent) != 1 || string(l.sent[0]) != "shared" {
		t.Fatalf("the layer was sent %q", l.sent)
	}
}

// failingRawConn is a RawConn whose Control fails.
type failingRawConn struct{ syscall.RawConn }

func (failingRawConn) Control(func(uintptr)) error { return errNoDescriptor }

// controlFailingFile is a File whose descriptor cannot be reached.
type controlFailingFile struct{ io.ReaderAt }

func (controlFailingFile) SyscallConn() (syscall.RawConn, error) { return failingRawConn{}, nil }

// The state a closed connection keeps answers what is asked of it: no local
// address once the socket is gone, the reason it closed, and no peer for a
// UDP connection that never had one.
func TestClosedConnectionAnswers(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	if addr := c.LocalAddr(); addr != nil {
		t.Errorf("LocalAddr with no socket = %v", addr)
	}
	if _, err := newFileSegment(controlFailingFile{}, 0, 1); err != errNoDescriptor {
		t.Errorf("newFileSegment through a failing Control = %v", err)
	}
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	if err := c.closedErrorOr(io.ErrNoProgress); !errors.Is(err, net.ErrClosed) {
		t.Errorf("closedErrorOr on a closed connection = %v, want net.ErrClosed", err)
	}
	u := newOfflineConnection(newOfflineServer(t))
	u.udp = &udpState{}
	if ap := u.RemoteAddrPort(); ap.IsValid() {
		t.Errorf("RemoteAddrPort of a UDP connection with no peer = %v", ap)
	}
}

// cutToRecords keeps the whole records before a write's last byte, across
// the buffers they span, so that the rest, one record or less, goes in a
// write of its own.
func TestCutToRecords(t *testing.T) {
	const r = kernelRecordSize
	for _, tc := range []struct {
		sizes []int
		cut   int
		kept  []int
	}{
		{[]int{r + 1}, r, []int{r}},
		{[]int{2 * r}, r, []int{r}},
		{[]int{2*r + 5}, 2 * r, []int{2 * r}},
		{[]int{100, r, 7}, r, []int{100, r - 100}},
		{[]int{r, r, 1}, 2 * r, []int{r, r}},
		{[]int{10, 3 * r}, 3 * r, []int{10, 3*r - 10}},
	} {
		var buffers [][]byte
		n := 0
		for _, s := range tc.sizes {
			buffers = append(buffers, make([]byte, s))
			n += s
		}
		kept, cut := cutToRecords(buffers, n)
		var sizes []int
		for _, b := range kept {
			sizes = append(sizes, len(b))
		}
		if cut != tc.cut || !slices.Equal(sizes, tc.kept) {
			t.Errorf("%v: cut %d into %v, want %d into %v", tc.sizes, cut, sizes, tc.cut, tc.kept)
		}
	}
}
