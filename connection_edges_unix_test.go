//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/outputwait"
)

// fillSocket writes to fd until the socket takes no more, so that the next
// send to it is queued.
func fillSocket(t *testing.T, fd int) {
	t.Helper()
	junk := make([]byte, 64<<10)
	for {
		_, err := syscall.Write(fd, junk)
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			return
		}
		if err != nil {
			t.Fatalf("filling the socket: %v", err)
		}
	}
}

// peerClosingConnection is offlineSocketConnection with a way to close the
// peer's end once, so that the connection's next write fails.
func peerClosingConnection(t *testing.T, e *Engine) (*Connection, func()) {
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
	closePeer := func() {
		if peer >= 0 {
			_ = syscall.Close(peer)
			peer = -1
		}
	}
	t.Cleanup(func() {
		if fd := c.fd.Swap(-1); fd >= 0 {
			_ = syscall.Close(int(fd))
		}
		closePeer()
	})
	return c, closePeer
}

// closingOf reports whether c has begun to close.
func closingOf(c *Connection) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closing
}

// fileLayer is a recordingLayer that can carry a file, when it says so, and
// records the ranges it is handed.
type fileLayer struct {
	recordingLayer
	can   bool
	files [][2]int64
}

func (l *fileLayer) CanSendFile() bool { return l.can }

func (l *fileLayer) SendFile(_ File, offset, count int64) error {
	l.files = append(l.files, [2]int64{offset, count})
	return nil
}

// Every send goes through the connection's layer: one it owns or in parts as
// the layer takes any bytes, and a file whole by the layer's SendFile when it
// can carry one, or read and handed to its Send when it cannot. QueuesFiles
// says which of the two a file would take.
func TestSendsThroughALayer(t *testing.T) {
	path, data := writeTestFile(t, 3000)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	c := newOfflineConnection(newOfflineServer(t))
	if !c.QueuesFiles() {
		t.Error("a connection without a layer does not queue files")
	}
	plain := &recordingLayer{}
	c.SetLayer(plain)
	if c.QueuesFiles() {
		t.Error("a connection under a layer that cannot carry files queues them")
	}
	if err := c.SendOwned([]byte("owned")); err != nil {
		t.Fatal(err)
	}
	if err := c.SendParts([]byte("he"), []byte("ad")); err != nil {
		t.Fatal(err)
	}
	if err := c.SendFile(f, 100, 2000); err != nil {
		t.Fatal(err)
	}
	if len(plain.sent) < 3 || string(plain.sent[0]) != "owned" || string(plain.sent[1]) != "head" ||
		!bytes.Equal(bytes.Join(plain.sent[2:], nil), data[100:2100]) {
		t.Fatalf("the layer was sent %d pieces", len(plain.sent))
	}

	carrier := &fileLayer{can: true}
	c.SetLayer(carrier)
	if !c.QueuesFiles() {
		t.Error("a connection under a layer that carries files does not queue them")
	}
	if err := c.SendFile(f, 5, 10); err != nil {
		t.Fatal(err)
	}
	if len(carrier.files) != 1 || carrier.files[0] != [2]int64{5, 10} {
		t.Fatalf("the layer carried %v", carrier.files)
	}
}

// SendFileRaw refuses a range it cannot send and a UDP connection, and sends
// nothing for an empty range.
func TestSendFileRawRefusals(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	if err := c.SendFileRaw(nil, -1, 1, nil); err == nil {
		t.Error("SendFileRaw of a negative offset succeeded")
	}
	if err := c.SendFileRaw(nil, 0, 0, nil); err != nil {
		t.Errorf("SendFileRaw of nothing = %v", err)
	}
	u := newOfflineConnection(newOfflineServer(t))
	u.udp = &udpState{}
	if err := u.SendFileRaw(nil, 0, 1, nil); err != ErrSendFileDatagram {
		t.Errorf("SendFileRaw on UDP = %v, want ErrSendFileDatagram", err)
	}
	if u.QueuesFiles() {
		t.Error("a UDP connection queues files")
	}
	if err := u.Flush(); err != nil {
		t.Errorf("Flush on UDP = %v", err)
	}
	if err := outputwait.Wait(u, 0); err != nil {
		t.Errorf("waiting for a UDP connection's output = %v", err)
	}
	if ran, err := u.ControlIdle(func(uintptr) error { return errors.New("ran") }); ran || err != nil {
		t.Errorf("ControlIdle on UDP = %v, %v", ran, err)
	}
}

// What the socket cannot take is queued, from a pooled buffer as it is and a
// shared message's head copied beside its body; a send past the connection's
// write queue limit closes it, whether it was queued behind a full socket or
// held by a cork.
func TestSendsToAFullSocket(t *testing.T) {
	server := newOfflineServer(t)
	c, _ := offlineSocketConnection(t, server)
	fillSocket(t, c.FD())
	if err := c.SendRawPooled(append(bufferpool.Get(8)[:0], "pooled"...)); err != nil {
		t.Fatal(err)
	}
	if err := c.SendPartsShared([]byte("head"), []byte("body")); err != nil {
		t.Fatal(err)
	}
	if got := outputwait.Queued(c); got != int64(len("pooled")+len("headbody")) {
		t.Fatalf("queued %d bytes", got)
	}
	c.SetWriteQueueLimit(1)
	if err := c.Send([]byte("more")); err != ErrSendQueueFull {
		t.Fatalf("Send past the limit = %v, want ErrSendQueueFull", err)
	}
	if !closingOf(c) {
		t.Fatal("a send past the limit left the connection open")
	}
	if err := c.SendParts([]byte("a"), []byte("b")); err != syscall.EPIPE {
		t.Fatalf("SendParts on a closing connection = %v, want EPIPE", err)
	}

	c, _ = offlineSocketConnection(t, server)
	fillSocket(t, c.FD())
	c.SetWriteQueueLimit(1)
	if err := c.SendParts([]byte("he"), []byte("ad")); err != ErrSendQueueFull {
		t.Fatalf("SendParts past the limit = %v, want ErrSendQueueFull", err)
	}

	c, _ = offlineSocketConnection(t, server)
	c.SetWriteQueueLimit(1)
	c.Cork()
	if err := c.SendParts([]byte("he"), []byte("ad")); err != ErrSendQueueFull {
		t.Fatalf("corked SendParts past the limit = %v, want ErrSendQueueFull", err)
	}
}

// CloseAfterSend on a corked connection first writes what the cork holds,
// and closes the connection with the error when that write fails.
func TestCloseAfterSendWhenTheUncorkFails(t *testing.T) {
	c, closePeer := peerClosingConnection(t, newOfflineServer(t))
	c.Cork()
	if err := c.Send([]byte("held")); err != nil {
		t.Fatal(err)
	}
	closePeer()
	c.CloseAfterSend()
	if !closingOf(c) {
		t.Fatal("a failed uncork left the connection open")
	}
}

// corkLayer is a CorkHolder that records each release of its cork.
type corkLayer struct {
	recordingLayer
	released int
}

func (l *corkLayer) ReleaseCorked() { l.released++ }

// A writer waiting for the connection's backlog to drain lets the cork of a
// layer that holds one go first, and gives up with the error the flush that
// follows meets.
func TestWaitOutputReleasesALayersCork(t *testing.T) {
	c, closePeer := peerClosingConnection(t, newOfflineServer(t))
	fillSocket(t, c.FD())
	if err := c.Send([]byte("queued")); err != nil {
		t.Fatal(err)
	}
	l := &corkLayer{}
	c.SetLayer(l)
	c.Cork()
	closePeer()
	if err := outputwait.Wait(c, 0); err == nil {
		t.Fatal("waiting on a socket whose peer is gone succeeded")
	}
	if l.released != 1 {
		t.Fatalf("the layer's cork was released %d times", l.released)
	}
}

// Reading urgent data from no socket reports why.
func TestPriorityInputWithoutASocket(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.handler = HandlerFuncs{}
	if err := c.drainPriorityInput(); err == nil {
		t.Fatal("drainPriorityInput without a socket = nil")
	}
}

// failingSealer fails to seal anything.
type failingSealer struct{}

var errSeal = errors.New("seal failed")

func (failingSealer) Seal([]byte, []byte) ([]byte, error) { return nil, errSeal }
func (failingSealer) SealedLen(n int) int                 { return n }

// A sealed file whose chunk cannot be sealed, or whose sealed chunk is past
// the connection's write queue limit, closes the connection with the reason.
func TestSendFileRawSealFailures(t *testing.T) {
	path, _ := writeTestFile(t, 64<<10)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, tc := range []struct {
		name   string
		sealer FileSealer
		limit  int64
		want   error
	}{
		{"seal", failingSealer{}, 0, errSeal},
		{"limit", &frameSealer{}, 1, ErrSendQueueFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closed := make(chan error, 1)
			c, _ := connectOne(t, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
			if tc.limit > 0 {
				c.SetWriteQueueLimit(tc.limit)
			}
			if err := c.SendFileRaw(f, 0, 64<<10, tc.sealer); err != nil && !errors.Is(err, tc.want) {
				t.Fatalf("SendFileRaw = %v", err)
			}
			select {
			case err := <-closed:
				if !errors.Is(err, tc.want) {
					t.Fatalf("closed with %v, want %v", err, tc.want)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the connection stayed open")
			}
		})
	}
}

// SendRawRecord refuses a record of no type and sends nothing for no bytes;
// a record a cork holds waits for the flush, one past the write queue limit
// closes the connection, and one the socket fails closes it with the error.
func TestSendRawRecordEdges(t *testing.T) {
	server := newOfflineServer(t)
	c, _ := offlineSocketConnection(t, server)
	if err := c.SendRawRecord(0, []byte("x")); err != errNoKernelRecords {
		t.Errorf("SendRawRecord of type 0 = %v", err)
	}
	if !kernelRecords {
		t.Skip("no kernel TLS records here")
	}
	if err := c.SendRawRecord(23, nil); err != nil {
		t.Errorf("SendRawRecord of nothing = %v", err)
	}
	c.Cork()
	if err := c.SendRawRecord(23, []byte("held")); err != nil {
		t.Errorf("corked SendRawRecord = %v", err)
	}
	c.SetWriteQueueLimit(1)
	if err := c.SendRawRecord(23, []byte("over")); err != ErrSendQueueFull {
		t.Errorf("SendRawRecord past the limit = %v, want ErrSendQueueFull", err)
	}

	c, closePeer := peerClosingConnection(t, server)
	closePeer()
	if err := c.SendRawRecord(23, []byte("gone")); err == nil {
		t.Error("SendRawRecord to a socket whose peer is gone succeeded")
	}
	if !closingOf(c) {
		t.Error("a failed record left the connection open")
	}
}
