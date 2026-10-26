//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// readAllFrom reads conn until it has want bytes or fails.

// Write is Send for code written against net.Conn: it reports everything
// written, and the error once the connection has closed. An empty Send writes
// nothing and is no error.
func TestConnectionWriteIsSend(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	if err := c.Send(nil); err != nil {
		t.Fatalf("Send(nil) = %v", err)
	}
	if err := c.SendRawPooled(bufferpool.Get(8)[:0]); err != nil {
		t.Fatalf("SendRawPooled of an empty buffer = %v", err)
	}
	if n, err := c.Write([]byte("hello")); n != 5 || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if got := readAllFrom(t, client, 5); string(got) != "hello" {
		t.Fatalf("client read %q", got)
	}
	_ = c.Close()
	if n, err := c.Write([]byte("late")); n != 0 || err == nil {
		t.Fatalf("Write after Close = %d, %v", n, err)
	}
}

// Read takes what the socket has without waiting: nothing for an empty
// buffer, ErrWouldBlock while the peer is silent, the bytes once they arrive,
// io.EOF once the peer has shut its side, and why it closed after Close.
func TestConnectionReadWithoutWaiting(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	// Reads held, the loop leaves the socket to Read.
	c.HoldReads(true)
	if n, err := c.Read(nil); n != 0 || err != nil {
		t.Fatalf("Read(nil) = %d, %v", n, err)
	}
	buf := make([]byte, 16)
	if _, err := c.Read(buf); err != ErrWouldBlock {
		t.Fatalf("Read on a silent peer = %v, want ErrWouldBlock", err)
	}
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	var got []byte
	if !waitFor(func() bool {
		n, _ := c.Read(buf)
		got = append(got, buf[:max(n, 0)]...)
		return len(got) >= 4
	}) || string(got) != "ping" {
		t.Fatalf("Read got %q", got)
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var err error
	if !waitFor(func() bool {
		_, err = c.Read(buf)
		return err != ErrWouldBlock
	}) || (err != io.EOF && !errors.Is(err, net.ErrClosed)) {
		t.Fatalf("Read after the peer shut its side = %v, want io.EOF", err)
	}
	_ = c.Close()
	// The reason it closed, which may be the end of input the loop saw.
	if _, err := c.Read(buf); !errors.Is(err, net.ErrClosed) && err != io.EOF {
		t.Fatalf("Read after Close = %v, want net.ErrClosed or io.EOF", err)
	}
}

// The raw connection's Read and Write report ErrWouldBlock when their function
// says it is not done, as net's RawConn would wait instead.
func TestRawConnReadAndWriteReportWouldBlock(t *testing.T) {
	c, _ := acceptOne(t, "tcp", HandlerFuncs{})
	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func(func(uintptr) bool) error{"Read": raw.Read, "Write": raw.Write} {
		if err := call(func(uintptr) bool { return false }); err != ErrWouldBlock {
			t.Errorf("%s with a function not done = %v, want ErrWouldBlock", name, err)
		}
		if err := call(func(uintptr) bool { return true }); err != nil {
			t.Errorf("%s = %v", name, err)
		}
	}
}

// CloseRead and CloseWrite may each be called again, which does nothing, and
// the peer reads the end of the stream after CloseWrite.
func TestCloseReadAndCloseWriteRepeat(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	for range 2 {
		if err := c.CloseRead(); err != nil {
			t.Fatalf("CloseRead = %v", err)
		}
		if err := c.CloseWrite(); err != nil {
			t.Fatalf("CloseWrite = %v", err)
		}
	}
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := client.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("client read %d, %v after CloseWrite; want io.EOF", n, err)
	}
}

// A deadline already past closes the connection at once with
// os.ErrDeadlineExceeded, and one set on a closed connection is refused.
func TestSetDeadlineInThePastCloses(t *testing.T) {
	closed := make(chan error, 1)
	c, _ := acceptOne(t, "tcp", HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	if err := c.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetDeadline = %v", err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("OnClose got %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a past deadline did not close the connection")
	}
	if err := c.SetDeadline(time.Now().Add(time.Hour)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetDeadline after the close = %v, want net.ErrClosed", err)
	}
}

// Keep-alive times the kernel cannot hold are refused with the setsockopt's
// errno wrapped as net wraps it, and a probe count of zero means net's
// default, as it does to net.KeepAliveConfig.
func TestKeepAliveConfigLimits(t *testing.T) {
	c, _ := acceptOne(t, "tcp", HandlerFuncs{})
	err := c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: 1 << 20 * time.Hour})
	var sysErr *os.SyscallError
	if !errors.As(err, &sysErr) {
		t.Fatalf("an idle time of %v = %v, want a *os.SyscallError", 1<<20*time.Hour, err)
	}
	if err := c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: -1, Interval: -1}); err != nil {
		t.Fatalf("SetKeepAliveConfig = %v", err)
	}
	if got := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepCount); got != defaultKeepAliveCount {
		t.Fatalf("keep-alive count %d, want the default %d", got, defaultKeepAliveCount)
	}
}

// failingFile is a File whose descriptor cannot be had.
type failingFile struct{ io.ReaderAt }

var errNoDescriptor = errors.New("no descriptor")

func (failingFile) SyscallConn() (syscall.RawConn, error) { return nil, errNoDescriptor }

// SendFile of nothing sends nothing; a file whose descriptor cannot be had is
// refused before anything is queued; and a connection that is closing takes
// no file, as it takes no Send.
func TestSendFileRefusals(t *testing.T) {
	c, _ := acceptOne(t, "tcp", HandlerFuncs{})
	path, _ := writeTestFile(t, 64)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := c.SendFile(f, 0, 0); err != nil {
		t.Fatalf("SendFile of nothing = %v", err)
	}
	if err := c.SendFile(failingFile{f}, 0, 10); err != errNoDescriptor {
		t.Fatalf("SendFile of a file with no descriptor = %v", err)
	}
	_ = c.Close()
	if err := c.SendFile(f, 0, 10); err != syscall.EPIPE {
		t.Fatalf("SendFile on a closed connection = %v, want EPIPE", err)
	}
}

// A descriptor that cannot be read as a file, such as a directory's, fails
// SendFile and closes the connection rather than leaving the peer waiting for
// bytes that will never come.
func TestSendFileOfADirectoryFails(t *testing.T) {
	closed := make(chan error, 1)
	c, _ := acceptOne(t, "tcp", HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	dir, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := c.SendFile(dir, 0, 10); err == nil {
		t.Fatal("SendFile of a directory succeeded")
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection stayed open")
	}
}

// SendBatch on a stream sends each datagram as a Send would, in order, and
// fails as Send does once the connection is closed, and SendDatagrams then
// says it sent none.
func TestSendBatchOnAStream(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	if err := c.SendBatch([][]byte{[]byte("one,"), []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if got := readAllFrom(t, client, 7); string(got) != "one,two" {
		t.Fatalf("client read %q", got)
	}
	_ = c.Close()
	if err := c.SendBatch([][]byte{[]byte("x")}); err == nil {
		t.Fatal("SendBatch on a closed connection succeeded")
	}
	if n, err := c.SendDatagrams([][]byte{[]byte("x")}); err == nil || n != 0 {
		t.Fatalf("SendDatagrams on a closed connection sent %d: %v", n, err)
	}
}

// CloseAfterSendLinger with nothing queued shuts the write side at once, so
// the peer reads the end of the stream, and closes when the peer ends its
// own side.
func TestCloseAfterSendLingerWithNothingQueued(t *testing.T) {
	closed := make(chan error, 1)
	c, client := acceptOne(t, "tcp", HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	c.CloseAfterSendLinger(10 * time.Second)
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := client.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("client read %d, %v; want io.EOF", n, err)
	}
	_ = client.Close()
	select {
	case err := <-closed:
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("OnClose got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lingering connection did not close when the peer did")
	}
}

// SendParts on a peer that is not reading queues what the socket will not
// take: the rest of the second part when the first went out whole, and both
// parts once something is queued ahead of them. Everything reaches the peer
// in order once it reads.
func TestSendPartsQueuesUnderBackpressure(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	_ = c.SetWriteBuffer(4 << 10)
	header := []byte("HDR:")
	body := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1MB
	var want []byte
	for range 3 {
		if err := c.SendParts(header, body); err != nil {
			t.Fatal(err)
		}
		want = append(append(want, header...), body...)
	}
	if c.pendingBytes.Load() == 0 {
		t.Fatal("nothing was queued: the socket took 3MB without the peer reading")
	}
	if err := c.SendParts(nil, nil); err != nil {
		t.Fatalf("SendParts of nothing = %v", err)
	}
	if got := readAllFrom(t, client, len(want)); !bytes.Equal(got, want) {
		t.Fatal("the peer read the parts out of order or damaged")
	}
}

// A read round stops at once when OnData shuts the read side, even with more
// of the peer's bytes still in the socket.
func TestCloseReadInsideOnDataStopsTheRound(t *testing.T) {
	calls := make(chan int, 16)
	config := DefaultConfig()
	config.ReadBufferSize = 16
	_, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		calls <- len(b)
		_ = c.CloseRead()
	}})
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write(bytes.Repeat([]byte("x"), 256)); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-calls:
		if n != 16 {
			t.Fatalf("OnData got %d bytes, want a full 16-byte read", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnData never ran")
	}
	select {
	case n := <-calls:
		t.Fatalf("OnData ran again with %d bytes after CloseRead", n)
	case <-time.After(100 * time.Millisecond):
	}
}

// SendParts that queues past the server-wide budget asks the event loop to
// pause the connection's reads, as Send does.
func TestSendPartsOverTheBudgetPausesReads(t *testing.T) {
	opened := make(chan *Connection, 1)
	config := DefaultConfig()
	config.MaxPendingBytes = 64 << 10
	_, addr := startEchoServer(t, config, HandlerFuncs{Open: func(c *Connection) { opened <- c }})
	client, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	c := <-opened
	_ = c.SetWriteBuffer(4 << 10)
	body := bytes.Repeat([]byte("x"), 256<<10)
	for range 2 {
		if err := c.SendParts([]byte("HDR:"), body); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(func() bool { return c.engine.readsPausedByBudget.Load() > 0 }) {
		t.Fatal("reads were never paused for the budget")
	}
	readAllFrom(t, client, 2*(4+len(body)))
}

// A connection dialed over TCP reports the peer it connected to.
func TestDialedConnectionRemoteAddrPort(t *testing.T) {
	_, addr := startEchoServer(t, DefaultConfig(), echoHandler())
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	got := make(chan string, 1)
	if err := client.Dial("tcp", addr, 5*time.Second, func(c *Connection, err error) {
		if err != nil {
			got <- err.Error()
			return
		}
		got <- c.RemoteAddrPort().String()
		c.Close()
	}); err != nil {
		t.Fatal(err)
	}
	if g := <-got; g != addr {
		t.Fatalf("RemoteAddrPort = %s, want %s", g, addr)
	}
}

// SetRemoteAddrPort replaces the address a connection reports for its peer,
// an IPv4 one mapped into IPv6 reported as IPv4, and ignores an address that
// is not valid.
func TestSetRemoteAddrPort(t *testing.T) {
	got := make(chan string, 1)
	_, addr := startEchoServer(t, DefaultConfig(), HandlerFuncs{
		Data: func(c *Connection, _ []byte) {
			c.SetRemoteAddrPort(netip.AddrPort{})
			before := c.RemoteAddrPort().String()
			c.SetRemoteAddrPort(netip.MustParseAddrPort("[::ffff:203.0.113.7]:4242"))
			got <- before + " " + c.RemoteAddrPort().String() + " " + c.RemoteAddr().String()
		},
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	want := conn.LocalAddr().String() + " 203.0.113.7:4242 203.0.113.7:4242"
	select {
	case g := <-got:
		if g != want {
			t.Fatalf("got %q, want %q", g, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no data arrived")
	}
}
