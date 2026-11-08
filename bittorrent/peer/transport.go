package peer

import (
	"net"
	"net/netip"
	"sync"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// transport is what a Conn runs on: a fib connection, or a net.Conn and
// goroutines.
type transport interface {
	// send takes over b, a buffer from package bufferpool, and queues it.
	send(b []byte)
	// close ends the connection; the transport then calls the Conn's
	// finish once nothing more will be read.
	close(err error)
	// notifyOutput is fib.Connection.NotifyOutput.
	notifyOutput(limit int64, fn func(error)) bool
	// holdReads stops reading from the peer, or resumes; the read in
	// progress is delivered.
	holdReads(hold bool)
	remoteAddr() net.Addr
	localAddr() net.Addr
}

// fibTransport is a Conn on a fib connection: the connection's rounds call
// the Conn's input and finish.
type fibTransport struct{ fc *fib.Connection }

func newFibTransport(fc *fib.Connection) *fibTransport {
	// Both sides of a peer connection upload: if each stopped reading
	// while its own sends backed up, two peers uploading to each other
	// would wait on each other for ever. A Conn paces uploads with
	// Congested instead.
	fc.SetWriteBackpressure(false)
	return &fibTransport{fc: fc}
}

func (t *fibTransport) send(b []byte) { _ = t.fc.SendRawPooled(b) }

func (t *fibTransport) close(err error) {
	if err != nil {
		t.fc.CloseWithError(err)
	} else {
		_ = t.fc.Close()
	}
}

func (t *fibTransport) notifyOutput(limit int64, fn func(error)) bool {
	return t.fc.NotifyOutput(limit, fn)
}

func (t *fibTransport) holdReads(hold bool) { t.fc.HoldReads(hold) }

func (t *fibTransport) remoteAddr() net.Addr { return t.fc.RemoteAddr() }
func (t *fibTransport) localAddr() net.Addr  { return t.fc.LocalAddr() }

// netTransport is a Conn on a net.Conn: one goroutine reads and feeds the
// Conn, another writes what it queues, so that a Conn never waits on a
// write with the peer waiting on it, as two peers uploading to each other
// would.
type netTransport struct {
	conn net.Conn

	mu      sync.Mutex
	cond    sync.Cond // the writer's
	queue   [][]byte
	queued  int64
	closed  bool
	waiters []outputWaiter
	held    bool
	resume  sync.Cond // the reader's, while held
}

type outputWaiter struct {
	limit int64
	fn    func(error)
}

func startNetTransport(c *Conn, conn net.Conn) {
	t := &netTransport{conn: conn}
	t.cond.L = &t.mu
	t.resume.L = &t.mu
	c.attach(t)
	go t.writeLoop()
	go t.readLoop(c)
}

func (t *netTransport) readLoop(c *Conn) {
	buf := make([]byte, 32<<10)
	for {
		t.mu.Lock()
		for t.held && !t.closed {
			t.resume.Wait()
		}
		t.mu.Unlock()
		n, err := t.conn.Read(buf)
		if n > 0 {
			c.input(buf[:n])
		}
		if err != nil {
			t.close(nil)
			c.finish(err)
			return
		}
	}
}

func (t *netTransport) writeLoop() {
	for {
		t.mu.Lock()
		for len(t.queue) == 0 && !t.closed {
			t.cond.Wait()
		}
		if t.closed {
			t.mu.Unlock()
			return
		}
		bufs := t.queue
		t.queue = nil
		t.mu.Unlock()

		var total int64
		for _, b := range bufs {
			total += int64(len(b))
		}
		nb := net.Buffers(append([][]byte(nil), bufs...))
		_, err := nb.WriteTo(t.conn)
		for _, b := range bufs {
			bufferpool.Put(b)
		}

		t.mu.Lock()
		t.queued -= total
		var ready []func(error)
		kept := t.waiters[:0]
		for _, w := range t.waiters {
			if t.queued <= w.limit/2 {
				ready = append(ready, w.fn)
			} else {
				kept = append(kept, w)
			}
		}
		t.waiters = kept
		t.mu.Unlock()
		for _, fn := range ready {
			fn(nil)
		}
		if err != nil {
			t.close(err)
			return
		}
	}
}

func (t *netTransport) send(b []byte) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		bufferpool.Put(b)
		return
	}
	t.queue = append(t.queue, b)
	t.queued += int64(len(b))
	t.cond.Signal()
	t.mu.Unlock()
}

func (t *netTransport) close(error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	for _, b := range t.queue {
		bufferpool.Put(b)
	}
	t.queue = nil
	waiters := t.waiters
	t.waiters = nil
	t.cond.Broadcast()
	t.resume.Broadcast()
	t.mu.Unlock()
	_ = t.conn.Close()
	for _, w := range waiters {
		w.fn(net.ErrClosed)
	}
}

func (t *netTransport) notifyOutput(limit int64, fn func(error)) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.queued <= limit {
		return false
	}
	t.waiters = append(t.waiters, outputWaiter{limit, fn})
	return true
}

func (t *netTransport) holdReads(hold bool) {
	t.mu.Lock()
	t.held = hold
	if !hold {
		t.resume.Broadcast()
	}
	t.mu.Unlock()
}

func (t *netTransport) remoteAddr() net.Addr { return t.conn.RemoteAddr() }
func (t *netTransport) localAddr() net.Addr  { return t.conn.LocalAddr() }

// addrPort is a's address and port, for a TCP or UDP address or another
// whose String is "host:port".
func addrPort(a net.Addr) (netip.AddrPort, bool) {
	switch a := a.(type) {
	case *net.TCPAddr:
		return a.AddrPort(), true
	case *net.UDPAddr:
		return a.AddrPort(), true
	case nil:
		return netip.AddrPort{}, false
	}
	ap, err := netip.ParseAddrPort(a.String())
	return ap, err == nil
}
