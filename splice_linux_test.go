//go:build linux

package fib

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// spliceRig is a server with two connections accepted from two clients: src,
// whose client sends a line, then the bytes the test splices, then whatever
// follows them, and dst, whose client reads what is spliced. Its handler
// splices from src when its line arrives, and keeps what OnData is given past
// the splice.
type spliceRig struct {
	server   *Engine
	stop     func()
	src, dst *Connection
	// srcAt and dstAt are src and dst for the handler, which runs beside the
	// test.
	srcAt, dstAt     atomic.Pointer[Connection]
	srcPeer, dstPeer net.Conn

	// n is how many bytes the splice moves, and begin, when set, what Begin
	// does instead of sending what arrived with the line and returning the
	// rest of n.
	n     int64
	begin func(r *spliceRig, early []byte) int64

	mu      sync.Mutex
	started bool
	head    []byte
	after   bytes.Buffer
	// spliceErr is what SpliceTo returned.
	spliceErr error

	readN     atomic.Int64
	readErr   chan error
	writtenCh chan error
	// order records which of Read and Written came first.
	order     []string
	srcClosed chan error
	dstClosed chan error
}

func newSpliceRig(t *testing.T, n int64) *spliceRig {
	t.Helper()
	r := &spliceRig{n: n, readErr: make(chan error, 1), writtenCh: make(chan error, 1),
		srcClosed: make(chan error, 1), dstClosed: make(chan error, 1)}
	opened := make(chan *Connection, 2)
	handler := HandlerFuncs{
		Open: func(c *Connection) { opened <- c },
		Data: func(c *Connection, b []byte) {
			if c == r.srcAt.Load() {
				r.onData(b)
			}
		},
		Close: func(c *Connection, err error) {
			switch c {
			case r.srcAt.Load():
				r.srcClosed <- err
			case r.dstAt.Load():
				r.dstClosed <- err
			}
		},
	}
	config := DefaultConfig()
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.ListenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	r.server = server
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	r.stop = sync.OnceFunc(func() {
		server.Stop()
		<-runDone
		server.Close()
	})
	t.Cleanup(r.stop)
	dial := func() (net.Conn, *Connection) {
		peer, err := net.Dial("tcp", addrs[0].String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { peer.Close() })
		select {
		case c := <-opened:
			return peer, c
		case <-time.After(5 * time.Second):
			t.Fatal("no connection opened")
			return nil, nil
		}
	}
	r.mu.Lock()
	r.srcPeer, r.src = dial()
	r.dstPeer, r.dst = dial()
	r.srcAt.Store(r.src)
	r.dstAt.Store(r.dst)
	r.mu.Unlock()
	return r
}

// onData splices from the connection once the line has arrived, and keeps
// what arrives after the splice.
func (r *spliceRig) onData(b []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		r.after.Write(b)
		return
	}
	r.head = append(r.head, b...)
	at := bytes.IndexByte(r.head, '\n')
	if at < 0 {
		return
	}
	r.started = true
	early := bytes.Clone(r.head[at+1:])
	r.spliceErr = r.src.SpliceTo(r.dst, Splice{
		Begin: func() int64 {
			if r.begin != nil {
				return r.begin(r, early)
			}
			_ = r.dst.Send(early)
			return r.n - int64(len(early))
		},
		Read: func(n int64, err error) {
			r.mu.Lock()
			r.order = append(r.order, "read")
			r.mu.Unlock()
			r.readN.Store(n)
			r.readErr <- err
		},
		Written: func(err error) {
			r.mu.Lock()
			r.order = append(r.order, "written")
			r.mu.Unlock()
			r.writtenCh <- err
		},
	})
}

// state describes the splice, for a test that finds it stuck.
func (r *spliceRig) state() string {
	src, dst := r.src, r.dst
	src.mu.Lock()
	s := fmt.Sprintf("src flags=%#x paused=%v stalled=%v scheduled=%v pending=%#x deferred=%v",
		src.flags.bits.Load()&0xffff, src.readPaused, src.readStalled, src.scheduled, src.pendingEvents, src.readDeferred)
	var j *spliceJob
	if d := src.deadlines; d != nil {
		j = d.spliceOut
	}
	src.mu.Unlock()
	dst.mu.Lock()
	s += fmt.Sprintf("; dst queue=%d flushing=%v corked=%v", len(dst.sends)-dst.sendHead, dst.flushing, dst.corked)
	if d := dst.deadlines; d != nil && d.spliceIn != nil {
		j = d.spliceIn
	}
	if j != nil {
		s += fmt.Sprintf("; job inPipe=%d srcDone=%v queued=%v waiting=%v err=%v", j.inPipe, j.srcDone, j.queued, j.waiting, j.err)
	}
	dst.mu.Unlock()
	r.mu.Lock()
	s += fmt.Sprintf("; head=%d after=%d readN=%d order=%v", len(r.head), r.after.Len(), r.readN.Load(), r.order)
	r.mu.Unlock()
	return s
}

func (r *spliceRig) afterBytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return bytes.Clone(r.after.Bytes())
}

func waitErr(t *testing.T, ch chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(15 * time.Second):
		t.Fatalf("no %s", what)
		return nil
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.IntN(256))
	}
	return b
}

// readUntil reads conn until want bytes or the deadline.
func readUntil(t *testing.T, conn net.Conn, want int) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	got := make([]byte, want)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read %d bytes: %v", want, err)
	}
	return got
}

// spliceWaitFor polls cond for up to 10 seconds.
func spliceWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// The splice moves exactly its bytes, the ones that arrived with the line
// going first through Begin; what follows them reaches OnData as ever; Read
// comes before Written; and the destination is free of the splice once it
// has all gone.
func TestSpliceToMovesExactBytes(t *testing.T) {
	for _, size := range []int{1, 100 << 10, 3<<20 + 7} {
		r := newSpliceRig(t, int64(size))
		body := randomBytes(size)
		next := []byte("GET /next HTTP/1.1\r\n\r\n")
		// The line goes on its own, so that nothing past the spliced bytes
		// reaches OnData with it: what OnData was handed is the handler's to
		// pass on, and the splice moves exactly the bytes that follow.
		_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
		spliceWaitFor(t, "the splice", func() bool { return r.dst.Splicing() })
		go func() { _, _ = r.srcPeer.Write(append(body, next...)) }()
		got := readUntil(t, r.dstPeer, size)
		if !bytes.Equal(got, body) {
			t.Fatalf("size %d: the spliced bytes differ", size)
		}
		if err := waitErr(t, r.readErr, "Read"); err != nil {
			t.Fatalf("Read: %v", err)
		}
		if err := waitErr(t, r.writtenCh, "Written"); err != nil {
			t.Fatalf("Written: %v", err)
		}
		if r.spliceErr != nil {
			t.Fatal(r.spliceErr)
		}
		r.mu.Lock()
		order := append([]string(nil), r.order...)
		r.mu.Unlock()
		if len(order) != 2 || order[0] != "read" {
			t.Fatalf("callbacks came %v", order)
		}
		spliceWaitFor(t, "the bytes past the splice", func() bool { return bytes.Equal(r.afterBytes(), next) })
		if r.dst.Splicing() {
			t.Fatal("the destination still splicing")
		}
		// Both connections go on as they were: dst sends, src reads.
		if err := r.dst.Send([]byte("after")); err != nil {
			t.Fatal(err)
		}
		if got := readUntil(t, r.dstPeer, 5); string(got) != "after" {
			t.Fatalf("after the splice dst sent %q", got)
		}
		_, _ = r.srcPeer.Write([]byte("more"))
		spliceWaitFor(t, "more", func() bool { return bytes.HasSuffix(r.afterBytes(), []byte("more")) })
	}
}

// What the destination had queued goes before the spliced bytes, and what is
// sent on it during the splice goes after them. The head is larger than the
// socket takes at once, so that it waits in the queue, ahead of the splice,
// and goes in writes that end where the splice begins.
func TestSpliceToKeepsDestinationOrder(t *testing.T) {
	const size = 2 << 20
	r := newSpliceRig(t, size)
	sentDuring := make(chan struct{})
	head := randomBytes(8 << 20)
	begun := make(chan struct{})
	r.begin = func(r *spliceRig, early []byte) int64 {
		defer close(begun)
		_ = r.dst.Send(head[:4<<20])
		_ = r.dst.Send(head[4<<20:])
		_ = r.dst.Send(early)
		return size - int64(len(early))
	}
	body := randomBytes(size)
	go func() {
		_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
		_, _ = r.srcPeer.Write(body[:size/2])
		<-sentDuring
		_, _ = r.srcPeer.Write(body[size/2:])
	}()
	<-begun
	// Begin has returned, and the splice's item is queued once its round
	// reads the socket.
	spliceWaitFor(t, "the splice to read", func() bool {
		r.dst.mu.Lock()
		defer r.dst.mu.Unlock()
		return r.dst.deadlines.spliceIn != nil && r.dst.deadlines.spliceIn.queued
	})
	_ = r.dst.Send([]byte("TAIL"))
	close(sentDuring)
	got := readUntil(t, r.dstPeer, len(head)+size+4)
	if !bytes.Equal(got[:len(head)], head) || !bytes.Equal(got[len(head):len(head)+size], body) || string(got[len(head)+size:]) != "TAIL" {
		t.Fatal("the destination's bytes came out of order")
	}
	if err := waitErr(t, r.writtenCh, "Written"); err != nil {
		t.Fatal(err)
	}
}

// A destination that reads slowly fills the pipe, which stops the source's
// reads until it drains, and a source that sends slowly leaves the
// destination waiting: both come back, and every byte arrives.
func TestSpliceToSlowPeers(t *testing.T) {
	const size = 6 << 20
	t.Run("slow destination", func(t *testing.T) {
		r := newSpliceRig(t, size)
		// Small buffers on the way out, so that the pipe fills while the
		// peer is not reading.
		_ = r.dst.SetWriteBuffer(64 << 10)
		_ = r.dstPeer.(*net.TCPConn).SetReadBuffer(64 << 10)
		paused := r.server.Stats().ReadsPausedByWatermark
		body := randomBytes(size)
		go func() { _, _ = r.srcPeer.Write(append([]byte("SPLICE\n"), body...)) }()
		got := make([]byte, 0, size)
		buf := make([]byte, 32<<10)
		_ = r.dstPeer.SetReadDeadline(time.Now().Add(30 * time.Second))
		pauses := 0
		for len(got) < size {
			if len(got) > (pauses+1)*size/4 && pauses < 3 {
				// Let the pipe and both sockets fill.
				time.Sleep(100 * time.Millisecond)
				pauses++
			}
			n, err := r.dstPeer.Read(buf)
			if err != nil {
				t.Fatalf("after %d bytes: %v; %s", len(got), err, r.state())
			}
			got = append(got, buf[:n]...)
		}
		if !bytes.Equal(got, body) {
			t.Fatal("the spliced bytes differ")
		}
		if err := waitErr(t, r.writtenCh, "Written"); err != nil {
			t.Fatal(err)
		}
		if r.server.Stats().ReadsPausedByWatermark == paused {
			t.Error("the source never waited for room in the pipe")
		}
	})
	t.Run("slow source", func(t *testing.T) {
		r := newSpliceRig(t, 256<<10)
		body := randomBytes(256 << 10)
		go func() {
			_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
			for off := 0; off < len(body); off += 8 << 10 {
				_, _ = r.srcPeer.Write(body[off : off+8<<10])
				time.Sleep(time.Millisecond)
			}
		}()
		if got := readUntil(t, r.dstPeer, len(body)); !bytes.Equal(got, body) {
			t.Fatal("the spliced bytes differ")
		}
		if err := waitErr(t, r.writtenCh, "Written"); err != nil {
			t.Fatal(err)
		}
	})
}

// A source that goes away mid-splice fails it: Read hears how much it took,
// Written why the rest will not come, and the destination, cut short, closes.
func TestSpliceToSourceGone(t *testing.T) {
	const size = 4 << 20
	r := newSpliceRig(t, size)
	half := randomBytes(size / 2)
	_, _ = r.srcPeer.Write(append([]byte("SPLICE\n"), half...))
	readUntil(t, r.dstPeer, len(half))
	r.srcPeer.Close()
	if err := waitErr(t, r.readErr, "Read"); err == nil {
		t.Fatal("Read heard no error")
	}
	if n := r.readN.Load(); n > size {
		t.Fatalf("Read took %d", n)
	}
	if err := waitErr(t, r.writtenCh, "Written"); err == nil {
		t.Fatal("Written heard no error")
	}
	waitErr(t, r.dstClosed, "the destination's close")
	waitErr(t, r.srcClosed, "the source's close")
	_ = r.dstPeer.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadAll(r.dstPeer); err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal(err)
	}
}

// A destination that goes away mid-splice fails it: the source's reads stay
// held, Read hearing how much the splice took, and once released the rest
// of what the peer sends reaches OnData, from the byte after the last one
// taken.
func TestSpliceToDestinationGone(t *testing.T) {
	const size = 8 << 20
	r := newSpliceRig(t, size)
	body := randomBytes(size)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
		_, _ = r.srcPeer.Write(body)
	}()
	readUntil(t, r.dstPeer, 64<<10)
	_ = r.dstPeer.(*net.TCPConn).SetLinger(0)
	r.dstPeer.Close()
	if err := waitErr(t, r.writtenCh, "Written"); err == nil {
		t.Fatal("Written heard no error")
	}
	if err := waitErr(t, r.readErr, "Read"); err == nil {
		t.Fatal("Read heard no error")
	}
	waitErr(t, r.dstClosed, "the destination's close")
	if !r.src.ReadsHeld() {
		t.Fatal("the source's reads are not held")
	}
	taken := r.readN.Load()
	r.src.HoldReads(false)
	<-sent
	r.mu.Lock()
	early := int64(len(r.head)) - int64(len("SPLICE\n"))
	r.mu.Unlock()
	want := size - early - taken
	spliceWaitFor(t, "the rest of the bytes", func() bool { return int64(len(r.afterBytes())) == want })
	if !bytes.Equal(r.afterBytes(), body[size-want:]) {
		t.Fatal("the bytes after the failed splice are not the rest of the stream")
	}
}

// Begin may give the splice up, and one with nothing to move ends at once.
func TestSpliceToBeginOutcomes(t *testing.T) {
	t.Run("aborted", func(t *testing.T) {
		r := newSpliceRig(t, 1<<20)
		r.begin = func(*spliceRig, []byte) int64 { return -1 }
		_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
		if err := waitErr(t, r.readErr, "Read"); !errors.Is(err, ErrSpliceAborted) {
			t.Fatalf("Read heard %v", err)
		}
		if err := waitErr(t, r.writtenCh, "Written"); !errors.Is(err, ErrSpliceAborted) {
			t.Fatalf("Written heard %v", err)
		}
		waitErr(t, r.dstClosed, "the destination's close")
		if !r.src.ReadsHeld() {
			t.Fatal("the source's reads are not held")
		}
		_, _ = r.srcPeer.Write([]byte("rest"))
		time.Sleep(20 * time.Millisecond)
		if len(r.afterBytes()) != 0 {
			t.Fatal("held reads went on")
		}
		r.src.HoldReads(false)
		spliceWaitFor(t, "the unspliced bytes", func() bool { return string(r.afterBytes()) == "rest" })
	})
	t.Run("nothing", func(t *testing.T) {
		r := newSpliceRig(t, 0)
		r.begin = func(r *spliceRig, early []byte) int64 {
			_ = r.dst.Send([]byte("HEAD"))
			return 0
		}
		_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
		if err := waitErr(t, r.readErr, "Read"); err != nil {
			t.Fatal(err)
		}
		if err := waitErr(t, r.writtenCh, "Written"); err != nil {
			t.Fatal(err)
		}
		if got := readUntil(t, r.dstPeer, 4); string(got) != "HEAD" {
			t.Fatalf("dst got %q", got)
		}
		_, _ = r.srcPeer.Write([]byte("next"))
		spliceWaitFor(t, "next", func() bool { return string(r.afterBytes()) == "next" })
	})
	t.Run("destination closed before Begin", func(t *testing.T) {
		r := newSpliceRig(t, 1<<20)
		r.begin = func(r *spliceRig, early []byte) int64 {
			r.dst.Close()
			spliceWaitFor(t, "the close", func() bool { return !r.dst.Splicing() })
			return 1 << 20
		}
		_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
		if err := waitErr(t, r.readErr, "Read"); err == nil {
			t.Fatal("Read heard no error")
		}
		if err := waitErr(t, r.writtenCh, "Written"); err == nil {
			t.Fatal("Written heard no error")
		}
	})
}

// SpliceTo refuses what it cannot splice, and a connection in a splice
// already.
func TestSpliceToRefuses(t *testing.T) {
	r := newSpliceRig(t, 1<<20)
	begin := func() int64 { return 1 }
	if err := r.src.SpliceTo(r.src, Splice{Begin: begin}); !errors.Is(err, ErrSpliceUnsupported) {
		t.Fatalf("onto itself: %v", err)
	}
	if err := r.src.SpliceTo(nil, Splice{Begin: begin}); !errors.Is(err, ErrSpliceUnsupported) {
		t.Fatalf("onto nothing: %v", err)
	}
	if err := r.src.SpliceTo(r.dst, Splice{}); !errors.Is(err, ErrSpliceUnsupported) {
		t.Fatalf("without Begin: %v", err)
	}
	if !r.src.Spliceable() {
		t.Fatal("a TCP connection is not spliceable")
	}
	_, _ = r.srcPeer.Write([]byte("SPLICE\n"))
	spliceWaitFor(t, "the splice", func() bool { return r.dst.Splicing() })
	if err := r.src.SpliceTo(r.dst, Splice{Begin: begin}); !errors.Is(err, ErrSpliceBusy) {
		t.Fatalf("a second splice from the source: %v", err)
	}
	// Another source onto the busy destination.
	other, _ := acceptOne(t, "tcp", HandlerFuncs{})
	if err := other.SpliceTo(r.dst, Splice{Begin: begin}); !errors.Is(err, ErrSpliceBusy) {
		t.Fatalf("a second splice onto the destination: %v", err)
	}
	closed, _ := acceptOne(t, "tcp", HandlerFuncs{})
	closed.Close()
	spliceWaitFor(t, "the close", func() bool {
		closed.mu.Lock()
		defer closed.mu.Unlock()
		return closed.closed
	})
	if err := closed.SpliceTo(other, Splice{Begin: begin}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("from a closed connection: %v", err)
	}
	if err := other.SpliceTo(closed, Splice{Begin: begin}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("onto a closed connection: %v", err)
	}
}

// Many splices at once each move their own bytes.
func TestSpliceToMany(t *testing.T) {
	const pairs, size = 32, 1 << 20
	var wg sync.WaitGroup
	for range pairs {
		r := newSpliceRig(t, size)
		body := randomBytes(size)
		wg.Add(1)
		go func() {
			defer wg.Done()
			go func() { _, _ = r.srcPeer.Write(append([]byte("SPLICE\n"), body...)) }()
			_ = r.dstPeer.SetReadDeadline(time.Now().Add(30 * time.Second))
			got := make([]byte, size)
			if _, err := io.ReadFull(r.dstPeer, got); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(got, body) {
				t.Error("the spliced bytes differ")
			}
			if err := waitErr(t, r.writtenCh, "Written"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

// The pool gives pipes back for reuse, refuses past its limit, and closes
// what it holds, and what comes back after, once it has closed.
func TestSplicePool(t *testing.T) {
	var p splicePool
	pipe, err := p.get()
	if err != nil {
		t.Fatal(err)
	}
	if pipe.size < splicePipeMin {
		t.Fatalf("a pipe of %d", pipe.size)
	}
	p.put(pipe, false)
	again, err := p.get()
	if err != nil || again != pipe {
		t.Fatalf("the idle pipe was not reused: %v", err)
	}
	p.put(again, true)
	if p.open != 0 {
		t.Fatalf("%d open after a discard", p.open)
	}
	p.open = maxSplicePipes
	if _, err := p.get(); !errors.Is(err, ErrSpliceBusy) {
		t.Fatalf("past the limit: %v", err)
	}
	p.open = 0
	a, _ := p.get()
	b, _ := p.get()
	p.put(a, false)
	p.close()
	p.put(b, false)
	if p.open != 0 || len(p.idle) != 0 {
		t.Fatalf("%d open, %d idle after the close", p.open, len(p.idle))
	}
	if _, err := p.get(); !errors.Is(err, ErrSpliceBusy) {
		t.Fatalf("after the close: %v", err)
	}
	if _, err := syscall.Write(a.w, []byte{1}); err == nil {
		t.Fatal("a closed pool's pipe is still open")
	}
}

// An engine that stops in the middle of a splice tells both callbacks.
func TestSpliceToEngineStop(t *testing.T) {
	r := newSpliceRig(t, 4<<20)
	_, _ = r.srcPeer.Write(append([]byte("SPLICE\n"), randomBytes(256<<10)...))
	spliceWaitFor(t, "the splice", func() bool { return r.dst.Splicing() })
	readUntil(t, r.dstPeer, 64<<10)
	r.stop()
	if err := waitErr(t, r.writtenCh, "Written"); err == nil {
		t.Fatal("Written heard no error")
	}
	if err := waitErr(t, r.readErr, "Read"); err == nil {
		t.Fatal("Read heard no error")
	}
}
