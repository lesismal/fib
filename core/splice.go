//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/lesismal/fib/sidepool"
)

var (
	// ErrSpliceUnsupported is what SpliceTo reports for a pair of connections
	// it cannot splice between: on a system without splice(2), or where
	// either side has a layer, such as TLS, that its bytes have to pass
	// through, or is UDP.
	ErrSpliceUnsupported = errors.New("fib: splice not supported for this connection")
	// ErrSpliceBusy is what SpliceTo reports when either connection is in a
	// splice already, or the engine has as many pipes open as it allows.
	ErrSpliceBusy = errors.New("fib: no splice available")
	// ErrSpliceAborted is what a splice whose Begin gave it up reports.
	ErrSpliceAborted = errors.New("fib: splice given up")
)

// Splice is what Connection.SpliceTo moves, and who it tells how that went.
type Splice struct {
	// Begin runs on the source connection's round once the splice has taken
	// its reads over, before anything more is read from its socket: nothing
	// is being handed to OnData then. It sends the destination whatever has
	// to go ahead of the spliced bytes, which are queued behind it, and
	// returns how many bytes of the source to move, or a negative count to
	// give the splice up.
	Begin func() int64
	// Read is told how many bytes have left the source's socket, on the
	// source's round, once the last of them has, with a nil err, or once the
	// rest never will: the source failed, ended or closed, the destination
	// failed, or Begin gave the splice up. The source's reads go back to
	// OnData after a splice that finished, from the byte that follows it,
	// and stay held after one that failed, as HoldReads(true) holds them,
	// for the caller to release or to close the connection. A splice that
	// finished has its Read called before the last of its bytes is handed to
	// the destination, so that nothing that answers them can come first.
	Read func(n int64, err error)
	// Written is told once every byte has reached the destination's socket,
	// with a nil err, or why they will not. A splice that cannot finish
	// closes the destination, whose stream it has cut in the middle.
	Written func(err error)
}

// SpliceTo moves the next bytes that arrive on c to dst's socket through a
// pipe, with splice(2), so that they never pass through user space: no read
// copies them out of c's socket, and no write copies them into fresh pages of
// dst's. They go after whatever dst has queued, and whatever is sent on dst
// meanwhile goes after them.
//
// It never waits. From c's next round, which it asks for, its reads go to
// the splice rather than to OnData: s.Begin says how many bytes to move, and
// each round moves what has arrived into the pipe and on to dst, stopping
// where either side has nothing more for now; dst's socket, as it drains,
// takes the rest. The pipe paces the two: while it is full, c reads no more,
// and the peer is held back by TCP. Exactly that many bytes are moved, so
// what follows them on c is handed to OnData as ever, and s.Read and
// s.Written report how it ended.
//
// It is for Linux, for TCP and Unix connections with nothing layered on
// them; elsewhere, or for such a pair as it cannot splice between, it reports
// ErrSpliceUnsupported, and for a connection in a splice already, or when the
// engine has as many pipes as it allows, ErrSpliceBusy. A caller that gets an
// error moves the bytes the ordinary way; s's callbacks are never called
// then.
func (c *Connection) SpliceTo(dst *Connection, s Splice) error {
	if dst == nil || dst == c || s.Begin == nil || !c.Spliceable() || !dst.Spliceable() {
		return ErrSpliceUnsupported
	}
	pool := &c.engine.splicePipes
	pipe, err := pool.get()
	if err != nil {
		return err
	}
	j := &spliceJob{src: c, dst: dst, cb: s, pipe: pipe, dstHeld: true}
	j.refs.Store(2)
	// The source is claimed first and the destination second, each under its
	// own lock, and the source's reads are turned over only once both are: a
	// close that comes before then finds the job not live and lets it go
	// without a word to the caller, who hears of it from the error instead.
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		pool.put(pipe, false)
		return net.ErrClosed
	}
	if d := c.deadlines; d != nil && d.spliceOut != nil {
		c.mu.Unlock()
		pool.put(pipe, false)
		return ErrSpliceBusy
	}
	c.ensureDeadlinesLocked().spliceOut = j
	c.mu.Unlock()

	dst.mu.Lock()
	switch d := dst.deadlines; {
	case dst.closing || dst.closed:
		err = net.ErrClosed
	case d != nil && d.spliceIn != nil:
		err = ErrSpliceBusy
	default:
		dst.ensureDeadlinesLocked().spliceIn = j
	}
	dst.mu.Unlock()

	c.mu.Lock()
	if err == nil && (c.closing || c.closed || c.deadlines.spliceOut != j) {
		err = net.ErrClosed
	}
	if err != nil {
		if d := c.deadlines; d != nil && d.spliceOut == j {
			d.spliceOut = nil
		}
		c.mu.Unlock()
		dst.mu.Lock()
		if d := dst.deadlines; d != nil && d.spliceIn == j {
			d.spliceIn = nil
		}
		dst.mu.Unlock()
		pool.put(pipe, false)
		return err
	}
	j.live.Store(true)
	c.flags.set(flagSplicing, true)
	c.mu.Unlock()
	// The round that runs Begin may be the one in progress, which takes the
	// splice up before its next read, or one this asks for.
	c.stallRead()
	return nil
}

// Spliceable reports whether c can be either side of a splice: on a system
// with splice(2), a TCP or Unix connection with no layer, such as TLS,
// between its socket and its handler.
func (c *Connection) Spliceable() bool {
	return spliceSupported && c.udp == nil && c.layer == nil && !c.flags.has(flagKernelTLS)
}

// Splicing reports whether a splice into c, as SpliceTo's destination, still
// has bytes to write to its socket. It waits for a write in flight, so that it
// never reports true for a splice whose last write has gone: a response that
// answers the spliced bytes can arrive before the call that wrote them has
// returned.
func (c *Connection) Splicing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadlines != nil && c.deadlines.spliceIn != nil
}

// ensureDeadlinesLocked returns the connection's deadlines, allocating them.
// Callers hold c.mu.
func (c *Connection) ensureDeadlinesLocked() *connectionDeadlines {
	if c.deadlines == nil {
		c.deadlines = &connectionDeadlines{}
	}
	return c.deadlines
}

// spliceRecord marks the item a splice queues on its destination: where it
// stands in the queue, the spliced bytes go, from the destination's
// connectionDeadlines.spliceIn. It is no TLS record type.
const spliceRecord uint8 = 0xff

// spliceJob is a splice between two connections, from SpliceTo to its last
// callback.
type spliceJob struct {
	src, dst *Connection
	cb       Splice
	pipe     splicePipe
	// live says SpliceTo has handed the source's reads over, after which
	// every end of the job is reported to its callbacks.
	live atomic.Bool
	// refs counts the sides still using the pipe, the source's and the
	// destination's; the last to let go gives it back (see release).
	refs atomic.Int32
	// readDone and writeDone record that Read and Written have been called.
	readDone  atomic.Bool
	writeDone atomic.Bool

	// The source's side, which only its rounds, and its close, which runs as
	// its last round, touch: begun records that Begin ran, left is what is
	// still to come off the source, and taken what has.
	begun bool
	left  int64
	taken int64

	// The rest is guarded by dst.mu. inPipe is what the pipe holds, which
	// the destination writes; srcDone records that the source has put its
	// last byte in it, queued that the destination's queue holds the job's
	// item, dstHeld that the destination has not let go of the job yet,
	// waiting that the source stopped for room in the pipe, and err is the
	// first failure.
	inPipe  int
	srcDone bool
	queued  bool
	dstHeld bool
	waiting bool
	err     error
}

// release lets go of one side's use of the pipe. The last gives it back to
// the pool, unless something failed: a pipe that may still hold bytes is
// closed instead.
func (j *spliceJob) release() {
	if j.refs.Add(-1) == 0 {
		j.src.engine.splicePipes.put(j.pipe, j.err != nil || j.inPipe != 0)
	}
}

// endRead tells Read how the source's side ended, once.
func (j *spliceJob) endRead(err error) {
	if j.readDone.CompareAndSwap(false, true) && j.cb.Read != nil {
		j.cb.Read(j.taken, err)
	}
}

// endWrite tells Written how the destination's side ended, once.
func (j *spliceJob) endWrite(err error) {
	if j.writeDone.CompareAndSwap(false, true) && j.cb.Written != nil {
		j.cb.Written(err)
	}
}

// failLocked records err as the job's failure, unless it has one. Callers
// hold dst.mu.
func (j *spliceJob) failLocked(err error) {
	if j.err == nil {
		j.err = err
	}
}

// detachDstLocked has the destination let go of the job, and reports whether
// it held it, in which case the caller lets go of its use of the pipe.
// Callers hold dst.mu.
func (j *spliceJob) detachDstLocked() bool {
	if !j.dstHeld {
		return false
	}
	j.dstHeld = false
	if d := j.dst.deadlines; d != nil && d.spliceIn == j {
		d.spliceIn = nil
	}
	return true
}

// detachSource ends the source's part in the job: its reads are its own
// again, held when held says so. It reports false when the source was no
// longer the job's, as its close may have made it.
func (c *Connection) detachSource(j *spliceJob, held bool) bool {
	c.mu.Lock()
	ours := c.deadlines != nil && c.deadlines.spliceOut == j
	if ours {
		c.deadlines.spliceOut = nil
		if held {
			c.flags.set(flagReadHeld, true)
		}
		c.flags.set(flagSplicing, false)
		c.flags.set(flagSpliceWait, false)
	}
	c.mu.Unlock()
	return ours
}

// failSource ends a job that failed on the source's side, err being why, on
// its round or as it closes, once the source has been detached: Read and
// Written hear of it, and the destination, whose stream has been cut, is
// closed.
func (j *spliceJob) failSource(err error) {
	dst := j.dst
	dst.mu.Lock()
	j.failLocked(err)
	err = j.err
	// A job whose item is queued is let go of by the destination's close,
	// which drops the item; one with none is let go of here.
	detached := !j.queued && j.detachDstLocked()
	dst.mu.Unlock()
	j.endRead(err)
	dst.closeWithError(err)
	j.endWrite(err)
	if detached {
		j.release()
	}
	j.release()
}

// abandonSource ends the source's side of a job that failed on the
// destination's: its reads stay held, and Read hears why.
func (j *spliceJob) abandonSource(err error) {
	if j.src.detachSource(j, true) {
		j.endRead(err)
		j.release()
	}
}

// dropSpliceInLocked lets go of a splice into c as c closes, with err as the
// reason, once markClosedLocked has dropped its item. Callers hold c.mu.
func (c *Connection) dropSpliceInLocked(err error) {
	d := c.deadlines
	if d == nil || d.spliceIn == nil {
		return
	}
	j := d.spliceIn
	if !j.live.Load() {
		// SpliceTo has not handed it out: if it does, the source's round
		// finds the destination gone, and lets go of the job then.
		d.spliceIn = nil
		return
	}
	if err == nil || err == io.EOF {
		err = net.ErrClosed
	}
	j.failLocked(err)
	j.queued = false
	j.detachDstLocked()
	err = j.err
	// Neither callback may run under the lock, nor on the loop, which may be
	// where this runs: Written runs on the side pool, and the source is told
	// on its round, which this asks for.
	sidepool.Post(func() {
		j.endWrite(err)
		j.release()
		src := j.src
		src.mu.Lock()
		ours := src.deadlines != nil && src.deadlines.spliceOut == j
		src.mu.Unlock()
		if ours {
			// Once err is set the source stops for room no more, so this is
			// the last of its waits.
			src.flags.set(flagSpliceWait, false)
			src.stallRead()
		}
	})
}

// endSpliceOnClose ends the splice c is the source of as c closes, with err
// as the reason, before OnClose: no round runs then.
func (c *Connection) endSpliceOnClose(err error) {
	c.mu.Lock()
	var j *spliceJob
	if d := c.deadlines; d != nil && d.spliceOut != nil {
		j = d.spliceOut
		d.spliceOut = nil
	}
	c.mu.Unlock()
	if j == nil || !j.live.Load() {
		return
	}
	if err == nil {
		err = net.ErrClosed
	} else if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	j.failSource(err)
}

// spliceRound is a read round of a connection whose reads a splice has: it
// moves what has arrived into the pipe and on to the destination. It reports
// more once the splice has finished, when the round goes on reading for
// OnData, and otherwise the error that ends the connection, if any.
func (c *Connection) spliceRound() (more bool, err error) {
	c.mu.Lock()
	var j *spliceJob
	if d := c.deadlines; d != nil {
		j = d.spliceOut
	}
	c.mu.Unlock()
	if j == nil {
		c.flags.set(flagSplicing, false)
		return true, nil
	}
	if c.flags.has(flagSpliceWait) {
		// The pipe is full; the destination's drain hands the read back.
		c.roundStalled = true
		c.stallRead()
		return false, nil
	}
	if !j.begun {
		if done, more := c.beginSplice(j); done {
			return more, nil
		}
	}
	dst := j.dst
	for {
		dst.mu.Lock()
		failed, room := j.err, j.pipe.size-j.inPipe
		dst.mu.Unlock()
		if failed != nil {
			j.abandonSource(failed)
			c.roundStalled = true
			c.stallRead()
			return false, nil
		}
		if room <= 0 {
			if c.spliceWait(j) == spliceStalled {
				return false, nil
			}
			continue
		}
		m, err := spliceIn(c.FD(), j.pipe.w, int(min(j.left, int64(room))))
		switch {
		case m > 0:
			j.left -= int64(m)
			j.taken += int64(m)
			if j.left == 0 {
				return c.spliceTaken(j, m), nil
			}
			j.publish(m, false)
		case err == syscall.EINTR:
		case err == syscall.EAGAIN:
			// The socket has nothing more, or the pipe has no room, which
			// splice does not tell apart: an empty pipe says it was the
			// socket, and the next edge brings more.
			switch c.spliceWait(j) {
			case spliceStalled, spliceEmpty:
				return false, nil
			}
		default:
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			if c.detachSource(j, false) {
				j.failSource(err)
			}
			if err == io.ErrUnexpectedEOF {
				return false, io.EOF
			}
			return false, err
		}
	}
}

// beginSplice runs Begin and queues the job's item on the destination, behind
// whatever Begin sent it. It reports done when the round has nothing more to
// splice, and then more as spliceRound does.
func (c *Connection) beginSplice(j *spliceJob) (done, more bool) {
	j.begun = true
	n := j.cb.Begin()
	dst := j.dst
	var err error
	if n < 0 {
		err = ErrSpliceAborted
	} else {
		dst.mu.Lock()
		if dst.closing || dst.closed || !j.dstHeld {
			err = net.ErrClosed
		} else {
			j.left = n
			j.queued = true
			dst.pushLocked(sendItem{record: spliceRecord})
		}
		dst.mu.Unlock()
	}
	if err != nil {
		if c.detachSource(j, true) {
			j.failSource(err)
		}
		c.roundStalled = true
		c.stallRead()
		return true, false
	}
	if n == 0 {
		return true, c.spliceTaken(j, 0)
	}
	return false, false
}

// spliceTaken ends the source's side of a job whose last m bytes have just
// gone into the pipe: Read hears of it before those bytes are handed to the
// destination, so that whatever answers them, which can only come once they
// have been written, finds the source done with. It reports whether the
// round goes on reading for OnData.
func (c *Connection) spliceTaken(j *spliceJob, m int) bool {
	c.detachSource(j, false)
	j.endRead(nil)
	j.publish(m, true)
	j.release()
	return true
}

// publish adds m bytes put in the pipe to what the destination writes, last
// marking the source's last, and has the destination write them now unless
// another goroutine is writing it or a round of its own holds its output,
// either of which writes them before it lets go.
func (j *spliceJob) publish(m int, last bool) {
	dst := j.dst
	dst.mu.Lock()
	j.inPipe += m
	if last {
		j.srcDone = true
	}
	if dst.closing || dst.closed || dst.flushing || dst.corked || dst.sendHead == len(dst.sends) {
		dst.mu.Unlock()
		return
	}
	// A failed write closes dst, which tells the job.
	_ = dst.flushLocked()
}

// The outcomes of spliceWait.
const (
	// spliceStalled stopped the round for room in the pipe.
	spliceStalled = iota
	// spliceEmpty found the pipe empty: there is room, and what would block
	// was the socket.
	spliceEmpty
	// spliceFailed found the job failed, which the round's next look ends.
	spliceFailed
)

// spliceWait stops the source's round for room in the pipe, unless the pipe
// has emptied meanwhile or the job has failed; see its outcomes.
func (c *Connection) spliceWait(j *spliceJob) int {
	dst := j.dst
	dst.mu.Lock()
	switch {
	case j.err != nil:
		dst.mu.Unlock()
		return spliceFailed
	case j.inPipe == 0:
		dst.mu.Unlock()
		return spliceEmpty
	}
	j.waiting = true
	c.flags.set(flagSpliceWait, true)
	dst.mu.Unlock()
	c.roundStalled = true
	c.stallRead()
	return spliceStalled
}

// flushSpliceLocked writes what the pipe of the splice at the head of c's
// queue holds. It reports progress, after which the flush goes on, or the
// error that stops it: EAGAIN for a socket that is full, or for a pipe that is
// empty with the source still to fill it, which has the destination written
// as it does (see publish); or the socket's failure. Callers hold c.mu, which
// it lets go of and takes again around the job's last callback.
func (c *Connection) flushSpliceLocked() (bool, error) {
	j := c.deadlines.spliceIn
	if j.inPipe == 0 {
		if !j.srcDone {
			return false, syscall.EAGAIN
		}
		c.sends[c.sendHead] = sendItem{}
		c.sendHead++
		j.queued = false
		j.detachDstLocked()
		c.mu.Unlock()
		j.endWrite(nil)
		j.release()
		c.mu.Lock()
		return true, nil
	}
	for {
		m, err := spliceOut(j.pipe.r, c.FD(), j.inPipe)
		if m > 0 {
			j.inPipe -= m
			if j.waiting {
				// The source stopped for room, which there is now.
				j.waiting = false
				j.src.flags.set(flagSpliceWait, false)
				j.src.engine.request(command{kind: commandRefresh, connection: j.src})
			}
			return true, nil
		}
		if err == syscall.EINTR {
			continue
		}
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return false, err
	}
}

// splicePipe is a pipe a splice moves its bytes through: r its read end, w its
// write end, and size how much it holds.
type splicePipe struct {
	r, w int
	size int
}

// maxSplicePipes bounds the pipes an engine, or each of its pollers, has open
// at once, each two descriptors: a splice that would need one more is refused
// with ErrSpliceBusy, and its bytes go the ordinary way.
const maxSplicePipes = 256

// splicePool keeps an engine's idle pipes for the splices to come, and closes
// them as the engine closes.
type splicePool struct {
	mu     sync.Mutex
	idle   []splicePipe
	open   int
	closed bool
}

// get takes an idle pipe, or opens one.
func (p *splicePool) get() (splicePipe, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		pipe := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return pipe, nil
	}
	if p.closed || p.open >= maxSplicePipes {
		p.mu.Unlock()
		return splicePipe{}, ErrSpliceBusy
	}
	p.open++
	p.mu.Unlock()
	pipe, err := openSplicePipe()
	if err != nil {
		p.mu.Lock()
		p.open--
		p.mu.Unlock()
		return splicePipe{}, err
	}
	return pipe, nil
}

// put gives a pipe back, or closes it when discard says it may hold bytes, or
// when the pool has closed.
func (p *splicePool) put(pipe splicePipe, discard bool) {
	p.mu.Lock()
	if discard || p.closed {
		p.open--
		p.mu.Unlock()
		closeSplicePipe(pipe)
		return
	}
	p.idle = append(p.idle, pipe)
	p.mu.Unlock()
}

// close closes the idle pipes, and those given back after.
func (p *splicePool) close() {
	p.mu.Lock()
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.open -= len(idle)
	p.mu.Unlock()
	for _, pipe := range idle {
		closeSplicePipe(pipe)
	}
}
