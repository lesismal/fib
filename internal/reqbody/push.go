package reqbody

import (
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"sync"

	"github.com/lesismal/fib/bufferpool"
)

// DefaultPushLimit is how much of a pushed body may wait in it, unsent,
// before OnDrain has its producer wait, when NewPusher is given no limit.
const DefaultPushLimit = 256 << 10

var (
	// ErrPushClosed is what Write reports once the body has been closed.
	ErrPushClosed = errors.New("http: write to a closed request body")
	// ErrPushStopped is what Write reports once the request the body belongs
	// to is over, whether it was answered, failed or abandoned: nothing more
	// of the body will be sent.
	ErrPushStopped = errors.New("http: request body no longer being sent")
)

// Sink is what a transport offers a pushed body to send it with. Its methods
// are called one at a time, by whichever goroutine pumps the body.
type Sink interface {
	// Room reports whether the transport can take more of the body now.
	// When it cannot, it arranges for Pusher.Kick to be called once it can,
	// and reports false.
	Room() bool
	// Send sends p, which it must not keep once it returns. An error means
	// the transport has failed, which it reports itself.
	Send(p []byte) error
	// End sends the end of the body, with its trailer, if any.
	End() error
	// Fail fails the request with err: the body itself failed, as its
	// producer said, or it was not as long as it declared.
	Fail(err error)
}

// Pusher is a request body that its producer pushes, a piece at a time,
// instead of one a transport reads, so that sending it takes no goroutine
// that waits: Write copies what it is given and returns at once, and the
// transport sends it as its flow control allows, from whichever goroutine
// gives it room, the producer's Write or the transport's own event. What
// waits to be sent is bounded by the producer, which OnDrain tells when to
// go on.
//
// It is an io.ReadCloser too, for a transport that reads a body rather than
// taking it pushed, such as net/http's: Read waits for what is written.
type Pusher struct {
	mu    sync.Mutex
	limit int
	// queue holds the pieces written and not yet sent, each a buffer from
	// package bufferpool; queued counts their bytes, and total every byte
	// ever written.
	queue  [][]byte
	queued int
	total  int64
	// closed is set by Close, failed by CloseWithError, and stopped once the
	// request is over. trailer is what CloseWithTrailer was given.
	closed  bool
	failed  error
	stopped error
	trailer stdhttp.Header
	// drains are the callbacks waiting in OnDrain.
	drains []func()
	// sink is the transport sending the body, and declared the length its
	// request declared, or -1. taken records that the sink has been given
	// some of it, sent what it has sent, and ended that the sink has been
	// told the body is over, one way or another.
	sink     Sink
	declared int64
	taken    bool
	sent     int64
	ended    bool
	// pumping is set while a goroutine hands the sink what it can take, and
	// again asks it to look once more before it lets go.
	pumping bool
	again   bool
	// readable wakes a Read waiting for the body.
	readable sync.Cond
	// reading is the piece a Read has taken part of.
	reading []byte
}

// NewPusher returns an empty body that lets limit bytes wait to be sent
// before OnDrain asks its producer to wait; zero means DefaultPushLimit.
func NewPusher(limit int) *Pusher {
	if limit <= 0 {
		limit = DefaultPushLimit
	}
	p := &Pusher{limit: limit, declared: -1}
	p.readable.L = &p.mu
	return p
}

// Write sends b, or queues a copy of it to be sent, and never waits. It reports
// ErrPushClosed after Close or CloseWithError, and ErrPushStopped once the
// request is over.
//
// b goes to the sink straight away, with no copy, when nothing written before
// it is waiting, no other goroutine is sending, and the sink has room, which
// is how a body whose transport keeps up goes: copied only where it has to
// wait.
func (p *Pusher) Write(b []byte) (int, error) {
	p.mu.Lock()
	switch {
	case p.stopped != nil:
		p.mu.Unlock()
		return 0, ErrPushStopped
	case p.closed || p.failed != nil:
		p.mu.Unlock()
		return 0, ErrPushClosed
	case len(b) == 0:
		p.mu.Unlock()
		return 0, nil
	}
	p.total += int64(len(b))
	if sink := p.sink; sink != nil && len(p.queue) == 0 && !p.pumping && !p.ended {
		// This goroutine pumps, as Kick would, for as long as it takes to
		// send b; a Kick meanwhile is left to it.
		p.pumping = true
		p.again = false
		p.mu.Unlock()
		sent := p.sendNow(sink, b)
		p.mu.Lock()
		if !sent {
			// The sink has no room, and kicks the body once it has.
			p.enqueueLocked(b)
		}
		p.pumpLocked()
		return len(b), nil
	}
	p.enqueueLocked(b)
	p.readable.Broadcast()
	p.mu.Unlock()
	p.Kick()
	return len(b), nil
}

// enqueueLocked queues a copy of b, taken from package bufferpool.
func (p *Pusher) enqueueLocked(b []byte) {
	piece := bufferpool.Get(len(b))[:len(b)]
	copy(piece, b)
	p.queue = append(p.queue, piece)
	p.queued += len(b)
}

// sendNow sends b, which nothing waits ahead of, through sink when it has room,
// and reports whether b is done with: sent, or not wanted any more. Only the
// goroutine that set pumping calls it.
func (p *Pusher) sendNow(sink Sink, b []byte) bool {
	if !sink.Room() {
		return false
	}
	p.mu.Lock()
	if p.stopped != nil || p.ended {
		p.mu.Unlock()
		return true
	}
	p.sent += int64(len(b))
	p.taken = true
	over := p.declared >= 0 && p.sent > p.declared
	if over {
		p.ended = true
	}
	p.mu.Unlock()
	if over {
		sink.Fail(fmt.Errorf("http: ContentLength=%d with Body length at least %d", p.declared, p.sent))
		return true
	}
	if err := sink.Send(b); err != nil {
		// The transport failed, which it reports.
		p.mu.Lock()
		p.ended = true
		p.mu.Unlock()
	}
	return true
}

// Close ends the body once what has been written is sent. It always
// reports nil.
func (p *Pusher) Close() error {
	p.mu.Lock()
	p.closed = true
	p.readable.Broadcast()
	p.mu.Unlock()
	p.Kick()
	return nil
}

// CloseWithTrailer is Close, with trailer sent after the body in place of
// its request's Trailer.
func (p *Pusher) CloseWithTrailer(trailer stdhttp.Header) error {
	p.mu.Lock()
	if !p.closed && p.failed == nil {
		p.trailer = trailer
	}
	p.mu.Unlock()
	return p.Close()
}

// Trailer is the trailer to send after the body: what CloseWithTrailer was
// given, or else fallback, the request's Trailer.
func (p *Pusher) Trailer(fallback stdhttp.Header) stdhttp.Header {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.trailer != nil {
		return p.trailer
	}
	return fallback
}

// CloseWithError ends the body short, which fails its request with err.
func (p *Pusher) CloseWithError(err error) {
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	p.mu.Lock()
	if p.failed == nil && !p.closed {
		p.failed = err
	}
	p.readable.Broadcast()
	p.mu.Unlock()
	p.Kick()
}

// Buffered reports how many bytes written are waiting to be sent.
func (p *Pusher) Buffered() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queued
}

// OnDrain paces the producer: while more than the body's limit waits to be
// sent it arranges for fn to run once no more than half of it does, or once
// the request is over, and reports true; otherwise it reports false and fn
// never runs. fn runs on the goroutine that sent the body down to that, the
// producer's or the transport's, and must not wait.
func (p *Pusher) OnDrain(fn func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queued <= p.limit || p.stopped != nil {
		return false
	}
	p.drains = append(p.drains, fn)
	return true
}

// takeDrainsLocked returns the OnDrain callbacks that are due: all of them
// once the request is over, and otherwise once half the limit is reached.
func (p *Pusher) takeDrainsLocked() []func() {
	if len(p.drains) == 0 || p.stopped == nil && p.queued > p.limit/2 {
		return nil
	}
	drains := p.drains
	p.drains = nil
	return drains
}

// Attach makes sink the transport that sends the body, for a request that
// declared length (-1 for none), and starts sending what is there. It reports
// ErrNotReplayable when another sink was given some of the body already, as
// when a request is sent again after its connection failed.
func (p *Pusher) Attach(sink Sink, declared int64) error {
	p.mu.Lock()
	if p.taken {
		p.mu.Unlock()
		return ErrNotReplayable
	}
	p.sink, p.declared = sink, declared
	p.mu.Unlock()
	p.Kick()
	return nil
}

// Stop ends the sending, once the request is over: what still waits is
// dropped, the producer's writes fail from here on, and its OnDrain
// callbacks run so that it does not wait for the body to drain.
func (p *Pusher) Stop() {
	p.mu.Lock()
	if p.stopped != nil {
		p.mu.Unlock()
		return
	}
	p.stopped = ErrPushStopped
	p.sink = nil
	for _, piece := range p.queue {
		bufferpool.Put(piece)
	}
	clear(p.queue)
	p.queue, p.queued = p.queue[:0], 0
	drains := p.takeDrainsLocked()
	p.readable.Broadcast()
	p.mu.Unlock()
	for _, fn := range drains {
		fn()
	}
}

// Kick hands the sink what it can take of the body. Whichever goroutine finds
// another pumping leaves it to that one, which looks once more before it lets
// go, so that nothing written, and no room made, is missed.
func (p *Pusher) Kick() {
	p.mu.Lock()
	if p.pumping {
		p.again = true
		p.mu.Unlock()
		return
	}
	p.pumping = true
	p.again = true
	p.pumpLocked()
}

// pumpLocked pumps for as long as another goroutine asks for one more look,
// and then lets go of pumping and of p.mu, which the caller holds, having set
// pumping.
func (p *Pusher) pumpLocked() {
	for p.again {
		p.again = false
		sink := p.sink
		if sink == nil || p.ended || p.stopped != nil {
			break
		}
		p.mu.Unlock()
		p.pump(sink)
		p.mu.Lock()
	}
	p.pumping = false
	p.mu.Unlock()
}

// pump sends pieces while the sink has room, and the end of the body once it
// has all gone. Only the goroutine that set pumping runs it.
func (p *Pusher) pump(sink Sink) {
	for {
		p.mu.Lock()
		if p.stopped != nil || p.ended {
			p.mu.Unlock()
			return
		}
		if p.failed != nil {
			// A body that failed fails its request now, whatever of it is
			// still waiting for room that may never come.
			err := p.failed
			p.ended = true
			for _, piece := range p.queue {
				bufferpool.Put(piece)
			}
			clear(p.queue)
			p.queue, p.queued = p.queue[:0], 0
			drains := p.takeDrainsLocked()
			p.mu.Unlock()
			for _, fn := range drains {
				fn()
			}
			sink.Fail(err)
			return
		}
		if len(p.queue) == 0 {
			var err error
			switch {
			case !p.closed:
				p.mu.Unlock()
				return
			case p.declared >= 0 && p.sent != p.declared:
				err = fmt.Errorf("http: ContentLength=%d with Body length %d", p.declared, p.sent)
			}
			if err != nil {
				p.ended = true
				p.mu.Unlock()
				sink.Fail(err)
				return
			}
			p.mu.Unlock()
			// The end goes out after what the transport still holds of the
			// body, so it waits for room as a piece would.
			if !sink.Room() {
				return
			}
			p.mu.Lock()
			if p.stopped != nil || p.ended {
				p.mu.Unlock()
				return
			}
			p.ended = true
			p.mu.Unlock()
			_ = sink.End()
			return
		}
		p.mu.Unlock()
		if !sink.Room() {
			return
		}
		p.mu.Lock()
		if p.stopped != nil || len(p.queue) == 0 {
			p.mu.Unlock()
			continue
		}
		piece := p.queue[0]
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.queued -= len(piece)
		p.sent += int64(len(piece))
		p.taken = true
		over := p.declared >= 0 && p.sent > p.declared
		if over {
			p.ended = true
		}
		drains := p.takeDrainsLocked()
		p.mu.Unlock()
		for _, fn := range drains {
			fn()
		}
		if over {
			bufferpool.Put(piece)
			sink.Fail(fmt.Errorf("http: ContentLength=%d with Body length at least %d", p.declared, p.sent))
			return
		}
		err := sink.Send(piece)
		bufferpool.Put(piece)
		if err != nil {
			// The transport failed, which it reports.
			p.mu.Lock()
			p.ended = true
			p.mu.Unlock()
			return
		}
	}
}

// Read is for a transport that reads the body rather than taking it pushed:
// it waits for what is written, and reports io.EOF once the body is closed
// and read, the error CloseWithError was given, or ErrPushStopped.
func (p *Pusher) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if len(p.reading) == 0 && len(p.queue) > 0 {
			p.reading = p.queue[0]
			p.queue[0] = nil
			p.queue = p.queue[1:]
		}
		if len(p.reading) > 0 {
			n := copy(b, p.reading)
			p.reading = p.reading[n:]
			p.queued -= n
			if len(p.reading) == 0 {
				p.reading = nil
			}
			p.taken = true
			drains := p.takeDrainsLocked()
			if len(drains) > 0 {
				p.mu.Unlock()
				for _, fn := range drains {
					fn()
				}
				p.mu.Lock()
			}
			return n, nil
		}
		switch {
		case p.stopped != nil:
			return 0, p.stopped
		case p.failed != nil:
			return 0, p.failed
		case p.closed:
			return 0, io.EOF
		}
		p.readable.Wait()
	}
}
