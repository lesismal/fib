//go:build linux || darwin || windows

package http

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// serverState is what a Parser keeps for the server driving it: the request
// being served, reachable without the parser's lock so that a close on the
// event-loop goroutine can cancel a request whose handler is still working on
// it.
type serverState struct {
	liveContext atomic.Pointer[Context]
}

func (p *Parser) resetServerState() { p.liveContext.Store(nil) }

// blockServerState is the part of a requestBlock a server serves the request
// with: the Context it is answered through.
type blockServerState struct {
	context Context
}

func (b *requestBlock) recycleServerState() { b.context.recycle() }

// A Context's lifetime is one atomic word, which the ordinary path through a
// request — begin, the handler's return, settle, and any Retain and Release —
// moves on with a compare-and-swap rather than under a lock. mu is left
// guarding what hands a callback from one goroutine to another: err, body,
// bodyDone and cancel.
//
// Two counts live in it, since a response and the objects it is written from
// do not end together. The response is held open by the handler's share and
// by every Retain not yet released, and is written when the last of them goes,
// as Retain describes. The Context, and the request and body it serves, stay
// the handler's for as long as anyone may still be using them: until the
// handler has returned, every Retain has been released — including one taken
// before the connection went and released after, when the response is long
// past — the connection has done with the request, and a cancellation or a
// response in the middle of being written has finished. Only then does the
// body's buffer go back, and do the Reuse options recycle anything.
const (
	// ctxHolds counts the Retains not yet released.
	ctxHolds uint64 = 1<<16 - 1
	// ctxServed marks a handler that has not returned yet, and ctxShare its
	// hold on the response, which its return gives back unless a Release too
	// many gave it back first.
	ctxServed uint64 = 1 << 16
	ctxShare  uint64 = 1 << 17
	// ctxConn marks a request the connection has not finished with yet.
	ctxConn uint64 = 1 << 18
	// ctxCancelling and ctxFinishing mark a cancellation and the writing of a
	// finished response that are still going on, on whichever goroutine is
	// doing them.
	ctxCancelling uint64 = 1 << 19
	ctxFinishing  uint64 = 1 << 20
	// ctxResume marks the connection's duty to carry on, left by settle for
	// whoever finishes the response, and ctxDelivering a body callback
	// running on the connection's worker, which cannot take that duty itself.
	ctxResume     uint64 = 1 << 21
	ctxDelivering uint64 = 1 << 22
	// ctxHandled marks a handler that has returned, once the body it asked
	// for while it ran has been handed over, and ctxHandover a handover owed
	// then; see later.
	ctxHandled  uint64 = 1 << 23
	ctxHandover uint64 = 1 << 24
	// The state takes two bits, and the generation the rest.
	ctxStateShift        = 25
	ctxStateMask  uint64 = 3 << ctxStateShift
	ctxGenShift          = 27

	// ctxAlive is what keeps the Context the request's.
	ctxAlive = ctxHolds | ctxServed | ctxConn | ctxCancelling | ctxFinishing
)

// The states a response is in: ctxOpen is still being written, ctxDone has
// been, and ctxCancelled is one whose connection went first. A fresh Context,
// whose word is zero, is open.
const (
	ctxOpen uint64 = iota
	ctxDone
	ctxCancelled
)

func ctxState(w uint64) uint64 { return w & ctxStateMask >> ctxStateShift }

func withState(w, state uint64) uint64 { return w&^ctxStateMask | state<<ctxStateShift }

// generation is the one the Context is serving, which a Context recycled
// since has moved past; see recycle.
func (c *Context) generation() uint64 { return c.word.Load() >> ctxGenShift }

// drop clears bits from the word, and ends the Context if nothing is left
// keeping it.
func (c *Context) drop(bits uint64) {
	for {
		w := c.word.Load()
		next := w &^ bits
		if c.word.CompareAndSwap(w, next) {
			if w&ctxAlive != 0 && next&ctxAlive == 0 {
				c.ended()
			}
			return
		}
	}
}

// ended runs once nothing can reach the Context any more but the server: the
// body's buffer goes back, and whatever the Reuse options recycle goes too.
func (c *Context) ended() {
	if c.whole != nil {
		c.whole.release()
	}
	if c.server != nil {
		c.server.recycle(c)
	}
}

// contextPool recycles Contexts on their own, for a server that recycles its
// Contexts but not its requests; see Config.ReuseContexts.
var contextPool = sync.Pool{New: func() any { return &Context{pooled: true} }}

// recycle clears the Context for another request. Its generation moves on,
// so that a cancellation meant for the request it served, which OnClose makes
// from a goroutine of its own, finds it serving another and leaves it alone.
// Every field but mu, deliverMu, word and pooled is reset, which
// TestContextRecycleResetsEveryField holds it to.
//
// A recycled Context waiting for its next request reads as finished, so that
// a handler that wrongly holds on to it past its end finds that Retain,
// Release and the rest do nothing, as they do on any finished response,
// rather than acting on a Context with no connection; reopen hands it out.
func (c *Context) recycle() {
	gen := c.generation() + 1
	c.word.Store(withState(gen<<ctxGenShift, ctxDone))
	c.Conn, c.Request = nil, nil
	c.wrote, c.closing, c.streamed = true, false, false
	c.w, c.stream, c.external = nil, nil, nil
	c.err, c.body, c.bodyDone, c.cancel = nil, nil, false, nil
	c.bodyHeld, c.handover = false, nil
	c.server, c.parser, c.whole, c.block = nil, nil, nil, nil
}

// reopen readies a recycled Context for the request it is about to serve.
func (c *Context) reopen() {
	c.word.Store(c.generation() << ctxGenShift)
	c.wrote = false
}

// Retain keeps the response open past the handler's return, so that the
// request can be answered later, from another goroutine or from the body
// callbacks OnBody delivers. Every Retain needs a Release; the response is
// finished and handed to the connection when the last hold goes, which is
// the handler's own return when it retained nothing.
//
// On an HTTP/1 connection a retained request holds the ones pipelined behind
// it: they are parsed and served once it is released, so responses keep their
// order. Nothing bounds how long a request may be retained, so a handler that
// never releases holds its connection until the peer or a timeout ends it.
//
// Retaining a request whose response is already written, or whose connection
// has already gone, does nothing.
//
// A handler may hold at most 65535 Retains on one request at a time.
func (c *Context) Retain() { c.retain() }

// retain is Retain, reporting whether it took a hold.
func (c *Context) retain() bool {
	for {
		w := c.word.Load()
		if ctxState(w) != ctxOpen {
			return false
		}
		if w&ctxHolds == ctxHolds {
			panic("http: too many Retains on one request")
		}
		if c.word.CompareAndSwap(w, w+1) {
			return true
		}
	}
}

// Release gives back one hold taken by Retain. The last one to go finishes
// the response, hands it to the connection and lets the connection carry on
// with whatever is pipelined behind it.
//
// Releasing more often than retaining ends the response early: the hold that
// serving the request took is given back by the handler's return, so a
// release that is not undoing a Retain is undoing that one. Releasing a
// response that is already written, or one whose connection has gone, writes
// nothing, so a Release on a failed request is safe and can be repeated. It
// still counts, though: a request whose connection went is the handler's
// until it has released every Retain it took, and no longer.
func (c *Context) Release() { c.giveBack(false) }

// Retained reports whether the response is still being held open.
func (c *Context) Retained() bool {
	w := c.word.Load()
	holds := w & ctxHolds
	if w&ctxShare != 0 {
		holds++
	}
	return ctxState(w) == ctxOpen && holds > 1
}

// Err is why the request ended before its response did — the connection
// closing, a read timeout, or a body that could not be read — or nil while it
// is still going. Writing to a Context whose connection has gone fails with
// the same reason, so a handler that does not check Err does not corrupt
// anything; it only finds out later.
func (c *Context) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// OnCancel registers fn to run if the request ends before its response does:
// the connection closing under it, a read timeout, or a body that failed. It
// is for a handler that retained the request and is working on it elsewhere,
// so it can stop and give back what it holds; a handler that takes the body
// through OnBody hears the same thing there and needs no second callback.
//
// fn runs once, on a goroutine of its own rather than on the event loop, and
// never after the response has been written. It still has to Release what it
// retained. Nothing more is written then, but the request, its body and the
// Context stay the handler's until it has, and are recycled or given back
// only after its last Release.
// Registering it on a request that has already ended runs it at once.
func (c *Context) OnCancel(fn func(error)) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	// cancelWith moves the state on under mu, so what is read here is either
	// before it, and fn is left for it to run, or after it, with err set.
	switch ctxState(c.word.Load()) {
	case ctxCancelled:
		err := c.err
		c.mu.Unlock()
		fn(err)
		return
	case ctxDone:
		c.mu.Unlock()
		return
	}
	c.cancel = fn
	c.mu.Unlock()
}

// BodyComplete reports whether the whole request body has arrived: always
// for a body read whole before the handler ran, and for a streamed one once
// its last byte has been taken off the connection; see
// Config.StreamRequestBody. A handler that finds it complete can
// read Request.Body to the end without meeting ErrWouldBlock, and one that
// does not can take the body through OnBody as it arrives.
func (c *Context) BodyComplete() bool {
	if stream := c.RequestBody(); stream != nil {
		return stream.Complete()
	}
	return true
}

// OnBody delivers the request body to fn as it arrives, rather than through
// Request.Body, the way a websocket handler's OnFrame is given a message
// frame by frame. fin marks the last call, and err a body that will not be
// finished — the connection went, it outgrew MaxStreamedBodyBytes, its
// framing was broken, or the body was closed — which is also a last call.
// data is only valid for the duration of the call.
//
// OnBody never calls fn itself and never waits: it registers fn and returns.
// What of the body has already arrived — all of it, when BodyComplete says
// so — is handed to fn once the handler returns, on the goroutine that ran
// it, or, when OnBody is called after the handler has returned, on a
// goroutine of its own. The rest follows on the connection's worker as it
// arrives. The calls are one at a time and in order, so a handler can take a
// body of any size without a goroutine and without it being buffered: what
// holds the connection back is fn itself.
//
// OnBody retains the request, and the server releases it once fn has
// returned from its last call. A handler that answers from fn therefore
// needs neither Retain nor Release; it answers when fin arrives, and the
// response is written after that call:
//
//	c.OnBody(func(data []byte, fin bool, err error) {
//		if err != nil {
//			file.Close()
//			return
//		}
//		file.Write(data)
//		if fin {
//			c.Respond(200, "text/plain", []byte("stored"))
//		}
//	})
//
// A handler that answers elsewhere, after the body has arrived, takes a
// Retain of its own in fn and releases it when it has answered.
//
// Request.Body is not readable once OnBody has taken it over. Calling OnBody
// twice replaces the callback, and calling it after the body has ended, or
// after the response has been written, delivers nothing, except on a request
// that has already failed, whose error it reports.
func (c *Context) OnBody(fn BodyFunc) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	if c.bodyDone {
		err := c.err
		c.mu.Unlock()
		if err != nil {
			c.later(func(bool) { fn(nil, true, err) })
		}
		return
	}
	if !c.bodyHeld && !c.retain() {
		// The response is written already, and the body with it.
		c.mu.Unlock()
		return
	}
	c.body, c.bodyHeld = fn, true
	c.mu.Unlock()
	if stream := c.RequestBody(); stream != nil {
		// The body is still arriving; the stream hands it on from here,
		// starting with whatever it has already taken off the connection.
		if stream.setSink(c.deliverBody, c.handling()) {
			c.later(stream.drain)
		}
		return
	}
	// The body was read whole before the handler ran, so it is all here. The
	// callback may keep none of it past its call, so a body still in the
	// server's buffer is handed over from there rather than copied out; the
	// hold taken above keeps the buffer until then.
	var data []byte
	if whole, ok := c.Request.Body.(*wholeBody); ok && !whole.released {
		data = whole.data[whole.read:]
		whole.read = len(whole.data)
	} else if c.Request.Body != nil {
		data, _ = io.ReadAll(c.Request.Body)
	}
	if c.Request.Body != nil {
		c.Request.Body = emptyBody()
	}
	c.later(func(worker bool) { c.deliverBody(data, true, nil, worker) })
}

// handling reports whether the handler is still running, which later then
// leaves a handover to its return.
func (c *Context) handling() bool {
	w := c.word.Load()
	return w&ctxServed != 0 && w&ctxHandled == 0
}

// later runs handover once the handler has returned: on the handler's own
// goroutine, after it returns, when it is still running, and on a goroutine
// of its own when it is not, so that OnBody never calls a body callback
// itself. handover is told whether it runs on the connection's worker.
func (c *Context) later(handover func(worker bool)) {
	c.mu.Lock()
	for {
		w := c.word.Load()
		if w&ctxServed == 0 || w&ctxHandled != 0 {
			break
		}
		if w&ctxHandover != 0 {
			// Only one handover is ever owed: a second OnBody replaces the
			// callback, which the first handover delivers to.
			c.mu.Unlock()
			return
		}
		c.handover = handover
		if c.word.CompareAndSwap(w, w|ctxHandover) {
			c.mu.Unlock()
			return
		}
		c.handover = nil
	}
	c.mu.Unlock()
	go handover(false)
}

// handled marks the handler's return, and hands over the body it asked for
// while it ran. It runs before the handler's hold is given back, so the
// response the body callbacks answer is still open.
func (c *Context) handled() {
	var w uint64
	for {
		w = c.word.Load()
		if c.word.CompareAndSwap(w, w|ctxHandled) {
			break
		}
	}
	if w&ctxHandover == 0 {
		return
	}
	c.mu.Lock()
	handover := c.handover
	c.handover = nil
	c.mu.Unlock()
	handover(true)
}

// begin takes the hold that serving a request stands on, which the handler's
// return gives back. conn says the connection has the request too, until
// endRequestLocked is done with it.
func (c *Context) begin(conn bool) {
	add := ctxServed | ctxShare
	if conn {
		add |= ctxConn
	}
	for {
		w := c.word.Load()
		if ctxState(w) != ctxOpen || w&ctxServed != 0 {
			return
		}
		if c.word.CompareAndSwap(w, w|add) {
			return
		}
	}
}

// returned is the handler's return, which gives back the hold serving the
// request took.
func (c *Context) returned() { c.giveBack(true) }

// giveBack gives back the handler's hold, when returning is set, or one taken
// by Retain, and when that was the last hold on an open response, writes it.
func (c *Context) giveBack(returning bool) {
	for {
		w := c.word.Load()
		next := w
		if returning {
			next &^= ctxServed
		}
		if ctxState(w) != ctxOpen {
			// Nothing more is written, but the hold still counts for how long
			// the request is the handler's.
			if !returning {
				if w&ctxHolds == 0 {
					return
				}
				next--
			}
			if c.word.CompareAndSwap(w, next) {
				if w&ctxAlive != 0 && next&ctxAlive == 0 {
					c.ended()
				}
				return
			}
			continue
		}
		switch {
		case returning:
			next &^= ctxShare
		case w&ctxHolds != 0:
			next--
		case w&ctxShare != 0:
			// A Release too many takes the handler's share.
			next &^= ctxShare
		default:
			return
		}
		if next&(ctxHolds|ctxShare) != 0 || w&(ctxHolds|ctxShare) == 0 {
			// Still held open, or the handler returning from a response a
			// Release too many has finished already.
			if c.word.CompareAndSwap(w, next) {
				if w&ctxAlive != 0 && next&ctxAlive == 0 {
					c.ended()
				}
				return
			}
			continue
		}
		// The last hold on the response. A release from inside a body callback
		// cannot carry the connection on: the worker delivering that callback
		// holds the parser, and it picks the duty up itself once the callback
		// returns.
		resume := w&ctxResume != 0 && w&ctxDelivering == 0
		next = withState(next, ctxDone) | ctxFinishing
		if resume {
			next &^= ctxResume
		}
		if !c.word.CompareAndSwap(w, next) {
			continue
		}
		if next&ctxResume == 0 {
			// While the duty is still owed the connection has to keep its
			// pointer to this request, since that is how the worker finds it
			// to take it.
			c.forget()
		}
		_ = c.Finish()
		if !returning {
			// Away from the connection's read round, nothing else will send
			// what was just written.
			_ = c.Conn.Flush()
		}
		if resume {
			c.server.finishRequest(c)
		}
		c.drop(ctxFinishing)
		return
	}
}

// settle hands the connection over once the handler has returned: it reports
// whether the response is finished, and otherwise leaves whoever finishes it
// the duty of letting the connection carry on.
func (c *Context) settle() bool {
	for {
		w := c.word.Load()
		if ctxState(w) != ctxOpen {
			return true
		}
		if c.server == nil || c.parser == nil || c.word.CompareAndSwap(w, w|ctxResume) {
			return false
		}
	}
}

// claimResume takes the duty a release could not, and reports that it did, so
// that the connection is carried on exactly once.
func (c *Context) claimResume() bool {
	for {
		w := c.word.Load()
		if ctxState(w) == ctxOpen || w&ctxResume == 0 {
			return false
		}
		if c.word.CompareAndSwap(w, w&^ctxResume) {
			c.forget()
			return true
		}
	}
}

// cancelWith ends a request whose connection went before its response did.
// Nothing more is written, the response's holds are void, and the handler
// hears about it through OnBody and OnCancel; the request stays the handler's
// until it has released what it retained. It runs once.
//
// gen is the generation the request was served in, which a Context recycled
// since has moved past; see recycle.
func (c *Context) cancelWith(err error, gen uint64) {
	if err == nil {
		err = net.ErrClosed
	}
	c.mu.Lock()
	var owed bool
	for {
		w := c.word.Load()
		if ctxState(w) != ctxOpen || w>>ctxGenShift != gen {
			c.mu.Unlock()
			return
		}
		next := withState(w&^(ctxShare|ctxResume), ctxCancelled) | ctxCancelling
		if c.word.CompareAndSwap(w, next) {
			// A connection owed the duty to carry on is not coming back for
			// the request; one that was not owed it yet still is, from the
			// worker the handler is running on.
			owed = w&ctxResume != 0
			break
		}
	}
	c.err = err
	onCancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	c.forget()
	c.deliverBody(nil, true, err, false)
	if onCancel != nil {
		onCancel(err)
	}
	done := ctxCancelling
	if owed {
		done |= ctxConn
	}
	c.drop(done)
}

// forget drops the connection's pointer to this request, so that a close
// arriving afterwards has nothing left to cancel.
func (c *Context) forget() {
	if c.parser != nil {
		c.parser.liveContext.CompareAndSwap(c, nil)
	}
}

// deliverBody hands one piece of the body to the callback OnBody registered,
// and after the last one gives back the hold OnBody took. deliverMu keeps the
// calls one at a time and in order, whichever goroutine they come from, and is
// held while the callback runs; the state lock is not, so the callback may
// Retain and Release from inside it. worker says the call is on the
// connection's worker, holding its parser.
func (c *Context) deliverBody(data []byte, fin bool, err error, worker bool) {
	c.deliverMu.Lock()
	defer c.deliverMu.Unlock()
	c.mu.Lock()
	fn := c.body
	last := fin || err != nil
	if c.bodyDone || fn == nil {
		if last {
			c.bodyDone = true
		}
		c.mu.Unlock()
		return
	}
	held := last && c.bodyHeld
	if last {
		c.bodyDone, c.bodyHeld = true, false
	}
	c.mu.Unlock()
	if worker {
		// A release from inside the callback cannot carry the connection on
		// while this goroutine holds the parser; see giveBack. deliverMu keeps
		// the callbacks one at a time, so one bit counts them.
		c.setDelivering(true)
	}
	fn(data, fin, err)
	if held {
		c.Release()
	}
	if worker {
		c.setDelivering(false)
	}
}

func (c *Context) setDelivering(on bool) {
	for {
		w := c.word.Load()
		next := w &^ ctxDelivering
		if on {
			next |= ctxDelivering
		}
		if c.word.CompareAndSwap(w, next) {
			return
		}
	}
}
