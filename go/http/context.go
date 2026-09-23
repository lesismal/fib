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

// contextPool recycles Contexts on their own, for a server that recycles its
// Contexts but not its requests; see Config.ReuseContexts.
var contextPool = sync.Pool{New: func() any { return &Context{pooled: true} }}

// recycle clears the Context for another request. The generation moves on
// under mu, so that a cancellation meant for the request it served, which
// OnClose makes from a goroutine of its own, finds it serving another and
// leaves it alone. Every field but mu, gen and pooled is reset, which
// TestContextRecycleResetsEveryField holds it to.
//
// A recycled Context waiting for its next request reads as finished, so that
// a handler that wrongly holds on to it past its response finds that Retain,
// Release and the rest do nothing, as they do on any finished response,
// rather than acting on a Context with no connection; reopen hands it out.
func (c *Context) recycle() {
	c.mu.Lock()
	c.gen.Add(1)
	c.Conn, c.Request = nil, nil
	c.wrote, c.closing, c.streamed = true, false, false
	c.w, c.stream, c.external = nil, nil, nil
	c.refs, c.state, c.err, c.resume, c.delivering = 0, ctxDone, nil, false, 0
	c.body, c.bodyDone, c.cancel = nil, false, nil
	c.server, c.parser, c.whole, c.block = nil, nil, nil, nil
	c.mu.Unlock()
}

// reopen readies a recycled Context for the request it is about to serve.
func (c *Context) reopen() {
	c.mu.Lock()
	c.state, c.wrote = ctxOpen, false
	c.mu.Unlock()
}

// A Context's response is held open by a reference count. Serving a request
// takes one hold, which the handler's return gives back, so a handler that
// answers and returns needs none of this. A handler that answers later takes
// another with Retain and gives it back with Release, and the response is
// written when the last hold goes.
const (
	// ctxOpen is a response still being written, ctxDone one that has been,
	// and ctxCancelled one whose connection went first.
	ctxOpen uint8 = iota
	ctxDone
	ctxCancelled
)

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
func (c *Context) Retain() {
	c.mu.Lock()
	if c.state == ctxOpen {
		c.refs++
	}
	c.mu.Unlock()
}

// Release gives back one hold taken by Retain. The last one to go finishes
// the response, hands it to the connection and lets the connection carry on
// with whatever is pipelined behind it.
//
// Releasing more often than retaining ends the response early: the hold that
// serving the request took is given back by the handler's return, so a
// release that is not undoing a Retain is undoing that one. Releasing a
// response that is already written, or one whose connection has gone, does
// nothing, so a Release on a failed request is safe and can be repeated.
func (c *Context) Release() { c.release(true) }

// Retained reports whether the response is still being held open.
func (c *Context) Retained() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == ctxOpen && c.refs > 1
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
// so it can stop and give back what it holds; a handler that reads the body
// through OnBody hears the same thing there and needs no second callback.
//
// fn runs once, on a goroutine of its own rather than on the event loop, and
// never after the response has been written. It still has to Release what it
// retained, which is then only the bookkeeping: nothing more is written.
// Registering it on a request that has already ended runs it at once.
func (c *Context) OnCancel(fn func(error)) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	if c.state == ctxCancelled {
		err := c.err
		c.mu.Unlock()
		fn(err)
		return
	}
	if c.state == ctxDone {
		c.mu.Unlock()
		return
	}
	c.cancel = fn
	c.mu.Unlock()
}

// OnBody delivers the request body to fn as it arrives, rather than through
// Request.Body, the way a websocket handler's OnFrame is given a message
// frame by frame. fin marks the last call, and err a body that will not be
// finished — the connection went, it outgrew MaxStreamedBodyBytes, or its
// framing was broken — which is also a last call. data is only valid for the
// duration of the call.
//
// A body arrives in pieces only when it streams, which
// Config.StreamRequestBodyThreshold decides; a body that was read whole
// before the handler ran arrives in one call with fin set. Either way fn runs
// on the connection's worker, one call at a time and in order, so a handler
// can take a body of any size without a goroutine and without it being
// buffered: what holds the connection back is fn itself.
//
// A handler that means to answer once the body has arrived retains the
// request first and releases it from fn:
//
//	c.Retain()
//	c.OnBody(func(data []byte, fin bool, err error) {
//		if err != nil {
//			file.Close()
//			c.Release()
//			return
//		}
//		file.Write(data)
//		if fin {
//			c.Respond(200, "text/plain", []byte("stored"))
//			c.Release()
//		}
//	})
//
// Request.Body is not readable once OnBody has taken it over. Calling OnBody
// twice replaces the callback, and calling it after the body has ended
// delivers nothing, except on a request that has already failed, whose error
// it reports at once.
func (c *Context) OnBody(fn BodyFunc) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	if c.bodyDone {
		err := c.err
		c.mu.Unlock()
		if err != nil {
			fn(nil, true, err)
		}
		return
	}
	c.body = fn
	c.mu.Unlock()
	if stream := c.RequestBody(); stream != nil {
		// The body is still arriving; the stream hands it on from here,
		// starting with whatever it has already taken off the connection.
		stream.setSink(c.deliverBody)
		return
	}
	// The body was read whole before the handler ran, so it is all here. The
	// callback may keep none of it past its call, so a body still in the
	// server's buffer is handed over from there rather than copied out.
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
	c.deliverBody(data, true, nil)
}

// begin takes the hold that serving a request stands on, which the handler's
// return gives back.
func (c *Context) begin() {
	c.mu.Lock()
	if c.state == ctxOpen && c.refs == 0 {
		c.refs = 1
	}
	c.mu.Unlock()
}

// release gives back one hold and, when it was the last, writes the response.
// flush asks for it to reach the socket at once, which a release away from
// the connection's read round wants; one inside a round is written with the
// rest of the round's output when it ends.
func (c *Context) release(flush bool) {
	c.mu.Lock()
	if c.state != ctxOpen || c.refs <= 0 {
		c.mu.Unlock()
		return
	}
	c.refs--
	if c.refs > 0 {
		c.mu.Unlock()
		return
	}
	c.state = ctxDone
	// A release from inside a body callback cannot carry the connection on:
	// the worker delivering that callback holds the parser, and it picks the
	// duty up itself once the callback returns.
	resume := c.resume && c.delivering == 0
	c.resume = c.resume && !resume
	owed := c.resume
	c.mu.Unlock()
	if !owed {
		// While the duty is still owed the connection has to keep its pointer
		// to this request, since that is how the worker finds it to take it.
		c.forget()
	}
	_ = c.Finish()
	if flush {
		_ = c.Conn.Flush()
	}
	if resume {
		c.server.finishRequest(c)
	}
}

// settle hands the connection over once the handler has returned: it reports
// whether the response is finished, and otherwise leaves whoever finishes it
// the duty of letting the connection carry on.
func (c *Context) settle() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ctxOpen {
		return true
	}
	if c.server != nil && c.parser != nil {
		c.resume = true
	}
	return false
}

// claimResume takes the duty a release could not, and reports that it did, so
// that the connection is carried on exactly once.
func (c *Context) claimResume() bool {
	c.mu.Lock()
	if c.state == ctxOpen || !c.resume {
		c.mu.Unlock()
		return false
	}
	c.resume = false
	c.mu.Unlock()
	c.forget()
	return true
}

// cancelWith ends a request whose connection went before its response did.
// Nothing more is written, every hold left on it is void, and the handler
// hears about it through OnBody and OnCancel. It runs once.
//
// gen is the generation the request was served in, which a Context recycled
// since has moved past; see recycle.
func (c *Context) cancelWith(err error, gen uint64) {
	if err == nil {
		err = net.ErrClosed
	}
	c.mu.Lock()
	if c.state != ctxOpen || c.gen.Load() != gen {
		c.mu.Unlock()
		return
	}
	c.state, c.err, c.refs = ctxCancelled, err, 0
	// There is no connection left to carry on with.
	c.resume = false
	onCancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	c.forget()
	c.deliverBody(nil, true, err)
	if onCancel != nil {
		onCancel(err)
	}
}

// forget drops the connection's pointer to this request, so that a close
// arriving afterwards has nothing left to cancel.
func (c *Context) forget() {
	if c.parser != nil {
		c.parser.liveContext.CompareAndSwap(c, nil)
	}
}

// deliverBody hands one piece of the body to the callback OnBody registered.
// deliverMu keeps the calls one at a time and in order, whichever goroutine
// they come from, and is held while the callback runs; the state lock is not,
// so the callback may Retain and Release from inside it.
func (c *Context) deliverBody(data []byte, fin bool, err error) {
	c.deliverMu.Lock()
	defer c.deliverMu.Unlock()
	c.mu.Lock()
	fn := c.body
	if c.bodyDone || fn == nil {
		if fin || err != nil {
			c.bodyDone = true
		}
		c.mu.Unlock()
		return
	}
	if fin || err != nil {
		c.bodyDone = true
	}
	c.delivering++
	c.mu.Unlock()
	fn(data, fin, err)
	c.mu.Lock()
	c.delivering--
	c.mu.Unlock()
}
