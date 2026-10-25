//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"context"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// serverState is what a Parser keeps for the server driving it: the request
// being served, reachable without the parser's lock so that a close can
// cancel a request whose handler is still working on it, and when its
// response has to be written by.
type serverState struct {
	liveContext atomic.Pointer[Context]
	// writeDeadline is when the response being written stops being allowed
	// to, under Config.WriteTimeout or as the handler set it with
	// SetWriteDeadline, or zero. It is set before the handler runs and read
	// by whatever writes the response.
	writeDeadline time.Time
	// bodyDeadline is what a handler's SetReadDeadline shares with the
	// connection for a body that streams, allocated with the connection's
	// first such body, and bodyArmed tells armRead to consult it. bodyArmed
	// is guarded by the parser's mu.
	bodyDeadline *bodyDeadline
	bodyArmed    bool
	// quietSince is when the engine's shutdown first found the connection
	// without a byte from its client, in Unix nanoseconds, or zero; see
	// ServerHandler.OnShutdown. Guarded by the parser's mu.
	quietSince int64
	// earlyPending is how many bytes of the parser's input, from the front of
	// what it has not parsed, arrived as TLS early data, 0-RTT (RFC 8446
	// section 2.3): a request that begins while it is positive is one, and
	// carries Early-Data, with only a safe method served; see
	// Config.EarlyData. Early data is a prefix of the connection's
	// application data, so a count tells a request pipelined behind a
	// retained one apart from what arrives once the handshake is over.
	// Guarded by the parser's mu.
	earlyPending int64
}

func (p *Parser) resetServerState() {
	p.liveContext.Store(nil)
	p.writeDeadline = time.Time{}
	p.bodyDeadline, p.bodyArmed = nil, false
	p.quietSince = 0
	p.earlyPending = 0
}

// bodyDeadline is a deadline a handler set with SetReadDeadline for the rest
// of its request's body to arrive by, which stands in for the one
// Config.ReadTimeout puts on it. The handler may set it from any goroutine,
// while the connection's worker re-arms the connection's read deadline at
// the end of every round it runs (see armRead); mu orders the two, so that
// neither undoes the other.
type bodyDeadline struct {
	mu sync.Mutex
	// open is the request whose body is still arriving and whose response is
	// still open, the only one a deadline set now can apply to, or nil.
	open *Context
	// owner is the request the handler set at for, or nil.
	owner *Context
	at    time.Time
	// applied records that the connection's read deadline is the handler's,
	// which armRead replaces once it no longer applies.
	applied bool
}

// openBody records that context's body streams and is still arriving, for
// SetReadDeadline to bound. Callers hold the parser's mu.
func (p *Parser) openBody(context *Context) {
	b := p.bodyDeadline
	if b == nil {
		b = new(bodyDeadline)
		p.bodyDeadline = b
	}
	b.mu.Lock()
	b.open, b.owner = context, nil
	b.mu.Unlock()
	p.bodyArmed = true
}

// closeBody forgets a deadline context's handler set, once the connection is
// done with its request: a Context the Reuse options recycle comes back as
// another request's, which must not find it. Callers hold the parser's mu.
func (p *Parser) closeBody(context *Context) {
	if b := p.bodyDeadline; b != nil {
		b.mu.Lock()
		if b.owner == context {
			b.owner = nil
		}
		b.mu.Unlock()
	}
}

// set gives c's body until t to arrive, or as long as it takes when t is
// zero, applying it to the connection at once if the body is still
// arriving; otherwise armRead applies it, or it has nothing left to bound.
func (b *bodyDeadline) set(c *Context, t time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctxState(c.word.Load()) != ctxOpen {
		return ErrResponseWritten
	}
	b.owner, b.at = c, t
	if b.open != c {
		return nil
	}
	b.applied = true
	return c.Conn.SetReadDeadline(t)
}

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
	// ctxReader marks a handler running on the goroutine that reads its
	// multiplexed connection, until it returns; see onReader.
	ctxReader uint64 = 1 << 25
	// The state takes two bits, and the generation the rest.
	ctxStateShift        = 26
	ctxStateMask  uint64 = 3 << ctxStateShift
	ctxGenShift          = 28

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
	if w := c.w; w != nil && w.hooks != nil && w.hooks.filters != nil {
		// Filters the response never closed: it failed, or never ended.
		err := w.err
		if err == nil {
			err = errBodyUnended
		}
		abortFilters(w.hooks.filters, err)
		w.hooks.filters = nil
	}
	if w := c.w; w != nil && w.hooks != nil && w.hooks.body != nil {
		err := w.err
		if err == nil {
			err = errBodyUnended
		}
		w.hooks.abortAsync(err)
	}
	if h := c.cancel.Load(); h != nil && h.stop != nil {
		stop := h.stop
		h.stop = nil
		stop(nil)
	}
	if c.whole != nil {
		c.whole.release()
	}
	if c.server != nil {
		c.server.recycle(c)
	} else if st := c.stream; st != nil {
		st.unref()
	} else if ender, ok := c.external.(StreamEnder); ok {
		ender.RequestEnded()
	}
}

// contextPool recycles Contexts on their own, for a server that recycles its
// Contexts but not its requests; see Config.ReuseContexts.
var contextPool = sync.Pool{New: func() any { return &Context{pooled: true} }}

// recycle clears the Context for another request. Its generation moves on,
// so that a cancellation meant for the request it served, which OnClose makes
// from a goroutine of its own, finds it serving another and leaves it alone.
// Every field but mu, deliverMu, word and pooled is reset, w to a cleared writer, which
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
	c.wrote, c.ends, c.streamed, c.status = true, 0, false, 0
	if w := c.w; w != nil {
		// Nothing reaches the writer but through the Context, so it is as
		// free to serve another request, once cleared, as the Context is.
		// The Context keeps it for its next request, which is as likely to
		// want one.
		w.reset()
	}
	c.stream, c.external = nil, nil
	c.err, c.body, c.bodyDone = nil, nil, false
	c.bodyHeld, c.handover = false, nil
	if h := c.cancel.Load(); h != nil {
		// Kept for the next request, which is as likely to want it.
		h.reset()
	}
	c.server, c.parser, c.whole, c.block = nil, nil, nil, nil
	if st := c.route; st != nil {
		if st.shared {
			c.route = nil
		} else {
			st.reset()
		}
	}
}

// SetValue keeps v with the request, for whatever serves it to read back
// with Value: an access log's entry, say, which middleware would otherwise
// attach with Request.WithContext, a copy of the request and a context value
// on every request. There is one slot, cleared when the request is done and
// its Context recycled, so code that shares it agrees on what it holds;
// several values go in a struct of the caller's. The slot lives with the
// Context's cancellation hooks, which a recycled Context keeps for its next
// request, so that the Context itself, which an HTTP/2 or HTTP/3 stream is
// allocated with, does not grow by it.
//
// The slot takes no lock, so that keeping a value costs a request a store
// and no more: SetValue is for whatever serves the request to call before
// it hands the request to any other goroutine, as a middleware does before
// it calls the next handler, and Value for whatever runs after that, the
// OnFinish hooks included, wherever they run. Two goroutines calling
// SetValue, or one calling SetValue while another calls Value, race.
func (c *Context) SetValue(v any) {
	h := c.cancel.Load()
	if h == nil {
		c.mu.Lock()
		h = c.hooks()
		c.mu.Unlock()
	}
	h.value = v
}

// Value returns what SetValue last set for this request, or nil.
func (c *Context) Value() any {
	if h := c.cancel.Load(); h != nil {
		return h.value
	}
	return nil
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
// Retaining a request whose response is already written does nothing, and
// neither does retaining one whose connection has gone, once the request has
// ended. A request whose connection went while its handler was still at it
// is held as one retained a moment before would be: nothing more is written,
// but the request and its Context stay the handler's until the Release, so a
// handler that retains and answers from elsewhere does not find its Context
// recycled under it, whenever the cancellation lands.
//
// A handler may hold at most 65535 Retains on one request at a time.
func (c *Context) Retain() { c.hold() }

// hold is Retain, reporting whether it took a hold.
func (c *Context) hold() bool {
	for {
		w := c.word.Load()
		if ctxState(w) == ctxOpen || ctxState(w) == ctxCancelled && w&ctxAlive != 0 {
			if w&ctxHolds == ctxHolds {
				panic("http: too many Retains on one request")
			}
			if c.word.CompareAndSwap(w, w+1) {
				return true
			}
			continue
		}
		return false
	}
}

// Async retains the request and returns a func that runs fn and then
// releases it, for the handler to hand to a goroutine or a pool of its own:
//
//	pool.Go(c.Async(func() {
//		_ = c.Respond(200, "text/plain", slowQuery())
//	}))
//
// The returned func runs fn once; calling it again does nothing, since the
// request may be another one's by then. The release is deferred, so a fn
// that panics still gives the hold back. If the request could not be
// retained, because its response is already written or its connection has
// gone and the request ended, fn still runs but nothing is released, so the
// func never gives back a hold it did not take. A func that is never called
// holds the request as a Retain never released does.
func (c *Context) Async(fn func()) func() {
	held := c.hold()
	var ran atomic.Bool
	return func() {
		if !ran.CompareAndSwap(false, true) {
			return
		}
		if held {
			defer c.Release()
		}
		fn()
	}
}

// retain takes a hold on an open response, as Retain does, reporting whether
// it took one; a cancelled request it leaves alone, which OnBody relies on.
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

// Retained reports whether the response is still being held open by a Retain
// not yet released, including once the handler has returned.
func (c *Context) Retained() bool {
	w := c.word.Load()
	return ctxState(w) == ctxOpen && w&ctxHolds != 0
}

// Status returns the status of the response sent, or begun through the
// ResponseWriter methods, so far: 0 before any, and the 101 or 2xx of a
// tunnel opened by Upgrade or Connect. Together with Retained, it lets
// whatever called the handler tell, once it returns, that the response is
// settled, and what it was, without an OnFinish hook on every request: a
// response that Status says is sent and that is not Retained is finished,
// with that status. Call it on the goroutine that answers, or once that one
// is known to be done.
func (c *Context) Status() int {
	if c.status != 0 {
		return int(c.status)
	}
	if w := c.w; w != nil {
		return w.status
	}
	return 0
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
// so it can stop and give back what it holds, and for one that waits in place,
// on a database or another server, so it can stop waiting: a peer that hangs
// up on it is heard while it waits, rather than once it returns, where the
// engine's backend can tell (see fib.HangupHandler). A handler that takes the
// body through OnBody hears the same thing there and needs no second
// callback.
//
// Every callback registered is kept, and they run in the reverse of the order
// they were registered, as the OnFinish hooks do, so that a middleware which
// registers one before calling the handler hears of the cancellation after
// the handler's own, as it would have left the request.
//
// Each fn runs once, on the engine's handler pool rather than in the
// connection's round, and never after the response has been written. It still
// has to Release what it retained. Nothing more is written then, but the
// request, its body and the Context stay the handler's until it has, and are
// recycled or given back only after its last Release.
// Registering it on a request that has already ended runs it at once, and
// registering it once the response has been written does nothing.
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
	if h := c.hooks(); h.fn == nil {
		h.fn = fn
	} else {
		h.add(fn)
	}
	c.mu.Unlock()
}

// cancelHooks is what hears of a request ending: OnCancel's callbacks, and
// the context Context handed out with what cancels it, once. They share a
// pointer so that a Context, which every HTTP/2 and HTTP/3 stream carries,
// grows by no more than one word for them.
type cancelHooks struct {
	// fn is the first OnCancel callback, and more those registered after it,
	// so that a request with one callback, the common case, keeps it in a
	// field and makes no slice for it. more is a pointer so that the hooks,
	// which a recycled Context clears on every request, grow by a word only.
	fn   func(error)
	more *[]func(error)
	ctx  context.Context
	stop context.CancelCauseFunc
	// value is what SetValue set, which lives here rather than in Context;
	// see SetValue.
	value any
}

// reset clears the hooks for the Context's next request, keeping the room
// more grew to for one that registers as many callbacks.
func (h *cancelHooks) reset() {
	more := h.more
	*h = cancelHooks{}
	if more != nil {
		clear(*more)
		*more = (*more)[:0]
		h.more = more
	}
}

// add keeps a callback past the first.
func (h *cancelHooks) add(fn func(error)) {
	if h.more == nil {
		h.more = &[]func(error){fn}
		return
	}
	*h.more = append(*h.more, fn)
}

// runCancel runs the callbacks OnCancel registered, in the reverse of the
// order they were registered.
func runCancel(fn func(error), more *[]func(error), err error) {
	if more != nil {
		for i := len(*more) - 1; i >= 0; i-- {
			(*more)[i](err)
		}
	}
	if fn != nil {
		fn(err)
	}
}

// hooks returns the Context's cancelHooks, made the first time they are
// needed. Callers hold mu.
func (c *Context) hooks() *cancelHooks {
	h := c.cancel.Load()
	if h == nil {
		h = new(cancelHooks)
		c.cancel.Store(h)
	}
	return h
}

// Context is a context.Context for the request, for code that hears of a
// request ending only through one, as httputil.ReverseProxy and
// database/sql do. Request.Context() is not cancelled; this one is, as
// net/http's request context is: when the request is cancelled, for any of
// the reasons OnCancel is told of, and otherwise once it has ended — the
// handler has returned and every Retain has been released. From then on its
// Done channel is closed, its Err is context.Canceled, and context.Cause
// reports the reason OnCancel was given, or context.Canceled for a request
// that ended in the ordinary way. Its values are Request.Context()'s.
//
// It is made the first time it is asked for, so a handler that does not ask
// pays nothing for it. It holds nothing of the Context, so it can outlive the
// request and goes on reporting how the request ended, even once the Reuse
// options have the Context serving another. Asked for once the request has
// ended, it returns a context that is already cancelled.
func (c *Context) Context() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h := c.cancel.Load(); h != nil && h.ctx != nil {
		return h.ctx
	}
	// cancelWith moves the state on under mu, as OnCancel relies on too.
	w := c.word.Load()
	if ctxState(w) != ctxOpen && w&ctxAlive == 0 {
		return endedContext
	}
	parent := context.Background()
	if c.Request != nil {
		parent = c.Request.Context()
	}
	ctx, stop := context.WithCancelCause(parent)
	h := c.hooks()
	if ctxState(w) == ctxCancelled {
		stop(c.err)
	} else {
		h.stop = stop
	}
	h.ctx = ctx
	return ctx
}

// endedContext is what Context returns for a request that has ended already.
var endedContext = func() context.Context {
	ctx, stop := context.WithCancel(context.Background())
	stop()
	return ctx
}()

// BodyComplete reports whether the whole request body has arrived: always
// for a body read whole before the handler ran, and for a streamed one once
// its last byte has been taken off the connection; see
// Config.StreamRequestBody. A handler that finds it complete can
// read Request.Body to the end without meeting ErrWouldBlock, and one that
// does not can take the body through OnBody as it arrives. A body that
// streams is never in Body, complete or not: whether Body holds the body is
// whether RequestBody is nil.
func (c *Context) BodyComplete() bool {
	if stream := c.RequestBody(); stream != nil {
		return stream.Complete()
	}
	return true
}

// Body returns the request body that arrived whole before the handler ran, as
// every body does that does not stream (see Config.StreamRequestBody): the
// server's own buffer, rather than a copy read out of Request.Body, which
// io.ReadAll makes by growing a buffer of its own a piece at a time. It is
// the whole body whatever has been read from Request.Body already, and
// reading Request.Body afterwards still returns it.
//
// The bytes stay the handler's for as long as the body does: until the
// response is finished, at the handler's return or at the last Release of a
// request it retained. They must not be changed, nor kept past that; keep a
// copy instead. Respond and Write copy what they are given, so the body can
// be answered with as it is.
//
// It returns nil for a request without a body, and for one whose body streams,
// which RequestBody and OnBody read instead.
func (c *Context) Body() []byte {
	if b := c.whole; b != nil && !b.released {
		return b.data
	}
	return nil
}

// Query returns the first value of the query parameter name, as
// Request.URL.Query().Get(name) does, decoded the same way and skipping the
// same malformed pairs, but without building the map Query builds on every
// call: a value that needs no decoding is a slice of the request's raw query,
// so looking one up allocates nothing. It returns "" when the parameter is
// absent, as Get does.
func (c *Context) Query(name string) string {
	if c.Request == nil || c.Request.URL == nil {
		return ""
	}
	value, _ := lookupQuery(c.Request.URL.RawQuery, name)
	return value
}

// lookupQuery finds name's first value in query, as url.ParseQuery followed
// by Get finds it: pairs are split at '&', a pair holding a ';' is skipped,
// and so is one whose key or value does not decode.
func lookupQuery(query, name string) (string, bool) {
	for query != "" {
		var pair string
		pair, query, _ = strings.Cut(query, "&")
		if pair == "" || strings.IndexByte(pair, ';') >= 0 {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		if !queryKeyIs(key, name) {
			continue
		}
		if strings.IndexByte(value, '%') < 0 && strings.IndexByte(value, '+') < 0 {
			return value, true
		}
		if decoded, err := url.QueryUnescape(value); err == nil {
			return decoded, true
		}
	}
	return "", false
}

// queryKeyIs reports whether the raw key of a query pair decodes to name. A
// key that does not decode is no key at all, as url.ParseQuery skips it.
func queryKeyIs(key, name string) bool {
	if strings.IndexByte(key, '%') < 0 && strings.IndexByte(key, '+') < 0 {
		return key == name
	}
	decoded, err := url.QueryUnescape(key)
	return err == nil && decoded == name
}

// OnBody delivers the request body to fn as it arrives, rather than through
// Request.Body, the way a websocket handler's OnFrame is given a message
// frame by frame. fin marks the last call, and err a body that will not be
// finished — the connection went, it outgrew MaxStreamedBodyBytes, its
// framing was broken, or the body was closed — which is also a last call.
// data is only valid for the duration of the call.
//
// OnBody never calls fn itself and never waits: it registers fn and returns,
// and no goroutine is started for fn. What of the body has already arrived —
// all of it, when BodyComplete says so — is handed to fn once the handler
// returns, on the goroutine that ran it; when OnBody is called after the
// handler has returned, it is handed over on the engine's handler pool, since
// no read of the connection will bring it again. The rest follows as the
// connection is read: on the worker that reads an HTTP/1 connection, and on
// the engine's handler pool for a multiplexed one, whose reader serves every
// stream on it.
// The calls are one at a time and in order, so a handler can take a body of
// any size without a goroutine and without it being buffered: what holds the
// connection back is fn itself.
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
		err := c.failureLocked()
		c.mu.Unlock()
		if err != nil {
			c.later(func(bool) { fn(nil, true, err) })
		}
		return
	}
	if !c.bodyHeld && !c.retain() {
		// The response is written already, and the body with it, or the
		// request was cancelled, which a handler running beside the
		// connection's reader may find it was a moment ago: cancelWith has
		// then already looked for a callback and found none, and the error
		// is reported here instead.
		err := c.failureLocked()
		c.mu.Unlock()
		if err != nil {
			c.later(func(bool) { fn(nil, true, err) })
		}
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

// failureLocked is what a body callback registered after the request failed
// is told, or nil for a request that has not failed: the error its streamed
// body failed with, which may be what ended the connection, as a body that
// outgrew MaxStreamedBodyBytes does, and otherwise why the request was
// cancelled. Callers hold mu.
func (c *Context) failureLocked() error {
	if c.err == nil {
		return nil
	}
	if stream := c.RequestBody(); stream != nil {
		if err := stream.failure(); err != nil {
			return err
		}
	}
	return c.err
}

// handling reports whether the handler is still running, which later then
// leaves a handover to its return.
func (c *Context) handling() bool {
	w := c.word.Load()
	return w&ctxServed != 0 && w&ctxHandled == 0
}

// later runs handover once the handler has returned: on the handler's own
// goroutine, after it returns, when it is still running, and on the engine's
// handler pool when it is not, so that OnBody never calls a body callback
// itself and no goroutine is started for one. handover is told whether it
// runs on the connection's worker.
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
	runOnStreams(c.Conn, func() { handover(false) })
}

// handled marks the handler's return, and hands over the body it asked for
// while it ran, on the handler's goroutine, which holds the connection's
// parser. It runs before the handler's hold
// is given back, so the response the body callbacks answer is still open.
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
// return gives back, and reports whether the handler has it: taken now, or
// before, when the request was handed to a pool. conn says the connection
// has the request too, until endRequestLocked is done with it, and reader
// that the handler runs on the goroutine reading its multiplexed connection.
//
// A request cancelled before anything held it has ended already: its body
// has gone back and its Context may be serving another request, so its
// handler must not run.
func (c *Context) begin(conn, reader bool) bool {
	add := ctxServed | ctxShare
	if conn {
		add |= ctxConn
	}
	if reader {
		add |= ctxReader
	}
	for {
		w := c.word.Load()
		if w&ctxServed != 0 {
			return true
		}
		if ctxState(w) != ctxOpen {
			return false
		}
		if c.word.CompareAndSwap(w, w|add) {
			return true
		}
	}
}

// onReader reports whether the handler runs on the goroutine that reads its
// multiplexed connection, which the stream pool makes it do once the
// connection has as many handlers running as it may. A response written
// there must not wait for the client to take what was sent before, since the
// goroutine that would read the client's go-ahead is the one waiting. Once
// the handler has returned, a response it retained is written from
// elsewhere, and may wait.
func (c *Context) onReader() bool { return c.word.Load()&ctxReader != 0 }

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
			// The handler is done with the goroutine it ran on too.
			next &^= ctxServed | ctxReader
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
		// The last hold on the response. One an OnHeaderAsync hook has still
		// to hold is handed to the hook, which it keeps the response open
		// for until its resume; see Context.OnHeaderAsync.
		var a *asyncHeader
		if !c.wrote && c.w != nil && c.w.hooks != nil && c.w.hooks.async != nil {
			a = c.pendingAsync()
		}
		if a != nil {
			if !c.word.CompareAndSwap(w, next+1) {
				continue
			}
			c.answerHeld(a, returning)
			return
		}
		// So is one whose body an asynchronous filter has still to end.
		if b := c.pendingBody(); b != nil {
			if !c.word.CompareAndSwap(w, next+1) {
				continue
			}
			c.answerBody(b)
			return
		}
		// A release from inside a body callback cannot carry the connection
		// on: the worker delivering that callback holds the parser, and it
		// picks the duty up itself once the callback returns.
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
		if !c.wrote && (c.w == nil || c.w.status == 0) {
			c.answerUnanswered()
		}
		_ = c.Finish()
		if !returning && c.Conn != nil {
			// Away from the connection's read round, nothing else will send
			// what was just written. A stream served outside this package
			// with no connection sends what it is given itself.
			_ = c.Conn.Flush()
		}
		if resume {
			c.server.finishRequest(c)
			c.drop(ctxFinishing)
			return
		}
		if c.finished(next&ctxResume != 0) {
			c.server.finishRequest(c)
		}
		return
	}
}

// answerUnanswered begins the response to a request the last hold went
// without answering, at the handler's return or at a Release, which there is
// no one left to answer: as net/http does, that is an empty 200, or a 413
// for a body ended for growing past its limit; see LimitBody.
func (c *Context) answerUnanswered() {
	if !c.wrote && (c.w == nil || c.w.status == 0) {
		status := stdhttp.StatusOK
		if c.bodyTooLarge() {
			status = stdhttp.StatusRequestEntityTooLarge
		}
		c.WriteHeader(status)
	}
}

// answerHeld is the last hold going on a response an OnHeaderAsync hook has
// still to hold, which giveBack handed that hold to: the response is
// finished as it would have been, which reaches the hook, and the hook's
// resume gives the hold back. A response that does not reach it gives the
// hold back at once.
func (c *Context) answerHeld(a *asyncHeader, returning bool) {
	a.mu.Lock()
	a.owned = true
	a.mu.Unlock()
	c.answerUnanswered()
	_ = c.Finish()
	a.mu.Lock()
	reached := a.started
	if !reached {
		a.passed, a.owned = true, false
	}
	a.mu.Unlock()
	if !reached {
		c.giveBack(false)
		return
	}
	if !returning && c.Conn != nil {
		// As giveBack does for what a release away from the connection's
		// round writes.
		_ = c.Conn.Flush()
	}
}

// finished ends the writing of a response a release finished without taking
// the duty to carry the connection on, and reports whether it takes that duty
// now: settle leaves it for the release when the handler returns while the
// response is being written, since the connection must not go on before the
// response is out. owed says the duty was there already, left to the worker
// delivering a body callback, which takes it itself unless that callback has
// returned.
func (c *Context) finished(owed bool) bool {
	for {
		w := c.word.Load()
		next := w &^ ctxFinishing
		// A duty settle left since the response was finished cannot be
		// this goroutine's to leave: settle runs on the worker, which is not
		// writing the response then.
		take := w&ctxResume != 0 && (!owed || w&ctxDelivering == 0)
		if take {
			next &^= ctxResume
		}
		if c.word.CompareAndSwap(w, next) {
			if w&ctxAlive != 0 && next&ctxAlive == 0 {
				c.ended()
			}
			if take {
				c.forget()
			}
			return take
		}
	}
}

// settle hands the connection over once the handler has returned: it reports
// whether the response is finished, and otherwise leaves whoever finishes it
// the duty of letting the connection carry on. A response a release on
// another goroutine is still writing is not finished yet: the connection
// going on would read what that release is still writing.
func (c *Context) settle() bool {
	for {
		w := c.word.Load()
		done := ctxState(w) != ctxOpen
		if done && w&ctxFinishing == 0 {
			return true
		}
		if c.server == nil || c.parser == nil {
			return done
		}
		if c.word.CompareAndSwap(w, w|ctxResume) {
			return false
		}
	}
}

// claimResume takes the duty a release could not, and reports that it did, so
// that the connection is carried on exactly once. A response still being
// written is left to the release writing it, which takes the duty once it is
// done; see finished.
func (c *Context) claimResume() bool {
	for {
		w := c.word.Load()
		if ctxState(w) == ctxOpen || w&ctxResume == 0 || w&ctxFinishing != 0 {
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
	var onCancel func(error)
	var more *[]func(error)
	var stop context.CancelCauseFunc
	if h := c.cancel.Load(); h != nil {
		// The context stays, for a Context asked for again to return. The
		// callbacks run outside the lock, so more is handed over whole rather
		// than kept for the next request: one cancelled is the rare case.
		onCancel, more, stop = h.fn, h.more, h.stop
		h.fn, h.more, h.stop = nil, nil, nil
	}
	c.mu.Unlock()
	c.forget()
	if stop != nil {
		stop(err)
	}
	if stream := c.RequestBody(); stream == nil {
		c.deliverBody(nil, true, err, false)
	} else if !stream.failSink(err) {
		// OnBody has not handed the body its callback, though it may have
		// registered it here already. The body's own failure is what the
		// callback is told, as failureLocked tells one registered later.
		bodyErr := err
		if failed := stream.failure(); failed != nil {
			bodyErr = failed
		}
		c.deliverBody(nil, true, bodyErr, false)
	}
	runCancel(onCancel, more, err)
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
