//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdhttp "net/http"
	"sync"
	"sync/atomic"

	"github.com/lesismal/fib/sidepool"
)

// asyncHeader is the state of a response whose header an OnHeaderAsync hook
// holds; see Context.OnHeaderAsync. Everything in it is guarded by mu, which
// the writer's methods take only on a response that registered such a hook.
type asyncHeader struct {
	mu sync.Mutex
	// room is where a Write that would hold more than maxHeldBody waits for
	// the header to go. watching says the first such wait registered gone
	// with OnCancel, and gone is why the request was cancelled since, which
	// ends the wait: a client that goes is not waited on for a resume.
	room     sync.Cond
	watching bool
	gone     error
	// started says a hook has been called, and active that the header is
	// held, waiting for the resume of the hook at index at. passed says the
	// hooks were passed over instead, the response not being retained.
	started bool
	active  bool
	passed  bool
	at      int
	// owned says the response is retained for the hold: by a hold taken
	// for it, or the last one, handed over by giveBack; resume releases it.
	owned bool
	// whole is a response sent whole that is held, or nil for one that
	// streams, whose body written meanwhile is raw. finish says Finish was
	// called meanwhile, and writable is what OnWritable was given.
	whole    *Response
	raw      []byte
	finish   bool
	writable func(error)
	// fns are the hooks' functions, by their index in the hooks' list.
	fns []asyncHook
}

// asyncHook is an OnHeaderAsync hook's function, and its index in the list
// of hooks.
type asyncHook struct {
	at int
	fn func(status int, header stdhttp.Header, resume func(error))
}

// fn returns the function of the hook at index at.
func (a *asyncHeader) fn(at int) func(int, stdhttp.Header, func(error)) {
	for _, h := range a.fns {
		if h.at == at {
			return h.fn
		}
	}
	return nil
}

// asyncHeader returns the state of an OnHeaderAsync hook, or nil when none
// is registered.
func (w *responseWriter) asyncHeader() *asyncHeader {
	if w.hooks == nil {
		return nil
	}
	return w.hooks.async
}

// hold keeps p, written while the header is held, and reports whether it
// did and whether more than maxHeldBody is now held. A Write, which may
// wait, waits for the header to go rather than hold more than that; once it
// has gone, hold reports false and the caller writes p as it would have.
// A wait ends too when the request is cancelled, for which hold reports the
// request's error.
func (a *asyncHeader) hold(c *Context, p []byte, wait bool) (held, above bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.active {
		switch {
		case !wait || len(a.raw)+len(p) <= maxHeldBody:
			a.raw = append(a.raw, p...)
			return true, len(a.raw) > maxHeldBody, nil
		case a.gone != nil:
			return false, false, a.gone
		case !a.watching:
			// Only a response that comes to wait pays for hearing of the
			// cancellation, and only once. OnCancel runs the callback at
			// once for a request cancelled already, which takes a.mu.
			a.watching = true
			a.mu.Unlock()
			c.OnCancel(a.cancelled)
			a.mu.Lock()
		default:
			a.room.Wait()
		}
	}
	return false, false, nil
}

// cancelled is the OnCancel callback of a response whose Write waits while
// the header is held: it ends the wait with err.
func (a *asyncHeader) cancelled(err error) {
	a.mu.Lock()
	a.gone = err
	a.room.Broadcast()
	a.mu.Unlock()
}

// held reports whether the header is held.
func (a *asyncHeader) held() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active
}

// deferFinish leaves a Finish called while the header is held to the
// resume, and reports whether it did.
func (a *asyncHeader) deferFinish() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active && a.whole == nil {
		a.finish = true
		return true
	}
	return false
}

// deferWritable keeps fn, given to OnWritable while the header is held, for
// the resume, and reports whether it did.
func (a *asyncHeader) deferWritable(fn func(error)) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active && a.whole == nil {
		a.writable = fn
		return true
	}
	return false
}

// retainLocked retains the response for the hold, unless it is already,
// and reports whether it is. Callers hold a.mu.
func (c *Context) retainLocked(a *asyncHeader) bool {
	if !a.owned && c.hold() {
		a.owned = true
	}
	return a.owned
}

// pendingAsync returns the state of an OnHeaderAsync hook that has not been
// reached yet on a response not yet sent, or nil. The last hold going hands
// itself over to such a hook, see giveBack, so that a response the handler
// leaves to be sent as the hold goes is held all the same.
func (c *Context) pendingAsync() *asyncHeader {
	if c.wrote || c.w == nil {
		return nil
	}
	a := c.w.asyncHeader()
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started || a.passed {
		return nil
	}
	return a
}

// holdWhole holds a response about to be sent whole at the OnHeaderAsync
// hook at index at, which the hooks before it have left in hooks.response,
// and calls the hook. It reports false, having run the hooks after it,
// when the response cannot be retained for the hold, its request having
// ended.
func (c *Context) holdWhole(hooks *responseHooks, at int) bool {
	a := hooks.async
	a.mu.Lock()
	if !c.retainLocked(a) {
		a.passed = true
		a.mu.Unlock()
		for at >= 0 {
			at = hooks.beforeFrom(&hooks.response, at-1)
		}
		return false
	}
	// The handler may reuse its body, and Respond its header, once this
	// returns.
	response := hooks.response
	hooks.response = Response{}
	response.Body = bytes.Clone(response.Body)
	response.Header = response.Header.Clone()
	response.Trailer = response.Trailer.Clone()
	a.whole = &response
	a.started, a.active, a.at = true, true, at
	c.wrote, c.status = true, uint16(response.StatusCode)
	fn := a.fn(at)
	a.mu.Unlock()
	fn(response.StatusCode, response.Header, c.resumer(a, at))
	return true
}

// holdHeader holds the header of a response that streams at the
// OnHeaderAsync hook at index at, and calls the hook. It reports false,
// having run the hooks after it, when the response cannot be retained for
// the hold, its request having ended.
func (c *Context) holdHeader(at int) bool {
	w := c.w
	a := w.hooks.async
	a.mu.Lock()
	if !c.retainLocked(a) {
		a.passed = true
		a.mu.Unlock()
		w.hooks.beforeHeaderFrom(w.status, w.sent, at-1, true)
		return false
	}
	a.started, a.active, a.at = true, true, at
	fn := a.fn(at)
	a.mu.Unlock()
	fn(w.status, w.sent, c.resumer(a, at))
	return true
}

// resumer is the resume an OnHeaderAsync hook at index at is given.
func (c *Context) resumer(a *asyncHeader, at int) func(error) {
	var once atomic.Bool
	return func(err error) {
		if !once.CompareAndSwap(false, true) {
			return
		}
		a.mu.Lock()
		if !a.active || a.at != at {
			a.mu.Unlock()
			return
		}
		if err == nil {
			// A request that ended meanwhile is not answered.
			err = c.Err()
		}
		if a.whole != nil {
			c.resumeWhole(a, err)
		} else {
			c.resumeHeader(a, err)
		}
	}
}

// resumeWhole sends a response held whole, once the hooks after the one
// that held it have run, unless err fails it. It is called holding a.mu,
// which it unlocks.
func (c *Context) resumeWhole(a *asyncHeader, err error) {
	hooks := c.w.hooks
	response := a.whole
	if err == nil {
		if at := hooks.beforeFrom(response, a.at-1); at >= 0 {
			a.at = at
			fn := a.fn(at)
			a.mu.Unlock()
			fn(response.StatusCode, response.Header, c.resumer(a, at))
			return
		}
		if err = hooks.filterWhole(response, c.Request.Method == stdhttp.MethodHead); err == nil {
			err = c.sendHeld(response, hooks)
		}
	}
	a.whole, a.active = nil, false
	if err != nil {
		c.abandon()
	}
	a.mu.Unlock()
	c.Release()
}

// resumeHeader settles the header of a response that streams, once the
// hooks after the one that held it have run, and sends what was written
// meanwhile, unless err fails it. It is called holding a.mu, which it
// unlocks.
func (c *Context) resumeHeader(a *asyncHeader, err error) {
	w := c.w
	if err == nil {
		if at := w.hooks.beforeHeaderFrom(w.status, w.sent, a.at-1, false); at >= 0 {
			a.at = at
			fn := a.fn(at)
			a.mu.Unlock()
			fn(w.status, w.sent, c.resumer(a, at))
			return
		}
		c.settleHeader()
		a.active = false
		raw := a.raw
		a.raw = nil
		if len(raw) > 0 {
			_, err = c.writeNoWait(raw)
		}
		if err == nil && a.finish {
			err = c.finishResponse()
		}
	}
	if err != nil {
		a.active = false
		a.raw = nil
		_ = w.fail(err)
		if !c.wrote {
			c.abandon()
		}
		w.finished = true
	}
	if fn := a.writable; fn != nil {
		a.writable = nil
		switch {
		case err != nil:
			sidepool.Post(func() { fn(err) })
		case !c.onWritable(fn):
			sidepool.Post(func() { fn(nil) })
		}
	}
	a.room.Broadcast()
	a.mu.Unlock()
	c.Release()
}
