//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	stdhttp "net/http"
	"sync"
	"sync/atomic"

	"github.com/lesismal/fib/sidepool"
)

// AsyncBodyFilter transforms a response body as it is written, as a
// BodyFilter does, with work that may not run on the goroutine writing the
// response, such as a call handed to a pool of its own; see
// Context.OnBodyFilterAsync.
type AsyncBodyFilter interface {
	// FilterAsync takes the next part of the body, p, and calls done once,
	// from any goroutine and from inside FilterAsync too, with what is sent
	// in its place, which may be empty, or with an error, which fails the
	// response. p is the filter's until it calls done, and out is sent, or
	// copied, before done returns. end says p is the last of the body, and
	// trailer is then the trailer the response ends with, never nil, which
	// the filter may change until it calls done; before the end it is nil.
	// One call is made at a time: the next waits for done.
	FilterAsync(p []byte, end bool, trailer stdhttp.Header, done func(out []byte, err error))
}

// OnBodyFilterAsync registers fn to run once, as the response's header is
// settled, as an OnBodyFilter hook does, and returns an AsyncBodyFilter that
// every part of the body goes through on its way out, or nil to leave the
// body as it is. It is how a middleware transforms a body with work that may
// not run on the goroutine writing the response, as OnHeaderAsync is for
// the header.
//
// What is written while a call is under way is held, up to 64 kilobytes, and
// goes to the next call whole: past that Write waits for the call to come
// back, as it waits for a slow client, WriteNoWait reports above, and
// OnWritable runs once what is held has gone through the filter and the
// client has taken its output. What the filter sends out is paced by the
// client the same way, the next call waiting for it. Finish leaves the end
// of the body to the filter's last call, which may change the trailer, and
// the response is retained until it comes back, so a handler that returns
// meanwhile leaves it to the filter. A response sent whole is retained and
// held, its body going to the filter in one last call. Flush does nothing
// while a call is under way or body is held. Filters registered after it
// with OnBodyFilter run on its output, from its done.
//
// A response whose client goes meanwhile ends as any other does, once the
// call under way comes back; a filter whose call never does keeps it open,
// so a filter bounds its calls. A filter that implements BodyAborter is
// aborted, as a BodyFilter is, when the response ends without its end.
// Registering it once the response has been begun does nothing.
func (c *Context) OnBodyFilterAsync(fn func(status int, header stdhttp.Header) AsyncBodyFilter) {
	if fn == nil || c.begun() {
		return
	}
	c.addHook(responseHook{filter: func(status int, header stdhttp.Header) BodyFilter {
		if f := fn(status, header); f != nil {
			return asyncFilter{f}
		}
		return nil
	}})
	h := c.w.hooks
	h.changes = true
	if h.body == nil {
		h.body = &asyncBody{}
		h.body.room.L = &h.body.mu
	}
}

// errAsyncFilter is what an asynchronous filter called as a BodyFilter
// reports, which nothing should do.
var errAsyncFilter = errors.New("http: an asynchronous body filter called synchronously")

// asyncFilter is an AsyncBodyFilter among a response's filters, where it
// marks where the body goes on asynchronously.
type asyncFilter struct{ f AsyncBodyFilter }

func (a asyncFilter) Filter([]byte) ([]byte, error) { return nil, errAsyncFilter }
func (a asyncFilter) Close() ([]byte, error)        { return nil, errAsyncFilter }

// Abort aborts the filter, when it is a BodyAborter.
func (a asyncFilter) Abort(err error) {
	if ab, ok := a.f.(BodyAborter); ok {
		ab.Abort(err)
	}
}

// asyncBody is the state of a response whose body goes through an
// asynchronous filter; see Context.OnBodyFilterAsync. Everything in it but
// on is guarded by mu.
type asyncBody struct {
	mu   sync.Mutex
	room sync.Cond
	// on says the header is settled with an asynchronous filter: every part
	// of the body written goes through here from then on.
	on atomic.Bool
	// stages are the filters from the first asynchronous one on, which take
	// what the synchronous filters before it, left in the hooks' filters,
	// make of the body; nil once they are closed or aborted.
	stages []BodyFilter
	// pending is the body written while a call is under way, or while the
	// filter's output waits for the client, which the next call takes, and
	// spare the buffer the last call had, for pending to be next.
	pending, spare []byte
	// busy says a call is under way, and paused that the next waits for the
	// client to take the last one's output. owned says the response is
	// retained for them.
	busy, paused, owned bool
	// finish says Finish was called, the end going with the next call, and
	// ended that it has gone to the filter.
	finish, ended bool
	// trailer is what the last call left the trailer, once it is back.
	trailer stdhttp.Header
	// writable is what OnWritable was given meanwhile; gone ends a Write
	// waiting for room, once watching has registered for it.
	writable func(error)
	gone     error
	watching bool
}

// asyncBody returns the state of a body that goes through an asynchronous
// filter, or nil when it does not.
func (w *responseWriter) asyncBody() *asyncBody {
	if w.hooks == nil || w.hooks.body == nil || !w.hooks.body.on.Load() {
		return nil
	}
	return w.hooks.body
}

// filterTrailer returns the trailer an asynchronous filter's last call left
// the response, or nil before it is back.
func (w *responseWriter) filterTrailer() stdhttp.Header {
	if w.hooks == nil || w.hooks.body == nil {
		return nil
	}
	return w.hooks.body.trailer
}

// splitAsync moves the filters from the first asynchronous one on to the
// stages of the asynchronous body, and reports whether there is one.
func (h *responseHooks) splitAsync() bool {
	b := h.body
	if b == nil {
		return false
	}
	for i, f := range h.filters {
		if _, ok := f.(asyncFilter); ok {
			b.stages = append([]BodyFilter(nil), h.filters[i:]...)
			if h.filters = h.filters[:i:i]; i == 0 {
				h.filters = nil
			}
			return true
		}
	}
	return false
}

// runStages passes p through stages, each asynchronous one handing on what
// it makes of it from its done, and calls done with what comes out. At the
// end, the synchronous ones are closed, and a stage that fails has those
// after it aborted.
func runStages(stages []BodyFilter, p []byte, end bool, trailer stdhttp.Header, done func([]byte, error)) {
	for i, f := range stages {
		if a, ok := f.(asyncFilter); ok {
			rest := stages[i+1:]
			a.f.FilterAsync(p, end, trailer, func(out []byte, err error) {
				if err != nil {
					if end {
						abortFilters(rest, err)
					}
					done(nil, err)
					return
				}
				runStages(rest, out, end, trailer, done)
			})
			return
		}
		out, err := f.Filter(p)
		if err == nil && end {
			var tail []byte
			if tail, err = f.Close(); err == nil && len(tail) > 0 {
				out = append(append([]byte(nil), out...), tail...)
			}
		}
		if err != nil {
			if end {
				abortFilters(stages[i+1:], err)
			}
			done(nil, err)
			return
		}
		p = out
	}
	done(p, nil)
}

// writeAsync is Write, which waits for room when wait is set, and
// WriteNoWait, for a body that goes through an asynchronous filter: p goes
// through the synchronous filters before it, and is held for the filter's
// next call.
func (c *Context) writeAsync(b *asyncBody, p []byte, wait bool) (n int, above bool, err error) {
	w := c.w
	b.mu.Lock()
	for {
		switch {
		case w.err != nil:
			err = w.err
		case b.finish || w.finished:
			err = ErrResponseWritten
		case !wait || len(b.pending) == 0 || len(b.pending)+len(p) <= maxHeldBody:
		case b.gone != nil:
			err = b.gone
		case !b.watching:
			// As a Write waiting on a held header does.
			b.watching = true
			b.mu.Unlock()
			c.OnCancel(b.cancelled)
			b.mu.Lock()
			continue
		default:
			b.room.Wait()
			continue
		}
		break
	}
	if err != nil {
		b.mu.Unlock()
		return 0, false, err
	}
	if len(p) == 0 {
		b.mu.Unlock()
		return 0, false, nil
	}
	q := p
	if w.hooks.filters != nil {
		if q, err = w.hooks.filter(p); err != nil {
			err = w.fail(err)
			b.mu.Unlock()
			return 0, false, err
		}
	}
	b.pending = append(b.pending, q...)
	above = len(b.pending) > maxHeldBody
	c.pump(b)
	return len(p), above, nil
}

// cancelled is the OnCancel callback of a response whose Write waits for
// room: it ends the wait with err.
func (b *asyncBody) cancelled(err error) {
	b.mu.Lock()
	b.gone = err
	b.room.Broadcast()
	b.mu.Unlock()
}

// finishAsync is Finish for a body that goes through an asynchronous
// filter, and reports whether it was: the end goes to the filter, which
// finishes the response from its done.
func (c *Context) finishAsync(b *asyncBody) bool {
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return false
	}
	b.finish = true
	c.pump(b)
	return true
}

// writableAsync keeps fn, given to OnWritable while a call is under way or
// body is held, for when they have gone, and reports whether it did.
func (b *asyncBody) writableAsync(fn func(error)) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.busy || b.paused || len(b.pending) > 0 {
		b.writable = fn
		return true
	}
	return false
}

// flushAsync is FlushError for a body that goes through an asynchronous
// filter: the header and what the filter sent go now, unless a call is under
// way or body is held, whose output will go as it comes.
func (c *Context) flushAsync(b *asyncBody) error {
	w := c.w
	b.mu.Lock()
	defer b.mu.Unlock()
	if w.err != nil || b.busy || b.paused || len(b.pending) > 0 || w.finished {
		return w.err
	}
	if !w.committed {
		if err := c.commit(false); err != nil {
			return w.fail(err)
		}
	}
	if c.isHTTP1() {
		if err := c.Conn.Flush(); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

// pump starts the filter's next call, when one is due and none is under
// way, and otherwise gives back what the response was retained for and
// hands OnWritable's function on. It is called holding b.mu, which it
// unlocks.
func (c *Context) pump(b *asyncBody) {
	w := c.w
	if b.busy || b.paused {
		b.mu.Unlock()
		return
	}
	if w.err != nil || b.ended || len(b.pending) == 0 && !b.finish {
		release := b.owned
		b.owned = false
		writable := b.writable
		b.writable = nil
		err := w.err
		b.room.Broadcast()
		b.mu.Unlock()
		if writable != nil {
			switch {
			case err != nil:
				sidepool.Post(func() { writable(err) })
			case !c.onWritable(writable):
				sidepool.Post(func() { writable(nil) })
			}
		}
		if release {
			c.Release()
		}
		return
	}
	if !b.owned {
		if !c.hold() {
			// The request ended: nothing written now reaches the client.
			err := c.Err()
			if err == nil {
				err = errBodyUnended
			}
			_ = w.fail(err)
			b.room.Broadcast()
			b.mu.Unlock()
			return
		}
		b.owned = true
	}
	in := b.pending
	b.pending, b.spare = b.spare[:0], nil
	end := b.finish
	var trailer stdhttp.Header
	if end {
		if w.hooks.filters != nil {
			// What the synchronous filters hold back goes in the last call.
			tail, err := w.hooks.endFilters(false)
			if err != nil {
				_ = w.fail(err)
				c.failAsync(b)
				return
			}
			in = append(in, tail...)
		}
		if trailer = w.trailer(); trailer == nil {
			trailer = make(stdhttp.Header)
		}
		b.ended = true
	}
	b.busy = true
	stages := b.stages
	b.mu.Unlock()
	runStages(stages, in, end, trailer, func(out []byte, err error) {
		c.asyncDone(b, in, end, trailer, out, err)
	})
}

// asyncDone is the done of the filter's call that took in: out goes to the
// client, and the next call is started, or the response finished after the
// last.
func (c *Context) asyncDone(b *asyncBody, in []byte, end bool, trailer stdhttp.Header, out []byte, err error) {
	w := c.w
	b.mu.Lock()
	b.busy = false
	if b.spare == nil {
		b.spare = in[:0]
	}
	if end {
		// Closed, or aborted by runStages when it failed.
		b.stages = nil
	}
	above := false
	if err == nil && w.err == nil && len(out) > 0 {
		above, err = c.sendNoWait(out)
	}
	if err != nil || w.err != nil {
		if err != nil {
			_ = w.fail(err)
		}
		c.failAsync(b)
		return
	}
	if end {
		b.trailer = trailer
		b.mu.Unlock()
		// A response that fails to finish is abandoned by finishResponse.
		_ = c.finishResponse()
		b.mu.Lock()
		c.pump(b)
		return
	}
	if above {
		b.paused = true
		b.mu.Unlock()
		if c.onWritable(func(err error) { c.asyncWritable(b, err) }) {
			return
		}
		b.mu.Lock()
		b.paused = false
	}
	c.pump(b)
}

// asyncWritable is the client having taken the filter's output, or the
// response having ended first with err.
func (c *Context) asyncWritable(b *asyncBody, err error) {
	b.mu.Lock()
	b.paused = false
	if err != nil {
		_ = c.w.fail(err)
		c.failAsync(b)
		return
	}
	c.pump(b)
}

// failAsync ends a response whose body failed going through the filter, as
// one whose sending failed ends. It is called holding b.mu, which it
// unlocks.
func (c *Context) failAsync(b *asyncBody) {
	w := c.w
	w.finished = true
	if !c.wrote {
		c.abandon()
	}
	b.pending = nil
	c.pump(b)
}

// abortAsync aborts the stages of a body that never ended, as the response
// ends.
func (h *responseHooks) abortAsync(err error) {
	if b := h.body; b != nil {
		b.mu.Lock()
		stages := b.stages
		b.stages = nil
		b.mu.Unlock()
		abortFilters(stages, err)
	}
}

// pendingBody returns the state of an asynchronous body whose end has not
// gone to its filter, or which may yet go through one, its header not being
// settled, on a response not yet sent, or nil. The last hold going hands
// itself over to it, see giveBack, so that the response stays open until
// the filter has ended it.
func (c *Context) pendingBody() *asyncBody {
	if c.wrote || c.w == nil || c.w.hooks == nil || c.w.hooks.body == nil {
		return nil
	}
	b := c.w.hooks.body
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended || b.owned || c.w.err != nil || !b.on.Load() && c.w.status != 0 {
		return nil
	}
	return b
}

// answerBody is the last hold going on a response whose body an
// asynchronous filter has still to end, which giveBack handed that hold to:
// the response is begun as it would have been, and the end goes to the
// filter, whose done gives the hold back. One whose body no filter takes
// gives the hold back at once.
func (c *Context) answerBody(b *asyncBody) {
	c.answerUnanswered()
	if c.w.asyncBody() == nil {
		c.giveBack(false)
		return
	}
	b.mu.Lock()
	b.owned = true
	b.finish = true
	c.pump(b)
}

// holdBodyWhole sends a response sent whole through the asynchronous
// filter, in one last call: it is retained, and sent from the call's done.
// The hooks have run, and the filters before the asynchronous one run here.
func (c *Context) holdBodyWhole(hooks *responseHooks, response Response) error {
	b := hooks.body
	body := response.Body
	if hooks.filters != nil {
		out, err := hooks.filter(body)
		if err == nil {
			body = append([]byte(nil), out...)
			var tail []byte
			if tail, err = hooks.endFilters(false); err == nil {
				body = append(body, tail...)
			}
		}
		if err != nil {
			hooks.abortAsync(err)
			return err
		}
	} else {
		// The handler may reuse its body once this returns.
		body = bytes.Clone(body)
	}
	if !c.hold() {
		err := c.Err()
		if err == nil {
			err = errBodyUnended
		}
		hooks.abortAsync(err)
		return err
	}
	response.Body = body
	response.Header = response.Header.Clone()
	if response.Header == nil {
		response.Header = make(stdhttp.Header)
	}
	trailer := response.Trailer.Clone()
	if trailer == nil {
		trailer = make(stdhttp.Header)
	}
	c.wrote, c.status = true, uint16(response.StatusCode)
	b.mu.Lock()
	b.ended = true
	stages := b.stages
	b.mu.Unlock()
	runStages(stages, body, true, trailer, func(out []byte, err error) {
		b.mu.Lock()
		b.stages = nil
		b.mu.Unlock()
		if err == nil {
			// A body sent whole on a stream may stay with it, so it is a
			// copy of its own.
			response.Body = bytes.Clone(out)
			delete(response.Header, "Content-Length")
			response.Trailer = nil
			if len(trailer) > 0 {
				response.Trailer = trailer
			}
			err = c.sendHeld(&response, hooks)
		}
		if err != nil {
			c.abandon()
		}
		c.Release()
	})
	return nil
}
