//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strconv"
)

// responseHooks are the callbacks OnHeader, OnResponse and OnFinish
// registered on a Context, in the order they were registered.
type responseHooks struct {
	list []responseHook
	// whole records that an OnResponse hook needs the whole body, changes
	// that an OnHeader or OnResponse hook may change the response, and ran
	// that the hooks that change it have run.
	whole   bool
	changes bool
	ran     bool
	// response is the copy of a whole response the hooks are handed.
	response Response
	// filters are what the OnBodyFilter hooks returned for this response,
	// in the order they run, while its body streams through them.
	filters []BodyFilter
	// async holds the response while an OnHeaderAsync hook decides its
	// header, or is nil when none is registered; see asyncHeader.
	async *asyncHeader
}

// responseHook is one registered callback; exactly one of its fields is set,
// or none for an OnHeaderAsync hook, whose function its responseHooks'
// async keeps, so that a hook stays at four words.
type responseHook struct {
	header   func(status int, header stdhttp.Header)
	response func(*Response)
	finish   func(status int, header stdhttp.Header, size int64)
	filter   func(status int, header stdhttp.Header) BodyFilter
}

// isAsync reports whether the hook is an OnHeaderAsync one.
func (hook *responseHook) isAsync() bool {
	return hook.header == nil && hook.response == nil && hook.finish == nil && hook.filter == nil
}

// BodyFilter transforms a response body as it is written, as a compressor
// does; see Context.OnBodyFilter. Its methods are called one at a time, from
// the goroutines the response is written on, and must not block.
type BodyFilter interface {
	// Filter takes the next part of the body and returns what is sent in its
	// place, which may be empty, as a compressor's is until it has enough to
	// emit. The result is sent, or copied, before Filter or Close is called
	// again, so it may be a buffer of the filter's own that it reuses.
	Filter(p []byte) ([]byte, error)
	// Close ends the body and returns what is left to send after it.
	Close() ([]byte, error)
}

// BodyFlusher is implemented by a BodyFilter that can hand over what it holds
// back without ending the body, as a compressor's sync flush does. A
// streaming response's Flush, and FlushError, send it, so that what the
// handler flushed reaches the client rather than waiting in the filter.
type BodyFlusher interface {
	Flush() ([]byte, error)
}

// BodyAborter is implemented by a BodyFilter that holds something to give
// back, an encoder from a pool or a copy of the body, when its response ends
// without the body ending: the response failed as it was written, its
// client went first, it was never finished, or it answers HEAD and sends no
// body. Abort is called in place of Close, once, with why, after the last
// Filter or Flush and on the goroutine that ends or recycles the response.
// A filter that does not implement it is dropped then without a word.
type BodyAborter interface {
	Abort(err error)
}

// errBodyUnended is why a filter is aborted when its response ended without
// the body ending and without an error of its own.
var errBodyUnended = errors.New("http: the response ended before its body did")

// abortFilters aborts the filters fs, which will not be closed, for err.
func abortFilters(fs []BodyFilter, err error) {
	for _, f := range fs {
		if a, ok := f.(BodyAborter); ok {
			a.Abort(err)
		}
	}
}

// OnHeader registers fn to run once, just before the response's header is
// settled, with its final status and its header, which fn may change. It is
// how a middleware adds fields to a response the handler writes, whichever
// way it writes it: WriteResponse, Respond or the ResponseWriter methods.
//
// Hooks run in the reverse of the order they were registered, as deferred
// calls do, so that the one registered by the outermost middleware sees the
// response last, as every middleware inside it has left it. fn runs on the
// goroutine that writes the response, and not at all for a response that is
// never written. Registering it once the response has been begun does
// nothing.
func (c *Context) OnHeader(fn func(status int, header stdhttp.Header)) {
	if fn != nil && !c.begun() {
		c.addHook(responseHook{header: fn})
		c.w.hooks.changes = true
	}
}

// OnResponse registers fn to run once, just before the response is sent,
// with the whole of it: fn may change its status, header, body and trailer,
// as a middleware that compresses a body or answers a conditional request
// does. It runs in turn with the OnHeader hooks, in the order they describe.
//
// A response the handler writes through the ResponseWriter methods is held
// until the handler is done and sent at once, so that fn sees all of its
// body: Flush does nothing for it, it does not stream however long it is, and
// on HTTP/1 a file is not sent by sendfile. The header fn is given is the
// response's own, a copy of any the handler passed to WriteResponse, and
// the body is the handler's: fn replaces it rather than writing into it, and
// keeps none of it past the call, since the handler may reuse it once the
// response is sent, as Context.JSON does; keep a copy instead.
// Registering it once the response has been begun does nothing.
func (c *Context) OnResponse(fn func(response *Response)) {
	if fn != nil && !c.begun() {
		c.addHook(responseHook{response: fn})
		c.w.hooks.whole = true
		c.w.hooks.changes = true
	}
}

// OnBodyFilter registers fn to run once, as the response's header is
// settled, with its final status and its header, after the OnHeader hooks
// registered before it and before those registered after, as they run. fn may
// change the header, and returns a BodyFilter that every part of the body
// goes through on its way out, or nil to leave the body as it is. It is how a
// middleware compresses a response without holding it whole: a response the
// handler writes through the ResponseWriter methods still streams, as it is
// written and flushed, and one sent whole, with WriteResponse or Respond, is
// filtered whole.
//
// fn is not called for a response whose status carries no body. For one sent
// whole, the header it is given has the body's length as its Content-Length,
// so that fn can tell a short body from a long one. A response
// it returns a filter for is sent without a Content-Length, the length the
// handler declared being that of the body before the filter: HTTP/1 frames
// it with chunks, or with its length when it is sent whole. Once one hook has
// returned a filter, the hooks after it are given no Content-Length either,
// since the body their filters see is what that filter makes of it. Nor does
// a filtered body go by sendfile. A response to HEAD sends no body, so fn
// should leave the header as a GET's would have it and return nil for one;
// a filter it returns anyway is aborted (see BodyAborter), and the header
// goes as fn left it.
// Filters registered on one response run in the order their hooks do, each
// on what the one before it returned. Registering it once the response has
// been begun does nothing.
func (c *Context) OnBodyFilter(fn func(status int, header stdhttp.Header) BodyFilter) {
	if fn != nil && !c.begun() {
		c.addHook(responseHook{filter: fn})
		c.w.hooks.changes = true
	}
}

// OnHeaderAsync registers fn to run once, where an OnHeader hook would run,
// with the response's final status and its header, which fn may change, and
// holds the header until resume is called, which may be from any goroutine
// and from inside fn: it is how a middleware decides a header with work that
// may not run on the goroutine writing the response, such as a call handed
// to a pool of its own. resume(nil) sends the header as fn's caller left it,
// and runs the hooks after it; resume with an error fails the response,
// which closes an HTTP/1 connection and resets an HTTP/2 or HTTP/3 stream.
// Only the first call counts.
//
// The response is retained while it is held, so a handler that returns
// meanwhile leaves it to resume. A response sent whole, with WriteResponse,
// Respond or as the handler finishes, is held as it is, its body copied. A
// response that streams through the ResponseWriter methods holds what is
// written, up to 64 kilobytes: past that Write waits for resume, as it
// waits for a slow client, or returns the request's error once the request
// is cancelled meanwhile, WriteNoWait reports above, and OnWritable runs
// once the held body has gone after resume. Flush does nothing while the
// header is held. A request whose client goes meanwhile ends as any other
// does, and its resume sends nothing. fn is not run for the response of
// Upgrade or Connect, which go at once. Registering it once the response has
// been begun does nothing.
func (c *Context) OnHeaderAsync(fn func(status int, header stdhttp.Header, resume func(error))) {
	if fn != nil && !c.begun() {
		c.addHook(responseHook{})
		h := c.w.hooks
		h.changes = true
		if h.async == nil {
			h.async = &asyncHeader{}
			h.async.room.L = &h.async.mu
		}
		h.async.fns = append(h.async.fns, asyncHook{at: len(h.list) - 1, fn: fn})
	}
}

// OnFinish registers fn to run once the response has been handed to the
// connection, with the status and header it was sent with and the length of
// the body it carried: none for a response to HEAD, or one whose status has
// no body. header must not be changed or kept past the call. The finish
// hooks run in the reverse of the order they were registered.
//
// fn runs on the goroutine that finished the response, and not at all for a
// response that is never written, such as one whose connection went first.
// Registering it once the response has been sent does nothing.
func (c *Context) OnFinish(fn func(status int, header stdhttp.Header, size int64)) {
	if fn != nil && !c.wrote {
		c.addHook(responseHook{finish: fn})
	}
}

// begun reports whether the response has been sent, or begun through the
// ResponseWriter methods.
func (c *Context) begun() bool { return c.wrote || c.w != nil && c.w.status != 0 }

// hooked returns the hooks registered on the response, or nil.
func (c *Context) hooked() *responseHooks {
	if c.w == nil {
		return nil
	}
	return c.w.hooks
}

func (c *Context) addHook(hook responseHook) {
	w := c.writer()
	if w.hooks == nil {
		if spare := w.spareHooks; spare != nil {
			w.spareHooks, w.hooks = nil, spare
		} else {
			w.hooks = &responseHooks{list: make([]responseHook, 0, 4)}
		}
	}
	w.hooks.list = append(w.hooks.list, hook)
}

// beforeFrom runs the hooks that change a response about to be sent whole,
// from the one at index from down; the first call, which writeResponse
// makes once, starts from the last. It stops at an OnHeaderAsync hook, which
// it leaves to its caller, and reports its index, or -1 once every hook has
// run.
func (h *responseHooks) beforeFrom(response *Response, from int) int {
	for i := from; i >= 0; i-- {
		switch hook := h.list[i]; {
		case hook.response != nil:
			hook.response(response)
		case hook.header != nil:
			if response.Header == nil {
				response.Header = make(stdhttp.Header)
			}
			hook.header(response.StatusCode, response.Header)
		case hook.filter != nil && statusHasBody(response.StatusCode):
			if response.Header == nil {
				response.Header = make(stdhttp.Header)
			}
			if _, ok := response.Header["Content-Length"]; !ok && len(h.filters) == 0 {
				// The body is all here, so its length is known: the hook
				// sees it, as it would one the handler declared.
				response.Header["Content-Length"] = []string{strconv.Itoa(len(response.Body))}
			}
			if f := hook.filter(response.StatusCode, response.Header); f != nil {
				h.filters = append(h.filters, f)
				// The hooks after it are given what it makes of the body,
				// whose length is not known.
				delete(response.Header, "Content-Length")
			}
		case hook.isAsync():
			if response.StatusCode == 0 {
				response.StatusCode = stdhttp.StatusOK
			}
			if response.Header == nil {
				response.Header = make(stdhttp.Header)
			}
			return i
		}
		if response.StatusCode == 0 {
			response.StatusCode = stdhttp.StatusOK
		}
	}
	return -1
}

// beforeHeader runs the OnHeader hooks for a response whose header is
// settled before its body is written; it has no OnResponse hooks. It runs at
// most once a response: its callers return before it once the header is
// settled, and a response that failed to go resets ran. It stops at an
// OnHeaderAsync hook, unless skipAsync says to pass over them, and reports
// its index, or -1 once every hook has run.
func (h *responseHooks) beforeHeader(status int, header stdhttp.Header, skipAsync bool) int {
	h.ran = true
	return h.beforeHeaderFrom(status, header, len(h.list)-1, skipAsync)
}

// beforeHeaderFrom is beforeHeader from the hook at index from down.
func (h *responseHooks) beforeHeaderFrom(status int, header stdhttp.Header, from int, skipAsync bool) int {
	for i := from; i >= 0; i-- {
		switch hook := h.list[i]; {
		case hook.header != nil:
			hook.header(status, header)
		case hook.filter != nil && statusHasBody(status):
			if f := hook.filter(status, header); f != nil {
				h.filters = append(h.filters, f)
				delete(header, "Content-Length")
			}
		case !skipAsync && hook.isAsync():
			return i
		}
	}
	return -1
}

// filter runs p through the filters, and returns what is sent in its place.
func (h *responseHooks) filter(p []byte) ([]byte, error) {
	for _, f := range h.filters {
		var err error
		if p, err = f.Filter(p); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// endFilters closes the filters, each in turn, the tail of each going through
// those after it, and returns what is left to send, in a slice of its own.
// flush says to flush them instead, where they can, rather than close them.
func (h *responseHooks) endFilters(flush bool) ([]byte, error) {
	var out []byte
	for i, f := range h.filters {
		var tail []byte
		var err error
		switch {
		case !flush:
			tail, err = f.Close()
		default:
			if fl, ok := f.(BodyFlusher); ok {
				tail, err = fl.Flush()
			}
		}
		if err != nil {
			if !flush {
				// The ones after it will not be closed now.
				abortFilters(h.filters[i+1:], err)
				h.filters = nil
			}
			return nil, err
		}
		for _, next := range h.filters[i+1:] {
			if len(tail) == 0 {
				break
			}
			if tail, err = next.Filter(tail); err != nil {
				if !flush {
					abortFilters(h.filters[i+1:], err)
					h.filters = nil
				}
				return nil, err
			}
		}
		out = append(out, tail...)
	}
	if !flush {
		h.filters = nil
	}
	return out, nil
}

// filterWhole runs the body of a response sent whole through the filters,
// and leaves its length to the body that comes out.
func (h *responseHooks) filterWhole(response *Response, head bool) error {
	if len(h.filters) == 0 {
		return nil
	}
	if head {
		abortFilters(h.filters, errBodyUnended)
		h.filters = nil
		return nil
	}
	body, err := h.filter(response.Body)
	if err != nil {
		return err
	}
	out := append([]byte(nil), body...)
	tail, err := h.endFilters(false)
	if err != nil {
		return err
	}
	response.Body = append(out, tail...)
	delete(response.Header, "Content-Length")
	return nil
}

// finished runs the OnFinish hooks.
func (h *responseHooks) finished(status int, header stdhttp.Header, size int64) {
	for i := len(h.list) - 1; i >= 0; i-- {
		if hook := h.list[i]; hook.finish != nil {
			hook.finish(status, header, size)
		}
	}
}
