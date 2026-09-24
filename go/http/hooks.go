//go:build linux || darwin || windows

package http

import (
	stdhttp "net/http"
)

// responseHooks are the callbacks OnHeader, OnResponse and OnFinish
// registered on a Context, in the order they were registered.
type responseHooks struct {
	list []responseHook
	// whole records that an OnResponse hook needs the whole body, and ran
	// that the hooks that change the response have run.
	whole bool
	ran   bool
}

// responseHook is one registered callback; exactly one of its fields is set.
type responseHook struct {
	header   func(status int, header stdhttp.Header)
	response func(*Response)
	finish   func(status int, header stdhttp.Header, size int64)
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
	}
}

// OnResponse registers fn to run once, just before the response is sent,
// with the whole of it: fn may change its status, header, body and trailer,
// as a middleware that compresses a body or answers a conditional request
// does. It runs in turn with the OnHeader hooks, in the order they describe.
//
// A response the handler writes through the ResponseWriter methods is held
// until the handler is done and sent at once, so that fn sees all of its
// body; on HTTP/1 that means Flush does nothing and a file is not sent by
// sendfile, as on HTTP/2 and HTTP/3 already. The header fn is given is the
// response's own, a copy of any the handler passed to WriteResponse, and
// the body is the handler's: fn replaces it rather than writing into it.
// Registering it once the response has been begun does nothing.
func (c *Context) OnResponse(fn func(response *Response)) {
	if fn != nil && !c.begun() {
		c.addHook(responseHook{response: fn})
		c.w.hooks.whole = true
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
		w.hooks = &responseHooks{list: make([]responseHook, 0, 4)}
	}
	w.hooks.list = append(w.hooks.list, hook)
}

// before runs the hooks that change a response about to be sent whole.
func (h *responseHooks) before(response *Response) {
	if h.ran {
		return
	}
	h.ran = true
	for i := len(h.list) - 1; i >= 0; i-- {
		switch hook := h.list[i]; {
		case hook.response != nil:
			hook.response(response)
		case hook.header != nil:
			if response.Header == nil {
				response.Header = make(stdhttp.Header)
			}
			hook.header(response.StatusCode, response.Header)
		}
		if response.StatusCode == 0 {
			response.StatusCode = stdhttp.StatusOK
		}
	}
}

// beforeHeader runs the OnHeader hooks for a response whose header is
// settled before its body is written; it has no OnResponse hooks.
func (h *responseHooks) beforeHeader(status int, header stdhttp.Header) {
	if h.ran {
		return
	}
	h.ran = true
	for i := len(h.list) - 1; i >= 0; i-- {
		if hook := h.list[i]; hook.header != nil {
			hook.header(status, header)
		}
	}
}

// finished runs the OnFinish hooks.
func (h *responseHooks) finished(status int, header stdhttp.Header, size int64) {
	for i := len(h.list) - 1; i >= 0; i-- {
		if hook := h.list[i]; hook.finish != nil {
			hook.finish(status, header, size)
		}
	}
}
