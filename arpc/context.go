//go:build linux || darwin || windows

package arpc

import (
	"math"
	"sync"
	"time"
)

var contextPool = sync.Pool{New: func() any { return &Context{} }}

// Context carries a received message, and the Client it came on, through its
// method's chain, or to the callback of an asynchronous call. It is a
// context.Context, one that never ends.
type Context struct {
	// Client is the connection the message came on.
	Client *Client
	// Message is the message.
	Message *Message

	index       int
	handlers    []HandlerFunc
	responseErr any
}

func newContext(c *Client, msg *Message, handlers []HandlerFunc) *Context {
	ctx := contextPool.Get().(*Context)
	ctx.Client = c
	ctx.Message = msg
	ctx.handlers = handlers
	return ctx
}

// Release releases the Message and pools the Context, which must not be used
// afterwards.
func (ctx *Context) Release() {
	if ctx.Message != nil {
		ctx.Message.Release()
	}
	*ctx = Context{}
	contextPool.Put(ctx)
}

// RunTask runs the chain on a task pool.
func (ctx *Context) RunTask() { ctx.run() }

// run runs the chain, and then the context done callback.
func (ctx *Context) run() {
	handler := ctx.Client.Handler
	defer func() {
		if recovered := recover(); recovered != nil {
			recoverHandler("handler of "+ctx.Message.Method(), recovered)
		}
		handler.OnContextDone(ctx)
	}()
	ctx.Next()
}

// ResponseError returns what Error, or Write with an error, sent.
func (ctx *Context) ResponseError() any { return ctx.responseErr }

// Get returns the value stored in the Message for key.
func (ctx *Context) Get(key any) (any, bool) { return ctx.Message.Get(key) }

// Set stores value for key in the Message. It does nothing if either is nil.
func (ctx *Context) Set(key, value any) { ctx.Message.Set(key, value) }

// Values returns the values of the Message.
func (ctx *Context) Values() map[any]any {
	if ctx.Message == nil {
		return nil
	}
	return ctx.Message.values
}

// Body returns the payload of the Message.
func (ctx *Context) Body() []byte { return ctx.Message.Data() }

// Bind decodes the payload into v, or returns the error an error response
// carries. A *[]byte is set to the payload itself, valid as long as the
// Message is; a *string gets a copy, and anything else is what the Client's
// Codec decodes.
func (ctx *Context) Bind(v any) error {
	msg := ctx.Message
	if msg.IsError() {
		return msg.Error()
	}
	if b, ok := v.(*[]byte); ok {
		*b = msg.Data()
		return nil
	}
	return bytesToValue(ctx.Client.Codec, msg.Data(), v)
}

// Write sends v as the response to the request, an error response if v is an
// error. A notify takes no response: writing one gets
// ErrContextResponseToNotify.
func (ctx *Context) Write(v any) error { return ctx.write(v, false) }

// WriteWithTimeout is Write. A response goes to the connection at once, so
// there is nothing for timeout to bound.
func (ctx *Context) WriteWithTimeout(v any, timeout time.Duration) error { return ctx.write(v, false) }

// Error sends v as an error response to the request, or, if v is nil, a
// response with an empty payload.
func (ctx *Context) Error(v any) error { return ctx.write(v, v != nil) }

func (ctx *Context) write(v any, isError bool) error {
	req := ctx.Message
	if req.Cmd() != CmdRequest {
		return ErrContextResponseToNotify
	}
	if _, ok := v.(error); ok {
		isError = true
	}
	if isError {
		ctx.responseErr = v
	}
	h := header{cmd: CmdResponse, method: req.method(), seq: req.Seq()}
	if isError {
		h.flags |= HeaderFlagMaskError
	}
	if req.IsAsync() {
		h.flags |= HeaderFlagMaskAsync
	}
	return ctx.Client.sendValue(h, v, req.values)
}

// Next runs the rest of the chain. The chain goes on by itself once a
// middleware returns, so a middleware calls Next only to do something after
// the rest of it, such as timing it.
func (ctx *Context) Next() {
	if index := ctx.index; index < len(ctx.handlers) {
		ctx.index++
		ctx.handlers[index](ctx)
	}
}

// Abort stops the chain after the handler that calls it.
func (ctx *Context) Abort() { ctx.index = math.MaxInt }

// Deadline is context.Context's: there is none.
func (ctx *Context) Deadline() (deadline time.Time, ok bool) { return }

// Done is context.Context's: the Context never ends, so it is nil.
func (ctx *Context) Done() <-chan struct{} { return nil }

// Err is context.Context's, always nil.
func (ctx *Context) Err() error { return nil }

// Value is context.Context's, the value Get returns for key.
func (ctx *Context) Value(key any) any {
	if ctx.Message == nil {
		return nil
	}
	value, _ := ctx.Message.Get(key)
	return value
}
