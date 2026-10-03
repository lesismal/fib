//go:build linux || darwin || windows

package arpc

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// sfKey is a call in flight. async keeps CallAsync apart from Call and
// CallContext, since they are answered differently.
type sfKey struct {
	method string
	key    string
	async  bool
}

// sfCall is one request shared by the calls that joined it.
//
// The first call, the leader, sends the request. A blocking one decodes the
// response once into result, keeping the payload in data for a follower whose
// response is of another type, and closes done; its followers wait on done
// and copy result. An asynchronous leader hands its response to the
// followers in subs instead.
type sfCall struct {
	done   chan struct{}
	data   []byte
	result any
	err    error

	// finished and subs are guarded by the group's mutex.
	finished bool
	subs     []*sfAsyncSub
}

// sfAsyncSub is a follower of an asynchronous call. Its handler runs once,
// with the leader's response or at its own timeout, whichever comes first.
type sfAsyncSub struct {
	handler AsyncHandlerFunc
	timer   *time.Timer
	fired   atomic.Bool
}

func (sub *sfAsyncSub) fire(ctx *Context, err error) {
	if sub.fired.CompareAndSwap(false, true) {
		if sub.timer != nil {
			sub.timer.Stop()
		}
		safeCall("async call handler", func() { sub.handler(ctx, err) })
	}
}

type singleflightGroup struct {
	mu    sync.Mutex
	calls map[sfKey]*sfCall
}

// acquire returns the call in flight for k, and whether the caller started
// it and is its leader.
func (g *singleflightGroup) acquire(k sfKey) (*sfCall, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if call, ok := g.calls[k]; ok {
		return call, false
	}
	if g.calls == nil {
		g.calls = map[sfKey]*sfCall{}
	}
	call := &sfCall{}
	if !k.async {
		call.done = make(chan struct{})
	}
	g.calls[k] = call
	return call, true
}

// finish publishes a blocking leader's outcome and wakes its followers.
func (g *singleflightGroup) finish(k sfKey, call *sfCall, data []byte, result any, err error) {
	call.data, call.result, call.err = data, result, err
	g.mu.Lock()
	if g.calls[k] == call {
		delete(g.calls, k)
	}
	g.mu.Unlock()
	close(call.done)
}

// addSub adds a follower to an asynchronous call, or reports that the leader
// has finished and the follower has to make a call of its own.
func (g *singleflightGroup) addSub(call *sfCall, sub *sfAsyncSub) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if call.finished {
		return false
	}
	call.subs = append(call.subs, sub)
	return true
}

// finishAsync ends an asynchronous call and returns its followers.
func (g *singleflightGroup) finishAsync(k sfKey, call *sfCall) []*sfAsyncSub {
	g.mu.Lock()
	defer g.mu.Unlock()
	call.finished = true
	subs := call.subs
	call.subs = nil
	if g.calls[k] == call {
		delete(g.calls, k)
	}
	return subs
}

// callSingleflight is Call and CallContext for a method that shares its
// calls. Each caller still waits only as long as its own ctx lets it.
func (c *Client) callSingleflight(ctx context.Context, method string, req, rsp any, key string, values []map[any]any) error {
	k := sfKey{method: method, key: key}
	call, leader := c.sfGroup.acquire(k)
	if leader {
		data, err := c.requestData(ctx, method, req, values)
		var result any
		if err == nil {
			result, err = c.parseSharedResult(data, rsp)
		}
		c.sfGroup.finish(k, call, data, result, err)
		return err
	}
	select {
	case <-call.done:
	case <-ctx.Done():
		return ErrClientTimeout
	case <-c.chClose:
		return ErrClientStopped
	}
	if call.err != nil {
		return call.err
	}
	return c.applySharedResult(call.result, call.data, rsp)
}

// requestData makes a call and returns a copy of the response's payload.
func (c *Client) requestData(ctx context.Context, method string, req any, values []map[any]any) ([]byte, error) {
	resp, err := c.roundTrip(method, req, nil, ctx.Done(), values)
	if err != nil {
		return nil, err
	}
	defer c.Handler.OnMessageDone(c, resp)
	if resp.Cmd() != CmdResponse {
		return nil, ErrInvalidRspMessage
	}
	if resp.IsError() {
		return nil, resp.Error()
	}
	return append([]byte(nil), resp.Data()...), nil
}

// parseSharedResult decodes data once into a new value of rsp's type, copies
// it into rsp, and returns it for the followers to copy. For an rsp that is
// not a pointer, data is decoded into it alone.
func (c *Client) parseSharedResult(data []byte, rsp any) (any, error) {
	rv := reflect.ValueOf(rsp)
	if rsp == nil || rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, bytesToValue(c.Codec, data, rsp)
	}
	holder := reflect.New(rv.Type().Elem())
	if err := bytesToValue(c.Codec, data, holder.Interface()); err != nil {
		return nil, err
	}
	rv.Elem().Set(holder.Elem())
	return holder.Interface(), nil
}

// applySharedResult copies the leader's decoded response into a follower's
// rsp. The copy is shallow, so what it refers to is shared and read only. An
// rsp of another type decodes data instead.
func (c *Client) applySharedResult(result any, data []byte, rsp any) error {
	if rsp == nil {
		return nil
	}
	if result != nil {
		rv, hv := reflect.ValueOf(rsp), reflect.ValueOf(result)
		if rv.Kind() == reflect.Pointer && !rv.IsNil() && rv.Type() == hv.Type() {
			rv.Elem().Set(hv.Elem())
			return nil
		}
	}
	return bytesToValue(c.Codec, data, rsp)
}

// callAsyncSingleflight is CallAsync for a method that shares its calls.
func (c *Client) callAsyncSingleflight(method string, req any, handler AsyncHandlerFunc, timeout time.Duration, key string, values []map[any]any) error {
	k := sfKey{method: method, key: key, async: true}
	call, leader := c.sfGroup.acquire(k)
	if leader {
		internal := func(ctx *Context, err error) {
			handler(ctx, err)
			c.fireAsyncSubs(ctx, err, c.sfGroup.finishAsync(k, call))
		}
		if err := c.callAsyncOnce(method, req, internal, timeout, values); err != nil {
			c.fireAsyncSubs(nil, err, c.sfGroup.finishAsync(k, call))
			return err
		}
		return nil
	}
	sub := &sfAsyncSub{handler: handler}
	sub.timer = time.AfterFunc(timeout, func() { sub.fire(nil, ErrTimeout) })
	if !c.sfGroup.addSub(call, sub) {
		sub.timer.Stop()
		return c.callAsyncOnce(method, req, handler, timeout, values)
	}
	return nil
}

// fireAsyncSubs hands an asynchronous leader's outcome to its followers on
// the task pool, each with a Context of its own over one copy of the
// response, which outlives the leader's.
func (c *Client) fireAsyncSubs(ctx *Context, err error, subs []*sfAsyncSub) {
	if len(subs) == 0 {
		return
	}
	var msg *Message
	if ctx != nil && ctx.Message != nil {
		msg = &Message{Buffer: append([]byte(nil), ctx.Message.Buffer...), values: ctx.Message.values}
	}
	for _, sub := range subs {
		var fctx *Context
		if msg != nil {
			fctx = &Context{Client: c, Message: msg}
		}
		c.goFunc(func() { sub.fire(fctx, err) })
	}
}
