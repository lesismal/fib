//go:build linux || darwin || windows

package arpc

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/taskpool"
)

// HandlerFunc handles a request or a notify, as a method's handler or as a
// middleware.
type HandlerFunc func(*Context)

// StreamHandlerFunc handles a Stream the peer opened.
type StreamHandlerFunc func(*Stream)

// AsyncHandlerFunc is the callback of Client.CallAsync. With a response, ctx
// holds it and err is the error it carries, if any. Without one, ctx is nil
// and err says why: ErrTimeout, ErrClientReconnecting or ErrClientStopped.
type AsyncHandlerFunc func(ctx *Context, err error)

// ReconnectInfo describes one attempt of a Client to reconnect, which
// Handler.HandleReconnect hears about.
type ReconnectInfo struct {
	// Times counts the attempts since the connection was lost, from 1.
	Times int
	// MaxTimes is Handler.MaxReconnectTimes, where zero or less is no limit.
	MaxTimes int
	// Addr is the remote address of the connection that was lost.
	Addr string
	// Success reports whether the attempt connected, and Err why not.
	Success bool
	Err     error
}

// route is a method's chain: the middlewares registered before it, its
// handler, and the middlewares registered after it.
type route struct {
	async    bool
	handlers []HandlerFunc
}

type streamRoute struct {
	async   bool
	handler StreamHandlerFunc
}

// Handler holds what Clients share: the methods and their middlewares, the
// coders, the callbacks and the settings. A Server and its Clients share one,
// and a dialed Client has its own unless it is given one.
//
// A Handler is configured before any Client uses it; it is not safe to change
// while one does.
type Handler struct {
	asyncResponse     bool
	streamQueueSize   int
	maxBodyLen        int
	maxReconnectTimes int
	reconnectInterval time.Duration

	onConnected    func(*Client)
	onDisconnected func(*Client)
	onReconnect    func(*Client, *ReconnectInfo)
	onMessageDone  func(*Client, *Message)
	onSessionMiss  func(*Client, *Message)
	onContextDone  func(*Context)

	malloc func(int) []byte
	free   func([]byte)

	routes        map[string]*route
	streams       map[string]*streamRoute
	singleflights map[string]func(req any) string
	middles       []HandlerFunc
	coders        []MessageCoder

	pool fib.TaskPool
}

// DefaultHandler is what NewServer and NewClient clone for a Server or Client
// given no Handler.
var DefaultHandler = NewHandler()

// NewHandler returns a Handler with the defaults: handlers run on the task
// pool, a Stream holds 4 messages its handler has not received, a message may
// be DefaultMaxBodyLen long, and a Client reconnects once a second for as long
// as it takes.
func NewHandler() *Handler {
	return &Handler{
		asyncResponse:     true,
		streamQueueSize:   4,
		maxBodyLen:        DefaultMaxBodyLen,
		reconnectInterval: time.Second,
	}
}

// Clone returns a copy of h with methods, middlewares and coders of its own;
// the callbacks and the task pool are shared.
func (h *Handler) Clone() *Handler {
	cp := *h
	cp.middles = append([]HandlerFunc(nil), h.middles...)
	cp.coders = append([]MessageCoder(nil), h.coders...)
	cp.routes = make(map[string]*route, len(h.routes))
	for k, v := range h.routes {
		cp.routes[k] = &route{async: v.async, handlers: append([]HandlerFunc(nil), v.handlers...)}
	}
	cp.streams = make(map[string]*streamRoute, len(h.streams))
	for k, v := range h.streams {
		sr := *v
		cp.streams[k] = &sr
	}
	if h.singleflights != nil {
		cp.singleflights = make(map[string]func(any) string, len(h.singleflights))
		for k, v := range h.singleflights {
			cp.singleflights[k] = v
		}
	}
	return &cp
}

// HandleConnected sets what runs when a Client is connected: when a Server
// accepts it, and when a dialed one first connects and every time it
// reconnects. It runs on the task pool, so the connection's first messages
// may be handled before it returns.
func (h *Handler) HandleConnected(f func(*Client)) { h.onConnected = f }

// HandleDisconnected sets what runs when a Client stops: when an accepted
// one's connection closes, and when a dialed one is stopped or gives up
// reconnecting. A dialed Client that reconnects has not stopped.
func (h *Handler) HandleDisconnected(f func(*Client)) { h.onDisconnected = f }

// HandleReconnect sets what runs after each attempt of a dialed Client to
// reconnect, successful or not; on success, before the connected callback.
func (h *Handler) HandleReconnect(f func(*Client, *ReconnectInfo)) { h.onReconnect = f }

// MaxReconnectTimes returns how many times a dialed Client tries to reconnect
// before it stops, where zero or less is no limit.
func (h *Handler) MaxReconnectTimes() int { return h.maxReconnectTimes }

// SetMaxReconnectTimes sets how many times a dialed Client tries to
// reconnect; zero or less, the default, is no limit.
func (h *Handler) SetMaxReconnectTimes(n int) { h.maxReconnectTimes = n }

// ReconnectInterval returns how long a dialed Client waits before each
// attempt to reconnect.
func (h *Handler) ReconnectInterval() time.Duration { return h.reconnectInterval }

// SetReconnectInterval sets how long a dialed Client waits before each
// attempt to reconnect, a second by default.
func (h *Handler) SetReconnectInterval(d time.Duration) { h.reconnectInterval = d }

// HandleMessageDone sets what runs on a received message once it has been
// handled, and on a Message PushMsg has sent. EnablePool sets one that
// releases it.
func (h *Handler) HandleMessageDone(f func(*Client, *Message)) { h.onMessageDone = f }

// OnMessageDone runs the message done callback.
func (h *Handler) OnMessageDone(c *Client, m *Message) {
	if h.onMessageDone != nil && m != nil {
		h.onMessageDone(c, m)
	}
}

// HandleSessionMiss sets what runs on a response nothing waits for any more,
// the response to a call that timed out, say, before OnMessageDone.
func (h *Handler) HandleSessionMiss(f func(*Client, *Message)) { h.onSessionMiss = f }

// HandleContextDone sets what runs on a Context once its chain, or the
// callback of an asynchronous call, has returned. EnablePool sets one that
// releases it.
func (h *Handler) HandleContextDone(f func(*Context)) { h.onContextDone = f }

// OnContextDone runs the context done callback.
func (h *Handler) OnContextDone(ctx *Context) {
	if h.onContextDone != nil {
		h.onContextDone(ctx)
	}
}

// AsyncResponse reports whether the methods and stream methods registered
// from now on run on the task pool rather than where their connection is
// read.
func (h *Handler) AsyncResponse() bool { return h.asyncResponse }

// SetAsyncResponse sets where the methods and stream methods registered from
// now on run: on the task pool, the default, or where their connection is
// read. The optional bool of Handle and HandleStream overrides it.
func (h *Handler) SetAsyncResponse(async bool) { h.asyncResponse = async }

// TaskPool returns the pool set by SetTaskPool, or nil when handlers run on
// fib.Engine.HandlerPool of their connection's engine.
func (h *Handler) TaskPool() fib.TaskPool { return h.pool }

// SetTaskPool sets the pool asynchronous handlers run on, which the package
// also hands the callbacks to that must not run on an event loop: connected,
// disconnected, and those of asynchronous calls that end without a response.
// Nil, the default, means fib.Engine.HandlerPool of the connection's engine,
// the pool fib's HTTP/2 and HTTP/3 handlers run on. A task the pool turns
// away runs on the goroutine that submitted it.
func (h *Handler) SetTaskPool(pool fib.TaskPool) { h.pool = pool }

// SetAsyncExecutor is SetTaskPool with a function that runs what it is given,
// as github.com/lesismal/arpc takes one. Nil restores the default pool.
func (h *Handler) SetAsyncExecutor(executor func(f func())) {
	if executor == nil {
		h.pool = nil
		return
	}
	h.pool = executorPool(executor)
}

// executorPool is a TaskPool that hands each task to a function.
type executorPool func(f func())

func (e executorPool) GoTask(task taskpool.Task) bool {
	e(task.RunTask)
	return true
}

func (e executorPool) GoTasks(tasks []taskpool.Task) int {
	for _, task := range tasks {
		e(task.RunTask)
	}
	return len(tasks)
}

// StreamQueueSize returns how many messages a Stream holds that its handler
// has not received.
func (h *Handler) StreamQueueSize() int { return h.streamQueueSize }

// SetStreamQueueSize sets how many messages a Stream holds that its handler
// has not received, 4 by default. A connection whose Stream is full reads
// nothing more until the handler receives one.
func (h *Handler) SetStreamQueueSize(size int) { h.streamQueueSize = size }

// MaxBodyLen returns the longest body, method and payload, a received message
// may have.
func (h *Handler) MaxBodyLen() int { return h.maxBodyLen }

// SetMaxBodyLen sets the longest body a received message may have,
// DefaultMaxBodyLen by default. A connection that sends a longer one is
// closed.
func (h *Handler) SetMaxBodyLen(l int) { h.maxBodyLen = l }

// Malloc returns a buffer for a message, from make unless HandleMalloc or
// EnablePool says otherwise.
func (h *Handler) Malloc(size int) []byte {
	if h.malloc != nil {
		return h.malloc(size)
	}
	return make([]byte, size)
}

// HandleMalloc sets what Malloc calls.
func (h *Handler) HandleMalloc(f func(size int) []byte) { h.malloc = f }

// Free takes back a buffer from Malloc, and does nothing unless HandleFree or
// EnablePool says otherwise.
func (h *Handler) Free(b []byte) {
	if h.free != nil {
		h.free(b)
	}
}

// HandleFree sets what Free calls.
func (h *Handler) HandleFree(f func([]byte)) { h.free = f }

// EnablePool(true) takes message buffers from package bufferpool and releases
// Contexts and Messages once they are handled, so neither may be used after
// its handler returns, nor a payload Bind gave a *[]byte; Message.Retain
// keeps a Message longer. EnablePool(false) goes back to allocating each
// buffer and releasing nothing.
func (h *Handler) EnablePool(enable bool) {
	if !enable {
		h.malloc, h.free, h.onContextDone, h.onMessageDone = nil, nil, nil, nil
		return
	}
	h.malloc = bufferpool.Get
	h.free = bufferpool.Put
	h.onContextDone = func(ctx *Context) { ctx.Release() }
	h.onMessageDone = func(_ *Client, m *Message) { m.Release() }
}

// Use adds a middleware to the chain of every method, those registered from
// now on and those already registered, where it runs after the method's own
// handler. The chain goes on once a middleware returns, unless it calls
// Context.Abort. Stream methods have no chain.
func (h *Handler) Use(mw HandlerFunc) {
	if mw == nil {
		return
	}
	next := func(ctx *Context) {
		mw(ctx)
		ctx.Next()
	}
	h.middles = append(h.middles, next)
	for k, v := range h.routes {
		h.routes[k] = &route{async: v.async, handlers: append(append([]HandlerFunc(nil), v.handlers...), next)}
	}
}

// UseCoder adds a MessageCoder.
func (h *Handler) UseCoder(coder MessageCoder) {
	if coder != nil {
		h.coders = append(h.coders, coder)
	}
}

// Coders returns the MessageCoders.
func (h *Handler) Coders() []MessageCoder { return h.coders }

// Handle registers the handler of method, after the middlewares registered so
// far. async, if given, says whether it runs on the task pool, overriding
// SetAsyncResponse. It panics for an empty method, which HandleNotFound
// registers, one too long, or one already registered.
func (h *Handler) Handle(method string, handler HandlerFunc, async ...bool) {
	if method == "" {
		panic("arpc: the empty method is the one no handler is registered for; register it with HandleNotFound")
	}
	h.handle(method, handler, async...)
}

// HandleNotFound registers the handler of the methods no other handler is
// registered for. Without one, a request for such a method gets the error
// response ErrMethodNotFound. It runs where the connection is read.
func (h *Handler) HandleNotFound(handler HandlerFunc) {
	h.handle("", handler, false)
}

// notFound is the chain of an unknown method before HandleNotFound or Handle
// has registered one.
var notFound = &route{handlers: []HandlerFunc{func(ctx *Context) {
	_ = ctx.Error(ErrMethodNotFound)
	ctx.Next()
}}}

func (h *Handler) handle(method string, handler HandlerFunc, async ...bool) {
	if len(method) > MaxMethodLen {
		panic(fmt.Sprintf("arpc: method length %d is over MaxMethodLen %d", len(method), MaxMethodLen))
	}
	if handler == nil {
		panic("arpc: nil handler for method " + method)
	}
	if h.routes == nil {
		h.routes = map[string]*route{}
	}
	if _, ok := h.routes[""]; !ok {
		h.routes[""] = &route{handlers: append(append([]HandlerFunc(nil), h.middles...), notFound.handlers...)}
	}
	if _, ok := h.routes[method]; ok && method != "" {
		panic("arpc: handler exists for method " + method)
	}
	r := &route{async: h.asyncResponse}
	if len(async) > 0 {
		r.async = async[0]
	}
	r.handlers = append(append([]HandlerFunc(nil), h.middles...), func(ctx *Context) {
		handler(ctx)
		ctx.Next()
	})
	h.routes[method] = r
}

// HandleStream registers the handler of the Streams the peer opens for
// method, which runs when the first message of one arrives. async overrides
// SetAsyncResponse, as it does for Handle.
func (h *Handler) HandleStream(method string, handler StreamHandlerFunc, async ...bool) {
	if len(method) == 0 || len(method) > MaxMethodLen {
		panic(fmt.Sprintf("arpc: stream method length %d, should be 1 to %d", len(method), MaxMethodLen))
	}
	if handler == nil {
		panic("arpc: nil stream handler for method " + method)
	}
	if h.streams == nil {
		h.streams = map[string]*streamRoute{}
	}
	if _, ok := h.streams[method]; ok {
		panic("arpc: stream handler exists for method " + method)
	}
	sr := &streamRoute{async: h.asyncResponse, handler: handler}
	if len(async) > 0 {
		sr.async = async[0]
	}
	h.streams[method] = sr
}

var (
	typeContext     = reflect.TypeFor[context.Context]()
	typeHandlerFunc = reflect.TypeFor[HandlerFunc]()
)

// bindingSuffix names the method Register pairs with a typed one.
const bindingSuffix = "Binding"

// Register registers the eligible methods of service, each under the route
// name + "." + its name, or just its name when name is empty. Eligible are
// exported methods that are either
//
//   - func(ctx context.Context, req *Req, rsp *Rsp), with Req and Rsp structs.
//     If service also has a HandlerFunc method of that name plus "Binding",
//     that one is registered for the route, and is expected to make req and
//     rsp and call the first. Otherwise a handler is made that does so by
//     reflection: it binds the request into a new req, calls the method with
//     the Context, and writes rsp as the response.
//   - a HandlerFunc, func(*arpc.Context), that is not the "Binding" of one of
//     the above.
//
// It panics if no method is eligible, or where Handle would; the error is
// only for a nil service.
func (h *Handler) Register(name string, service any) error {
	if service == nil {
		return fmt.Errorf("arpc: Register: nil service")
	}
	sv := reflect.ValueOf(service)
	st := sv.Type()
	routeName := func(method string) string {
		if name == "" {
			return method
		}
		return name + "." + method
	}
	paired := map[string]bool{}
	registered := 0
	for i := 0; i < st.NumMethod(); i++ {
		method := st.Method(i)
		if !method.IsExported() || strings.HasSuffix(method.Name, bindingSuffix) {
			continue
		}
		mt := method.Type
		if mt.NumIn() != 4 || mt.NumOut() != 0 || mt.In(1) != typeContext ||
			!isStructPtr(mt.In(2)) || !isStructPtr(mt.In(3)) {
			continue
		}
		bindingName := method.Name + bindingSuffix
		if binding := sv.MethodByName(bindingName); binding.IsValid() && binding.Type().ConvertibleTo(typeHandlerFunc) {
			h.Handle(routeName(method.Name), binding.Convert(typeHandlerFunc).Interface().(HandlerFunc))
			paired[bindingName] = true
		} else {
			h.Handle(routeName(method.Name), structHandler(sv.Method(i), mt.In(2), mt.In(3)))
		}
		registered++
	}
	for i := 0; i < st.NumMethod(); i++ {
		method := st.Method(i)
		if !method.IsExported() || paired[method.Name] {
			continue
		}
		mv := sv.Method(i)
		if !mv.Type().ConvertibleTo(typeHandlerFunc) {
			continue
		}
		h.Handle(routeName(method.Name), mv.Convert(typeHandlerFunc).Interface().(HandlerFunc))
		registered++
	}
	if registered == 0 {
		panic(fmt.Sprintf("arpc: Register: no eligible method on %v", st))
	}
	return nil
}

func isStructPtr(t reflect.Type) bool {
	return t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct
}

// structHandler is the handler Register makes for fn, a bound
// func(context.Context, *Req, *Rsp) with no "Binding" method.
func structHandler(fn reflect.Value, reqType, rspType reflect.Type) HandlerFunc {
	return func(ctx *Context) {
		req := reflect.New(reqType.Elem())
		if err := ctx.Bind(req.Interface()); err != nil {
			_ = ctx.Error(err)
			return
		}
		rsp := reflect.New(rspType.Elem())
		fn.Call([]reflect.Value{reflect.ValueOf(ctx), req, rsp})
		_ = ctx.Write(rsp.Interface())
	}
}

// Singleflight makes the calls of a Client for method share one request
// while one is in flight with the same key: Call, CallContext and CallAsync.
// keyFunc computes the key from the request; without one, it is req.String()
// for a fmt.Stringer and fmt.Sprint(req) otherwise.
func (h *Handler) Singleflight(method string, keyFunc ...func(req any) string) {
	if method == "" {
		panic("arpc: Singleflight of the empty method")
	}
	if h.singleflights == nil {
		h.singleflights = map[string]func(any) string{}
	}
	kf := defaultSingleflightKey
	if len(keyFunc) > 0 && keyFunc[0] != nil {
		kf = keyFunc[0]
	}
	h.singleflights[method] = kf
}

// SingleflightKey reports whether method shares its calls and, if it does,
// the key req's call shares by.
func (h *Handler) SingleflightKey(method string, req any) (string, bool) {
	kf, ok := h.singleflights[method]
	if !ok {
		return "", false
	}
	return kf(req), true
}

func defaultSingleflightKey(req any) string {
	if s, ok := req.(fmt.Stringer); ok {
		return s.String()
	}
	return fmt.Sprint(req)
}

// NewMessage builds a Message with h's buffers, as the package-level
// NewMessage does.
func (h *Handler) NewMessage(cmd byte, method string, v any, seq uint64, codec Codec, values map[any]any) *Message {
	return NewMessage(cmd, method, v, seq, h, codec, values)
}

// NewMessageWithBuffer wraps buffer, a whole encoded message, in a Message.
// With EnablePool, buffer should come from Malloc, since releasing the Message
// frees it.
func (h *Handler) NewMessageWithBuffer(buffer []byte) *Message {
	msg := messagePool.Get().(*Message)
	msg.Buffer = buffer
	msg.handler = h
	return msg
}

// onMessage dispatches a message c received: it answers a ping, decodes the
// message with the coders, and then runs the chain of a request or notify,
// completes the call a response answers, or feeds a Stream.
func (h *Handler) onMessage(c *Client, msg *Message) {
	switch msg.Cmd() {
	case CmdPing:
		h.OnMessageDone(c, msg)
		c.pong()
		return
	case CmdPong:
		h.OnMessageDone(c, msg)
		return
	}
	for i := len(h.coders) - 1; i >= 0; i-- {
		msg = h.coders[i].Decode(c, msg)
	}
	if ml := msg.MethodLen(); ml <= 0 || ml > MaxMethodLen || ml > len(msg.Buffer)-HeadLen {
		logger().Warn("arpc: dropped a message with an invalid method length", "length", ml, "remote", c.remoteAddr())
		h.OnMessageDone(c, msg)
		return
	}
	switch cmd := msg.Cmd(); cmd {
	case CmdRequest, CmdNotify:
		r, ok := h.routes[msg.method()]
		if !ok {
			logger().Warn("arpc: no handler for method", "method", msg.Method(), "remote", c.remoteAddr())
			if r, ok = h.routes[""]; !ok {
				r = notFound
			}
		}
		ctx := newContext(c, msg, r.handlers)
		if r.async {
			c.runTask(ctx)
		} else {
			ctx.run()
		}
	case CmdResponse:
		c.onResponse(msg)
	case CmdStream:
		c.onStreamMessage(msg)
	default:
		h.OnMessageDone(c, msg)
		c.closeWithError(fmt.Errorf("%w %d", ErrInvalidCmd, cmd))
	}
}

// recoverHandler logs a panic of a handler, which leaves its connection open.
func recoverHandler(what string, recovered any) {
	logger().Error("arpc: "+what+" panicked", "panic", recovered, "stack", string(debug.Stack()))
}
