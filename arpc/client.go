//go:build linux || darwin || windows

package arpc

import (
	"context"
	stdtls "crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/taskpool"
	fibtls "github.com/lesismal/fib/tls"
)

const (
	// TimeZero as the timeout of PushMsg means not to wait.
	TimeZero time.Duration = 0
	// TimeForever as the timeout of PushMsg means to wait for as long as it
	// takes.
	TimeForever time.Duration = 1<<63 - 1
)

// DialerFunc opens a Client's connection: it dials with handler and reports
// the outcome to done, as fib.Engine.DialWithHandler does. A Client calls it
// again to reconnect.
type DialerFunc func(handler fib.Handler, done func(*fib.Connection, error)) error

// TCPDialer dials addr on engine, as fib.Engine.DialWithHandler does.
func TCPDialer(engine *fib.Engine, network, addr string, timeout time.Duration) DialerFunc {
	return func(handler fib.Handler, done func(*fib.Connection, error)) error {
		return engine.DialWithHandler(network, addr, timeout, handler, done)
	}
}

// TLSDialer dials addr on engine over TLS, as package tls's Dial does.
func TLSDialer(engine *fib.Engine, network, addr string, timeout time.Duration, config *stdtls.Config) DialerFunc {
	return func(handler fib.Handler, done func(*fib.Connection, error)) error {
		return fibtls.Dial(engine, network, addr, timeout, config, handler, done)
	}
}

type clientState int32

const (
	stateConnecting clientState = iota
	stateRunning
	stateReconnecting
	stateStopped
)

// Client is one arpc connection, on either side: one Dial or NewClient opens,
// which reconnects when its connection breaks, or one a Server accepts. Both
// call the peer and serve its calls. It is safe for concurrent use.
type Client struct {
	// Codec encodes and decodes payloads.
	Codec Codec
	// Handler handles the messages and events of the connection.
	Handler *Handler
	// Dialer opens a dialed Client's connection; nil for an accepted one.
	Dialer DialerFunc

	seq atomic.Uint64
	// state is the clientState, changed under mu.
	state atomic.Int32
	// conn is the connection while the Client is running, and nil otherwise.
	conn atomic.Pointer[fib.Connection]
	// enginePool is the HandlerPool of conn's engine.
	enginePool atomic.Pointer[taskpool.TaskPool]
	// chClose is closed when the Client stops.
	chClose  chan struct{}
	finished atomic.Bool
	server   *Server
	handler  *clientHandler

	mu            sync.Mutex
	sessions      map[uint64]chan *Message
	asyncHandlers map[uint64]*asyncHandler
	streamLocal   map[uint64]*Stream
	streamRemote  map[uint64]*Stream
	values        map[any]any

	sfGroup singleflightGroup
}

type asyncHandler struct {
	timer   *time.Timer
	handler AsyncHandlerFunc
}

func newClient(codec Codec, handler *Handler) *Client {
	return &Client{
		Codec:         codec,
		Handler:       handler,
		chClose:       make(chan struct{}),
		sessions:      map[uint64]chan *Message{},
		asyncHandlers: map[uint64]*asyncHandler{},
		streamLocal:   map[uint64]*Stream{},
		streamRemote:  map[uint64]*Stream{},
	}
}

// NewClient opens a Client with dialer, and waits until it is connected or
// the dial has failed. handler is the Client's Handler, or a clone of
// DefaultHandler if it is nil; its Codec is DefaultCodec.
//
// It must not be called where the engine's event loop would have to run for
// it to return: in a fib.Handler's OnOpen, or in a done callback of a dial.
// Stop the Client before closing its engine, which ends its connections
// without a word to their handlers.
func NewClient(dialer DialerFunc, handler *Handler) (*Client, error) {
	if handler == nil {
		handler = DefaultHandler.Clone()
	}
	c := newClient(DefaultCodec, handler)
	c.Dialer = dialer
	c.handler = &clientHandler{c: c}
	dialed := make(chan error, 1)
	if err := dialer(c.handler, func(_ *fib.Connection, err error) { dialed <- err }); err != nil {
		return nil, err
	}
	if err := <-dialed; err != nil {
		c.mu.Lock()
		c.state.Store(int32(stateStopped))
		c.mu.Unlock()
		return nil, err
	}
	if c.Conn() == nil {
		// The connection closed as it opened.
		c.Stop()
		return nil, ErrClientStopped
	}
	if handler.onConnected != nil {
		c.goFunc(c.connected)
	}
	return c, nil
}

// Dial is NewClient with TCPDialer.
func Dial(engine *fib.Engine, network, addr string, timeout time.Duration, handler *Handler) (*Client, error) {
	return NewClient(TCPDialer(engine, network, addr, timeout), handler)
}

// clientHandler is the fib.Handler of a dialed Client's connections.
type clientHandler struct{ c *Client }

func (h *clientHandler) OnOpen(conn *fib.Connection) {
	if !h.c.attach(conn) {
		conn.Close()
	}
}

func (h *clientHandler) OnData(conn *fib.Connection, data []byte) {
	if st, ok := conn.Attachment().(*connState); ok {
		st.feed(conn, data)
	}
}

func (h *clientHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *clientHandler) OnClose(conn *fib.Connection, err error) {
	if st, ok := conn.Attachment().(*connState); ok {
		st.release()
	}
	h.c.onClose(conn)
}

// attach makes conn the Client's connection, unless the Client has stopped.
func (c *Client) attach(conn *fib.Connection) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if clientState(c.state.Load()) == stateStopped {
		return false
	}
	if pool := conn.Engine().HandlerPool(); pool != nil {
		c.enginePool.Store(pool)
	}
	conn.SetAttachment(&connState{client: c})
	c.conn.Store(conn)
	c.state.Store(int32(stateRunning))
	return true
}

// onClose ends conn: the calls and Streams waiting on it fail, and the Client
// either reconnects or stops.
func (c *Client) onClose(conn *fib.Connection) {
	c.mu.Lock()
	if c.conn.Load() != conn {
		c.mu.Unlock()
		return
	}
	c.conn.Store(nil)
	reconnect := c.Dialer != nil && clientState(c.state.Load()) != stateStopped
	cause := ErrClientStopped
	if reconnect {
		cause = ErrClientReconnecting
		c.state.Store(int32(stateReconnecting))
	} else {
		c.stopLocked()
	}
	pending := c.takePendingLocked()
	c.mu.Unlock()
	pending.fail(c, cause)
	if !reconnect {
		c.finish()
		return
	}
	addr := ""
	if remote := conn.RemoteAddr(); remote != nil {
		addr = remote.String()
	}
	c.redial(addr, 1)
}

// terminate stops a Client whose connection ended without OnClose, as those
// of an engine that closes do.
func (c *Client) terminate() {
	c.mu.Lock()
	c.conn.Store(nil)
	c.stopLocked()
	pending := c.takePendingLocked()
	c.mu.Unlock()
	pending.fail(c, ErrClientStopped)
	c.finish()
}

// stopLocked moves the Client to stopped. Callers hold mu.
func (c *Client) stopLocked() {
	if clientState(c.state.Load()) != stateStopped {
		c.state.Store(int32(stateStopped))
		close(c.chClose)
	}
}

// redial makes attempt number times to reconnect: the first at once, and
// each later one after Handler.ReconnectInterval.
func (c *Client) redial(addr string, times int) {
	dial := func() {
		if clientState(c.state.Load()) == stateStopped {
			return
		}
		done := func(_ *fib.Connection, err error) {
			// The event loop runs done, which must not wait for the callbacks.
			go c.redialed(addr, times, err)
		}
		if err := c.Dialer(c.handler, done); err != nil {
			go c.redialed(addr, times, err)
		}
	}
	if times == 1 {
		dial()
		return
	}
	time.AfterFunc(c.Handler.reconnectInterval, dial)
}

// redialed reports an attempt to reconnect, and makes the next one if it
// failed and the attempts have not run out.
func (c *Client) redialed(addr string, times int, err error) {
	if err == nil && c.Conn() == nil {
		// Stopped while dialing, so OnOpen closed the connection.
		return
	}
	info := &ReconnectInfo{Times: times, MaxTimes: c.Handler.maxReconnectTimes, Addr: addr, Success: err == nil, Err: err}
	if f := c.Handler.onReconnect; f != nil {
		safeCall("reconnect callback", func() { f(c, info) })
	}
	if err == nil {
		c.connected()
		return
	}
	if clientState(c.state.Load()) == stateStopped {
		return
	}
	// An engine that has stopped dials nothing more, however many times it
	// is asked.
	if max := c.Handler.maxReconnectTimes; max > 0 && times >= max || errors.Is(err, net.ErrClosed) {
		c.mu.Lock()
		c.stopLocked()
		c.mu.Unlock()
		c.finish()
		return
	}
	c.redial(addr, times+1)
}

// connected runs the connected callback.
func (c *Client) connected() {
	if f := c.Handler.onConnected; f != nil {
		safeCall("connected callback", func() { f(c) })
	}
}

// finish runs, once, what follows the Client stopping.
func (c *Client) finish() {
	if c.finished.Swap(true) {
		return
	}
	if c.server != nil {
		c.server.removeClient(c)
	}
	if f := c.Handler.onDisconnected; f != nil {
		c.goFunc(func() { f(c) })
	}
}

// pending is what fails when a connection is lost.
type pending struct {
	asyncHandlers map[uint64]*asyncHandler
	streams       []*Stream
}

// takePendingLocked fails the calls waiting for a response, and takes the
// asynchronous calls and Streams for fail. Callers hold mu.
func (c *Client) takePendingLocked() pending {
	for seq, ch := range c.sessions {
		close(ch)
		delete(c.sessions, seq)
	}
	p := pending{asyncHandlers: c.asyncHandlers}
	c.asyncHandlers = map[uint64]*asyncHandler{}
	for _, s := range c.streamLocal {
		p.streams = append(p.streams, s)
	}
	for _, s := range c.streamRemote {
		p.streams = append(p.streams, s)
	}
	c.streamLocal = map[uint64]*Stream{}
	c.streamRemote = map[uint64]*Stream{}
	return p
}

func (p pending) fail(c *Client, cause error) {
	for _, ah := range p.asyncHandlers {
		ah.timer.Stop()
		handler := ah.handler
		c.goFunc(func() { handler(nil, cause) })
	}
	for _, s := range p.streams {
		s.abort()
	}
}

// Conn returns the connection, or nil while the Client has none.
func (c *Client) Conn() *fib.Connection { return c.conn.Load() }

// IsClient reports whether the Client was dialed.
func (c *Client) IsClient() bool { return c.Dialer != nil }

// IsServer reports whether a Server accepted the Client.
func (c *Client) IsServer() bool { return c.Dialer == nil }

// CheckState returns nil while the Client is connected, ErrClientStopped once
// it has stopped, and ErrClientReconnecting otherwise.
func (c *Client) CheckState() error {
	_, err := c.liveConn()
	return err
}

func (c *Client) liveConn() (*fib.Connection, error) {
	if conn := c.conn.Load(); conn != nil {
		return conn, nil
	}
	if clientState(c.state.Load()) == stateStopped {
		return nil, ErrClientStopped
	}
	return nil, ErrClientReconnecting
}

// brokenErr is why a call whose connection broke failed.
func (c *Client) brokenErr() error {
	if clientState(c.state.Load()) == stateStopped {
		return ErrClientStopped
	}
	return ErrClientReconnecting
}

func (c *Client) remoteAddr() string {
	if conn := c.conn.Load(); conn != nil {
		if addr := conn.RemoteAddr(); addr != nil {
			return addr.String()
		}
	}
	return ""
}

func (c *Client) closeWithError(err error) {
	if conn := c.conn.Load(); conn != nil {
		conn.CloseWithError(err)
	}
}

// Stop closes the connection, and a dialed Client stops reconnecting. The
// disconnected callback follows.
func (c *Client) Stop() {
	c.mu.Lock()
	if clientState(c.state.Load()) == stateStopped {
		c.mu.Unlock()
		return
	}
	c.stopLocked()
	conn := c.conn.Load()
	c.mu.Unlock()
	if conn != nil {
		// OnClose finishes.
		conn.Close()
		return
	}
	c.finish()
}

// Get returns the value stored on the Client for key.
func (c *Client) Get(key any) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	return value, ok
}

// Set stores value on the Client for key. It does nothing if either is nil.
func (c *Client) Set(key, value any) {
	if key == nil || value == nil {
		return
	}
	c.mu.Lock()
	if c.values == nil {
		c.values = map[any]any{}
	}
	c.values[key] = value
	c.mu.Unlock()
}

// Delete deletes the value stored for key.
func (c *Client) Delete(key any) {
	c.mu.Lock()
	delete(c.values, key)
	c.mu.Unlock()
}

// Ping sends a ping, which the peer answers with a pong.
func (c *Client) Ping() {
	if conn := c.conn.Load(); conn != nil {
		_ = c.write(conn, pingFrame[:], nil)
	}
}

func (c *Client) pong() {
	if conn := c.conn.Load(); conn != nil {
		_ = c.write(conn, pongFrame[:], nil)
	}
}

// Keepalive pings the peer every interval, 30 seconds if it is zero or less,
// until the Client stops.
func (c *Client) Keepalive(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	var tick func()
	tick = func() {
		if clientState(c.state.Load()) == stateStopped {
			return
		}
		c.Ping()
		time.AfterFunc(interval, tick)
	}
	time.AfterFunc(interval, tick)
}

// NewMessage builds a Message with the next sequence number and the Client's
// Handler and Codec, for PushMsg.
func (c *Client) NewMessage(cmd byte, method string, v any, values ...map[any]any) *Message {
	return NewMessage(cmd, method, v, c.seq.Add(1), c.Handler, c.Codec, firstValues(values))
}

func firstValues(values []map[any]any) map[any]any {
	if len(values) > 0 {
		return values[0]
	}
	return nil
}

// sendValue sends the message h describes with v, encoded by the Codec, as
// its payload. values are the Message's, for the coders.
func (c *Client) sendValue(h header, v any, values map[any]any) error {
	data, err := valueToBytes(c.Codec, v)
	if err != nil {
		return err
	}
	return c.sendData(h, data, values)
}

func (c *Client) sendData(h header, data []byte, values map[any]any) error {
	conn, err := c.liveConn()
	if err != nil {
		return err
	}
	if len(c.Handler.coders) != 0 {
		return c.sendMessage(conn, newMessage(c.Handler, h, data, values))
	}
	// The header goes to the connection beside the payload, which the
	// connection copies only if the socket cannot take it at once.
	head := bufferpool.Get(HeadLen + len(h.method))
	h.put(head, len(data))
	err = c.write(conn, head, data)
	bufferpool.Put(head)
	return err
}

// sendMessage encodes msg with the coders and sends it.
func (c *Client) sendMessage(conn *fib.Connection, msg *Message) error {
	for _, coder := range c.Handler.coders {
		msg = coder.Encode(c, msg)
	}
	err := c.write(conn, msg.Buffer, nil)
	c.Handler.OnMessageDone(c, msg)
	return err
}

// maxSpare is the largest buffer write keeps for the next batch.
const maxSpare = 64 << 10

// write sends one message, first followed by second, on conn. While another
// goroutine is writing to conn, the message is appended to what that
// goroutine writes next instead, and write returns at once: handlers
// finishing together on the pool then reach the socket in one write between
// them, rather than in a write each, every one taking the connection's lock
// for its syscall. The writing goroutine keeps on until nothing more has been
// appended.
//
// An appended message's error is not its sender's to see, as it is not for
// a message the connection queues.
func (c *Client) write(conn *fib.Connection, first, second []byte) error {
	st, ok := conn.Attachment().(*connState)
	if !ok {
		return conn.SendParts(first, second)
	}
	st.outMu.Lock()
	if st.writing {
		st.out = append(append(st.out, first...), second...)
		st.outMu.Unlock()
		return nil
	}
	st.writing = true
	st.outMu.Unlock()
	err := conn.SendParts(first, second)
	for {
		st.outMu.Lock()
		if len(st.out) == 0 {
			st.writing = false
			st.outMu.Unlock()
			return err
		}
		batch := st.out
		st.out = st.spare[:0]
		st.outMu.Unlock()
		_ = conn.Send(batch)
		if cap(batch) > maxSpare {
			batch = nil
		}
		// Only the writing goroutine touches spare.
		st.spare = batch
	}
}

// PushMsg sends msg as it is, encoded by the coders, and then hands it to
// OnMessageDone. A message goes to the connection at once, so there is
// nothing for timeout to bound.
func (c *Client) PushMsg(msg *Message, timeout time.Duration) error {
	conn, err := c.liveConn()
	if err != nil {
		c.Handler.OnMessageDone(c, msg)
		return err
	}
	return c.sendMessage(conn, msg)
}

func (c *Client) checkStateAndMethod(method string) error {
	if err := c.CheckState(); err != nil {
		return err
	}
	return checkMethod(method)
}

func checkTimeout(timeout time.Duration) error {
	if timeout == 0 {
		return ErrClientInvalidTimeoutZero
	}
	if timeout < 0 {
		return ErrClientInvalidTimeoutLessThanZero
	}
	return nil
}

// Call sends a request for method with req and waits up to timeout for the
// response, which it decodes into rsp. values, if given, are the request
// Message's, for the coders.
//
// req and rsp may be a []byte or a string, a pointer to one for rsp, or what
// the Codec encodes. An error response is returned as an error.
func (c *Client) Call(method string, req, rsp any, timeout time.Duration, values ...map[any]any) error {
	if err := c.checkStateAndMethod(method); err != nil {
		return err
	}
	if err := checkTimeout(timeout); err != nil {
		return err
	}
	if key, ok := c.Handler.SingleflightKey(method, req); ok {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return c.callSingleflight(ctx, method, req, rsp, key, values)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	resp, err := c.roundTrip(method, req, timer.C, nil, values)
	if err != nil {
		return err
	}
	err = c.parseResponse(resp, rsp)
	c.Handler.OnMessageDone(c, resp)
	return err
}

// CallContext is Call waiting until ctx ends instead of for a timeout.
func (c *Client) CallContext(ctx context.Context, method string, req, rsp any, values ...map[any]any) error {
	if err := c.checkStateAndMethod(method); err != nil {
		return err
	}
	if key, ok := c.Handler.SingleflightKey(method, req); ok {
		return c.callSingleflight(ctx, method, req, rsp, key, values)
	}
	resp, err := c.roundTrip(method, req, nil, ctx.Done(), values)
	if err != nil {
		return err
	}
	err = c.parseResponse(resp, rsp)
	c.Handler.OnMessageDone(c, resp)
	return err
}

// CallWith is CallContext.
func (c *Client) CallWith(ctx context.Context, method string, req, rsp any, values ...map[any]any) error {
	return c.CallContext(ctx, method, req, rsp, values...)
}

var sessionPool = sync.Pool{New: func() any { return make(chan *Message, 1) }}

// roundTrip sends a request and waits for its response until timeout fires or
// done is closed. The caller hands the response to OnMessageDone.
func (c *Client) roundTrip(method string, req any, timeout <-chan time.Time, done <-chan struct{}, values []map[any]any) (*Message, error) {
	data, err := valueToBytes(c.Codec, req)
	if err != nil {
		return nil, err
	}
	seq := c.seq.Add(1)
	ch := sessionPool.Get().(chan *Message)
	c.mu.Lock()
	if err := c.CheckState(); err != nil {
		c.mu.Unlock()
		sessionPool.Put(ch)
		return nil, err
	}
	c.sessions[seq] = ch
	c.mu.Unlock()
	defer c.endSession(seq, ch)
	if err := c.sendData(header{cmd: CmdRequest, method: method, seq: seq}, data, firstValues(values)); err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		if resp == nil {
			return nil, c.brokenErr()
		}
		return resp, nil
	case <-timeout:
		return nil, ErrClientTimeout
	case <-done:
		return nil, ErrClientTimeout
	case <-c.chClose:
		return nil, ErrClientStopped
	}
}

// endSession forgets the call of seq, and pools its channel unless the
// connection's loss closed it. A response that arrived after the caller gave
// up waiting is a session miss.
func (c *Client) endSession(seq uint64, ch chan *Message) {
	c.mu.Lock()
	delete(c.sessions, seq)
	c.mu.Unlock()
	select {
	case msg, ok := <-ch:
		if !ok {
			return
		}
		c.sessionMiss(msg)
	default:
	}
	sessionPool.Put(ch)
}

func (c *Client) sessionMiss(msg *Message) {
	if f := c.Handler.onSessionMiss; f != nil {
		f(c, msg)
	}
	c.Handler.OnMessageDone(c, msg)
}

// parseResponse decodes resp into rsp: a *string or a *[]byte gets a copy of
// the payload, and anything else is what the Codec decodes.
func (c *Client) parseResponse(resp *Message, rsp any) error {
	if resp.Cmd() != CmdResponse {
		return ErrInvalidRspMessage
	}
	if resp.IsError() {
		return resp.Error()
	}
	return bytesToValue(c.Codec, resp.Data(), rsp)
}

// onResponse completes the call msg answers.
func (c *Client) onResponse(msg *Message) {
	seq := msg.Seq()
	if !msg.IsAsync() {
		c.mu.Lock()
		ch, ok := c.sessions[seq]
		if ok {
			// Delivered under the lock, so that once endSession has
			// deleted the call nothing more arrives on its channel.
			delete(c.sessions, seq)
			ch <- msg
		}
		c.mu.Unlock()
		if !ok {
			logger().Debug("arpc: response to a call that has ended", "method", msg.Method(), "remote", c.remoteAddr())
			c.sessionMiss(msg)
		}
		return
	}
	ah := c.takeAsyncHandler(seq)
	if ah == nil {
		logger().Debug("arpc: response to an asynchronous call that has ended", "method", msg.Method(), "remote", c.remoteAddr())
		c.sessionMiss(msg)
		return
	}
	ctx := newContext(c, msg, nil)
	safeCall("async call handler", func() { ah.handler(ctx, msg.Error()) })
	c.Handler.OnContextDone(ctx)
}

func (c *Client) takeAsyncHandler(seq uint64) *asyncHandler {
	c.mu.Lock()
	ah, ok := c.asyncHandlers[seq]
	if ok {
		delete(c.asyncHandlers, seq)
		ah.timer.Stop()
	}
	c.mu.Unlock()
	return ah
}

// CallAsync sends a request for method with req and returns without waiting
// for the response. handler runs once: with the response, where the
// connection is read; or, without one, with ErrTimeout once timeout has
// passed or ErrClientReconnecting or ErrClientStopped if the connection is
// lost, on the task pool. It does not run if CallAsync returns an error.
func (c *Client) CallAsync(method string, req any, handler AsyncHandlerFunc, timeout time.Duration, values ...map[any]any) error {
	if err := c.checkStateAndMethod(method); err != nil {
		return err
	}
	if err := checkTimeout(timeout); err != nil {
		return err
	}
	if handler == nil {
		return ErrClientInvalidAsyncHandler
	}
	if key, ok := c.Handler.SingleflightKey(method, req); ok {
		return c.callAsyncSingleflight(method, req, handler, timeout, key, values)
	}
	return c.callAsyncOnce(method, req, handler, timeout, values)
}

func (c *Client) callAsyncOnce(method string, req any, handler AsyncHandlerFunc, timeout time.Duration, values []map[any]any) error {
	data, err := valueToBytes(c.Codec, req)
	if err != nil {
		return err
	}
	seq := c.seq.Add(1)
	ah := &asyncHandler{handler: handler}
	c.mu.Lock()
	if err := c.CheckState(); err != nil {
		c.mu.Unlock()
		return err
	}
	c.asyncHandlers[seq] = ah
	ah.timer = time.AfterFunc(timeout, func() {
		if ah := c.takeAsyncHandler(seq); ah != nil {
			safeCall("async call handler", func() { ah.handler(nil, ErrTimeout) })
		}
	})
	c.mu.Unlock()
	h := header{cmd: CmdRequest, flags: HeaderFlagMaskAsync, method: method, seq: seq}
	if err := c.sendData(h, data, firstValues(values)); err != nil {
		if c.takeAsyncHandler(seq) == nil {
			// The connection's loss or the timeout took the call first and
			// has the handler, so it is not the caller's error to report.
			return nil
		}
		return err
	}
	return nil
}

// Notify sends a notify for method with data, which the peer does not answer.
// A notify goes to the connection at once, so there is nothing for timeout to
// bound, but a negative one is still an error.
func (c *Client) Notify(method string, data any, timeout time.Duration, values ...map[any]any) error {
	if err := c.checkStateAndMethod(method); err != nil {
		return err
	}
	if timeout < 0 {
		return ErrClientInvalidTimeoutLessThanZero
	}
	return c.notify(method, data, values)
}

// NotifyContext is Notify. A notify goes to the connection at once, so there
// is nothing for ctx to bound.
func (c *Client) NotifyContext(ctx context.Context, method string, data any, values ...map[any]any) error {
	if err := c.checkStateAndMethod(method); err != nil {
		return err
	}
	return c.notify(method, data, values)
}

// NotifyWith is NotifyContext.
func (c *Client) NotifyWith(ctx context.Context, method string, data any, values ...map[any]any) error {
	return c.NotifyContext(ctx, method, data, values...)
}

func (c *Client) notify(method string, data any, values []map[any]any) error {
	h := header{cmd: CmdNotify, flags: HeaderFlagMaskAsync, method: method, seq: c.seq.Add(1)}
	return c.sendValue(h, data, firstValues(values))
}

// taskPool is the pool asynchronous work of the Client runs on, nil if there
// is none.
func (c *Client) taskPool() fib.TaskPool {
	if pool := c.Handler.pool; pool != nil {
		return pool
	}
	if pool := c.enginePool.Load(); pool != nil {
		return pool
	}
	return nil
}

// runTask runs a handler on the task pool, or here if there is none or it
// turns the task away.
func (c *Client) runTask(task taskpool.Task) {
	if pool := c.taskPool(); pool != nil && pool.GoTask(task) {
		return
	}
	task.RunTask()
}

// goFunc runs a callback on the task pool, or on a goroutine of its own if
// there is none or it turns the callback away: a callback is not run where it
// is called, which may be an event loop.
func (c *Client) goFunc(f func()) {
	task := funcTask(f)
	if pool := c.taskPool(); pool != nil && pool.GoTask(task) {
		return
	}
	go task.RunTask()
}

// funcTask is a callback on the task pool. A panic is logged rather than
// handed to the pool.
type funcTask func()

func (f funcTask) RunTask() { safeCall("callback", f) }

func safeCall(what string, f func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			recoverHandler(what, recovered)
		}
	}()
	f()
}

// connState is a connection's own state: on the reading side the message it
// is receiving, which it hands to the Client's Handler once whole, and on the
// writing side what write appends while another goroutine writes.
type connState struct {
	client *Client
	head   [HeadLen]byte
	// headLen is how much of a header that arrived split is in head.
	headLen int
	// msg is the message being received, filled up to filled.
	msg    *Message
	filled int

	outMu   sync.Mutex
	out     []byte
	spare   []byte
	writing bool
}

// feed takes what the connection read and dispatches every message it
// completes.
func (st *connState) feed(conn *fib.Connection, data []byte) {
	c, h := st.client, st.client.Handler
	for len(data) > 0 {
		if st.msg == nil {
			var head []byte
			if st.headLen == 0 && len(data) >= HeadLen {
				head, data = data[:HeadLen], data[HeadLen:]
			} else {
				n := copy(st.head[st.headLen:], data)
				st.headLen += n
				data = data[n:]
				if st.headLen < HeadLen {
					return
				}
				head, st.headLen = st.head[:], 0
			}
			bodyLen := binary.LittleEndian.Uint32(head[HeaderIndexBodyLenBegin:])
			if uint64(bodyLen) > uint64(h.maxBodyLen) {
				conn.CloseWithError(fmt.Errorf("%w: %d bytes", ErrBodyTooLarge, bodyLen))
				return
			}
			msg := messagePool.Get().(*Message)
			msg.handler = h
			msg.Buffer = h.Malloc(HeadLen + int(bodyLen))
			copy(msg.Buffer, head)
			if bodyLen == 0 {
				h.onMessage(c, msg)
				continue
			}
			st.msg, st.filled = msg, HeadLen
		}
		n := copy(st.msg.Buffer[st.filled:], data)
		st.filled += n
		data = data[n:]
		if st.filled < len(st.msg.Buffer) {
			return
		}
		msg := st.msg
		st.msg = nil
		h.onMessage(c, msg)
	}
}

// release frees a message the connection closed in the middle of.
func (st *connState) release() {
	if st.msg != nil {
		st.msg.Release()
		st.msg = nil
	}
}

// ClientPool is a fixed set of dialed Clients.
type ClientPool struct {
	round   atomic.Uint64
	clients []*Client
}

// NewClientPool dials size Clients with dialer, sharing handler, or a clone of
// DefaultHandler if it is nil. If one fails, those dialed are stopped.
func NewClientPool(dialer DialerFunc, size int, handler *Handler) (*ClientPool, error) {
	dialers := make([]DialerFunc, size)
	for i := range dialers {
		dialers[i] = dialer
	}
	return NewClientPoolFromDialers(dialers, handler)
}

// NewClientPoolFromDialers is NewClientPool with a Client for each dialer.
func NewClientPoolFromDialers(dialers []DialerFunc, handler *Handler) (*ClientPool, error) {
	if len(dialers) == 0 {
		return nil, ErrClientInvalidPoolDialers
	}
	if handler == nil {
		handler = DefaultHandler.Clone()
	}
	pool := &ClientPool{}
	pool.round.Store(^uint64(0))
	for _, dialer := range dialers {
		c, err := NewClient(dialer, handler)
		if err != nil {
			pool.Stop()
			return nil, err
		}
		pool.clients = append(pool.clients, c)
	}
	return pool, nil
}

// Size returns the number of Clients.
func (p *ClientPool) Size() int { return len(p.clients) }

// Get returns the Client at index, modulo the size.
func (p *ClientPool) Get(index int) *Client { return p.clients[uint64(index)%uint64(len(p.clients))] }

// Next returns the next Client in turn that is connected, or, if none is, the
// last one it tried.
func (p *ClientPool) Next() *Client {
	n := uint64(len(p.clients))
	var c *Client
	for range n {
		c = p.clients[p.round.Add(1)%n]
		if c.Conn() != nil {
			return c
		}
	}
	return c
}

// Handler returns the Handler the Clients share.
func (p *ClientPool) Handler() *Handler { return p.clients[0].Handler }

// Stop stops every Client.
func (p *ClientPool) Stop() {
	for _, c := range p.clients {
		c.Stop()
	}
}
