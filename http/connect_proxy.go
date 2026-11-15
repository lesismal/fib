//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/base64"
	"errors"
	"net"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
)

// Defaults for the ConnectProxyConfig fields left at zero.
const (
	DefaultConnectDialTimeout    = 10 * time.Second
	DefaultConnectMaxQueuedBytes = 256 << 10
)

// ConnectProxyConfig is how a ConnectProxy serves its clients.
type ConnectProxyConfig struct {
	// Authenticate, when set, has every client log in with the Basic
	// credentials of a Proxy-Authorization header (RFC 9110 section
	// 11.7.2), and answers one that does not with 407 Proxy Authentication
	// Required. Nil admits every client. Realm names what the client logs
	// in to; empty means "proxy".
	Authenticate func(username, password string) bool
	Realm        string
	// Allow decides each request once its client has logged in: a nil error
	// grants it, and an error answers it with 403 Forbidden. Nil grants
	// everything, which leaves the proxy open to whoever can reach it.
	Allow func(r *stdhttp.Request) error
	// Dial reaches the host and port a request names, with the contract of
	// fib.Engine.DialWithHandler, on the engine of the client's connection.
	// Nil dials directly; a socks.Dialer's Dial reaches it through a SOCKS
	// proxy.
	Dial func(engine *fib.Engine, network, addr string, timeout time.Duration,
		handler fib.Handler, done func(*fib.Connection, error)) error
	// DialTimeout bounds the dial. Zero means DefaultConnectDialTimeout,
	// and a negative value leaves it to the operating system.
	DialTimeout time.Duration
	// MaxQueuedBytes is how much of what one side of a tunnel sends may wait
	// for the other side before the proxy stops taking more from the first
	// side, so that a fast sender is held back by flow control rather than
	// by memory here. Zero means DefaultConnectMaxQueuedBytes.
	MaxQueuedBytes int
	// IdleTimeout closes a tunnel and its connection to the target once
	// nothing has gone either way for this long. Zero or less means no idle
	// timeout.
	IdleTimeout time.Duration
	// Next serves the requests that are not CONNECTs to a host and port, an
	// extended CONNECT among them. Nil answers them with 405 Method Not
	// Allowed.
	Next Handler
}

// ConnectProxy is a Handler that serves CONNECT requests (RFC 9110 section
// 9.3.6) over HTTP/1.x, HTTP/2 and HTTP/3: it dials the host and port the
// request names, accepts the request with Context.Connect once the dial has
// succeeded, and carries the bytes between the tunnel and that connection
// from the callback that received them. A request it cannot reach is
// answered with 502 Bad Gateway, or 504 Gateway Timeout when the dial timed
// out; one whose target is not a host and port, with 400 Bad Request.
//
// Clients use it as an HTTPS proxy: curl -x, a browser's proxy settings and
// the HTTPS_PROXY variable all tunnel through CONNECT. A request for an
// http:// URL a client sends a proxy whole, in absolute form, rather than
// through CONNECT, and is Next's to serve.
//
// Its handler does not wait for the dial: it retains the request, returns,
// and the dial's callback answers it.
type ConnectProxy struct {
	config ConnectProxyConfig
}

// NewConnectProxy returns a ConnectProxy, with the defaults in place of
// config's zeros.
func NewConnectProxy(config ConnectProxyConfig) *ConnectProxy {
	if config.Realm == "" {
		config.Realm = "proxy"
	}
	if config.Dial == nil {
		config.Dial = (*fib.Engine).DialWithHandler
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = DefaultConnectDialTimeout
	} else if config.DialTimeout < 0 {
		config.DialTimeout = 0
	}
	if config.MaxQueuedBytes <= 0 {
		config.MaxQueuedBytes = DefaultConnectMaxQueuedBytes
	}
	return &ConnectProxy{config: config}
}

func (p *ConnectProxy) ServeHTTP(c *Context) {
	r := c.Request
	if _, extended := r.Header[":protocol"]; r.Method != stdhttp.MethodConnect || extended {
		if p.config.Next != nil {
			p.config.Next.ServeHTTP(c)
			return
		}
		p.answer(c, stdhttp.StatusMethodNotAllowed, stdhttp.Header{"Allow": {stdhttp.MethodConnect}})
		return
	}
	// Each protocol's parser sets URL.Host to a CONNECT's target.
	target := r.URL.Host
	if _, port, err := net.SplitHostPort(target); err != nil || !validPort(port) {
		p.answer(c, stdhttp.StatusBadRequest, nil)
		return
	}
	if p.config.Authenticate != nil && !p.loggedIn(r) {
		p.answer(c, stdhttp.StatusProxyAuthRequired,
			stdhttp.Header{"Proxy-Authenticate": {`Basic realm="` + p.config.Realm + `"`}})
		return
	}
	if p.config.Allow != nil && p.config.Allow(r) != nil {
		p.answer(c, stdhttp.StatusForbidden, nil)
		return
	}
	relay := &connectRelay{limit: p.config.MaxQueuedBytes, idle: p.config.IdleTimeout}
	// The request is answered once the dial is done, from its callback. That
	// may come while this handler, or one wrapping it, still runs on the
	// goroutine serving the request, which on HTTP/1 holds the connection's
	// parser: the answer waits for the handler's return, and is made on
	// that goroutine then, or on the handler pool once it has returned.
	c.Retain()
	if err := p.config.Dial(c.Conn.Engine(), "tcp", target, p.config.DialTimeout, (*relayTarget)(relay),
		func(_ *fib.Connection, err error) {
			c.later(func(worker bool) { p.dialed(c, relay, err, worker) })
		}); err != nil {
		p.dialed(c, relay, err, false)
	}
}

// dialed answers the request c, retained while its target was dialed, once
// the dial is done: the tunnel opens, or err says why it could not. worker
// says it runs on the goroutine that served c, after the handler returned.
func (p *ConnectProxy) dialed(c *Context, relay *connectRelay, err error, worker bool) {
	defer c.Release()
	if err != nil {
		status := stdhttp.StatusBadGateway
		var netErr net.Error
		if errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
			status = stdhttp.StatusGatewayTimeout
		}
		p.answer(c, status, nil)
		return
	}
	if _, err := c.switchTunnelOn("", nil, (*relayClient)(relay), worker); err != nil {
		relay.target.Close()
		p.answer(c, stdhttp.StatusBadRequest, nil)
	}
}

// validPort reports whether port is a port number, which a CONNECT's
// target needs.
func validPort(port string) bool {
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n > 0
}

// loggedIn reports whether r carries Basic credentials Authenticate takes.
func (p *ConnectProxy) loggedIn(r *stdhttp.Request) bool {
	scheme, encoded, ok := strings.Cut(r.Header.Get("Proxy-Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Basic") {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return false
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	return ok && p.config.Authenticate(username, password)
}

// answer refuses a request with status and a short body, ending an HTTP/1
// connection afterwards: a client that sent a CONNECT may have sent the
// tunnel's first bytes behind it, which the connection cannot parse.
func (p *ConnectProxy) answer(c *Context, status int, header stdhttp.Header) {
	if header == nil {
		header = make(stdhttp.Header, 2)
	}
	header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.Request.ProtoMajor == 1 && c.Request.Method == stdhttp.MethodConnect {
		header.Set("Connection", "close")
	}
	// A response that cannot be written has lost its connection already.
	_ = c.WriteResponse(Response{StatusCode: status, Header: header,
		Body: []byte(stdhttp.StatusText(status) + "\n")})
}

// connectRelay carries a CONNECT's bytes between its tunnel and the
// connection to its target, each side's callbacks sending to the other.
// The target's reads are held from its open until the tunnel opens, so
// that nothing it sends comes before the 200 that accepts the request.
//
// Each side is paced by the other's backlog, not its own, which
// SetWriteBackpressure turns off on the connections the relay has to
// itself: a target that waits to send its answers before it reads more
// would otherwise wait for this side forever. One goroutine at a time per
// direction waits for the backlog to drain, which holding and waiting say;
// over HTTP/2 and HTTP/3 what the client's window allowed still arrives
// while its reads are held.
type connectRelay struct {
	limit  int
	target *fib.Connection

	// idle, when set, closes both sides once nothing has gone either way
	// for that long: last is when something last did, in Unix nanoseconds,
	// and timer, set under mu, looks at it from the tunnel's open until a
	// side closes.
	idle  time.Duration
	last  atomic.Int64
	timer *time.Timer
	ended atomic.Bool

	holdingTarget, holdingClient atomic.Bool

	mu sync.Mutex
	// tunnel is set once it opens, and targetClosed once the target has
	// closed: the second of the two to happen ends the tunnel.
	tunnel       *Tunnel
	targetClosed bool
}

// touch records that something went through the relay, for its idle
// timeout.
func (r *connectRelay) touch() {
	if r.idle > 0 {
		r.last.Store(time.Now().UnixNano())
	}
}

// checkIdle closes a relay that carried nothing for the idle timeout, and
// looks again when it last carried something otherwise.
func (r *connectRelay) checkIdle() {
	if r.ended.Load() {
		return
	}
	since := time.Since(time.Unix(0, r.last.Load()))
	r.mu.Lock()
	t, timer := r.tunnel, r.timer
	r.mu.Unlock()
	if since >= r.idle {
		_ = t.Close()
		r.target.Close()
		return
	}
	timer.Reset(r.idle - since)
}

// stopIdle stops the idle timeout once a side has closed.
func (r *connectRelay) stopIdle() {
	if !r.ended.CompareAndSwap(false, true) {
		return
	}
	r.mu.Lock()
	timer := r.timer
	r.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

// relayTarget is the handler of the connection to the target.
type relayTarget connectRelay

func (h *relayTarget) OnOpen(c *fib.Connection) {
	h.target = c
	c.SetWriteBackpressure(false)
	c.HoldReads(true)
}

// OnData sends what the target sent through the tunnel, and holds the
// target's reads while the tunnel has more than the limit to send.
func (h *relayTarget) OnData(c *fib.Connection, data []byte) {
	(*connectRelay)(h).touch()
	t := h.tunnel
	if t.Send(data) != nil || t.Unsent() <= h.limit || !h.holdingTarget.CompareAndSwap(false, true) {
		// A tunnel that has ended closes the target from OnTunnelClose.
		return
	}
	c.HoldReads(true)
	release := func(error) {
		c.HoldReads(false)
		h.holdingTarget.Store(false)
	}
	if !t.NotifySendable(h.limit, release) {
		// It drained since Unsent looked.
		release(nil)
	}
}

func (h *relayTarget) OnPriorityData(*fib.Connection, []byte) {}

func (h *relayTarget) OnClose(*fib.Connection, error) {
	(*connectRelay)(h).stopIdle()
	h.mu.Lock()
	h.targetClosed = true
	t := h.tunnel
	h.mu.Unlock()
	if t != nil {
		t.CloseAfterSend()
	}
}

// relayClient is the tunnel's handler.
type relayClient connectRelay

func (h *relayClient) OnTunnelOpen(t *Tunnel) {
	h.mu.Lock()
	h.tunnel = t
	closed := h.targetClosed
	if !closed && h.idle > 0 {
		h.last.Store(time.Now().UnixNano())
		h.timer = time.AfterFunc(h.idle, (*connectRelay)(h).checkIdle)
	}
	h.mu.Unlock()
	if closed {
		t.CloseAfterSend()
		return
	}
	if t.w == TunnelWriter(t.conn) {
		// HTTP/1: the client's connection is the tunnel's alone.
		t.conn.SetWriteBackpressure(false)
	}
	h.target.HoldReads(false)
}

// OnTunnelData sends what the client sent to the target, and holds the
// tunnel's reads while the target has more than the limit queued.
func (h *relayClient) OnTunnelData(t *Tunnel, data []byte) {
	(*connectRelay)(h).touch()
	target := h.target
	if target.Send(data) != nil || outputwait.Pending == nil || outputwait.Pending(target) <= int64(h.limit) ||
		!h.holdingClient.CompareAndSwap(false, true) {
		// A target that has closed ends the tunnel from its OnClose.
		return
	}
	t.HoldReads(true)
	go func() {
		_ = outputwait.Wait(target, int64(h.limit))
		t.HoldReads(false)
		h.holdingClient.Store(false)
	}()
}

func (h *relayClient) OnTunnelClose(*Tunnel, error) {
	(*connectRelay)(h).stopIdle()
	h.target.CloseAfterSend()
}
