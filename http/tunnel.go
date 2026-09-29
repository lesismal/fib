//go:build linux || darwin || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"sync"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// A Tunnel is what Context.Upgrade switches a request to: a stream of bytes
// in each direction, carrying another protocol, such as WebSocket. Over
// HTTP/1.1 it is the connection itself, once 101 Switching Protocols has gone
// out (RFC 9110 section 7.8). Over HTTP/2 and HTTP/3 it is the request's own
// stream, opened by an extended CONNECT (RFC 8441, RFC 9220) and accepted with
// a 200 that leaves it open: what the peer sends in its DATA frames arrives
// through the tunnel, and what is sent through it goes out in DATA frames of
// the stream's own, while the connection goes on carrying other requests.

var (
	// ErrNotUpgradable is what Upgrade reports for a request that did not ask
	// for the protocol, or that cannot switch: one whose body is still
	// arriving on HTTP/1, an HTTP/1.0 one, or one served by a protocol that has
	// no tunnels.
	ErrNotUpgradable = errors.New("http: request cannot be upgraded")
	// ErrTunnelBacklog is what a tunnel reports when more is waiting in it than
	// its limit: bytes the peer sent before the handler upgraded, or on HTTP/2,
	// bytes sent through it that the peer's flow control has not let go yet.
	ErrTunnelBacklog = errors.New("http: tunnel backlog too large")
)

// TunnelHandler takes what arrives through a Tunnel. Its calls are one at a
// time and in order, whichever goroutine they come from: OnTunnelOpen first,
// then OnTunnelData for every piece of what the peer sends, and then, once,
// OnTunnelClose when the tunnel has ended — the peer ended its side or reset
// it, the tunnel was closed, or its connection went.
type TunnelHandler interface {
	// OnTunnelOpen is called before Upgrade returns, from its goroutine.
	OnTunnelOpen(t *Tunnel)
	// OnTunnelData's data is valid only for the duration of the call.
	OnTunnelData(t *Tunnel, data []byte)
	OnTunnelClose(t *Tunnel, err error)
}

// TunnelWriter is what a Tunnel sends through: the connection itself on
// HTTP/1, which a *fib.Connection is, or a stream of a protocol that frames
// what it sends, as a protocol served outside this package gives NewTunnelFeed.
type TunnelWriter interface {
	// SendParts sends first and then second, which the caller may reuse once
	// it returns.
	SendParts(first, second []byte) error
	// SendOwned sends data, which the writer takes over.
	SendOwned(data []byte) error
	// CloseAfterSend ends the tunnel once what has been sent through it has
	// gone: the connection, or this side of the stream.
	CloseAfterSend()
	// Close ends the tunnel at once, dropping what has not gone yet.
	Close() error
}

// Tunnel is a request switched to another protocol by Context.Upgrade. Its
// methods are safe to call from any goroutine.
type Tunnel struct {
	conn  *fib.Connection
	w     TunnelWriter
	limit int

	mu      sync.Mutex
	handler TunnelHandler
	// queued is what arrived while no call could be made: before the tunnel
	// was opened, or while another call was running.
	queued []byte
	// opened is set once the handler has it; delivering while a goroutine is
	// making the handler's calls, which is then the one to make those queued
	// behind it; ended once the tunnel has ended, and told once OnTunnelClose
	// has been called.
	opened, delivering, ended, told bool
	err                             error
}

// Conn is the connection the tunnel runs on. On HTTP/2 and HTTP/3 it is
// shared with other requests, so nothing is to be sent on it directly.
func (t *Tunnel) Conn() *fib.Connection { return t.conn }

// Send sends data, which the caller may reuse once Send returns.
func (t *Tunnel) Send(data []byte) error { return t.w.SendParts(data, nil) }

// SendParts sends first and then second, without joining them first.
func (t *Tunnel) SendParts(first, second []byte) error { return t.w.SendParts(first, second) }

// SendOwned sends data, which the tunnel takes over: the caller must not use it
// afterwards.
func (t *Tunnel) SendOwned(data []byte) error { return t.w.SendOwned(data) }

// CloseAfterSend ends the tunnel once what has been sent through it has gone.
// On HTTP/2 and HTTP/3 that ends this side of the stream, and the tunnel ends
// when the peer ends its side too.
func (t *Tunnel) CloseAfterSend() { t.w.CloseAfterSend() }

// Close ends the tunnel at once: the connection on HTTP/1, and a reset of the
// stream on HTTP/2 and HTTP/3.
func (t *Tunnel) Close() error { return t.w.Close() }

// deliver hands data to the handler, or queues it behind a call being made,
// or until the tunnel opens.
func (t *Tunnel) deliver(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	t.mu.Lock()
	if t.ended {
		t.mu.Unlock()
		return nil
	}
	if !t.opened || t.delivering {
		if t.limit > 0 && len(t.queued)+len(data) > t.limit {
			t.mu.Unlock()
			return ErrTunnelBacklog
		}
		t.queued = bufferpool.Append(t.queued, data)
		t.mu.Unlock()
		return nil
	}
	t.delivering = true
	handler := t.handler
	t.mu.Unlock()
	handler.OnTunnelData(t, data)
	t.drain()
	return nil
}

// drain makes the calls that queued while this goroutine was making one, and
// OnTunnelClose once the tunnel has ended and nothing is left before it. It
// runs with delivering set, and clears it.
func (t *Tunnel) drain() {
	for {
		t.mu.Lock()
		queued := t.queued
		t.queued = nil
		switch {
		case len(queued) > 0:
			t.mu.Unlock()
			t.handler.OnTunnelData(t, queued)
			bufferpool.Put(queued)
		case t.ended && !t.told:
			t.told = true
			err := t.err
			t.mu.Unlock()
			t.handler.OnTunnelClose(t, err)
		default:
			t.delivering = false
			t.mu.Unlock()
			return
		}
	}
}

// open hands the tunnel to handler, and with it whatever arrived before.
func (t *Tunnel) open(handler TunnelHandler) {
	t.mu.Lock()
	t.handler = handler
	t.opened, t.delivering = true, true
	t.mu.Unlock()
	handler.OnTunnelOpen(t)
	t.drain()
}

// end ends the tunnel, which the handler hears once what arrived before has
// reached it.
func (t *Tunnel) end(err error) {
	t.mu.Lock()
	if t.ended {
		t.mu.Unlock()
		return
	}
	t.ended, t.err = true, err
	if !t.opened || t.delivering {
		// The tunnel's opening, or the goroutine making its calls, tells it.
		t.mu.Unlock()
		return
	}
	t.delivering = true
	t.mu.Unlock()
	t.drain()
}

// TunnelFeed is the producing side of a Tunnel for the protocol it runs on:
// HTTP/1 and HTTP/2 here, and HTTP/3 in package http3. The protocol makes one
// when an extended CONNECT arrives, before its handler runs, writes into it
// what the peer sends, and ends it when the stream ends.
type TunnelFeed struct {
	tunnel Tunnel
}

// NewTunnelFeed starts the tunnel of a request that arrived on conn, which
// sends through w. limit bounds what is held for the handler before it
// upgrades, or while it is busy with an earlier piece; zero leaves it
// unbounded.
func NewTunnelFeed(conn *fib.Connection, w TunnelWriter, limit int) *TunnelFeed {
	return &TunnelFeed{tunnel: Tunnel{conn: conn, w: w, limit: limit}}
}

// Tunnel is the tunnel the feed produces.
func (f *TunnelFeed) Tunnel() *Tunnel { return &f.tunnel }

// Write hands the handler what the peer sent, which it may keep only for the
// call, or holds it until the handler can take it; data may be reused once
// Write returns. It reports ErrTunnelBacklog when that is more than the limit.
// What arrives after the tunnel has ended is dropped.
func (f *TunnelFeed) Write(data []byte) error { return f.tunnel.deliver(data) }

// End ends the tunnel, with err saying why: nil or io.EOF for a peer that
// ended its side. The handler hears of it once, after what arrived before.
func (f *TunnelFeed) End(err error) { f.tunnel.end(err) }

// StreamUpgrader is implemented by a Stream whose request can become a tunnel:
// an extended CONNECT on a protocol served outside this package.
type StreamUpgrader interface {
	// Upgrade accepts req's extended CONNECT for protocol with a 200 carrying
	// header, leaving the stream open, and returns the stream's tunnel, which
	// the Context then opens. It reports ErrNotUpgradable when req is not an
	// extended CONNECT for protocol.
	Upgrade(req *stdhttp.Request, protocol string, header stdhttp.Header) (*Tunnel, error)
}

// Upgrade switches the request to protocol, whose data handler then takes,
// and returns the tunnel it runs through. header is sent with the response
// that accepts the switch, which Upgrade writes: 101 Switching Protocols on
// HTTP/1.1, and 200 on HTTP/2 and HTTP/3. The OnHeader and OnFinish hooks run
// for that response.
//
// On HTTP/1.1 the request has to ask for protocol in its Upgrade header, and
// the whole connection switches; whatever the client sent after the request
// is the tunnel's first data. On HTTP/2 and HTTP/3 the request has to be an
// extended CONNECT whose :protocol is protocol (RFC 8441, RFC 9220), which
// the servers here accept and report in Request.Header as ":protocol", and
// only its stream switches. Upgrade reports ErrNotUpgradable for any other
// request.
//
// It is called by the handler, before it returns and before it has written
// any of the response; handler's OnTunnelOpen has run by the time it returns.
func (c *Context) Upgrade(protocol string, header stdhttp.Header, handler TunnelHandler) (*Tunnel, error) {
	if handler == nil || protocol == "" || !validHeaderValue(protocol) {
		return nil, errors.New("http: invalid upgrade")
	}
	if c.begun() {
		return nil, ErrResponseWritten
	}
	if !c.handling() {
		return nil, ErrNotUpgradable
	}
	status := stdhttp.StatusOK
	if c.external == nil && c.stream == nil {
		status = stdhttp.StatusSwitchingProtocols
	}
	if hooks := c.hooked(); hooks != nil {
		// The hooks may change the header, which is the caller's.
		header = header.Clone()
		if header == nil {
			header = make(stdhttp.Header)
		}
		hooks.beforeHeader(status, header)
	}
	var t *Tunnel
	var err error
	switch {
	case c.external != nil:
		upgrader, ok := c.external.(StreamUpgrader)
		if !ok {
			return nil, ErrNotUpgradable
		}
		t, err = upgrader.Upgrade(c.Request, protocol, header)
	case c.stream != nil:
		t, err = c.stream.upgrade(c.Request, protocol, header)
	default:
		t, err = c.upgradeHTTP1(protocol, header)
	}
	if err != nil {
		if hooks := c.hooked(); hooks != nil {
			// Nothing was sent, so the response the handler answers with
			// instead still has its header hooks to run.
			hooks.ran = false
		}
		return nil, err
	}
	c.wrote = true
	if hooks := c.hooked(); hooks != nil {
		hooks.finished(status, header, 0)
	}
	t.open(handler)
	return t, nil
}

// upgradeHTTP1 switches an HTTP/1.1 connection to protocol. The handler runs
// on the goroutine driving the connection, which holds its parser, so nothing
// else reads the connection until the handler returns and drive sees it has
// switched.
func (c *Context) upgradeHTTP1(protocol string, header stdhttp.Header) (*Tunnel, error) {
	parser, server := c.parser, c.server
	request := c.Request
	if parser == nil || server == nil || parser.stream != nil || !request.ProtoAtLeast(1, 1) ||
		!headerHasToken(request.Header, "Connection", "upgrade") || !upgradeOffers(request.Header, protocol) {
		return nil, ErrNotUpgradable
	}
	out := make([]byte, 0, headCapacity(header)+64+len(protocol))
	out = append(out, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: "...)
	out = append(out, protocol...)
	out = append(out, crlf...)
	out, err := appendHeaderLines(out, header, func(key string) bool {
		return key == "Connection" || key == "Upgrade" || key == "Content-Length" || key == "Transfer-Encoding"
	})
	if err != nil {
		return nil, err
	}
	out = append(out, crlf...)
	conn := c.Conn
	if err := conn.SendOwned(out); err != nil {
		return nil, err
	}
	// The connection is the tunnel's from here on; see drive.
	rest := parser.TakeBuffered()
	server.releaseTimeouts(conn, parser)
	parser.upgraded = true
	feed := NewTunnelFeed(conn, conn, 0)
	conn.SetAttachment(feed)
	_ = feed.Write(rest)
	bufferpool.Put(rest)
	return feed.Tunnel(), nil
}

// upgradeOffers reports whether an Upgrade header offers protocol, whose
// version, if the offer names one, is ignored.
func upgradeOffers(header stdhttp.Header, protocol string) bool {
	for _, value := range header["Upgrade"] {
		for item := range strings.SplitSeq(value, ",") {
			name, _, _ := strings.Cut(strings.TrimSpace(item), "/")
			if strings.EqualFold(name, protocol) {
				return true
			}
		}
	}
	return false
}
