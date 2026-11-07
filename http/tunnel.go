//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"sync"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/outputwait"
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

// TunnelShutdownHandler is a TunnelHandler told that the server is shutting
// down; see fib.Engine.Shutdown. A tunnel is the protocol it carries' to end,
// and keeps its connection, and so the shutdown, open until it does:
// OnTunnelShutdown is that protocol's cue to end it, as a WebSocket does by
// sending a close frame with 1001 (going away).
//
// It is called once per tunnel, after OnTunnelOpen, on a goroutine of the
// server's, and may run while another of the tunnel's calls does; a tunnel
// that ends first may not hear it at all.
type TunnelShutdownHandler interface {
	TunnelHandler
	OnTunnelShutdown(t *Tunnel)
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
	// has been called. greeted is set once OnTunnelOpen has returned, and
	// shutdown once the server has said it is shutting down, which the
	// handler hears after the first of the two if it came second; see
	// TunnelShutdownHandler.
	opened, delivering, ended, told bool
	greeted, shutdown               bool
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

// SendShared sends data, which the tunnel only reads, so the same array may
// go through many tunnels at once; the caller must not change it afterwards.
// Over HTTP/1.1 it is fib.Connection.SendShared, which queues data itself
// rather than a copy. A writer that has no such method frames what it sends,
// and is handed data as SendParts would hand it.
func (t *Tunnel) SendShared(data []byte) error {
	if s, ok := t.w.(interface{ SendShared([]byte) error }); ok {
		return s.SendShared(data)
	}
	return t.w.SendParts(data, nil)
}

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
	t.mu.Lock()
	t.greeted = true
	shutdown := t.shutdown
	t.mu.Unlock()
	if shutdown {
		t.tellShutdown()
	}
	t.drain()
}

// markShutdown records that the server is shutting down, and reports whether
// the handler is to be told now, with tellShutdown: once, for a tunnel whose
// OnTunnelOpen has returned and that has not ended. One still opening is
// told by open once OnTunnelOpen returns.
func (t *Tunnel) markShutdown() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.shutdown || t.ended {
		return false
	}
	t.shutdown = true
	return t.greeted
}

// tellShutdown tells the handler, if it takes it, that the server is
// shutting down.
func (t *Tunnel) tellShutdown() {
	t.mu.Lock()
	handler := t.handler
	t.mu.Unlock()
	if h, ok := handler.(TunnelShutdownHandler); ok {
		h.OnTunnelShutdown(t)
	}
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

// Shutdown tells the tunnel's handler that the server is shutting down, if
// it is a TunnelShutdownHandler: now, if the tunnel is open, or as it opens.
// It does so once however often it is called, and not for a tunnel that has
// ended. The protocol calls it when the engine shuts down, from a goroutine
// that may wait, since the handler's call runs on it.
func (f *TunnelFeed) Shutdown() {
	if f.tunnel.markShutdown() {
		f.tunnel.tellShutdown()
	}
}

// StreamUpgrader is implemented by a Stream whose request can become a tunnel:
// a CONNECT on a protocol served outside this package.
type StreamUpgrader interface {
	// Upgrade accepts req's extended CONNECT for protocol, or with protocol
	// empty req's CONNECT to a host and port, with a 200 carrying header,
	// leaving the stream open, and returns the stream's tunnel, which the
	// Context then opens. It reports ErrNotUpgradable when req is not such a
	// CONNECT.
	Upgrade(req *stdhttp.Request, protocol string, header stdhttp.Header) (*Tunnel, error)
}

// TunnelPacer is implemented by a TunnelWriter that can pace its tunnel, as
// those of HTTP/2 and HTTP/3 do; see Tunnel.HoldReads, Tunnel.Unsent and
// Tunnel.WaitSendable, which call it.
type TunnelPacer interface {
	HoldReads(hold bool)
	Unsent() int
	WaitSendable(limit int) error
}

// TunnelNotifier is implemented by a TunnelWriter that can tell when what
// its tunnel holds back has drained, as those of HTTP/2 and HTTP/3 do; see
// Tunnel.NotifySendable, which calls it.
type TunnelNotifier interface {
	NotifySendable(limit int, fn func(error)) bool
}

// HoldReads stops or resumes taking what the peer sends through the tunnel,
// so that a handler that passes it on to something slower paces the peer by
// flow control rather than by memory here: over HTTP/1.1 the connection
// stops reading its socket, and over HTTP/2 and HTTP/3 the stream stops
// giving the peer room for more, so that at most what its window allowed
// already still arrives. Holds do not nest: the last call wins. It may be
// called from any goroutine, OnTunnelData included, and does nothing on a
// tunnel whose writer cannot pace it.
func (t *Tunnel) HoldReads(hold bool) {
	switch w := t.w.(type) {
	case *fib.Connection:
		w.HoldReads(hold)
	case TunnelPacer:
		w.HoldReads(hold)
	}
}

// SetWriteBackpressure says whether what is waiting to go through the
// tunnel holds back what it takes in, as it does by default. A relay that
// passes each of two tunnels on to the other, and paces them with HoldReads
// and NotifySendable, turns it off on both, as fibhttp.ConnectProxy does:
// left on, a side whose sends to its peer back up stops reading that peer,
// and a peer that answers as it reads, waiting to send before it reads
// more, then waits for it forever. Over HTTP/1.1, where the connection is
// the tunnel's alone, it is Connection.SetWriteBackpressure; over HTTP/2
// and HTTP/3 the stream's flow control paces the tunnel and the shared
// connection is left as it is, so it does nothing there.
func (t *Tunnel) SetWriteBackpressure(on bool) {
	if c, ok := t.w.(*fib.Connection); ok && c == t.conn {
		c.SetWriteBackpressure(on)
	}
}

// Unsent reports how many bytes sent through the tunnel are still waiting
// to go to the peer: queued for the connection's socket over HTTP/1.1, and
// held back by the peer's flow control or the connection's over HTTP/2 and
// HTTP/3. It is zero on a tunnel whose writer cannot tell.
func (t *Tunnel) Unsent() int {
	switch w := t.w.(type) {
	case *fib.Connection:
		if outputwait.Unsent != nil {
			return int(outputwait.Unsent(w))
		}
	case TunnelPacer:
		return w.Unsent()
	}
	return 0
}

// WaitSendable waits while more than limit bytes sent through the tunnel are
// still waiting to go, and reports an error once the tunnel has ended. What
// it waits for is the peer reading, which the connection's own callbacks
// carry, so it must not be called from OnTunnelData, nor from anything else
// the connection runs; a goroutine of the caller's is the place. It returns
// at once on a tunnel whose writer cannot pace it.
func (t *Tunnel) WaitSendable(limit int) error {
	switch w := t.w.(type) {
	case *fib.Connection:
		if outputwait.Wait != nil {
			return outputwait.Wait(w, int64(limit))
		}
	case TunnelPacer:
		return w.WaitSendable(limit)
	}
	return nil
}

// NotifySendable is WaitSendable for a caller that may not wait, such as a
// relay that holds the other side's reads while this one drains: when more
// than limit bytes sent through the tunnel are still waiting to go it
// arranges for fn to run, on another goroutine, once no more than half of
// limit are, or with an error once the tunnel has ended, and reports true;
// when no more than limit are, it reports false and fn never runs. It may be
// called from any goroutine, OnTunnelData included, and reports false on a
// tunnel whose writer cannot tell. Over HTTP/1.1 it is
// Connection.NotifyOutput.
func (t *Tunnel) NotifySendable(limit int, fn func(error)) bool {
	switch w := t.w.(type) {
	case *fib.Connection:
		return w.NotifyOutput(int64(limit), fn)
	case TunnelNotifier:
		return w.NotifySendable(limit, fn)
	}
	return false
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
// It is called by the handler, before it has written any of the response:
// before it returns, or later, from any goroutine, on a request it retained,
// as a proxy does once the server it passes the request on to has accepted
// the switch. A retained HTTP/1 request holds its connection meanwhile:
// nothing the client sends after it is read as another request, and it is
// the tunnel's first data once the switch is made. handler's OnTunnelOpen
// has run by the time Upgrade returns.
func (c *Context) Upgrade(protocol string, header stdhttp.Header, handler TunnelHandler) (*Tunnel, error) {
	if handler == nil || protocol == "" || !validHeaderValue(protocol) {
		return nil, errors.New("http: invalid upgrade")
	}
	return c.switchTunnel(protocol, header, handler)
}

// Connect accepts a CONNECT request (RFC 9110 section 9.3.6), which asks
// the server to open a tunnel to the host and port in Request.Host, and
// returns the tunnel it runs through, as Upgrade does for a protocol: header
// goes out with the 200 that accepts it, the OnHeader and OnFinish hooks run
// for that response, and handler takes what the client sends through it.
// Reaching the host and carrying the bytes there and back is the handler's;
// ConnectProxy does both.
//
// On HTTP/1.x the whole connection switches, and whatever the client sent
// after the request is the tunnel's first data. On HTTP/2 and HTTP/3 the
// request is a CONNECT that names no :protocol (RFC 9113 section 8.5, RFC
// 9114 section 4.4), and only its stream switches. Connect reports
// ErrNotUpgradable for any other request, and like Upgrade it is called by
// the handler before it has written any of the response, before it returns
// or later on a request it retained.
func (c *Context) Connect(header stdhttp.Header, handler TunnelHandler) (*Tunnel, error) {
	if handler == nil {
		return nil, errors.New("http: invalid upgrade")
	}
	return c.switchTunnel("", header, handler)
}

// switchTunnel switches the request to a tunnel: for protocol, or for a
// CONNECT to a host and port when protocol is empty.
func (c *Context) switchTunnel(protocol string, header stdhttp.Header, handler TunnelHandler) (*Tunnel, error) {
	if c.begun() {
		return nil, ErrResponseWritten
	}
	late := !c.handling()
	if late && !c.Retained() || protocol == "" && c.Request.Method != stdhttp.MethodConnect {
		return nil, ErrNotUpgradable
	}
	upgrader, _ := c.external.(StreamUpgrader)
	if c.external != nil && upgrader == nil {
		// Refused before the hooks run, so the response the handler answers
		// with instead still has them to run.
		return nil, ErrNotUpgradable
	}
	status := stdhttp.StatusOK
	if c.external == nil && c.stream == nil && protocol != "" {
		status = stdhttp.StatusSwitchingProtocols
	}
	if hooks := c.hooked(); hooks != nil && hooks.changes {
		// The hooks may change the header, which is the caller's.
		header = header.Clone()
		if header == nil {
			header = make(stdhttp.Header)
		}
		hooks.beforeHeader(status, header, true)
	}
	var t *Tunnel
	var err error
	switch {
	case c.external != nil:
		t, err = upgrader.Upgrade(c.Request, protocol, header)
	case c.stream != nil:
		t, err = c.stream.upgrade(c.Request, protocol, header)
	default:
		if late && c.parser != nil {
			// Away from the round that served the request, the connection is
			// the parser's: a round reading it waits for the switch, and then
			// hands what it read to the tunnel.
			c.parser.mu.Lock()
		}
		if protocol == "" {
			t, err = c.connectHTTP1(header)
		} else {
			t, err = c.upgradeHTTP1(protocol, header)
		}
		if late && c.parser != nil {
			c.parser.mu.Unlock()
		}
	}
	if err != nil {
		if hooks := c.hooked(); hooks != nil {
			// Nothing was sent, so the response the handler answers with
			// instead still has its header hooks to run.
			hooks.ran = false
		}
		return nil, err
	}
	c.wrote, c.status = true, uint16(status)
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
	if parser == nil || server == nil || parser.stream != nil || parser.spent || !request.ProtoAtLeast(1, 1) ||
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
	return c.switchHTTP1(out)
}

// connectHTTP1 accepts a CONNECT on HTTP/1.x, which has no content (RFC 9110
// section 9.3.6): what follows its header is the tunnel's.
func (c *Context) connectHTTP1(header stdhttp.Header) (*Tunnel, error) {
	parser, server := c.parser, c.server
	request := c.Request
	if parser == nil || server == nil || parser.stream != nil || parser.spent || request.ContentLength != 0 ||
		len(request.TransferEncoding) > 0 {
		return nil, ErrNotUpgradable
	}
	out := make([]byte, 0, headCapacity(header)+32)
	out = append(out, "HTTP/1.1 200 OK\r\n"...)
	// A 2xx answer to CONNECT has no content, and says nothing of its length
	// (RFC 9110 section 8.6).
	out, err := appendHeaderLines(out, header, func(key string) bool {
		return key == "Content-Length" || key == "Transfer-Encoding"
	})
	if err != nil {
		return nil, err
	}
	out = append(out, crlf...)
	return c.switchHTTP1(out)
}

// switchHTTP1 sends head, the response that switches an HTTP/1 connection,
// and makes the connection the tunnel's.
func (c *Context) switchHTTP1(head []byte) (*Tunnel, error) {
	parser, server := c.parser, c.server
	conn := c.Conn
	if err := conn.SendOwned(head); err != nil {
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
