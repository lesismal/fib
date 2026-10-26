//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Package http3 serves and sends HTTP/3 (RFC 9114) over the fib engine's UDP
// sockets, with QUIC (RFC 9000) and QPACK (RFC 9204) implemented here and
// no dependencies beyond the standard library.
//
// A server is a UDP engine whose handler is NewHandler's. Requests go to the
// same http.Handler that serves HTTP/1 and HTTP/2 in package http, answered
// through the same Context:
//
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Network: "udp", Addr: ":443", Handler: http3.NewHandler(tlsConfig, handler)})
//
// The engine demultiplexes datagrams by peer address, and each address that
// sends a QUIC Initial gets a connection of its own; connection migration is
// therefore not supported, and the server says so to its clients.
//
// Browsers find an HTTP/3 server through the Alt-Svc header of a response
// over HTTP/1 or HTTP/2; AltSvc builds its value, and AltSvcClient follows it
// from a Go client, falling back to TCP when the QUIC attempt fails.
package http3

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	stdhttp "net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	fibtls "github.com/lesismal/fib/tls"
)

// NextProto is the ALPN protocol ID of HTTP/3.
const NextProto = "h3"

// Config configures a server.
type Config struct {
	// BlockingTLSHandshake runs the TLS handshake of every connection through
	// crypto/tls's QUICConn, which parks a goroutine in it for as long as it
	// waits for the client. By default the handshake is a state machine that
	// takes none, where the TLSConfig allows it (see package tls).
	BlockingTLSHandshake bool
	// EarlyData lets clients that resume a session send requests in their
	// first flight, 0-RTT data (RFC 9001 section 4.6), which saves them a
	// round trip. Such data is not protected against replay: an attacker who
	// recorded it can send it again. A request that arrives in it carries
	// "Early-Data: 1" (RFC 8470), and only a safe method (GET, HEAD, OPTIONS,
	// TRACE) is served; anything else is answered 425 Too Early, which tells
	// the client to send it again once the handshake is over.
	EarlyData bool
	// EarlyDataUnsafe serves every method in early data, which then is up to
	// the handler to guard against replays, by the Early-Data header.
	EarlyDataUnsafe bool
	// TLSConfig is required, with the server's certificates. HTTP/3 needs
	// TLS 1.3 and the "h3" ALPN protocol, which the handler sees to.
	TLSConfig *tls.Config
	// MaxHeaderBytes bounds a request's header section, decoded and counted
	// as RFC 9114 section 4.2.2 does; the server also tells clients so.
	MaxHeaderBytes int
	// MaxBodyBytes bounds a request body that is read whole before its
	// handler runs, which is every body unless StreamRequestBody is set.
	MaxBodyBytes int64
	// StreamRequestBody hands a request to its handler as soon as its body
	// has begun, as http.Config.StreamRequestBody does for HTTP/1 and
	// HTTP/2: once its header has arrived, for a body with a
	// content-length, and once some of it has, for one without. The body
	// arrives as an *http.BodyStream in Request.Body, which reads what has
	// arrived without waiting for the rest, and which Context.OnBody takes
	// as it arrives; Context.BodyComplete tells the two cases apart. The
	// client is paced by QUIC's flow control, which gives it room for more
	// only as the handler consumes what it has. Off, the default, a handler
	// runs only once its request has arrived whole.
	StreamRequestBody bool
	// StreamRequestBodyThreshold keeps the smaller bodies read whole when
	// StreamRequestBody is set: only a body whose content-length is larger
	// than this, or one without a content-length of which more than this has
	// arrived, streams. A streamed body's handler is not called until at
	// least this many bytes of it have arrived, so that a handler that knows
	// its ordinary requests are smaller can read Request.Body whole and
	// leave Context.OnBody to the few endpoints that take large bodies. Zero
	// streams every body as soon as its header has arrived.
	StreamRequestBodyThreshold int64
	// StreamRequestBodyAtHeader, with StreamRequestBody, picks requests
	// whose bodies stream to their handlers as soon as their headers have
	// arrived, whatever StreamRequestBodyThreshold says, as package http's
	// Config.StreamRequestBodyAtHeader does. HTTP/3 cannot tell at the
	// header whether a body follows that declares no content-length, so a
	// request picked streams even if its body turns out empty.
	StreamRequestBodyAtHeader func(*stdhttp.Request) bool
	// MaxStreamedBodyBytes bounds a streamed body, which MaxBodyBytes does
	// not; zero leaves it unbounded.
	MaxStreamedBodyBytes int64
	// MaxConcurrentStreams is how many requests a client may have open on
	// one connection.
	MaxConcurrentStreams uint64
	// HandshakeTimeout bounds a connection's QUIC handshake, from its first
	// packet; a connection that has not finished it by then is closed. Zero
	// means the QUIC default, 10 seconds.
	HandshakeTimeout time.Duration
	// MaxHandshakes bounds the QUIC handshakes the server has under way at
	// once, from a client's first Initial to the end of its handshake: past
	// it, a new client's Initial is dropped, and the client sends it again
	// later. RetryHandshakes is how many may be under way before a new
	// client is first asked, with a Retry, to prove that the address it
	// sends from is its own (RFC 9000 section 8.1.2). That costs the client
	// a round trip, but spares the server the TLS work, and the state, of an
	// Initial whose source address is forged, which a flood of them would
	// otherwise have it spend on each until MaxHandshakes turned everyone
	// away. A client whose handshake completes is given a NEW_TOKEN token,
	// good for 10 minutes and bound to its address (RFC 9000 section
	// 8.1.3): its next connection from that address carries the token in
	// its first Initial, and is taken in without the Retry. Below zero,
	// RetryHandshakes asks every client. Zero means DefaultMaxHandshakes and
	// DefaultRetryHandshakes. Transport.DisableNewTokens turns the tokens
	// off.
	MaxHandshakes   int
	RetryHandshakes int
	// StatelessResetKey, if set, is the key the server's stateless reset
	// tokens are made with (RFC 9000 section 10.3). A packet for a
	// connection the server does not have is answered with a stateless
	// reset, which ends the client's side of it at once rather than when it
	// times out; the client believes one only from a server that knew its
	// connection. By default each handler has a random key of its own, so
	// the connections a server had before it restarted, or that another
	// server behind the same address has, time out instead: a key kept
	// across restarts, and shared by such servers, has them reset.
	StatelessResetKey *quic.ResetKey
	// IdleTimeout bounds how long a connection may carry no request: once it
	// has had none open for this long, it is retired as on shutdown, with
	// GOAWAY, and closed once that has gone, as http.Config.IdleTimeout does
	// on HTTP/2. It differs from MaxIdleTimeout, QUIC's, which only closes a
	// connection that hears no packet at all, and which a client that pings
	// keeps open with no request. Zero means ReadTimeout; both zero means no
	// limit. DefaultConfig sets http.DefaultIdleTimeout.
	IdleTimeout time.Duration
	// ReadHeaderTimeout bounds how long a request's header may take to
	// arrive, from the first of its stream's bytes that did not complete it:
	// a stream whose header has not all arrived by then is reset with
	// H3_REQUEST_CANCELLED, which leaves the connection serving. Zero means
	// ReadTimeout; both zero means no limit. DefaultConfig sets
	// http.DefaultReadHeaderTimeout.
	ReadHeaderTimeout time.Duration
	// MaxIdleTimeout closes a connection that has been silent this long.
	// Keep it below the engine's UDPIdleTimeout, or the engine closes a
	// quiet peer first.
	MaxIdleTimeout time.Duration
	// ReadTimeout and ReadBodyTimeout bound each request's body on its own,
	// from its header, as http.Config's do on HTTP/2, and whichever ends
	// first applies. A body that streams to its handler and outlasts it
	// fails with os.ErrDeadlineExceeded, which the handler is told, and a
	// handler can lift it, or set another, with Context.SetReadDeadline; one
	// still being collected before its handler runs has its stream reset
	// with H3_REQUEST_CANCELLED. The rest of the connection goes on serving.
	// Zero means no limit; DefaultConfig sets http.DefaultReadBodyTimeout
	// for ReadBodyTimeout.
	ReadTimeout     time.Duration
	ReadBodyTimeout time.Duration
	// MaxBodyHold bounds how long Context.HoldBody may hold a streamed body
	// at a time, as http.Config.MaxBodyHold does: zero means
	// http.DefaultMaxBodyHold, and below zero no limit.
	MaxBodyHold time.Duration
	// WriteTimeout bounds how long a response may take to be written,
	// measured from when its handler is called, for each request on its own,
	// as http.Config.WriteTimeout does on HTTP/2. A stream whose handler has
	// not finished writing its response by then is reset with
	// H3_INTERNAL_ERROR, which ends that request and leaves the rest of the
	// connection serving: a handler waiting for a client that stopped taking
	// its body is let go, its Context is cancelled, and a Write made after it
	// has passed fails with os.ErrDeadlineExceeded. A handler can lift it,
	// or set another, for its own response with Context.SetWriteDeadline.
	// Zero means no limit.
	WriteTimeout time.Duration
	// MaxDatagramSize is the largest UDP payload the server sends once a
	// connection's handshake is over, if the client allows it, until path
	// MTU discovery finds the path carries more. Zero means 1200 bytes,
	// which every path carries; a network known to carry more saves packets
	// with more, up to 1452. A connection whose larger datagrams stop
	// getting through goes back to 1200.
	MaxDatagramSize int
	// DisablePathMTUDiscovery turns path MTU discovery (RFC 8899) off. With
	// it on, the default, each connection probes for the largest datagram
	// its path carries once the handshake is confirmed, up to
	// MaxProbeDatagramSize and what the client allows, and sends datagrams
	// of that size. The engine's UDP socket is set not to fragment the
	// datagrams it sends (see fib.Connection.SetDontFragment), which
	// discovery needs; where it cannot be, there is no discovery.
	DisablePathMTUDiscovery bool
	// MaxProbeDatagramSize is the largest UDP payload path MTU discovery
	// looks for. Zero means 1452, and so does more: what an Ethernet MTU of
	// 1500 bytes leaves for UDP over IPv6.
	MaxProbeDatagramSize int
	// Transport is how the QUIC connections under the server behave, beyond
	// what the fields above set; see TransportConfig.
	Transport TransportConfig
	// StreamPool runs request handlers on a pool of their own, so that the
	// requests a client has open on one QUIC connection are served
	// concurrently rather than one after another on the goroutine that
	// reads its datagrams. Its zero value is the default, which does that
	// on the handler pool of the engine the connection came from (see
	// fib.Engine.HandlerPool), shared with every other server whose
	// connections come from that engine, HTTP/2 servers included, and sets
	// no per-connection limit; see http.StreamPoolConfig.
	StreamPool fibhttp.StreamPoolConfig
	// DisableReuse leaves each request's objects to the collector. By
	// default the server recycles them once the request has ended: the
	// *http.Request, its Header and URL, and the *Context it is answered
	// through, which the request's stream holds together, under the rule
	// http.Config.ReuseRequests describes: a handler must not keep any of
	// them past its response and its last Release. A request whose body
	// streams to its handler, and an extended CONNECT's, are left to the
	// collector either way.
	DisableReuse bool
}

// The handshakes a server has under way at once by default, past which
// it asks new clients to prove their addresses with a Retry, and drops
// their Initials; see Config.MaxHandshakes.
const (
	DefaultMaxHandshakes   = 4096
	DefaultRetryHandshakes = 1024
)

// DefaultConfig returns the defaults, which are what zero values in a Config
// mean too.
func DefaultConfig() Config {
	return Config{
		MaxHeaderBytes:       1 << 20,
		MaxBodyBytes:         16 << 20,
		MaxConcurrentStreams: 100,
		MaxIdleTimeout:       30 * time.Second,
		MaxHandshakes:        DefaultMaxHandshakes,
		RetryHandshakes:      DefaultRetryHandshakes,
		// A connection goes once it has been quiet for MaxIdleTimeout, so
		// the body is the one read a client can stall for good.
		ReadBodyTimeout:   fibhttp.DefaultReadBodyTimeout,
		ReadHeaderTimeout: fibhttp.DefaultReadHeaderTimeout,
		IdleTimeout:       fibhttp.DefaultIdleTimeout,
	}
}

// ConfigureTLS returns a copy of config offering "h3" through ALPN, which a
// QUIC handshake has to agree on, and requiring TLS 1.3, which QUIC is built
// on.
func ConfigureTLS(config *tls.Config) *tls.Config {
	if config == nil {
		config = &tls.Config{}
	}
	config = config.Clone()
	protos := []string{NextProto}
	for _, p := range config.NextProtos {
		if p != NextProto {
			protos = append(protos, p)
		}
	}
	config.NextProtos = protos
	config.MinVersion = max(config.MinVersion, tls.VersionTLS13)
	return config
}

// AltSvc is the Alt-Svc header value that tells a client, on a response it
// gets over HTTP/1 or HTTP/2, that the same origin speaks HTTP/3 on port.
func AltSvc(port int) string {
	return fmt.Sprintf(`%s=":%d"; ma=86400`, NextProto, port)
}

// ServerHandler serves HTTP/3 on a UDP engine. It is the engine's
// fib.Handler, and each peer's QUIC connection hangs off the peer's
// fib.Connection.
type ServerHandler struct {
	handler  fibhttp.Handler
	config   Config
	quic     quic.Config
	resetKey quic.ResetKey
	// tokenKey makes the tokens of the server's Retry packets, and checks
	// them, and handshakes counts the handshakes under way; see
	// Config.MaxHandshakes.
	tokenKey   *quic.TokenKey
	handshakes atomic.Int64
	// streams runs request handlers away from the goroutine that reads
	// their connection, or is nil when they run on it.
	streams *fibhttp.StreamPool
	// routes finds a connection by the connection ID its client's packets
	// carry, for packets that arrive from an address no connection is on,
	// as they do once a client's address changes under it; see route.
	routesMu sync.RWMutex
	routes   map[string]*quic.Conn
}

// NewHandler serves handler over HTTP/3 with the default limits.
func NewHandler(tlsConfig *tls.Config, handler fibhttp.Handler) *ServerHandler {
	config := DefaultConfig()
	config.TLSConfig = tlsConfig
	return NewHandlerWithConfig(config, handler)
}

// NewHandlerWithConfig serves handler over HTTP/3.
func NewHandlerWithConfig(config Config, handler fibhttp.Handler) *ServerHandler {
	defaults := DefaultConfig()
	if config.MaxHeaderBytes <= 0 {
		config.MaxHeaderBytes = defaults.MaxHeaderBytes
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if config.MaxConcurrentStreams == 0 {
		config.MaxConcurrentStreams = defaults.MaxConcurrentStreams
	}
	if config.MaxIdleTimeout <= 0 {
		config.MaxIdleTimeout = defaults.MaxIdleTimeout
	}
	if config.MaxHandshakes <= 0 {
		config.MaxHandshakes = defaults.MaxHandshakes
	}
	if config.RetryHandshakes == 0 {
		config.RetryHandshakes = defaults.RetryHandshakes
	}
	if handler == nil {
		handler = fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		})
	}
	h := &ServerHandler{handler: handler, config: config, resetKey: quic.NewResetKey(), tokenKey: quic.NewTokenKey(),
		streams: fibhttp.NewStreamPool(config.StreamPool), routes: make(map[string]*quic.Conn)}
	if config.StatelessResetKey != nil {
		h.resetKey = *config.StatelessResetKey
	}
	h.quic = quic.Config{
		NewTLS:             h.newTLS(config.BlockingTLSHandshake),
		TLSConfig:          ConfigureTLS(config.TLSConfig),
		MaxIdleTimeout:     config.MaxIdleTimeout,
		HandshakeTimeout:   config.HandshakeTimeout,
		MaxIncomingStreams: config.MaxConcurrentStreams,
		ResetKey:           &h.resetKey,
		// A client that had a Retry is let in with its token however few
		// handshakes are under way by the time it comes back.
		TokenKey:                h.tokenKey,
		MaxDatagramSize:         config.MaxDatagramSize,
		DisablePathMTUDiscovery: config.DisablePathMTUDiscovery,
		MaxProbeDatagramSize:    config.MaxProbeDatagramSize,
		// fib hands each datagram over as a buffer of its own from the
		// pool, which QUIC gives back once it has done with it.
		RecycleDatagrams: true,
		EarlyData:        config.EarlyData,
		// A client that floods the connection with PING, resets and the
		// like is told it was too much (RFC 9114 section 8.1).
		ExcessiveLoadCode: uint64(ErrCodeExcessiveLoad),
	}
	config.Transport.apply(&h.quic)
	return h
}

func (h *ServerHandler) OnOpen(*fib.Connection)                 {}
func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}

// OnData hands a datagram to the connection it is for, starting one if it is
// a client's first. The engine hands each UDP datagram over as a buffer of its
// own from the pool, which QUIC gives back once it has done with it.
//
// A client address's fib.Connection is shared by every QUIC connection that
// comes from it (see udpPeer), and a datagram goes to the one its connection
// ID names: a client may run several connections from one socket, as
// quic-go's does, and the first flight of a new one can arrive while the
// server still has the last.
//
// A datagram from an address with no connection that belongs to one all the
// same, by the connection ID in it, comes from a client whose address
// changed: the peer's fib.Connection then serves that connection too, as a
// path of it, which it moves to once the client proves to be there (see
// quic.Conn.HandlePathDatagram). The engine hands the new address's
// datagrams to whichever of its loops the address falls to, and the QUIC
// connection, which takes datagrams from any goroutine, follows it there.
func (h *ServerHandler) OnData(c *fib.Connection, data []byte) {
	peer := peerOf(c)
	if path := h.pathFor(peer, quic.DatagramDCID(data), data); path != nil {
		path.qc.HandlePathDatagram(path, c.RemoteAddr(), data)
	}
}

// pathFor returns the path of the connection data, a datagram to cid, is
// for: one already on the peer's address, one that moved here, or one data
// starts. It returns nil for a datagram no connection takes, having answered
// or dropped it.
func (h *ServerHandler) pathFor(peer *udpPeer, cid, data []byte) *peerPath {
	c := peer.c
	if path := peer.find(cid); path != nil {
		return path
	}
	if peer.isClosing() {
		// The address's connection is closing, and would take a connection
		// started or moved onto it with it. The datagram is dropped, and the
		// client's next one, sent again, arrives on the connection the
		// address gets after: a ClientHello taken here and lost with it
		// would have spent its ticket's early data.
		return nil
	}
	if vn := quic.VersionNegotiation(data); vn != nil {
		_ = c.Send(vn)
		peer.closeIfUnused()
		return nil
	}
	if qc := h.route(data); qc != nil {
		// nil if the address started closing since: the datagram is dropped.
		return peer.add(qc)
	}
	if !quic.IsInitial(data) {
		if quic.ShortHeaderDCID(data) == nil {
			// A Handshake or 0-RTT packet, which route does not look up: a
			// client whose address changed before its handshake packets
			// stopped still sends some from the new address. They cannot
			// move the connection, but the 1-RTT packets behind them can,
			// so the datagram is dropped and the path kept. Closed, it would
			// still take the next of them while it closed, and the
			// connection that moved onto it would end with it.
			return nil
		}
		// A packet for a connection this server no longer has, perhaps from
		// before a restart: tell the client, so that it need not time out.
		if reset := h.resetKey.StatelessReset(data); reset != nil {
			_ = c.Send(reset)
		}
		peer.closeIfUnused()
		return nil
	}
	if tc := h.quic.TLSConfig; tc == nil || len(tc.Certificates) == 0 && tc.GetCertificate == nil && tc.GetConfigForClient == nil {
		peer.closeIfUnused()
		return nil
	}
	if c.ShuttingDown() {
		// No connection starts while the engine shuts down, or the
		// binding drains, as no TCP one is accepted; the client's Initial
		// is dropped, as one from an address the engine does not know is.
		peer.closeIfUnused()
		return nil
	}
	if !h.admit(c, data) {
		peer.closeIfUnused()
		return nil
	}
	h.accept(peer, data)
	return nil
}

// admit reports whether a client's Initial in data, from c's address, may
// start a connection, given the handshakes under way: past MaxHandshakes it
// may not, and past RetryHandshakes, one without a good token is answered
// with a Retry instead (see Config.MaxHandshakes), or told why the token it
// has is not good (see quic.Config.TokenKey).
func (h *ServerHandler) admit(c *fib.Connection, data []byte) bool {
	n := h.handshakes.Load()
	if n >= int64(h.config.MaxHandshakes) {
		return false
	}
	retry := h.config.RetryHandshakes
	if retry >= 0 && n < int64(retry) {
		return true
	}
	addr := c.RemoteAddr()
	status, kind := h.tokenKey.CheckToken(data, addr)
	switch {
	case status == quic.TokenValid:
		// A token of a Retry, or of a NEW_TOKEN frame of an earlier
		// connection from this address: the client's address is its own,
		// and no Retry is needed (RFC 9000 section 8.1.3).
		return true
	case status == quic.TokenInvalid && kind == quic.TokenKindRetry:
		// Accept answers an Initial whose Retry token is not good with
		// INVALID_TOKEN: the client takes no second Retry (RFC 9000
		// section 8.1.2).
		return true
	}
	if packet := h.tokenKey.Retry(data, addr); packet != nil {
		_ = c.Send(packet)
	}
	return false
}

// accept starts the connection a client's Initial in data begins, on a path
// of its own on the peer's address.
func (h *ServerHandler) accept(peer *udpPeer, data []byte) {
	c := peer.c
	sc := &serverConn{h: h, conn: c, streams: make(map[uint64]*requestStream)}
	if addr := c.RemoteAddr(); addr != nil {
		sc.remoteAddr = addr.String()
	}
	h.handshakes.Add(1)
	sc.handshaking.Store(true)
	// A client's GOAWAY names a push ID; nothing here promises pushes, so
	// only the checks the control stream makes matter.
	sc.peer.onGoAway = func(uint64) error { return nil }
	path := peer.reserve()
	if path == nil {
		// Closing since pathFor looked; see there.
		sc.handshakeOver()
		return
	}
	qc := quic.Accept(path, c.RemoteAddr(), h.quic, sc, data)
	if qc == nil {
		sc.handshakeOver()
		peer.release(path)
		return
	}
	h.addRoute(qc)
	peer.bind(path, qc)
}

// route returns the connection a datagram's connection ID names, if it is a
// 1-RTT packet of one; only those can move a connection to a new path.
func (h *ServerHandler) route(datagram []byte) *quic.Conn {
	cid := quic.ShortHeaderDCID(datagram)
	if cid == nil {
		return nil
	}
	h.routesMu.RLock()
	qc := h.routes[string(cid)]
	h.routesMu.RUnlock()
	return qc
}

// addRoute routes the connection IDs qc owns to it: the one of its handshake
// as it is accepted, and those it issues later as it issues them (see
// serverConn.OnConnectionID).
func (h *ServerHandler) addRoute(qc *quic.Conn) {
	h.routesMu.Lock()
	for _, cid := range qc.LocalCIDs() {
		h.routes[string(cid)] = qc
	}
	h.routesMu.Unlock()
}

// removeRoute forgets the routes of qc's connection IDs, or of cid alone, one
// its client retired, when it is not nil.
func (h *ServerHandler) removeRoute(qc *quic.Conn, cid []byte) {
	cids := [][]byte{cid}
	if cid == nil {
		cids = qc.LocalCIDs()
	}
	h.routesMu.Lock()
	for _, cid := range cids {
		if h.routes[string(cid)] == qc {
			delete(h.routes, string(cid))
		}
	}
	h.routesMu.Unlock()
}

// OnDatagrams hands a burst of datagrams to the connections they are for,
// each run of them that is for one connection together, which it answers
// together. A datagram that starts a connection starts it as OnData does.
func (h *ServerHandler) OnDatagrams(c *fib.Connection, datagrams [][]byte) {
	peer := peerOf(c)
	remote := c.RemoteAddr()
	var run *peerPath
	first := 0
	for i, d := range datagrams {
		cid := quic.DatagramDCID(d)
		if run != nil && run.qc.Owns(cid) {
			continue
		}
		if run != nil {
			run.qc.HandlePathDatagrams(run, remote, datagrams[first:i])
		}
		run, first = h.pathFor(peer, cid, d), i
		if run == nil {
			// Answered, dropped or the start of a connection, which has
			// taken it; the next datagram looks for its own.
			first = i + 1
		}
	}
	if run != nil {
		run.qc.HandlePathDatagrams(run, remote, datagrams[first:])
	}
}

// OnClose ends the connections that send on the address with it: one that
// has left it, or not moved to it, goes alone.
func (h *ServerHandler) OnClose(c *fib.Connection, err error) {
	peer, ok := c.Attachment().(*udpPeer)
	c.SetAttachment(nil)
	if !ok {
		return
	}
	if err == nil {
		err = net.ErrClosed
	}
	for _, path := range peer.detach() {
		if path.qc != nil && path.qc.SendsOn(path) {
			path.qc.Abort(err)
		}
	}
}

// OnEngineClose tells the clients of the connections that send on the
// address that the server is going, with CONNECTION_CLOSE and H3_NO_ERROR,
// as Engine.Close is about to end them: they would otherwise go on sending
// to it until they timed out. A connection's requests end with it, as
// those of an HTTP/2 connection the engine closes do; Engine.Shutdown is
// the graceful way.
func (h *ServerHandler) OnEngineClose(c *fib.Connection) {
	peer, ok := c.Attachment().(*udpPeer)
	if !ok {
		return
	}
	for _, qc := range peer.sending() {
		qc.Close(uint64(ErrCodeNoError), "server closed")
	}
}

// OnShutdown ends the connections on the address as the engine shuts down;
// see fib.Engine.Shutdown. Each HTTP/3 connection sends GOAWAY, finishes the
// requests it has, and closes once its streams have; the address's
// fib.Connection, which would otherwise stay for the client's next
// connection until it had been quiet for Config.UDPIdleTimeout, closes once
// none is left on it, since no new one starts now.
//
// It runs on the event loop, and again on each round of the shutdown until
// the address closes; the connections are retired on the engine's handler
// pool, since that takes their locks, which their own goroutines may hold.
func (h *ServerHandler) OnShutdown(c *fib.Connection) {
	peer, ok := c.Attachment().(*udpPeer)
	if !ok {
		// Its first datagram has yet to be handled; a later round finds it.
		return
	}
	conns := peer.shutdown()
	if conns == nil {
		_ = c.Close()
		return
	}
	for _, qc := range conns {
		if sc, ok := qc.Handler().(*serverConn); ok && sc.retiring.CompareAndSwap(false, true) {
			elsewhere(c, func() { sc.retire(qc) })
		}
	}
}

// elsewhere runs fn on the engine's handler pool, or on a goroutine of its
// own once the pool has stopped taking work.
func elsewhere(c *fib.Connection, fn func()) {
	if pool := c.Engine().HandlerPool(); pool == nil || !pool.Go(fn) {
		go fn()
	}
}

// udpPeer is a client address's fib.Connection, shared by the QUIC
// connections that come from it. A client may run several from one socket,
// and starts the next one before the server is done with the last: quic-go
// does both. Each connection sends on a path of its own (see peerPath), and
// one ending lets its path go without closing the address's fib.Connection,
// which the engine closes once the address has been quiet for
// Config.UDPIdleTimeout: closed at once, the first flight of a connection the
// client starts next would arrive at a fib.Connection that is closing, and
// be lost with it, its early data included.
type udpPeer struct {
	c     *fib.Connection
	mu    sync.Mutex
	paths []*peerPath
	// used is set once a connection has had a path here; see closeIfUnused.
	used bool
	// closing is set once the handler closes the address's fib.Connection;
	// see pathFor.
	closing bool
}

// peerOf returns c's udpPeer, making it with the first datagram from the
// address.
func peerOf(c *fib.Connection) *udpPeer {
	if peer, ok := c.Attachment().(*udpPeer); ok {
		return peer
	}
	peer := &udpPeer{c: c}
	c.SetAttachment(peer)
	return peer
}

// find returns the path of the connection that owns cid, if one is on the
// address.
func (p *udpPeer) find(cid []byte) *peerPath {
	if cid == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, path := range p.paths {
		if path.qc != nil && path.qc.Owns(cid) {
			return path
		}
	}
	return nil
}

// add returns qc's path on the address, adding one for a connection that has
// just reached it, or nil once the address is closing.
func (p *udpPeer) add(qc *quic.Conn) *peerPath {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return nil
	}
	for _, path := range p.paths {
		if path.qc == qc {
			return path
		}
	}
	path := &peerPath{peer: p, qc: qc}
	p.paths = append(p.paths, path)
	p.used = true
	return path
}

// reserve adds a path for a connection being accepted, which bind names once
// it exists, or returns nil once the address is closing.
func (p *udpPeer) reserve() *peerPath {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return nil
	}
	path := &peerPath{peer: p}
	p.paths = append(p.paths, path)
	p.used = true
	return path
}

func (p *udpPeer) bind(path *peerPath, qc *quic.Conn) {
	p.mu.Lock()
	path.qc = qc
	p.mu.Unlock()
}

// release lets path go.
func (p *udpPeer) release(path *peerPath) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, held := range p.paths {
		if held == path {
			p.paths = append(p.paths[:i], p.paths[i+1:]...)
			return
		}
	}
}

// detach takes every path off the address, which is closing.
func (p *udpPeer) detach() []*peerPath {
	p.mu.Lock()
	defer p.mu.Unlock()
	paths := p.paths
	p.paths = nil
	return paths
}

// closeIfUnused closes the address's fib.Connection when no connection has
// used it: a datagram that started none, from an address that never had one,
// leaves nothing to keep it for. An address that had one is kept for the
// client's next (see udpPeer).
func (p *udpPeer) closeIfUnused() {
	p.mu.Lock()
	unused := !p.used
	if unused {
		p.closing = true
	}
	p.mu.Unlock()
	if unused {
		_ = p.c.Close()
	}
}

// shutdown returns the connections on the address, as the engine shuts
// down. With none left, it returns nil and marks the address closing, which
// the handler then closes.
func (p *udpPeer) shutdown() []*quic.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.paths) == 0 {
		p.closing = true
		return nil
	}
	conns := make([]*quic.Conn, 0, len(p.paths))
	for _, path := range p.paths {
		// A path whose connection is still being accepted has none yet,
		// and keeps the address open until a later round.
		if path.qc != nil && !slices.Contains(conns, path.qc) {
			conns = append(conns, path.qc)
		}
	}
	return conns
}

// sending returns the connections that send on the address.
func (p *udpPeer) sending() []*quic.Conn {
	p.mu.Lock()
	paths := slices.Clone(p.paths)
	p.mu.Unlock()
	var conns []*quic.Conn
	for _, path := range paths {
		if path.qc != nil && path.qc.SendsOn(path) {
			conns = append(conns, path.qc)
		}
	}
	return conns
}

func (p *udpPeer) isClosing() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closing
}

// peerPath is one QUIC connection's use of an address: the quic.PacketConn
// it sends on there.
type peerPath struct {
	peer *udpPeer
	// qc is set once the connection exists; see udpPeer.reserve.
	qc *quic.Conn
}

func (p *peerPath) Send(datagram []byte) error { return p.peer.c.Send(datagram) }

// SendDatagrams makes the path a quic.DatagramsSender, so that a round of
// datagrams goes out with one system call where the address's
// fib.Connection can, and those a full socket refuses are sent again rather
// than lost.
func (p *peerPath) SendDatagrams(datagrams [][]byte) (int, error) {
	return p.peer.c.SendDatagrams(datagrams)
}

// SetDontFragment makes the path a quic.DontFragmenter, which path MTU
// discovery needs: the datagrams of the address's socket are sent whole or
// not at all.
func (p *peerPath) SetDontFragment() error { return p.peer.c.SetDontFragment() }

// NotifyWritable makes the path a quic.WritableNotifier, so that datagrams
// a full socket refused go out as soon as it has room.
func (p *peerPath) NotifyWritable(fn func()) bool { return p.peer.c.NotifyWritable(fn) }

// Close lets the connection's use of the address go; the address's
// fib.Connection stays for the client's next connection, until it is idle
// (see udpPeer).
func (p *peerPath) Close() error {
	p.peer.release(p)
	return nil
}

var (
	_ fib.DatagramsHandler     = (*ServerHandler)(nil)
	_ fib.ShutdownHandler      = (*ServerHandler)(nil)
	_ fib.EngineCloseHandler   = (*ServerHandler)(nil)
	_ fib.EngineCloseHandler   = clientPath{}
	_ quic.CallsDoneHandler    = (*serverConn)(nil)
	_ quic.PathHandler         = (*serverConn)(nil)
	_ quic.ConnectionIDHandler = (*serverConn)(nil)
	_ quic.DatagramsSender     = (*peerPath)(nil)
	_ quic.DatagramsSender     = (*fib.Connection)(nil)
	_ quic.DontFragmenter      = (*peerPath)(nil)
	_ quic.DontFragmenter      = (*fib.Connection)(nil)
	_ quic.WritableNotifier    = (*peerPath)(nil)
	_ quic.WritableNotifier    = (*fib.Connection)(nil)
)

// serverConn is one client's HTTP/3 connection.
type serverConn struct {
	h          *ServerHandler
	conn       *fib.Connection
	qc         *quic.Conn
	remoteAddr string
	tlsState   *tls.ConnectionState
	control    *quic.Stream
	peer       peerStreams

	// gate counts the requests of this connection running on the engine's
	// handler pool, which is how the per-connection limit is kept, and batch
	// holds those of a run of QUIC's calls until the run ends, which hands
	// them to the pool together (see OnCallsDone).
	gate  fibhttp.StreamGate
	batch fibhttp.StreamBatch

	// decoder decodes the connection's field sections into fields, whose
	// array the next one reuses. Only the goroutine QUIC calls the
	// connection's handler on uses them.
	decoder qpack.Decoder
	fields  []qpack.HeaderField

	mu sync.Mutex
	// streams are the requests not yet answered.
	streams map[uint64]*requestStream

	// dead is the list of the connection's recycled request streams that
	// nothing can reach any more, which recycleDead hands back to the pool.
	dead atomic.Pointer[requestStream]
	// nextID is the stream ID after the highest request seen, which is
	// what GOAWAY names.
	nextID    uint64
	goingAway bool
	goAwayID  uint64
	// retiring says the engine's shutdown has had retire run, which it does
	// once however often it is told; see ServerHandler.OnShutdown.
	retiring atomic.Bool
	// handshaking says the connection counts among its handler's handshakes
	// under way; see handshakeOver.
	handshaking atomic.Bool
	// idleTimer retires the connection once it has carried no request for
	// Config.IdleTimeout: idleSince is when the last one ended, or the
	// connection started, and lastRequest when the latest request stream
	// opened, which puts off a timer that comes while its header is still
	// arriving. Guarded by mu; see armIdleLocked.
	idleTimer   *time.Timer
	idleSince   time.Time
	lastRequest time.Time
}

func (sc *serverConn) OnHandshake(qc *quic.Conn) {
	sc.handshakeOver()
	state := qc.ConnectionState()
	sc.tlsState = &state
	sc.start(qc)
}

// handshakeOver takes the connection off its handler's handshakes under
// way, once: its handshake is over, or the connection is.
func (sc *serverConn) handshakeOver() {
	if sc.handshaking.CompareAndSwap(true, false) {
		sc.h.handshakes.Add(-1)
	}
}

// OnEarlyData starts the connection ahead of its handshake, for the requests
// in the client's first flight. Their responses go out as soon as the
// server's side of the handshake has its keys.
func (sc *serverConn) OnEarlyData(qc *quic.Conn) {
	state := qc.ConnectionState()
	sc.tlsState = &state
	sc.start(qc)
}

// start opens the connection's control stream, once.
func (sc *serverConn) start(qc *quic.Conn) {
	if sc.qc != nil {
		return
	}
	sc.qc = qc
	control, err := openControl(qc, sc.h.config.MaxHeaderBytes, [2]uint64{settingEnableConnectProtocol, 1})
	if err != nil {
		closeWith(qc, connErr(ErrCodeStreamCreationError, "cannot open the control stream"))
		return
	}
	sc.mu.Lock()
	sc.control = control
	// A connection that opens and then asks for nothing is idle, and bounded
	// like any other idle connection.
	sc.armIdleLocked()
	sc.mu.Unlock()
}

// idleTimeout is Config.IdleTimeout, or ReadTimeout when it is not set.
func idleTimeout(config *Config) time.Duration {
	if config.IdleTimeout != 0 {
		return config.IdleTimeout
	}
	return config.ReadTimeout
}

// headerTimeout is Config.ReadHeaderTimeout, or ReadTimeout when it is not
// set.
func headerTimeout(config *Config) time.Duration {
	if config.ReadHeaderTimeout != 0 {
		return config.ReadHeaderTimeout
	}
	return config.ReadTimeout
}

// armIdleLocked starts timing the connection's idleness, now that it carries
// no request. Callers hold sc.mu.
func (sc *serverConn) armIdleLocked() {
	d := idleTimeout(&sc.h.config)
	if d <= 0 || sc.goingAway || sc.streams == nil {
		return
	}
	sc.idleSince = time.Now()
	if sc.idleTimer == nil {
		sc.idleTimer = time.AfterFunc(d, sc.idleExpired)
	} else {
		sc.idleTimer.Reset(d)
	}
}

// busyLocked stops timing idleness, now that a request is open. Callers hold
// sc.mu.
func (sc *serverConn) busyLocked() {
	if sc.idleTimer != nil {
		sc.idleTimer.Stop()
	}
}

// idleExpired retires a connection that has carried no request for
// IdleTimeout, with GOAWAY, and closes it once that has gone. A request
// stream that opened since its idleness began, whose header is still
// arriving, puts it off by as long again from then; a timer that fired as a
// request opened, or for an idleness that began again since, finds the
// connection busy, or idle for less, and leaves it alone.
func (sc *serverConn) idleExpired() {
	d := idleTimeout(&sc.h.config)
	sc.mu.Lock()
	if sc.goingAway || sc.streams == nil || len(sc.streams) > 0 {
		sc.mu.Unlock()
		return
	}
	if sc.lastRequest.After(sc.idleSince) {
		sc.idleSince = sc.lastRequest
	}
	if wait := d - time.Since(sc.idleSince); wait > 0 {
		sc.idleTimer.Reset(wait)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	sc.goAway()
	sc.qc.CloseWhenDone(uint64(ErrCodeNoError), "")
}

func (sc *serverConn) fail(err error) { closeWith(sc.qc, err) }

// OnPathChange gives the requests that follow the client's new address.
// Requests are read on the goroutine QUIC calls the handler on, as this is.
func (sc *serverConn) OnPathChange(qc *quic.Conn) {
	if addr := qc.RemoteAddr(); addr != nil {
		sc.remoteAddr = addr.String()
	}
}

func (sc *serverConn) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	switch st := s.Context.(type) {
	case *requestStream:
		st.feed(data, fin)
	case *uniStream:
		st.feed(data, fin)
	case nil:
		if s.Bidirectional() {
			rs := sc.newRequestStream(s)
			if rs == nil {
				// Refused: what more arrives for it before the reset
				// takes effect is passed over, and a typed nil here
				// would be fed as a request stream.
				s.Context = retiredStream{}
				return
			}
			s.Context = rs
			rs.feed(data, fin)
			return
		}
		u := newUniStream(&sc.peer, s, sc.fail)
		s.Context = u
		u.feed(data, fin)
	}
}

// newRequestStream starts reading a request, or refuses it once the
// connection is going away.
func (sc *serverConn) newRequestStream(s *quic.Stream) *requestStream {
	sc.mu.Lock()
	id := s.ID()
	if sc.goingAway && id >= sc.goAwayID {
		sc.mu.Unlock()
		abortStream(s, ErrCodeRequestRejected)
		return nil
	}
	sc.nextID = max(sc.nextID, id+4)
	if sc.idleTimer != nil {
		sc.lastRequest = time.Now()
	}
	sc.mu.Unlock()
	var rs *requestStream
	if !sc.h.config.DisableReuse && !sc.h.config.StreamRequestBody {
		rs = requestStreamPool.Get().(*requestStream)
		rs.pooled = true
	} else {
		rs = new(requestStream)
	}
	rs.sc, rs.s, rs.declared = sc, s, -1
	rs.parser = frameParser{maxFrame: uint64(sc.h.config.MaxHeaderBytes)}
	rs.refs.Store(1)
	if sc.h.config.StreamRequestBody {
		// The stream's flow control is given back by the request from here,
		// so that a body that streams paces the client; see streamedBody.
		rs.streamed = &streamedBody{}
		s.HoldCredit()
	}
	return rs
}

func (sc *serverConn) OnStreamReset(s *quic.Stream, _ uint64) {
	switch st := s.Context.(type) {
	case *requestStream:
		st.peerReset()
	case *uniStream:
		if st.typ == streamControl || st.typ == streamQPACKEncoder || st.typ == streamQPACKDecoder {
			sc.fail(connErr(ErrCodeClosedCriticalStream, "critical stream reset"))
		}
	}
}

func (sc *serverConn) OnStopSending(s *quic.Stream, _ uint64) {
	if st, ok := s.Context.(*requestStream); ok {
		st.peerReset()
	}
}

func (sc *serverConn) OnStreamsAvailable(*quic.Conn) {}

// OnConnectionID routes a connection ID the connection issues to its client,
// before the client has it, and forgets one the client retired: a client
// may send to any it has, as it does to a new one when its address changes.
func (sc *serverConn) OnConnectionID(qc *quic.Conn, cid []byte, active bool) {
	if active {
		sc.h.routesMu.Lock()
		sc.h.routes[string(cid)] = qc
		sc.h.routesMu.Unlock()
		return
	}
	sc.h.removeRoute(qc, cid)
}

// OnCallsDone hands the requests that arrived whole during the run of calls
// to the handler pool, together.
func (sc *serverConn) OnCallsDone(*quic.Conn) {
	sc.h.streams.Submit(&sc.batch)
	sc.recycleDead()
}

func (sc *serverConn) OnClose(qc *quic.Conn, _ error) {
	sc.handshakeOver()
	sc.h.removeRoute(qc, nil)
	sc.mu.Lock()
	streams := sc.streams
	sc.streams = nil
	sc.busyLocked()
	sc.mu.Unlock()
	for _, rs := range streams {
		// Closed before the handler hears of it; see peerReset.
		rs.mu.Lock()
		rs.closed = true
		stopped := rs.stopWriteTimerLocked()
		readStopped := rs.stopReadTimerLocked()
		rs.mu.Unlock()
		rs.cancel(net.ErrClosed)
		if stopped {
			rs.unref()
		}
		if readStopped {
			rs.unref()
		}
		rs.failBody(net.ErrClosed)
	}
}

// track records a request as open until it is answered.
func (sc *serverConn) track(rs *requestStream) {
	sc.mu.Lock()
	if sc.streams != nil {
		sc.streams[rs.s.ID()] = rs
		sc.busyLocked()
	}
	sc.mu.Unlock()
}

// untrack forgets an answered or abandoned request, and finishes going away
// once the last one is gone.
func (sc *serverConn) untrack(rs *requestStream) {
	sc.mu.Lock()
	id := rs.s.ID()
	tracked := sc.streams[id] == rs
	delete(sc.streams, id)
	done := sc.goingAway && len(sc.streams) == 0
	if tracked && len(sc.streams) == 0 {
		sc.armIdleLocked()
	}
	sc.mu.Unlock()
	if tracked {
		rs.unref()
	}
	if done {
		sc.qc.CloseWhenDone(uint64(ErrCodeNoError), "")
	}
}

// goAway retires the connection gracefully: the client is told which
// requests will be served, those finish, and then the connection closes.
func (sc *serverConn) goAway() {
	sc.mu.Lock()
	if sc.goingAway {
		sc.mu.Unlock()
		return
	}
	sc.goingAway = true
	sc.goAwayID = sc.nextID
	control := sc.control
	sc.mu.Unlock()
	if control != nil {
		payload := quic.AppendVarint(nil, sc.goAwayID)
		_ = control.Write(append(appendFrameHeader(nil, frameGoAway, len(payload)), payload...), false)
	}
}

// retire is the server shutting down: GOAWAY tells the client which
// requests will be served, those finish, and the connection closes once
// every stream has, at once if none is open. A tunnel's stream finishes when
// the protocol it carries ends it, which its handler is told to do; see
// fibhttp.TunnelShutdownHandler.
func (sc *serverConn) retire(qc *quic.Conn) {
	sc.goAway()
	var tunnels []*fibhttp.TunnelFeed
	sc.mu.Lock()
	for _, rs := range sc.streams {
		if rs.tunnel != nil {
			tunnels = append(tunnels, rs.tunnel.TunnelFeed)
		}
	}
	sc.mu.Unlock()
	for _, tunnel := range tunnels {
		tunnel.Shutdown()
	}
	qc.CloseWhenDone(uint64(ErrCodeNoError), "")
}

// requestStream is a request being read and then answered.
type requestStream struct {
	sc       *serverConn
	s        *quic.Stream
	parser   frameParser
	req      *stdhttp.Request
	body     []byte
	declared int64
	trailers bool
	// done is set once the request needs no more reading: it has gone to
	// the handler whole, its streamed body has ended, or it has been
	// refused.
	done bool
	// refs is described with pooled, below; it sits here, beside the bools,
	// where it fits in what would otherwise be padding.
	refs atomic.Int32
	// streamed is what the request keeps when the server streams bodies, or
	// nil when it does not.
	streamed *streamedBody
	// tunnel is the tunnel of a CONNECT, extended (RFC 9220) or to a host and
	// port (RFC 9114 section 4.4), or nil. It is set before the handler runs,
	// which may switch the stream to it with Context.Upgrade or
	// Context.Connect; what arrives on the stream waits in it until then.
	tunnel *streamTunnel

	mu sync.Mutex
	// remoteDone is set when the whole request has arrived.
	remoteDone bool

	responded bool
	closed    bool
	// upgraded is set once the handler has switched the stream to its
	// tunnel; see tunnel.
	upgraded bool
	// streaming is set while a response that streams is being written,
	// from BeginResponse to EndResponse.
	streaming bool
	// expected is set while the connection expects the response, and holds
	// what it has to send for it; see quic.Conn.ExpectWrite.
	expected bool
	// timedOut marks a stream reset because its response outlasted
	// Config.WriteTimeout, which writing to it then reports.
	timedOut bool
	// pooled is described below; it sits here, with the other bools.
	pooled bool
	// writeTimer resets the stream once Config.WriteTimeout has passed, or
	// is nil; see armWriteTimeout.
	writeTimer *time.Timer
	// collectTimer resets the stream once its header has outlasted
	// Config.ReadHeaderTimeout, or its body, collected before its handler
	// runs, ReadTimeout or ReadBodyTimeout, or is nil; see armPendingTimer. It holds a reference to the stream until
	// it is stopped or has run. A body that streams is timed by its
	// streamedBody's readTimer instead.
	collectTimer *time.Timer

	// block is the request, its URL and the Context it is answered through,
	// allocated with the stream, and values holds its header's values.
	block  fibhttp.StreamRequest
	values [requestValues]string

	// pooled marks a stream the server recycles once nothing can reach it:
	// refs counts what still may, the connection's table until untrack and
	// the Context from the handler's dispatch until it ends, and the last
	// of them to go puts the stream on the connection's dead list through
	// nextDead; see serverConn.recycleDead. A request header taken from the
	// pool is headerFromPool. Both fields sit higher up, where they pack.
	nextDead       *requestStream
	headerFromPool *pooledHeader
}

// requestStreamPool holds the request streams connections have recycled.
var requestStreamPool = sync.Pool{New: func() any { return new(requestStream) }}

// pooledHeader is a recycled request Header, with room for its values.
type pooledHeader struct {
	header stdhttp.Header
	values [8]string
}

var headerPool = sync.Pool{New: func() any { return &pooledHeader{header: make(stdhttp.Header)} }}

// retiredStream is the Context of a QUIC stream whose request stream has
// been recycled, which the connection's callbacks pass over: nil would have
// a late packet start a new request.
type retiredStream struct{}

// unref gives up one of rs's references, and puts a recycled stream that
// nothing can reach any more on its connection's dead list.
func (rs *requestStream) unref() {
	if rs.refs.Add(-1) != 0 || !rs.pooled {
		return
	}
	sc := rs.sc
	for {
		head := sc.dead.Load()
		rs.nextDead = head
		if sc.dead.CompareAndSwap(head, rs) {
			return
		}
	}
}

// RequestEnded is told that nothing but the server can reach the request
// any more; see http.StreamEnder.
func (rs *requestStream) RequestEnded() { rs.unref() }

// recycleDead hands the request streams on the dead list back to the pool.
// It runs on the goroutine QUIC calls the connection's handler on, which is
// where a QUIC stream's callbacks reach its request stream through its
// Context: that is pointed elsewhere first, so that a packet arriving for the
// stream later does not reach the request the stream goes on to serve.
func (sc *serverConn) recycleDead() {
	for rs := sc.dead.Swap(nil); rs != nil; {
		next := rs.nextDead
		if s := rs.s; s != nil && s.Context == rs {
			s.Context = retiredStream{}
		}
		rs.recycle()
		requestStreamPool.Put(rs)
		rs = next
	}
}

// recycle clears rs for another request. TestRequestStreamRecycleResets
// holds it to clearing every field.
func (rs *requestStream) recycle() {
	if h := rs.headerFromPool; h != nil {
		clear(h.header)
		clear(h.values[:])
		headerPool.Put(h)
	}
	rs.block.Recycle()
	rs.sc, rs.s, rs.req, rs.body = nil, nil, nil, nil
	rs.parser = frameParser{}
	rs.declared, rs.trailers, rs.done = 0, false, false
	rs.streamed, rs.tunnel = nil, nil
	rs.remoteDone, rs.responded, rs.closed, rs.upgraded, rs.expected = false, false, false, false, false
	rs.streaming, rs.timedOut, rs.writeTimer, rs.collectTimer = false, false, nil, nil
	rs.values = [requestValues]string{}
	rs.pooled, rs.nextDead, rs.headerFromPool = false, nil, nil
	rs.refs.Store(0)
}

// requestValues is how many of a request's header values its stream has
// room for when the stream is not recycled; a recycled one keeps them in the
// header it takes from the pool. It is also what keeps a requestStream
// at 896 bytes, one of the allocator's size classes; one more value
// would put it in the class of 1024.
const requestValues = 2

// streamedBody is what a request stream keeps when the server streams bodies;
// see Config.StreamRequestBody. The stream then gives back its flow control
// itself: what is not body at once, a body read whole as it arrives, and a
// body that streams as the handler consumes it.
type streamedBody struct {
	// feed is where the body is written once it streams to the handler, and
	// trailer the trailer that arrived after it, for when it ends.
	feed    *fibhttp.BodyFeed
	trailer stdhttp.Header
	// fed says the stream has been delivered data before: its first
	// delivery counts as consumed already, since QUIC gave it back before
	// the request could hold it. fedBody is how much of a delivery went to
	// the feed, and is given back only once the handler has consumed it.
	fed     bool
	fedBody int
	// skip is how much of what the handler will consume was given back when
	// it arrived, before the body streamed. The handler's goroutines consume
	// it, hence the lock.
	mu   sync.Mutex
	skip int64
	// readTimer fails the body once the deadline its handler set with
	// SetReadDeadline has passed, or is nil; see SetReadDeadline. It is
	// guarded by the request stream's mu, and holds a reference to the
	// stream until it is stopped or has run.
	readTimer *time.Timer
	// headerAt is when the request's header arrived, which the read
	// timeouts are measured from, or zero when they are not set.
	headerAt time.Time
}

// addSkip records n bytes of the body that were given back as they arrived.
func (b *streamedBody) addSkip(n int) {
	b.mu.Lock()
	b.skip += int64(n)
	b.mu.Unlock()
}

// credit gives s back the room n bytes the handler consumed took, less what
// was given back already.
func (b *streamedBody) credit(s *quic.Stream, n int) {
	b.mu.Lock()
	skip := min(int64(n), b.skip)
	b.skip -= skip
	b.mu.Unlock()
	s.Credit(n - int(skip))
}

func (rs *requestStream) feed(data []byte, fin bool) {
	if rs.done {
		return
	}
	b := rs.streamed
	if b != nil {
		b.fedBody = 0
	}
	if err := rs.parser.feed(data, rs.onData, rs.onFrame); err != nil {
		rs.sc.fail(err)
		return
	}
	if rs.tunnel != nil && rs.tunnel.credit(rs.s, len(data)) {
		// A paced tunnel gives back what it was delivered itself.
	} else if b != nil {
		// What of this delivery went to the handler's body is given back
		// once the handler has consumed it; the rest now.
		if b.fed {
			rs.s.Credit(len(data) - b.fedBody)
		} else {
			b.fed = true
			b.addSkip(b.fedBody)
		}
	}
	if !fin && !rs.done {
		rs.armPendingTimer()
	}
	if !fin || rs.done {
		return
	}
	rs.mu.Lock()
	rs.remoteDone = true
	readStopped := rs.stopReadTimerLocked()
	rs.mu.Unlock()
	if readStopped {
		rs.unref()
	}
	if rs.parser.midFrame() {
		rs.sc.fail(connErr(ErrCodeFrameError, "request stream ended inside a frame"))
		return
	}
	if rs.req == nil {
		rs.abort(ErrCodeRequestIncomplete)
		return
	}
	if rs.tunnel != nil {
		rs.done = true
		rs.peerEndedTunnel()
		return
	}
	if b != nil && b.feed != nil {
		rs.done = true
		if b.feed.EndsShort() {
			// Aborted before the handler hears of it; see onData.
			rs.abortWith(ErrCodeMessageError, fibhttp.ErrBodyShort)
			return
		}
		_ = b.feed.End(b.trailer)
		return
	}
	rs.finish()
}

func (rs *requestStream) onData(chunk []byte) error {
	if rs.done {
		return nil
	}
	if rs.req == nil || rs.trailers {
		return connErr(ErrCodeFrameUnexpected, "DATA before HEADERS or after trailers")
	}
	if rs.tunnel != nil {
		if err := rs.tunnel.Write(chunk); err != nil {
			rs.abort(ErrCodeExcessiveLoad)
		}
		return nil
	}
	if b := rs.streamed; b != nil {
		if b.feed != nil {
			b.fedBody += len(chunk)
			if b.feed.Overruns(len(chunk)) {
				// Longer than its content-length. The stream is aborted
				// before the handler hears of it, so that nothing it sends
				// on hearing it, such as the response OnBody's end lets go,
				// leaves ahead of the reset; the chunk is then credited as
				// the body credits it. A body past MaxStreamedBodyBytes
				// instead fails with the handler told, which may still answer
				// it; what more arrives is dropped.
				rs.abortWith(ErrCodeMessageError, fibhttp.ErrBodyOverrun)
			}
			_ = b.feed.Write(chunk)
			return nil
		}
		config := &rs.sc.h.config
		got := int64(len(rs.body) + len(chunk))
		if rs.declared < 0 && got > config.StreamRequestBodyThreshold ||
			rs.declared > config.StreamRequestBodyThreshold && got >= config.StreamRequestBodyThreshold {
			// A body streams once the threshold of it has arrived: more than
			// that for one of no declared length. What arrived before was
			// given back as it came, this chunk included.
			buffered := bufferpool.Append(rs.body, chunk)
			rs.body = nil
			rs.startStream(buffered)
			return nil
		}
	}
	waiting := rs.streamed != nil && rs.declared > rs.sc.h.config.StreamRequestBodyThreshold
	if !waiting && int64(len(rs.body)+len(chunk)) > rs.sc.h.config.MaxBodyBytes {
		rs.reject(stdhttp.StatusRequestEntityTooLarge)
		return nil
	}
	if rs.body == nil && rs.declared > 0 && !waiting {
		// The datagram the body arrives in goes back to the pool once QUIC
		// has done with it, so the body is gathered in a pooled buffer of
		// its own, sized once when its length is known, which the Context
		// gives back once the response is finished.
		rs.body = bufferpool.Get(int(rs.declared))[:0]
	}
	rs.body = bufferpool.Append(rs.body, chunk)
	return nil
}

// dropBody gives back a body that will not be served.
func (rs *requestStream) dropBody() {
	bufferpool.Put(rs.body)
	rs.body = nil
}

func (rs *requestStream) onFrame(typ uint64, payload []byte) error {
	if rs.done {
		return nil
	}
	switch typ {
	case frameHeaders:
	case frameCancelPush, frameSettings, framePushPromise, frameGoAway, frameMaxPushID:
		return connErr(ErrCodeFrameUnexpected, "control frame on a request stream")
	default:
		if reservedFrame(typ) {
			return connErr(ErrCodeFrameUnexpected, "HTTP/2 frame type")
		}
		return nil
	}
	if rs.trailers {
		return connErr(ErrCodeFrameUnexpected, "HEADERS after trailers")
	}
	sc := rs.sc
	fields, err := decodeFields(&sc.decoder, sc.fields[:0], payload, sc.h.config.MaxHeaderBytes)
	// What was decoded is copied out before the next section is, so the
	// array can take that one.
	defer func() {
		clear(fields)
		sc.fields = fields[:0]
	}()
	if errors.Is(err, qpack.ErrTooLarge) {
		rs.reject(stdhttp.StatusRequestHeaderFieldsTooLarge)
		return nil
	}
	if err != nil {
		return err
	}
	if rs.req != nil {
		trailer, err := trailerFrom(fields)
		if err != nil {
			rs.abort(ErrCodeMessageError)
			return nil
		}
		rs.trailers = true
		if b := rs.streamed; b != nil && b.feed != nil {
			// The handler has the request already; the trailer becomes its
			// Trailer when the body ends.
			b.trailer = trailer
			return nil
		}
		rs.req.Trailer = trailer
		return nil
	}
	// The header has arrived, so a timer a delivery before armed for it is
	// done; the body's, if it has one to come, is armed after this delivery
	// (see armPendingTimer), measured from here.
	rs.mu.Lock()
	headerStopped := rs.stopCollectTimerLocked()
	rs.mu.Unlock()
	if headerStopped {
		rs.unref()
	}
	if b := rs.streamed; b != nil && (sc.h.config.ReadTimeout > 0 || sc.h.config.ReadBodyTimeout > 0) {
		b.headerAt = time.Now()
	}
	var header stdhttp.Header
	values := rs.values[:]
	if rs.pooled {
		pooled := headerPool.Get().(*pooledHeader)
		rs.headerFromPool = pooled
		header, values = pooled.header, pooled.values[:]
	}
	req, err := newRequest(fields, &rs.block, header, values)
	if err != nil {
		rs.abort(ErrCodeMessageError)
		return nil
	}
	req.RemoteAddr = rs.sc.remoteAddr
	req.TLS = rs.sc.tlsState
	rs.req = req
	rs.sc.track(rs)
	if rs.s.EarlyData() {
		// Sent before the handshake was over, so it may be a replay
		// (RFC 8470).
		req.Header.Set("Early-Data", "1")
		if !rs.sc.h.config.EarlyDataUnsafe && !safeMethod(req.Method) {
			rs.reject(stdhttp.StatusTooEarly)
			return nil
		}
	}
	if _, ok := req.Header[":protocol"]; ok || req.Method == stdhttp.MethodConnect {
		// A CONNECT, extended or to a host and port: its handler runs now,
		// and may switch the stream to a tunnel.
		rs.serveTunnel()
		return nil
	}
	atHeader := rs.streamed != nil && rs.sc.h.config.StreamRequestBodyAtHeader != nil &&
		rs.sc.h.config.StreamRequestBodyAtHeader(req)
	if cl := req.Header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			rs.abort(ErrCodeMessageError)
			return nil
		}
		config := &rs.sc.h.config
		streams := rs.streamed != nil && (n > config.StreamRequestBodyThreshold || n > 0 && atHeader)
		if !streams && n > config.MaxBodyBytes || streams && config.MaxStreamedBodyBytes > 0 && n > config.MaxStreamedBodyBytes {
			rs.reject(stdhttp.StatusRequestEntityTooLarge)
			return nil
		}
		rs.declared = n
	}
	if expect, ok := req.Header["Expect"]; ok &&
		(len(expect) != 1 || !strings.EqualFold(strings.TrimSpace(expect[0]), "100-continue")) {
		// An expectation this server cannot meet (RFC 9110 section 10.1.1).
		rs.reject(stdhttp.StatusExpectationFailed)
		return nil
	}
	if atHeader && rs.declared != 0 || rs.streamed != nil && rs.declared > rs.sc.h.config.StreamRequestBodyThreshold &&
		rs.sc.h.config.StreamRequestBodyThreshold == 0 {
		// The body streams to the handler, which runs now.
		rs.startStream(nil)
		return nil
	}
	if strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
		// The body is read whole before the handler runs, so there is no
		// reason to keep the client waiting for permission to send it.
		_ = rs.WriteInterim(stdhttp.StatusContinue, nil)
	}
	return nil
}

// startStream runs the handler for a request whose body is still arriving,
// which streams to it through a BodyFeed. buffered is what of the body has
// arrived already, a buffer from the pool, whose room was given back as it
// came.
func (rs *requestStream) startStream(buffered []byte) {
	sc, b := rs.sc, rs.streamed
	var sendContinue func()
	if len(buffered) == 0 && strings.EqualFold(rs.req.Header.Get("Expect"), "100-continue") {
		// Asked for once the handler asks for the body, so that one which
		// refuses the request answers it before the upload starts.
		sendContinue = func() { _ = rs.WriteInterim(stdhttp.StatusContinue, nil) }
	}
	// The body is no longer collected; from here its deadline is the feed's.
	rs.mu.Lock()
	collectStopped := rs.stopCollectTimerLocked()
	rs.mu.Unlock()
	if collectStopped {
		rs.unref()
	}
	_, b.feed = rs.block.StreamContext(sc.conn, rs, fibhttp.BodyFeedConfig{
		Declared: rs.declared,
		Limit:    sc.h.config.MaxStreamedBodyBytes,
		Continue: sendContinue,
		MaxHold:  sc.h.config.MaxBodyHold,
		Credit:   func(n int) { b.credit(rs.s, n) },
	})
	// ReadTimeout and ReadBodyTimeout bound the body as a deadline the
	// handler could have set, which it can lift or move the same way,
	// measured from the header for a body collected until now.
	start := b.headerAt
	if start.IsZero() {
		start = time.Now()
	}
	if at := bodyDeadline(&sc.h.config, start); !at.IsZero() {
		_ = rs.SetReadDeadline(at)
	}
	b.addSkip(len(buffered))
	_ = b.feed.Write(buffered)
	bufferpool.Put(buffered)
	rs.armWriteTimeout()
	sc.h.streams.Queue(&sc.batch, &sc.gate, sc.conn, sc.h.handler, &rs.block)
}

// bodyDeadline is the read deadline config's timeouts put on a body that
// begins to stream at start, or zero for none.
func bodyDeadline(config *Config, start time.Time) time.Time {
	var at time.Time
	for _, d := range [...]time.Duration{config.ReadTimeout, config.ReadBodyTimeout} {
		if bound := start.Add(d); d > 0 && (at.IsZero() || bound.Before(at)) {
			at = bound
		}
	}
	return at
}

// failBody tells the handler of a body still streaming that the rest of it
// will not arrive. Its callback may answer the request, which may be what is
// failing it, but Fail hands the callback its last call on the handler pool
// rather than making it here.
func (rs *requestStream) failBody(err error) {
	if b := rs.streamed; b != nil && b.feed != nil {
		b.feed.Fail(err)
	}
	if rs.tunnel != nil {
		rs.tunnel.End(err)
	}
}

// finish runs the handler for a request that has arrived whole.
func (rs *requestStream) finish() {
	if rs.declared >= 0 && rs.declared != int64(len(rs.body)) {
		rs.abort(ErrCodeMessageError)
		return
	}
	rs.done = true
	req := rs.req
	req.ContentLength = int64(len(rs.body))
	sc := rs.sc
	rs.block.Context(sc.conn, rs, rs.body)
	rs.body = nil
	// The Context holds the stream from here until it ends.
	rs.refs.Add(1)
	// The response comes from another goroutine, soon: the connection holds
	// what it has to send, the acknowledgement of the request included, to
	// send it with the response, and with the rest of the burst's.
	rs.mu.Lock()
	rs.expected = true
	rs.mu.Unlock()
	sc.qc.ExpectWrite()
	rs.armWriteTimeout()
	// Served through fibhttp rather than by calling the handler, so that a
	// handler which retains the request, or reads its body through OnBody,
	// works here too. The pool runs it away from this goroutine, which reads
	// the connection, unless the connection is at its concurrency limit: then
	// it runs here, and nothing more is read from the connection until it has
	// been answered. The requests that arrived together go to the pool
	// together, once QUIC has told of them all.
	sc.h.streams.Queue(&sc.batch, &sc.gate, sc.conn, sc.h.handler, &rs.block)
}

// abort gives up on a malformed or incomplete request.
func (rs *requestStream) abort(code ErrorCode) {
	rs.abortWith(code, fmt.Errorf("http3: request stream aborted: %v", code))
}

// abortWith is abort with the error the request's body and handler hear.
func (rs *requestStream) abortWith(code ErrorCode, err error) {
	rs.done = true
	rs.dropBody()
	// Closed before the handler hears of it; see peerReset.
	rs.mu.Lock()
	rs.closed = true
	stopped := rs.stopWriteTimerLocked()
	readStopped := rs.stopReadTimerLocked()
	rs.mu.Unlock()
	rs.failBody(err)
	rs.cancel(err)
	if stopped {
		rs.unref()
	}
	if readStopped {
		rs.unref()
	}
	// The client's error, which counts against the streams it may have the
	// server reset, as Rapid Reset's do.
	rs.s.ResetForPeerError(uint64(code))
	if rs.req != nil {
		rs.sc.untrack(rs)
	}
}

// reject answers a request with an error status before it has arrived
// whole, and stops reading it.
func (rs *requestStream) reject(status int) {
	rs.done = true
	rs.dropBody()
	req := rs.req
	if req == nil {
		req = &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 3, Header: make(stdhttp.Header)}
	}
	_ = rs.WriteResponse(req, fibhttp.Response{
		StatusCode: status,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte(stdhttp.StatusText(status) + "\n"),
	})
}

// cancel tells a handler still at work on the request, whose response has
// not been written whole, that the stream failed under it; see
// http.StreamRequest.Cancel. It runs on the goroutine QUIC calls the
// connection's handler on, which is the one that recycles rs, before rs is
// untracked.
func (rs *requestStream) cancel(err error) {
	rs.mu.Lock()
	open := !rs.responded || rs.streaming
	rs.mu.Unlock()
	if open {
		rs.block.Cancel(err)
	}
}

// peerReset is the client abandoning the request.
func (rs *requestStream) peerReset() {
	rs.done = true
	rs.dropBody()
	// The stream is closed before the handler hears of the reset, through
	// OnCancel or its body: told first, it could answer or Upgrade on a
	// stream that still read as open, and get a tunnel the reset never ends.
	rs.mu.Lock()
	wasOpen := !rs.closed && (!rs.responded || rs.upgraded || rs.streaming)
	rs.closed = true
	stopped := rs.stopWriteTimerLocked()
	readStopped := rs.stopReadTimerLocked()
	rs.mu.Unlock()
	rs.failBody(errRequestReset)
	rs.cancel(errRequestReset)
	if stopped {
		rs.unref()
	}
	if readStopped {
		rs.unref()
	}
	if wasOpen {
		rs.s.Reset(uint64(ErrCodeRequestCancelled))
		if rs.req != nil {
			rs.sc.untrack(rs)
		}
	}
}

// WriteResponse sends the response: HEADERS, then the body in one DATA
// frame, then the end of the stream. QUIC flow and congestion control pace
// the body.
func (rs *requestStream) WriteResponse(req *stdhttp.Request, response fibhttp.Response) error {
	// After the response is written, so that what was held leaves with it.
	defer rs.writeDone()
	status := response.StatusCode
	if status < 200 || status > 999 {
		return fmt.Errorf("http3: invalid status code %d", status)
	}
	if err := checkHeader(response.Header); err != nil {
		return err
	}
	if max, limited := rs.fieldLimit(); limited {
		size := uint64(statusSize) + headerSize(response.Header, func(name string) bool { return name == "content-length" })
		if status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified {
			size += lengthSize(responseLength(req, response))
		}
		if size > max || bodyAllowed(req, status) && trailerSize(response.Trailer) > max {
			return errFieldSectionTooLarge
		}
	}
	rs.mu.Lock()
	switch {
	case rs.responded:
		rs.mu.Unlock()
		return errors.New("http: response already written")
	case rs.closed:
		err := rs.closedErrLocked()
		rs.mu.Unlock()
		return err
	}
	rs.responded = true
	// The write the connection was told to expect ends that as it is made.
	expected := rs.expected
	rs.expected = false
	// The response is the stream's to send from here; the clock stops.
	stopped := rs.stopWriteTimerLocked()
	rs.mu.Unlock()
	if stopped {
		rs.unref()
	}

	// The field sections go back to the pool once the frames are built from
	// them; the frames' own buffer the stream takes over.
	block := bufferpool.Append(nil, qpack.Prefix)
	defer func() { bufferpool.Put(block) }()
	// The numbers are formatted on the stack: AppendField keeps nothing of
	// what it is given, so the strings made of them need no allocation.
	var digits [20]byte
	block = qpack.AppendField(block, ":status", string(strconv.AppendInt(digits[:0], int64(status), 10)), false)
	statusBody := status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
	if statusBody {
		block = qpack.AppendField(block, "content-length", string(strconv.AppendInt(digits[:0], responseLength(req, response), 10)), false)
	}
	block = appendHeader(block, response.Header, func(name string) bool { return name == "content-length" })
	body := response.Body
	if !bodyAllowed(req, status) {
		body = nil
	}
	// A body that is the request's own is in the server's buffer for that
	// request, which goes back to the pool once the response is written: this
	// response goes on reading it as the client's windows, and then its
	// acknowledgements, let it go, so it takes a copy of that one, as it did
	// of every body. Every other body is kept where the handler gave it,
	// whatever its size: the whole response costs its body's memory once, as
	// it does on HTTP/2, rather than a copy and a pool class larger than it,
	// and a handler has one rule to keep rather than one per size. See
	// fibhttp.Response.Body for what that asks of the handler.
	owned := false
	if len(body) > 0 && rs.block.BodyWithin(body) {
		copied := bufferpool.Get(len(body))[:len(body)]
		copy(copied, body)
		body, owned = copied, true
	}
	// The frames are the stream's own: the HEADERS frame and the DATA frame's
	// header in one buffer from the pool, and a trailer, behind the body, in
	// another.
	head := bufferpool.Get(len(block) + 16)[:0]
	head = appendHeadersFrame(head, block)
	if len(body) > 0 {
		head = appendFrameHeader(head, frameData, len(body))
	}
	var tail []byte
	if bodyAllowed(req, status) && len(response.Trailer) > 0 {
		if tail = appendTrailer(bufferpool.Get(256)[:0], response.Trailer); len(tail) == 0 {
			bufferpool.Put(tail)
			tail = nil
		}
	}
	parts := [3]quic.Part{
		{Data: head, Owned: true},
		{Data: body, Owned: owned},
		{Data: tail, Owned: true},
	}
	err := rs.s.WriteFinalParts(parts[:], expected)
	// The response is all this stream sends, and a request whose response
	// is written wants nothing more of what arrives for it, so QUIC is told
	// to forget the request as it takes the response rather than once the
	// client has acknowledged it.
	rs.mu.Lock()
	remoteDone := rs.remoteDone
	rs.mu.Unlock()
	if !remoteDone {
		// Answered before the request finished arriving: the rest of it is
		// not wanted (RFC 9114 section 4.1), and a handler still taking it
		// hears that it is not coming.
		rs.s.StopSending(uint64(ErrCodeNoError))
		rs.failBody(fibhttp.ErrBodyAbandoned)
	}
	if response.Close {
		// One request cannot close a connection that others share, so
		// Close retires it gracefully.
		rs.sc.goAway()
	}
	if rs.req != nil {
		rs.sc.untrack(rs)
	}
	if err != nil {
		return errStreamClosed
	}
	return nil
}

// appendTrailer appends a HEADERS frame carrying what of trailer HTTP/3 can
// send, if that is anything.
func appendTrailer(out []byte, trailer stdhttp.Header) []byte {
	if len(trailer) == 0 {
		return out
	}
	block := bufferpool.Append(nil, qpack.Prefix)
	for key, values := range trailer {
		name := strings.ToLower(key)
		if !validTrailer(name, values) {
			continue
		}
		for _, value := range values {
			block = qpack.AppendField(block, name, value, false)
		}
	}
	if len(block) > len(qpack.Prefix) {
		out = appendHeadersFrame(out, block)
	}
	bufferpool.Put(block)
	return out
}

// armWriteTimeout starts the clock Config.WriteTimeout puts on the response,
// as the handler is about to run: once it has run out, a stream whose
// response its handler has not finished writing is reset, as net/http's
// HTTP/2 server resets a stream, ending the stream rather than the
// connection its client shares with others. A handler waiting for the client
// to take what it wrote is let go, its Context is cancelled, and what it
// writes from then on fails with os.ErrDeadlineExceeded. The timer holds a
// reference to the stream until it is stopped or has run. Without a
// WriteTimeout there is no timer.
func (rs *requestStream) armWriteTimeout() {
	d := rs.sc.h.config.WriteTimeout
	if d <= 0 {
		return
	}
	rs.refs.Add(1)
	rs.mu.Lock()
	rs.writeTimer = time.AfterFunc(d, rs.writeTimedOut)
	rs.mu.Unlock()
}

// SetWriteDeadline replaces the clock armWriteTimeout started on the
// response with one that runs out at t, or with none when t is zero; see
// http.StreamDeadliner. A timer that has fired already has the stream on its
// way to being reset, which it reports.
func (rs *requestStream) SetWriteDeadline(t time.Time) error {
	rs.mu.Lock()
	if rs.closed {
		err := rs.closedErrLocked()
		rs.mu.Unlock()
		return err
	}
	if rs.responded && !rs.streaming {
		// The response has been written whole; there is nothing left to
		// bound.
		rs.mu.Unlock()
		return nil
	}
	stopped := false
	if timer := rs.writeTimer; timer != nil {
		if !timer.Stop() {
			rs.mu.Unlock()
			return os.ErrDeadlineExceeded
		}
		rs.writeTimer, stopped = nil, true
	}
	if !t.IsZero() {
		// The new timer takes the stopped one's reference, or one of its own.
		if stopped {
			stopped = false
		} else {
			rs.refs.Add(1)
		}
		rs.writeTimer = time.AfterFunc(time.Until(t), rs.writeTimedOut)
	}
	rs.mu.Unlock()
	if stopped {
		rs.unref()
	}
	return nil
}

// SetReadDeadline gives the rest of the request body until t to arrive, or
// as long as it takes when t is zero; see http.StreamDeadliner. Once t has
// passed, a body still arriving fails with os.ErrDeadlineExceeded: the stream
// stays open for the handler to answer, and what more of the body arrives is
// dropped, its room given back. A stream has no read deadline otherwise, and
// a body that has all arrived has nothing left to bound. A timer that has
// fired already has failed the body, or is about to, which it reports.
func (rs *requestStream) SetReadDeadline(t time.Time) error {
	rs.mu.Lock()
	if rs.closed {
		err := rs.closedErrLocked()
		rs.mu.Unlock()
		return err
	}
	b := rs.streamed
	if rs.remoteDone || b == nil || b.feed == nil {
		rs.mu.Unlock()
		return nil
	}
	stopped := false
	if timer := b.readTimer; timer != nil {
		if !timer.Stop() {
			rs.mu.Unlock()
			return os.ErrDeadlineExceeded
		}
		b.readTimer, stopped = nil, true
	}
	if !t.IsZero() {
		// The new timer takes the stopped one's reference, or one of its own.
		if stopped {
			stopped = false
		} else {
			rs.refs.Add(1)
		}
		b.readTimer = time.AfterFunc(time.Until(t), rs.readTimedOut)
	}
	rs.mu.Unlock()
	if stopped {
		rs.unref()
	}
	return nil
}

// readTimedOut fails a body that outlasted the deadline its handler set;
// see SetReadDeadline. A timer stopped once it had fired finds readTimer
// gone, and leaves the body alone.
func (rs *requestStream) readTimedOut() {
	defer rs.unref()
	rs.mu.Lock()
	b := rs.streamed
	fire := b.readTimer != nil && !rs.closed && !rs.remoteDone
	b.readTimer = nil
	rs.mu.Unlock()
	if fire {
		b.feed.Fail(os.ErrDeadlineExceeded)
	}
}

// armPendingTimer bounds what is left of a request a delivery did not
// complete, unless a timer does already: its header by ReadHeaderTimeout,
// from now, and a body collected before its handler runs by the read
// timeouts, from its header. A request that arrives whole in one delivery,
// as most do, arms none. A body that streams to its handler is bounded by
// the feed's deadline instead (see startStream), and a tunnel by nothing
// here.
func (rs *requestStream) armPendingTimer() {
	config := &rs.sc.h.config
	var d time.Duration
	switch b := rs.streamed; {
	case rs.req == nil:
		d = headerTimeout(config)
	case rs.tunnel != nil || b != nil && b.feed != nil:
		return
	default:
		start := time.Now()
		if b != nil && !b.headerAt.IsZero() {
			start = b.headerAt
		}
		at := bodyDeadline(config, start)
		if at.IsZero() {
			return
		}
		d = max(time.Until(at), time.Nanosecond)
	}
	if d <= 0 {
		return
	}
	rs.mu.Lock()
	if rs.collectTimer == nil && !rs.closed && !rs.remoteDone {
		rs.refs.Add(1)
		rs.collectTimer = time.AfterFunc(d, rs.collectTimedOut)
	}
	rs.mu.Unlock()
}

// collectTimedOut resets a stream whose header did not arrive by
// ReadHeaderTimeout, or whose body did not by its read timeouts while it was
// collected: its handler has not run, so there is nobody else to tell. A timer stopped once it had fired finds collectTimer
// gone, and leaves the stream alone. The reset is the stream's own, safe
// from here; the connection hears the client's answering reset and lets
// the stream go as for any other.
func (rs *requestStream) collectTimedOut() {
	defer rs.unref()
	rs.mu.Lock()
	fire := rs.collectTimer != nil && !rs.closed && !rs.remoteDone
	rs.collectTimer = nil
	rs.mu.Unlock()
	if fire {
		abortStream(rs.s, ErrCodeRequestCancelled)
	}
}

// stopCollectTimerLocked stops the collect timer, if there is one, and
// reports whether the caller is to give up the reference it held.
func (rs *requestStream) stopCollectTimerLocked() bool {
	t := rs.collectTimer
	if t == nil {
		return false
	}
	rs.collectTimer = nil
	return t.Stop()
}

// stopReadTimerLocked stops the read timer, if there is one, as
// stopWriteTimerLocked stops the write timer: the collect timer while the
// body is collected, and the feed's once it streams.
func (rs *requestStream) stopReadTimerLocked() bool {
	if rs.collectTimer != nil {
		return rs.stopCollectTimerLocked()
	}
	b := rs.streamed
	if b == nil || b.readTimer == nil {
		return false
	}
	t := b.readTimer
	b.readTimer = nil
	return t.Stop()
}

var (
	_ fibhttp.StreamDeadliner = (*requestStream)(nil)
	_ fibhttp.StreamAbandoner = (*requestStream)(nil)
)

// stopWriteTimerLocked stops the write timer, if there is one, and reports
// whether the caller is to give up the reference it held, which it is to do
// once it has let go of rs.mu; a timer that has fired gives it up itself.
func (rs *requestStream) stopWriteTimerLocked() bool {
	t := rs.writeTimer
	rs.writeTimer = nil
	return t != nil && t.Stop()
}

// writeTimedOut resets a stream whose response outlasted Config.WriteTimeout;
// see armWriteTimeout.
func (rs *requestStream) writeTimedOut() {
	defer rs.unref()
	rs.mu.Lock()
	open := rs.writeTimer != nil && !rs.closed && (!rs.responded || rs.streaming)
	rs.writeTimer = nil
	readStopped := false
	if open {
		rs.closed, rs.timedOut = true, true
		readStopped = rs.stopReadTimerLocked()
	}
	rs.mu.Unlock()
	if readStopped {
		rs.unref()
	}
	if !open {
		return
	}
	rs.block.Cancel(os.ErrDeadlineExceeded)
	rs.failBody(os.ErrDeadlineExceeded)
	// Resetting the stream lets a writer waiting in WaitSendable go.
	abortStream(rs.s, ErrCodeInternalError)
	// A handler that has written nothing yet would otherwise keep the
	// connection holding what it sends for the write it expects.
	rs.writeDone()
	rs.sc.untrack(rs)
}

// Abandon resets a stream whose response cannot be sent, a status or a
// header HTTP/3 cannot carry found once the handler is done, so that its
// client is not left waiting; see http.StreamAbandoner.
func (rs *requestStream) Abandon() {
	rs.mu.Lock()
	if rs.closed || rs.responded && !rs.streaming {
		rs.mu.Unlock()
		return
	}
	rs.closed = true
	stopped := rs.stopWriteTimerLocked()
	readStopped := rs.stopReadTimerLocked()
	rs.mu.Unlock()
	if stopped {
		rs.unref()
	}
	if readStopped {
		rs.unref()
	}
	rs.failBody(fibhttp.ErrBodyAbandoned)
	abortStream(rs.s, ErrCodeInternalError)
	// Nothing will be written for the write the connection holds its
	// output for.
	rs.writeDone()
	rs.sc.untrack(rs)
}

// closedErrLocked is what writing to rs reports once it has been closed.
// Callers hold rs.mu.
func (rs *requestStream) closedErrLocked() error {
	if rs.timedOut {
		return os.ErrDeadlineExceeded
	}
	return errStreamClosed
}

// closedErr is closedErrLocked for a caller without rs.mu.
func (rs *requestStream) closedErr() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.closedErrLocked()
}

// writeDone tells the connection, once, that the response finish told it to
// expect has been written.
func (rs *requestStream) writeDone() {
	rs.mu.Lock()
	expected := rs.expected
	rs.expected = false
	rs.mu.Unlock()
	if expected {
		rs.sc.qc.WriteDone()
	}
}

// WriteInterim sends an informational 1xx response ahead of the final one.
func (rs *requestStream) WriteInterim(status int, header stdhttp.Header) error {
	if err := checkHeader(header); err != nil {
		return err
	}
	if max, limited := rs.fieldLimit(); limited && uint64(statusSize)+headerSize(header, nil) > max {
		return errFieldSectionTooLarge
	}
	rs.mu.Lock()
	switch {
	case rs.responded:
		rs.mu.Unlock()
		return errors.New("http: interim response after the final one")
	case rs.closed:
		err := rs.closedErrLocked()
		rs.mu.Unlock()
		return err
	}
	rs.mu.Unlock()
	block := append([]byte(nil), qpack.Prefix...)
	block = qpack.AppendField(block, ":status", strconv.Itoa(status), false)
	block = appendHeader(block, header, nil)
	if err := rs.s.Write(appendHeadersFrame(nil, block), false); err != nil {
		return errStreamClosed
	}
	return nil
}

// Push is not supported: HTTP/3 server push needs the client to ask for it
// with MAX_PUSH_ID, and browsers do not.
func (rs *requestStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error {
	return stdhttp.ErrNotSupported
}

var (
	_ fibhttp.Stream           = (*requestStream)(nil)
	_ fibhttp.ResponseStreamer = (*requestStream)(nil)
	_ fibhttp.ResponseNotifier = (*requestStream)(nil)
)

// newTLS is how a connection's TLS handshake is made: by this module's state
// machine where it can, which is the default, and otherwise by crypto/tls. The
// connections share the keys their session tickets are sealed under.
func (h *ServerHandler) newTLS(blocking bool) func(bool, *tls.Config) quic.TLSConn {
	if blocking {
		return nil
	}
	var once sync.Once
	var server *fibtls.QUICServer
	return func(_ bool, config *tls.Config) quic.TLSConn {
		once.Do(func() { server = fibtls.NewQUICServer(config) })
		if server == nil {
			return nil
		}
		return server.NewConn()
	}
}

// safeMethod says whether a request with method can be served from early
// data, which may be replayed: the methods that change nothing.
func safeMethod(method string) bool {
	switch method {
	case stdhttp.MethodGet, stdhttp.MethodHead, stdhttp.MethodOptions, stdhttp.MethodTrace:
		return true
	}
	return false
}
