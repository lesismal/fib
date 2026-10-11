//go:build linux || darwin || windows

package http3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	fibtls "github.com/lesismal/fib/tls"
)

var (
	// ErrClientClosed is what requests still waiting for a connection get
	// when the client is closed, and what new requests get afterwards.
	ErrClientClosed = errors.New("http3: client closed")
	// ErrUnsupportedScheme is what a request for anything but https:// gets:
	// HTTP/3 is always encrypted.
	ErrUnsupportedScheme = errors.New("http3: unsupported URL scheme")
	// errRequestTimeout is what a request gets when ClientConfig.Timeout runs
	// out. errors.Is matches it against os.ErrDeadlineExceeded.
	errRequestTimeout = fmt.Errorf("http3: request timed out: %w", os.ErrDeadlineExceeded)
	errBodyTooLarge   = errors.New("http3: response body too large")
	errHeaderTooLarge = errors.New("http3: response header too large")
)

// ClientConfig bounds what a Client waits for and keeps.
type ClientConfig struct {
	// Timeout bounds a request from Do until its response is complete:
	// waiting for a connection, the handshake, sending and reading all
	// count. Zero means no limit beyond the request's context.
	Timeout time.Duration
	// DialTimeout bounds resolving the host and starting the connection;
	// the QUIC handshake is bounded by HandshakeTimeout.
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	// IdleConnTimeout closes a connection that has carried no request for
	// this long. Zero keeps it until the server or the QUIC idle timeout
	// closes it.
	IdleConnTimeout time.Duration
	// MaxIdleTimeout is the QUIC idle timeout: a connection that hears
	// nothing from the server for this long is gone.
	MaxIdleTimeout time.Duration
	// MaxResponseHeaderBytes and MaxResponseBodyBytes bound what one
	// response may hold. The body is buffered whole before the callback
	// runs, so the second is also the memory one response can take. A body
	// that streams (see StreamResponseBody) is bounded by
	// MaxStreamedBodyBytes instead.
	MaxResponseHeaderBytes int
	MaxResponseBodyBytes   int64
	// StreamResponseBody runs the callback of a response before its body has
	// all arrived, as http.ClientConfig.StreamResponseBody does for HTTP/1
	// and HTTP/2: once the header has, for a body with a content-length,
	// and once some of the body has, for one without. The body is then
	// taken inside the callback with ClientResponse.OnBody, which delivers
	// it piece by piece, or read from Body without waiting. A callback that
	// does neither leaves the rest of the body to be thrown away. Off, the
	// default, the callback runs once the response is complete. Client.Go
	// always waits for the whole response. The stream's flow control is
	// given back to the server only as the body is consumed, so a reader
	// that falls behind slows its own stream down, and no other.
	StreamResponseBody bool
	// StreamResponseBodyThreshold keeps the smaller bodies buffered whole
	// when StreamResponseBody is set: only a body whose content-length is
	// larger than this, or one without a length, streams, and its callback
	// is not run until that much of it has arrived. Zero streams every body
	// as soon as its header has arrived.
	StreamResponseBodyThreshold int64
	// MaxStreamedBodyBytes bounds a streamed body, which MaxResponseBodyBytes
	// does not. Zero, the default, leaves a streamed body unbounded.
	MaxStreamedBodyBytes int64
	// BlockingTLSHandshake runs the TLS handshake of every connection through
	// crypto/tls's QUICConn, which parks a goroutine in it for as long as it
	// waits for the server. By default the handshake is a state machine that
	// takes none, where the TLSConfig allows it (see package tls).
	BlockingTLSHandshake bool
	// TLSConfig is used for every connection. Nil means the defaults;
	// either way a config naming no server gets the request's host, and
	// ALPN offers "h3".
	TLSConfig *tls.Config
}

// DefaultClientConfig returns the defaults, which zero limits in a
// ClientConfig also stand for.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Timeout:                30 * time.Second,
		DialTimeout:            10 * time.Second,
		HandshakeTimeout:       10 * time.Second,
		IdleConnTimeout:        90 * time.Second,
		MaxIdleTimeout:         30 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		MaxResponseBodyBytes:   64 << 20,
	}
}

// Client sends HTTP/3 requests over connections an engine dials, without
// blocking the caller: the response goes to a callback, by default with its
// body already buffered once it has arrived; see
// ClientConfig.StreamResponseBody for handing it a large body as it arrives.
//
// A client keeps one connection per host and port and sends every request
// to it on a stream of its own, as many at a time as the server allows;
// the rest wait for a stream to free up. Requests the server has not begun,
// because it went away or refused them, are sent again on a new connection.
//
// The engine may be any running engine, TCP or UDP, a server's own or one
// made for clients: client connections carry their own handler. Close the
// client before the engine.
type Client struct {
	engine    *fib.Engine
	config    ClientConfig
	tlsConfig *tls.Config
	mu        sync.Mutex
	conns     map[string]*clientConn
	closed    bool
}

// NewClient returns a client that dials through engine. Zero values in
// config take the defaults.
func NewClient(engine *fib.Engine, config ClientConfig) *Client {
	defaults := DefaultClientConfig()
	if config.MaxResponseHeaderBytes <= 0 {
		config.MaxResponseHeaderBytes = defaults.MaxResponseHeaderBytes
	}
	if config.MaxResponseBodyBytes <= 0 {
		config.MaxResponseBodyBytes = defaults.MaxResponseBodyBytes
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = defaults.HandshakeTimeout
	}
	if config.MaxIdleTimeout <= 0 {
		config.MaxIdleTimeout = defaults.MaxIdleTimeout
	}
	tlsConfig := config.TLSConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}
	tlsConfig = tlsConfig.Clone()
	tlsConfig.NextProtos = []string{NextProto}
	tlsConfig.MinVersion = max(tlsConfig.MinVersion, tls.VersionTLS13)
	return &Client{engine: engine, config: config, tlsConfig: tlsConfig, conns: make(map[string]*clientConn)}
}

// Do sends req and calls callback exactly once with its response or the
// reason there is none. It does not wait for the network, though it does
// read req.Body, which must therefore not block for long.
//
// callback may run on any goroutine: an engine worker, a timer, or the
// caller's own for a request Do rejects outright. It must not block for
// long.
//
// The callback gets a *fibhttp.ClientResponse, which embeds the net/http
// response and, with ClientConfig.StreamResponseBody, takes a large body as it
// arrives with OnBody; see package http's ClientResponse. The Timeout then also
// ends a body that is streaming.
func (c *Client) Do(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error)) {
	c.do(req, callback, false)
}

// do is Do, with whole set when the response must arrive whole whatever the
// client's configuration says.
func (c *Client) do(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error), whole bool) {
	if callback == nil {
		callback = func(*fibhttp.ClientResponse, error) {}
	}
	if req.URL == nil || req.URL.Scheme != "https" {
		callback(nil, ErrUnsupportedScheme)
		return
	}
	var body []byte
	if req.Body != nil && req.Body != stdhttp.NoBody {
		var err error
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			callback(nil, err)
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	r := &clientRequest{req: req, body: body, callback: callback, whole: whole}
	fields, trailer, err := requestFields(req, len(body))
	if err != nil {
		callback(nil, err)
		return
	}
	r.fields, r.trailer = fields, trailer
	r.mu.Lock()
	if c.config.Timeout > 0 {
		r.timer = time.AfterFunc(c.config.Timeout, func() { r.abort(errRequestTimeout) })
	}
	if ctx := req.Context(); ctx.Done() != nil {
		r.stopContext = context.AfterFunc(ctx, func() { r.abort(ctx.Err()) })
	}
	r.mu.Unlock()
	c.enqueue(r)
}

// Future is a request in flight, for callers that would rather wait on it
// than be called back.
type Future struct {
	done chan struct{}
	resp *stdhttp.Response
	err  error
}

// Go sends req like Do and returns a Future for its outcome.
func (c *Client) Go(req *stdhttp.Request) *Future {
	f := &Future{done: make(chan struct{})}
	c.do(req, func(resp *fibhttp.ClientResponse, err error) {
		if resp != nil {
			f.resp = resp.Response
		}
		f.err = err
		close(f.done)
	}, true)
	return f
}

// Wait blocks until the response has arrived or the request has failed.
func (f *Future) Wait() (*stdhttp.Response, error) {
	<-f.done
	return f.resp, f.err
}

// Done is closed once Wait would no longer block, for use in a select.
func (f *Future) Done() <-chan struct{} { return f.done }

// Close stops the client taking requests. Requests still waiting for a
// stream fail with ErrClientClosed and idle connections are closed;
// requests already sent finish as they would have.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	conns := make([]*clientConn, 0, len(c.conns))
	for _, cc := range c.conns {
		conns = append(conns, cc)
	}
	c.mu.Unlock()
	for _, cc := range conns {
		cc.mu.Lock()
		waiting := cc.waiting
		cc.waiting = nil
		cc.mu.Unlock()
		for _, r := range waiting {
			r.finish(nil, ErrClientClosed)
		}
		cc.closeIfIdle()
	}
}

// connKey is the connection a request goes to: its address, and the name
// the server's certificate has to carry.
func connKey(req *stdhttp.Request) (addr, serverName string) {
	host := req.URL.Host
	name, port, err := net.SplitHostPort(host)
	if err != nil {
		name, port = strings.Trim(host, "[]"), "443"
	}
	return net.JoinHostPort(name, port), name
}

// enqueue queues r on its host's connection, dialing one if there is none.
func (c *Client) enqueue(r *clientRequest) {
	addr, serverName := connKey(r.req)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		r.finish(nil, ErrClientClosed)
		return
	}
	key := addr + "|" + serverName
	cc := c.conns[key]
	dial := cc == nil
	if dial {
		cc = &clientConn{client: c, key: key, addr: addr, serverName: serverName,
			streams: make(map[uint64]*clientStream)}
		cc.peer.isClient = true
		cc.peer.onGoAway = cc.handleGoAway
		c.conns[key] = cc
	}
	cc.mu.Lock()
	cc.waiting = append(cc.waiting, r)
	cc.mu.Unlock()
	c.mu.Unlock()
	if dial {
		cc.dial()
	} else {
		cc.startWaiting()
	}
}

// forget drops a connection that takes no more requests.
func (c *Client) forget(cc *clientConn) {
	c.mu.Lock()
	if c.conns[cc.key] == cc {
		delete(c.conns, cc.key)
	}
	c.mu.Unlock()
}

// clientRequest is one request from Do until its callback.
type clientRequest struct {
	req    *stdhttp.Request
	body   []byte
	fields []qpack.HeaderField
	// trailer is the trailer section that ends the request, if it has one.
	trailer  []qpack.HeaderField
	callback func(*fibhttp.ClientResponse, error)
	// whole asks for the response to arrive whole even when the client would
	// stream it.
	whole bool

	mu          sync.Mutex
	done        bool
	timer       *time.Timer
	stopContext func() bool
	stream      *clientStream
	// body is the stream of the response, once its callback has run on it,
	// which fired says it has.
	respBody *fibhttp.ResponseStream
	fired    bool
	// retried is set once the request has been sent again, which happens
	// at most once.
	retried bool
}

// finish calls the callback, unless it has been called already.
//
// A response that streams has its callback run by the stream, before it is
// complete, and finish then only reports how it ended: an error goes to the
// stream, and a completion is the stream's own to report.
func (r *clientRequest) finish(resp *stdhttp.Response, err error) bool {
	if !r.markDone() {
		return false
	}
	r.mu.Lock()
	body, fired := r.respBody, r.fired
	r.mu.Unlock()
	switch {
	case fired:
		if err != nil {
			body.Fail(err)
		}
	case err != nil:
		r.callback(nil, err)
	default:
		fibhttp.DeliverResponse(resp, r.abort, r.callback)
	}
	return true
}

// markDone settles the request, once, and stops what would abandon it.
func (r *clientRequest) markDone() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return false
	}
	r.done = true
	if r.timer != nil {
		r.timer.Stop()
	}
	if r.stopContext != nil {
		r.stopContext()
	}
	r.stream = nil
	return true
}

// begin records that the stream is about to run the callback. It reports false
// if the request has been settled, in which case there is nothing to call.
func (r *clientRequest) begin(s *fibhttp.ResponseStream) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return false
	}
	r.respBody, r.fired = s, true
	return true
}

// abort fails the request and abandons its stream, if it has one.
func (r *clientRequest) abort(err error) {
	r.mu.Lock()
	st := r.stream
	r.mu.Unlock()
	if r.finish(nil, err) && st != nil {
		st.cancel()
	}
}

func (r *clientRequest) attach(st *clientStream) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return false
	}
	r.stream = st
	return true
}

func (r *clientRequest) isDone() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

// retry sends the request again on a new connection, once, for a request
// the server never began on.
func (r *clientRequest) retry(c *Client, err error) {
	r.mu.Lock()
	again := !r.retried && !r.done
	r.retried = true
	r.stream = nil
	r.mu.Unlock()
	if !again {
		r.finish(nil, err)
		return
	}
	c.enqueue(r)
}

// requestFields lists a request's fields, pseudo-headers first, and its
// trailer fields, checking them all before any is encoded.
func requestFields(req *stdhttp.Request, bodyLen int) (fields, trailer []qpack.HeaderField, err error) {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	method := req.Method
	if method == "" {
		method = stdhttp.MethodGet
	}
	if !validMethod(method) {
		return nil, nil, errors.New("http3: invalid method " + strconv.Quote(method))
	}
	fields = []qpack.HeaderField{{Name: ":method", Value: method}}
	if method == stdhttp.MethodConnect {
		fields = append(fields, qpack.HeaderField{Name: ":authority", Value: host})
	} else {
		fields = append(fields,
			qpack.HeaderField{Name: ":scheme", Value: "https"},
			qpack.HeaderField{Name: ":authority", Value: host},
			qpack.HeaderField{Name: ":path", Value: req.URL.RequestURI()})
	}
	userAgent := false
	for key, values := range req.Header {
		name := strings.ToLower(key)
		if connectionHeaders[name] || name == "host" || name == "content-length" || name == "te" {
			continue
		}
		if !validName(name) {
			return nil, nil, errors.New("http3: invalid request header name " + strconv.Quote(key))
		}
		userAgent = userAgent || name == "user-agent"
		for _, value := range values {
			if !sendableValue(value) {
				return nil, nil, errors.New("http3: invalid request header value for " + key)
			}
			fields = append(fields, qpack.HeaderField{Name: name, Value: value})
		}
	}
	if !userAgent {
		fields = append(fields, qpack.HeaderField{Name: "user-agent", Value: "Go-http-client/3"})
	}
	switch {
	case bodyLen > 0:
		fields = append(fields, qpack.HeaderField{Name: "content-length", Value: strconv.Itoa(bodyLen)})
	case method == stdhttp.MethodPost || method == stdhttp.MethodPut || method == stdhttp.MethodPatch:
		fields = append(fields, qpack.HeaderField{Name: "content-length", Value: "0"})
	}
	var declared []string
	for key, values := range req.Trailer {
		name := strings.ToLower(key)
		if !validName(name) || strings.HasPrefix(name, ":") || connectionHeaders[name] {
			return nil, nil, errors.New("http3: invalid request trailer name " + strconv.Quote(key))
		}
		declared = append(declared, name)
		for _, value := range values {
			if !sendableValue(value) {
				return nil, nil, errors.New("http3: invalid request trailer value for " + key)
			}
			trailer = append(trailer, qpack.HeaderField{Name: name, Value: value})
		}
	}
	if len(declared) > 0 {
		// Servers keep only the trailers a request announced, so the names
		// go out ahead of the body in a Trailer field of this side's making.
		slices.Sort(declared)
		fields = append(fields, qpack.HeaderField{Name: "trailer", Value: strings.Join(declared, ",")})
	}
	return fields, trailer, nil
}

func validMethod(method string) bool {
	for i := 0; i < len(method); i++ {
		if c := method[i]; c <= ' ' || c >= 0x7f || strings.IndexByte("()<>@,;:\\\"/[]?={}", c) >= 0 {
			return false
		}
	}
	return method != ""
}

// clientConn is the connection to one host.
type clientConn struct {
	client     *Client
	key        string
	addr       string
	serverName string
	peer       peerStreams
	// decoder decodes the connection's field sections. Only the goroutine
	// QUIC calls the connection's handler on uses it.
	decoder qpack.Decoder

	mu       sync.Mutex
	fc       *fib.Connection
	qc       *quic.Conn
	tlsState *tls.ConnectionState
	ready    bool
	closed   bool
	// goingAway is set once the server has said it takes no more
	// requests, or the client is done with the connection.
	goingAway bool
	waiting   []*clientRequest
	streams   map[uint64]*clientStream
	idleTimer *time.Timer
}

// newTLS is how a connection's TLS handshake is made: by this module's state
// machine where it can, which is the default, and otherwise by crypto/tls.
func (c *Client) newTLS() func(bool, *tls.Config) quic.TLSConn {
	if c.config.BlockingTLSHandshake {
		return nil
	}
	return func(_ bool, config *tls.Config) quic.TLSConn {
		if conn := fibtls.NewQUICClient(config); conn != nil {
			return conn
		}
		return nil
	}
}

func (cc *clientConn) dial() {
	c := cc.client
	err := c.engine.DialWithHandler("udp", cc.addr, c.config.DialTimeout, clientPath{cc}, func(fc *fib.Connection, err error) {
		if err != nil {
			cc.failed(err)
			return
		}
		tlsConfig := c.tlsConfig
		if tlsConfig.ServerName == "" {
			tlsConfig = tlsConfig.Clone()
			tlsConfig.ServerName = cc.serverName
		}
		config := quic.Config{
			TLSConfig:        tlsConfig,
			NewTLS:           c.newTLS(),
			MaxIdleTimeout:   c.config.MaxIdleTimeout,
			HandshakeTimeout: c.config.HandshakeTimeout,
			// The server's control and QPACK streams, and room to spare.
			MaxIncomingUniStreams: 16,
			// Servers may not open request streams; one is the least QUIC
			// allows advertising that means it.
			MaxIncomingStreams: 1,
			// fib hands each datagram over as a buffer of its own from the
			// pool, which QUIC gives back once it has done with it.
			RecycleDatagrams: true,
		}
		cc.mu.Lock()
		cc.fc = fc
		qc, err := quic.Dial(fc, fc.RemoteAddr(), config, cc)
		cc.qc = qc
		cc.mu.Unlock()
		if err != nil {
			fc.Close()
			cc.failed(err)
		}
	})
	if err != nil {
		cc.failed(err)
	}
}

// failed fails every request waiting on a connection that never came up.
func (cc *clientConn) failed(err error) {
	cc.client.forget(cc)
	cc.mu.Lock()
	cc.closed = true
	waiting := cc.waiting
	cc.waiting = nil
	cc.mu.Unlock()
	for _, r := range waiting {
		r.finish(nil, err)
	}
}

// clientPath is the fib.Handler of a connection's UDP socket, which feeds
// the QUIC connection its datagrams.
type clientPath struct{ cc *clientConn }

func (p clientPath) OnOpen(*fib.Connection)                 {}
func (p clientPath) OnPriorityData(*fib.Connection, []byte) {}

func (p clientPath) OnData(_ *fib.Connection, data []byte) {
	p.cc.mu.Lock()
	qc := p.cc.qc
	p.cc.mu.Unlock()
	if qc != nil {
		qc.HandleDatagram(data)
	}
}

func (p clientPath) OnDatagrams(_ *fib.Connection, datagrams [][]byte) {
	p.cc.mu.Lock()
	qc := p.cc.qc
	p.cc.mu.Unlock()
	if qc != nil {
		qc.HandleDatagrams(datagrams)
	}
}

func (p clientPath) OnClose(_ *fib.Connection, err error) {
	p.cc.mu.Lock()
	qc := p.cc.qc
	p.cc.mu.Unlock()
	if qc != nil {
		if err == nil {
			err = net.ErrClosed
		}
		qc.Abort(err)
	}
}

func (cc *clientConn) OnHandshake(qc *quic.Conn) {
	state := qc.ConnectionState()
	if _, err := openControl(qc, cc.client.config.MaxResponseHeaderBytes); err != nil {
		closeWith(qc, connErr(ErrCodeStreamCreationError, "cannot open the control stream"))
		return
	}
	cc.mu.Lock()
	cc.tlsState = &state
	cc.ready = true
	cc.mu.Unlock()
	cc.startWaiting()
}

// startWaiting puts waiting requests on streams, as many as the server
// allows.
func (cc *clientConn) startWaiting() {
	for {
		cc.mu.Lock()
		if !cc.ready || cc.closed || cc.goingAway || len(cc.waiting) == 0 {
			cc.mu.Unlock()
			return
		}
		r := cc.waiting[0]
		if r.isDone() {
			cc.waiting = cc.waiting[1:]
			cc.mu.Unlock()
			continue
		}
		s, err := cc.qc.OpenStream()
		if errors.Is(err, quic.ErrStreamLimit) {
			cc.mu.Unlock()
			return
		}
		if err != nil {
			cc.mu.Unlock()
			return
		}
		cc.waiting = cc.waiting[1:]
		st := &clientStream{cc: cc, s: s, r: r, parser: frameParser{maxFrame: uint64(cc.client.config.MaxResponseHeaderBytes)}}
		if cc.client.config.StreamResponseBody && !r.whole {
			// The stream's flow control is given back from here, so that a
			// body that streams paces the server; see clientStream.feed.
			s.HoldCredit()
			st.paced = true
		}
		s.Context = st
		cc.streams[s.ID()] = st
		if cc.idleTimer != nil {
			cc.idleTimer.Stop()
			cc.idleTimer = nil
		}
		cc.mu.Unlock()
		if !r.attach(st) {
			st.cancel()
			continue
		}
		st.send()
	}
}

func (cc *clientConn) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	switch st := s.Context.(type) {
	case *clientStream:
		st.feed(data, fin)
	case *uniStream:
		st.feed(data, fin)
	case nil:
		if s.Bidirectional() {
			// Servers may not open bidirectional streams.
			closeWith(s.Conn(), connErr(ErrCodeStreamCreationError, "server-initiated bidirectional stream"))
			return
		}
		u := newUniStream(&cc.peer, s, func(err error) { closeWith(s.Conn(), err) })
		s.Context = u
		u.feed(data, fin)
	}
}

func (cc *clientConn) OnStreamReset(s *quic.Stream, code uint64) {
	switch st := s.Context.(type) {
	case *clientStream:
		st.reset(ErrorCode(code))
	case *uniStream:
		if st.typ == streamControl || st.typ == streamQPACKEncoder || st.typ == streamQPACKDecoder {
			closeWith(s.Conn(), connErr(ErrCodeClosedCriticalStream, "critical stream reset"))
		}
	}
}

// OnStopSending is the server declining the rest of a request body, which
// it may do once it has what it needs to answer; the answer still comes.
func (cc *clientConn) OnStopSending(*quic.Stream, uint64) {}

func (cc *clientConn) OnStreamsAvailable(*quic.Conn) { cc.startWaiting() }

func (cc *clientConn) OnClose(_ *quic.Conn, err error) {
	c := cc.client
	c.forget(cc)
	cc.mu.Lock()
	wasReady := cc.ready
	cc.closed = true
	waiting := cc.waiting
	cc.waiting = nil
	streams := cc.streams
	cc.streams = map[uint64]*clientStream{}
	if cc.idleTimer != nil {
		cc.idleTimer.Stop()
	}
	cc.mu.Unlock()
	if err == nil {
		err = net.ErrClosed
	}
	for _, st := range streams {
		st.r.finish(nil, err)
	}
	for _, r := range waiting {
		if wasReady {
			// Never sent: another connection may carry it.
			r.retry(c, err)
		} else {
			r.finish(nil, err)
		}
	}
}

// handleGoAway is the server saying it will not serve requests from stream
// id on: those are sent again elsewhere, and so is everything waiting.
func (cc *clientConn) handleGoAway(id uint64) error {
	c := cc.client
	c.forget(cc)
	cc.mu.Lock()
	cc.goingAway = true
	waiting := cc.waiting
	cc.waiting = nil
	var unserved []*clientStream
	for sid, st := range cc.streams {
		if sid >= id {
			unserved = append(unserved, st)
			delete(cc.streams, sid)
		}
	}
	cc.mu.Unlock()
	for _, st := range unserved {
		abortStream(st.s, ErrCodeRequestCancelled)
		st.r.retry(c, &StreamError{Code: ErrCodeRequestRejected, Remote: true})
	}
	for _, r := range waiting {
		c.enqueue(r)
	}
	cc.closeIfIdle()
	return nil
}

// streamDone forgets a finished stream and, if the connection has nothing
// left to do, lets it go idle.
func (cc *clientConn) streamDone(st *clientStream) {
	cc.mu.Lock()
	if cc.streams[st.s.ID()] == st {
		delete(cc.streams, st.s.ID())
	}
	idle := len(cc.streams) == 0 && len(cc.waiting) == 0 && !cc.closed
	cc.mu.Unlock()
	if !idle {
		return
	}
	c := cc.client
	c.mu.Lock()
	clientClosed := c.closed
	c.mu.Unlock()
	cc.mu.Lock()
	if cc.goingAway || clientClosed {
		cc.mu.Unlock()
		cc.closeIfIdle()
		return
	}
	if timeout := c.config.IdleConnTimeout; timeout > 0 && cc.idleTimer == nil {
		cc.idleTimer = time.AfterFunc(timeout, func() {
			cc.mu.Lock()
			cc.idleTimer = nil
			idle := len(cc.streams) == 0 && len(cc.waiting) == 0
			if idle {
				cc.goingAway = true
			}
			cc.mu.Unlock()
			if idle {
				c.forget(cc)
				cc.closeIfIdle()
			}
		})
	}
	cc.mu.Unlock()
}

// closeIfIdle closes the connection if it has nothing in flight.
func (cc *clientConn) closeIfIdle() {
	cc.mu.Lock()
	qc := cc.qc
	idle := len(cc.streams) == 0 && len(cc.waiting) == 0 && !cc.closed
	cc.mu.Unlock()
	if idle && qc != nil {
		qc.Close(uint64(ErrCodeNoError), "")
	}
}

// clientStream is one request's stream.
type clientStream struct {
	cc     *clientConn
	s      *quic.Stream
	r      *clientRequest
	parser frameParser
	resp   *stdhttp.Response
	body   []byte
	// trailers is set once the trailer section has arrived.
	trailers bool
	done     bool
	// paced says the stream holds its flow control back, to give it as the
	// body is consumed: by Credit for a body that streams, and in feed for
	// whatever else a delivery carried. stream is set when the body streams
	// to the callback, passed counts what has gone to it, and fedBody how
	// much of the delivery in progress did.
	paced   bool
	stream  *fibhttp.ResponseStream
	passed  int64
	fedBody int
}

// send writes the request: HEADERS, the body in one DATA frame, and the
// end of the stream.
func (st *clientStream) send() {
	r := st.r
	// The stream copies what it is written, so every buffer built here goes
	// back to the pool once the frames are on it.
	block := bufferpool.Append(nil, qpack.Prefix)
	for _, f := range r.fields {
		block = qpack.AppendField(block, f.Name, f.Value, sensitive(f.Name))
	}
	out := bufferpool.Get(len(block) + len(r.body) + 16)[:0]
	out = appendHeadersFrame(out, block)
	bufferpool.Put(block)
	if len(r.body) > 0 {
		out = appendFrameHeader(out, frameData, len(r.body))
		out = append(out, r.body...)
	}
	if len(r.trailer) > 0 {
		trailer := bufferpool.Append(nil, qpack.Prefix)
		for _, f := range r.trailer {
			trailer = qpack.AppendField(trailer, f.Name, f.Value, sensitive(f.Name))
		}
		out = appendHeadersFrame(out, trailer)
		bufferpool.Put(trailer)
	}
	if err := st.s.Write(out, true); err != nil {
		st.fail(err)
	}
	bufferpool.Put(out)
}

func (st *clientStream) feed(data []byte, fin bool) {
	if st.done {
		return
	}
	st.fedBody = 0
	err := st.parser.feed(data, st.onData, st.onFrame)
	if st.paced {
		// What of this delivery is not body that streams is given back now;
		// the body is, as the callback consumes it.
		st.s.Credit(len(data) - st.fedBody)
	}
	if err != nil {
		var ce *connError
		// A field section that does not decode leaves the peer's encoder
		// and this side's decoder out of step, so it ends the connection
		// rather than the request (RFC 9204 section 2.2).
		if errors.As(err, &ce) || errors.Is(err, qpack.ErrDecompression) {
			closeWith(st.s.Conn(), err)
			return
		}
		st.fail(err)
		return
	}
	if !fin || st.done {
		return
	}
	if st.parser.midFrame() {
		closeWith(st.s.Conn(), connErr(ErrCodeFrameError, "response stream ended inside a frame"))
		return
	}
	st.complete()
}

func (st *clientStream) onData(chunk []byte) error {
	if st.done {
		return nil
	}
	if st.resp == nil || st.trailers {
		return connErr(ErrCodeFrameUnexpected, "DATA before HEADERS or after trailers")
	}
	if st.stream != nil {
		st.passed += int64(len(chunk))
		if limit := st.cc.client.config.MaxStreamedBodyBytes; limit > 0 && st.passed > limit {
			st.fail(errBodyTooLarge)
			return nil
		}
		st.fedBody += len(chunk)
		st.stream.Data(chunk)
		return nil
	}
	if int64(len(st.body)+len(chunk)) > st.cc.client.config.MaxResponseBodyBytes {
		st.fail(errBodyTooLarge)
		return nil
	}
	st.body = append(st.body, chunk...)
	return nil
}

func (st *clientStream) onFrame(typ uint64, payload []byte) error {
	if st.done {
		return nil
	}
	switch typ {
	case frameHeaders:
	case framePushPromise:
		// No MAX_PUSH_ID was sent, so there is no push ID it may use.
		return connErr(ErrCodeIDError, "PUSH_PROMISE without MAX_PUSH_ID")
	case frameCancelPush, frameSettings, frameGoAway, frameMaxPushID:
		return connErr(ErrCodeFrameUnexpected, "control frame on a request stream")
	default:
		if reservedFrame(typ) {
			return connErr(ErrCodeFrameUnexpected, "HTTP/2 frame type")
		}
		return nil
	}
	if st.trailers {
		return connErr(ErrCodeFrameUnexpected, "HEADERS after trailers")
	}
	fields, err := decodeFields(&st.cc.decoder, nil, payload, st.cc.client.config.MaxResponseHeaderBytes)
	if errors.Is(err, qpack.ErrTooLarge) {
		st.fail(errHeaderTooLarge)
		return nil
	}
	if err != nil {
		return err
	}
	if st.resp != nil {
		trailer, err := trailerFrom(fields)
		if err != nil {
			st.fail(fmt.Errorf("http3: malformed trailers: %w", err))
			return nil
		}
		st.resp.Trailer = trailer
		st.trailers = true
		return nil
	}
	resp, err := newResponse(fields, st.r.req)
	if err != nil {
		st.fail(fmt.Errorf("http3: malformed response: %w", err))
		return nil
	}
	// A nil response is an interim one, which only precedes the answer.
	st.resp = resp
	config := &st.cc.client.config
	if resp != nil && st.paced && bodyAllowed(st.r.req, resp.StatusCode) &&
		!(resp.ContentLength >= 0 && resp.ContentLength <= max(config.StreamResponseBodyThreshold, 0)) {
		if limit := config.MaxStreamedBodyBytes; limit > 0 && resp.ContentLength > limit {
			st.fail(errBodyTooLarge)
			return nil
		}
		st.cc.mu.Lock()
		resp.TLS = st.cc.tlsState
		st.cc.mu.Unlock()
		r := st.r
		st.stream = fibhttp.NewResponseStream(resp, r.req, fibhttp.StreamOptions{Threshold: config.StreamResponseBodyThreshold}, fibhttp.StreamHooks{
			Begin:    r.begin,
			Callback: func(cr *fibhttp.ClientResponse) { r.callback(cr, nil) },
			Whole:    func(resp *stdhttp.Response) { r.finish(resp, nil) },
			Done:     r.markDone,
			Abort:    r.abort,
			Credit:   func(n int64) { st.s.Credit(int(n)) },
		})
		st.stream.Start()
	}
	return nil
}

// complete hands a finished response to its request.
func (st *clientStream) complete() {
	st.done = true
	resp := st.resp
	if resp == nil {
		st.fail(errors.New("http3: response ended before its header"))
		return
	}
	if st.stream != nil {
		if resp.ContentLength >= 0 && resp.ContentLength != st.passed {
			st.fail(errors.New("http3: response body length does not match Content-Length"))
			return
		}
		st.cc.streamDone(st)
		st.stream.End()
		return
	}
	if bodyAllowed(st.r.req, resp.StatusCode) {
		if resp.ContentLength >= 0 && resp.ContentLength != int64(len(st.body)) {
			st.fail(errors.New("http3: response body length does not match Content-Length"))
			return
		}
		resp.ContentLength = int64(len(st.body))
	}
	resp.Body = fibhttp.NewBufferedBody(st.body)
	st.body = nil
	st.cc.mu.Lock()
	resp.TLS = st.cc.tlsState
	st.cc.mu.Unlock()
	st.cc.streamDone(st)
	st.r.finish(resp, nil)
}

// fail ends the request with err and abandons the stream.
func (st *clientStream) fail(err error) {
	st.done = true
	st.body = nil
	abortStream(st.s, ErrCodeRequestCancelled)
	st.cc.streamDone(st)
	st.r.finish(nil, err)
}

// cancel abandons the stream of a request that has already failed.
func (st *clientStream) cancel() {
	abortStream(st.s, ErrCodeRequestCancelled)
	st.cc.streamDone(st)
}

// reset is the server abandoning the response. A request it rejected
// without processing may go again.
func (st *clientStream) reset(code ErrorCode) {
	if st.done {
		return
	}
	st.done = true
	st.body = nil
	st.cc.streamDone(st)
	err := &StreamError{Code: code, Remote: true}
	if code == ErrCodeRequestRejected {
		st.r.retry(st.cc.client, err)
		return
	}
	st.r.finish(nil, err)
}
