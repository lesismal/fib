//go:build linux || darwin || windows

package http

import (
	stdtls "crypto/tls"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/bufferpool"
	fibtls "github.com/lesismal/fib/go/tls"
)

type Handler interface {
	ServeHTTP(*Context, *stdhttp.Request)
}

type HandlerFunc func(*Context, *stdhttp.Request)

func (f HandlerFunc) ServeHTTP(c *Context, r *stdhttp.Request) { f(c, r) }

type Response struct {
	StatusCode int
	Header     stdhttp.Header
	Body       []byte
	// Trailer is sent after the body. HTTP/1.1 carries it by sending the
	// body chunked, and announces its names in a Trailer header; HTTP/2 and
	// HTTP/3 send it as a trailing header block. An HTTP/1.0 client, and a
	// response that has no body, gets none of it.
	Trailer stdhttp.Header
	Close   bool
}

// Context is the connection a request arrived on, and the response to it.
// Respond with Respond or WriteResponse, or write the response piece by piece
// through the http.ResponseWriter methods Context has, rather than by
// sending on Conn: on an HTTP/2 or HTTP/3 connection the response is framed
// for its stream, and bytes sent on Conn directly would corrupt the
// connection.
//
// Since Context is an http.ResponseWriter, http.ServeFile, http.ServeContent
// and other net/http helpers can answer through it; see ReadFrom for how
// they send files.
type Context struct {
	Conn    *fib.Connection
	Request *stdhttp.Request
	wrote   bool
	// closing records that the response ends the HTTP/1 connection, so that
	// no request pipelined behind it is served.
	closing bool
	// w is the response being written through the ResponseWriter methods.
	w *responseWriter
	// stream is the HTTP/2 stream the request arrived on, or nil for HTTP/1.
	stream *h2ServerStream
	// external is the stream of a protocol served outside this package.
	external Stream

	// word is the response's and the Context's lifetime: the holds on each,
	// whether the response has been written or cancelled, and the generation
	// a recycled Context is serving; see ctxHolds. mu guards what hands a
	// callback between goroutines, and err, why the response was cancelled.
	// pooled records that the Context came from contextPool.
	word   atomic.Uint64
	mu     sync.Mutex
	pooled bool
	err    error
	// body is OnBody's callback and bodyDone that it has had its last call.
	// cancel is OnCancel's. deliverMu keeps body callbacks one at a time.
	body      BodyFunc
	bodyDone  bool
	cancel    func(error)
	deliverMu sync.Mutex
	// server and parser are the HTTP/1 connection this request arrived on,
	// which a release away from the worker needs to carry on with. They are
	// nil for HTTP/2, HTTP/3 and anything else multiplexed, where a response
	// holds up nothing else.
	server *ServerHandler
	parser *Parser
	// whole is the request's body when it arrived whole, whose buffer goes
	// back to the pool once the response is finished. block is where the
	// request was allocated, when the simple parser took it, and streamed
	// records that its body was still arriving when the handler ran.
	whole    *wholeBody
	block    *requestBlock
	streamed bool
}

// Stream answers a request that arrived over a protocol served outside this
// package, as HTTP/3 is by package http3, so that the same Handler serves
// it through the same Context.
type Stream interface {
	// WriteResponse sends the final response to req.
	WriteResponse(req *stdhttp.Request, response Response) error
	// WriteInterim sends an informational 1xx response, already checked to
	// be one.
	WriteInterim(status int, header stdhttp.Header) error
	// Push promises target to the client, as Context.Push does.
	Push(req *stdhttp.Request, target string, opts *stdhttp.PushOptions) error
}

// NewStreamContext returns the Context through which a handler answers req,
// which arrived on conn and is answered through stream.
func NewStreamContext(conn *fib.Connection, req *stdhttp.Request, stream Stream) *Context {
	return &Context{Conn: conn, Request: req, external: stream}
}

// StreamRequest is what serving one request that arrived over a protocol
// served outside this package takes from this package — the request, its
// URL, the Context that answers it, its body and its place on a StreamPool —
// for that protocol to allocate together with what it keeps of its own for
// the request, rather than each on its own. Nothing in it is reused: like a
// request and its Context, all of it lives for as long as anything refers to
// any of it. The zero value is ready to use, for one request.
type StreamRequest struct {
	Request stdhttp.Request
	URL     url.URL
	context Context
	body    wholeBody
	task    streamTask
}

// Context returns the Context through which a handler answers Request,
// which arrived on conn and is answered through stream, as NewStreamContext
// does. body is the request's body, which arrived whole before the handler
// runs: a buffer from package bufferpool, which the Context takes over as the
// request's Body and ContentLength. The handler reads it as it would any
// body, and the buffer goes back to the pool once the response is finished —
// the handler's return, or the last Release of a request it retained — as
// the body of a request the HTTP/1 server read whole does; a read after that
// reports ErrBodyReleased.
func (r *StreamRequest) Context(conn *fib.Connection, stream Stream, body []byte) *Context {
	c := &r.context
	c.Conn, c.Request, c.external = conn, &r.Request, stream
	if len(body) == 0 {
		bufferpool.Put(body)
		return c
	}
	r.body = wholeBody{data: body, pooled: true}
	r.Request.Body, r.Request.ContentLength = &r.body, int64(len(body))
	c.whole = &r.body
	return c
}

// ParseRequestTarget parses an origin-form request target into u, as
// url.ParseRequestURI does, but without allocating for the plain path and
// query nearly every request has: those are slices of target. It is for
// protocols served outside this package, as HTTP/3 is by package http3.
func ParseRequestTarget(u *url.URL, target string) error {
	if target == "" || !simpleTarget(target) {
		parsed, err := url.ParseRequestURI(target)
		if err != nil {
			return err
		}
		*u = *parsed
		return nil
	}
	*u = url.URL{}
	splitTarget(u, target)
	return nil
}

// RequestBody is the request's body when it is still arriving, or nil when
// the whole body was read before the handler ran. Reading Request.Body needs
// no such test: it is an io.ReadCloser either way.
func (c *Context) RequestBody() *BodyStream {
	stream, _ := c.Request.Body.(*BodyStream)
	return stream
}

func (c *Context) Respond(status int, contentType string, body []byte) error {
	if contentType == "" {
		return c.WriteResponse(Response{StatusCode: status, Body: body})
	}
	if c.isHTTP1() {
		// HTTP/1 writes the one field straight into the response's head,
		// without a header map to build and walk.
		if c.w != nil && c.w.status != 0 {
			return ErrResponseWritten
		}
		return c.writeHTTP1(Response{StatusCode: status, Body: body}, contentType)
	}
	// Every path a response takes — HTTP/1's marshalResponse, HTTP/2's
	// encoder, an external stream's — has read the header by the time it
	// returns, and this one is not the caller's to keep, so it goes back to
	// be the next reply's. That is a map and a value slice per response,
	// which for a handler that answers with Respond is most of what
	// answering allocates.
	header := contentTypeHeaders.Get().(stdhttp.Header)
	header["Content-Type"] = append(header["Content-Type"][:0], contentType)
	err := c.WriteResponse(Response{StatusCode: status, Header: header, Body: body})
	contentTypeHeaders.Put(header)
	return err
}

// contentTypeHeaders recycles the one-field headers Respond builds. A header
// in it holds Content-Type and nothing else, which is why it needs no
// clearing on the way in or out.
var contentTypeHeaders = sync.Pool{New: func() any {
	return stdhttp.Header{"Content-Type": make([]string, 0, 1)}
}}

// WriteResponse sends the whole response at once. It fails once the
// response has been begun through the ResponseWriter methods.
func (c *Context) WriteResponse(response Response) error {
	if c.w != nil && c.w.status != 0 {
		return ErrResponseWritten
	}
	return c.writeResponse(response)
}

func (c *Context) writeResponse(response Response) error {
	if c.wrote {
		return ErrResponseWritten
	}
	if response.StatusCode == 0 {
		response.StatusCode = stdhttp.StatusOK
	}
	if c.external != nil {
		if err := c.external.WriteResponse(c.Request, response); err != nil {
			return err
		}
		c.wrote = true
		return nil
	}
	if c.stream != nil {
		// HTTP/2 multiplexes the connection, so Close retires it gracefully
		// with GOAWAY rather than cutting the other streams short.
		if err := c.stream.respond(c.Request, response); err != nil {
			return err
		}
		c.wrote = true
		return nil
	}
	return c.writeHTTP1(response, "")
}

// writeHTTP1 is writeResponse for HTTP/1. contentType, when it is not empty,
// is sent as a Content-Type field besides those of response.Header, which
// then has none of its own.
func (c *Context) writeHTTP1(response Response, contentType string) error {
	if c.wrote {
		return ErrResponseWritten
	}
	if response.StatusCode == 0 {
		response.StatusCode = stdhttp.StatusOK
	}
	closeConnection := response.Close || c.Request.Close || headerHasToken(response.Header, "Connection", "close")
	if err := c.sendResponse(response, contentType, closeConnection); err != nil {
		return err
	}
	c.wrote = true
	if closeConnection {
		c.closing = true
		c.Conn.CloseAfterSend()
	}
	return nil
}

// Push promises target to the client ahead of its asking, as http.Pusher
// does: target is an absolute path, or an absolute URL with the request's
// scheme, and opts may set the method (GET or HEAD) and request headers.
// The promised request is served through the handler, on its own stream,
// before Push returns; call Push before responding, since a promise must
// arrive before the response that would lead the client to ask for it.
//
// Push returns http.ErrNotSupported on HTTP/1 and HTTP/3, on a request that
// was itself pushed, and when the client has disabled push, as browsers and
// Go's own client do.
func (c *Context) Push(target string, opts *stdhttp.PushOptions) error {
	if c.external != nil {
		return c.external.Push(c.Request, target, opts)
	}
	if c.stream == nil {
		return stdhttp.ErrNotSupported
	}
	return c.stream.push(c.Request, target, opts)
}

// WriteInterim sends an informational 1xx response ahead of the final one,
// such as 103 Early Hints with Link headers. It may be called several times
// before WriteResponse. 101 is not allowed, and an HTTP/1.0 client, which
// does not know interim responses, gets http.ErrNotSupported.
//
// A request that expects 100-continue gets its 100 Continue without asking:
// the body is read whole before the handler runs. A streamed body is the
// exception — it asks for it on its first read — so a handler that refuses
// such a request answers it without the upload ever starting.
func (c *Context) WriteInterim(status int, header stdhttp.Header) error {
	if status < 100 || status > 199 || status == stdhttp.StatusSwitchingProtocols {
		return fmt.Errorf("http: invalid interim status code %d", status)
	}
	if c.wrote || c.w != nil && c.w.status != 0 {
		return errors.New("http: interim response after the final one")
	}
	if c.external != nil {
		return c.external.WriteInterim(status, header)
	}
	if c.stream != nil {
		return c.stream.writeInterim(status, header)
	}
	if !c.Request.ProtoAtLeast(1, 1) {
		return stdhttp.ErrNotSupported
	}
	out := make([]byte, 0, 64)
	out = append(out, "HTTP/1.1 "...)
	out = strconv.AppendInt(out, int64(status), 10)
	out = append(out, ' ')
	out = append(out, stdhttp.StatusText(status)...)
	out = append(out, '\r', '\n')
	out, err := appendHeaderLines(out, header, nil)
	if err != nil {
		return err
	}
	return c.Conn.SendOwned(append(out, '\r', '\n'))
}

var _ stdhttp.Pusher = (*Context)(nil)

type ServerHandler struct {
	handler Handler
	config  Config
	// timed records that at least one read timeout is set, so a server
	// without any does no clock or timer work per read round.
	timed bool
	// streams runs the handlers of HTTP/2 requests away from the goroutine
	// that reads their connection, or is nil when they run on it.
	streams *StreamPool
}

func NewHandler(handler Handler) *ServerHandler {
	return NewHandlerWithConfig(DefaultConfig(), handler)
}

func NewHandlerWithConfig(config Config, handler Handler) *ServerHandler {
	if handler == nil {
		handler = HandlerFunc(func(c *Context, _ *stdhttp.Request) {
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		})
	}
	h := &ServerHandler{
		handler: handler,
		config:  config,
		timed:   config.ReadHeaderTimeout > 0 || config.ReadTimeout > 0 || config.IdleTimeout > 0,
	}
	if !config.DisableHTTP2 {
		h.streams = NewStreamPool(config.StreamPool)
	}
	return h
}

// headerTimeout bounds a header's arrival, and idleTimeout a kept-alive
// connection between requests. Both fall back to ReadTimeout when they are
// not set, as net/http.Server resolves them.
func (h *ServerHandler) headerTimeout() time.Duration {
	if h.config.ReadHeaderTimeout > 0 {
		return h.config.ReadHeaderTimeout
	}
	return h.config.ReadTimeout
}

func (h *ServerHandler) idleTimeout() time.Duration {
	if h.config.IdleTimeout > 0 {
		return h.config.IdleTimeout
	}
	return h.config.ReadTimeout
}

// armRead sets the read deadline that matches what the connection is waiting
// for. A connection whose request has all arrived is left without one, so a
// handler is never cut off by the timeout that bounded its request's arrival,
// and a connection this parser has finished with — one that is closing, or
// that has become HTTP/2 — is left alone entirely. Callers hold parser.mu.
func (h *ServerHandler) armRead(c *fib.Connection, parser *Parser) {
	if !h.timed || parser.spent {
		return
	}
	var at time.Time
	switch state := parser.waitState(); state {
	case waitIdle:
		if d := h.idleTimeout(); d > 0 {
			at = time.Now().Add(d)
		}
	case waitHeader, waitBody:
		if parser.requestStart.IsZero() {
			parser.requestStart = time.Now()
		}
		d := h.config.ReadTimeout
		if state == waitHeader {
			d = h.headerTimeout()
		}
		if d > 0 {
			at = parser.requestStart.Add(d)
		}
	}
	_ = c.SetReadDeadline(at)
}

// releaseTimeouts hands the connection over to another protocol: this parser
// stops timing it, and the deadline it had goes with it.
func (h *ServerHandler) releaseTimeouts(c *fib.Connection, parser *Parser) {
	parser.spent = true
	if h.timed {
		_ = c.SetReadDeadline(time.Time{})
	}
}

func (h *ServerHandler) OnOpen(c *fib.Connection) {
	parser := h.newParser(c)
	c.SetAttachment(parser)
	// A connection that opens and then says nothing is idle, and bounded like
	// any other idle connection.
	parser.mu.Lock()
	h.armRead(c, parser)
	parser.mu.Unlock()
}

// newParser starts a connection's parser, recording the peer's address so
// that each request carries it in RemoteAddr as net/http's do.
func (h *ServerHandler) newParser(c *fib.Connection) *Parser {
	parser := NewParser(h.config)
	parser.conn = c
	if addr := c.RemoteAddr(); addr != nil {
		parser.remoteAddr = addr.String()
	}
	return parser
}

func (h *ServerHandler) OnData(c *fib.Connection, data []byte) {
	var parser *Parser
	switch state := c.Attachment().(type) {
	case *h2ServerConn:
		state.feed(data)
		return
	case *Parser:
		parser = state
	default:
		parser = h.newParser(c)
		c.SetAttachment(parser)
	}
	h.drive(c, parser, data)
}

// drive turns whatever the connection has into requests and serves them,
// handlers and all: nothing a handler does here waits on the peer, so the
// connection's worker carries a request from its first byte to its response
// without another goroutine. It also runs on whichever goroutine released the
// last hold on a retained request, and holds the parser's lock throughout, so
// a connection is never parsed by two goroutines at once and a request
// pipelined behind a retained one waits its turn.
func (h *ServerHandler) drive(c *fib.Connection, parser *Parser, data []byte) {
	parser.mu.Lock()
	defer parser.mu.Unlock()
	if parser.spent {
		// The connection is ending, or has become HTTP/2; what is still
		// arriving belongs to a message this parser will not answer.
		return
	}
	// Whatever this round leaves the connection waiting for decides which
	// timeout bounds it.
	defer h.armRead(c, parser)
	// The requests are parsed straight out of data, which is the
	// connection's until the round ends; whatever is left of it is copied
	// before then. Deferred after armRead so that it runs first, and the
	// timeout is chosen by what the parser really holds.
	defer parser.own()
	if !parser.sniffed {
		if h.config.DisableHTTP2 {
			parser.sniffed = true
		} else if data = h.sniff(c, parser, data); data == nil {
			return
		}
	}
	// One request at a time, since one may switch the connection to HTTP/2
	// and leave what follows it to the new protocol.
	var err error
	for ; ; data = nil {
		if parser.stream != nil {
			// A streamed body owns the connection until its last byte: the
			// bytes arriving now are the body, not the next request.
			var done, resumed bool
			done, err = parser.pumpBody(data)
			if context := parser.liveContext.Load(); context != nil && context.claimResume() {
				// A body callback answered and released the request from
				// inside this round. It could not carry the connection on
				// while this goroutine held the parser, so that is done here.
				if h.endRequestLocked(context) {
					c.CloseAfterSend()
					return
				}
				resumed = true
			}
			if err != nil {
				h.failStream(c, parser, err)
				return
			}
			if !done && !resumed {
				return
			}
			continue
		}
		if parser.busy {
			// A request is still being answered: one whose handler retained
			// it. Keep what arrives; the release that writes its response
			// drives the connection on from here.
			parser.appendBuffer(data)
			return
		}
		var request *stdhttp.Request
		var complete bool
		request, complete, err = parser.feedBorrowed(data)
		if err != nil || !complete {
			break
		}
		request.RemoteAddr = parser.remoteAddr
		request.TLS = h.tlsState(c, parser)
		if status := checkRequest(request); status != 0 {
			err = requestError(status)
			break
		}
		if settings, ok := h.h2cUpgrade(c, request); ok && parser.stream == nil {
			h.upgradeH2C(c, parser, request, settings)
			return
		}
		block := parser.block
		if block != nil && &block.request != request {
			block = nil
		}
		parser.block = nil
		context := h.newContext(block)
		context.Conn, context.Request, context.server, context.parser = c, request, h, parser
		context.whole, _ = request.Body.(*wholeBody)
		context.block, context.streamed = block, parser.stream != nil
		// Nothing behind this request is served until its response is
		// written, so that pipelined responses keep their order, and the
		// connection knows which request to cancel if it closes first.
		context.begin(true)
		parser.busy = true
		parser.liveContext.Store(context)
		var bodyErr error
		if parser.stream != nil {
			// Hand the body what has already arrived before the handler runs,
			// so that its first read is of everything the connection has and
			// a body that came in with its header reads straight to io.EOF.
			_, bodyErr = parser.pumpBody(nil)
		}
		if h.timed && parser.stream == nil {
			// The request has all arrived, so nothing is waiting on the peer
			// any more: the handler's time is its own, and the deadline for
			// whatever comes next is set when this round ends. A request
			// whose body is still coming keeps the deadline that bounds it.
			_ = c.SetReadDeadline(time.Time{})
		}
		// The handler runs here, on the connection's worker, whether or not
		// its body has all arrived: reading a streamed body never waits, so
		// there is nothing for a goroutine of its own to wait on.
		serveRequest(h.handler, context)
		if bodyErr != nil {
			// The body was refused or misframed before the handler even saw
			// it, which its own read told it; the connection cannot go on.
			h.failStream(c, parser, bodyErr)
			return
		}
		if !context.settle() {
			// The handler kept the response open. If its body is still
			// arriving, feeding it is this loop's job; otherwise the release
			// that writes the response carries the connection on.
			if parser.stream == nil {
				return
			}
			continue
		}
		if h.endRequestLocked(context) {
			c.CloseAfterSend()
			return
		}
		// A body the handler did not take is discarded by the loop, which
		// goes back to parsing once it has run out.
		continue
	}
	if parser.wantContinue {
		// The request waits for permission to send its body; the body is
		// read whole before the handler runs, so permission is given at once.
		// A streamed body asks for it on its first read instead.
		parser.wantContinue = false
		_ = c.Send([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
	}
	if err != nil {
		parser.spent = true
		h.refuse(c, err)
	}
}

// endRequestLocked settles the connection once a request's response has been
// written: whatever the handler left of the body is retired, and the answer
// is whether the connection has to close rather than serve anything more.
// Callers hold parser.mu.
func (h *ServerHandler) endRequestLocked(context *Context) bool {
	parser := context.parser
	parser.busy = false
	// Whatever the handler left of the body has to come off the connection
	// before the next request on it can be parsed, and abandon says when
	// there is too much of it left to be worth reading and dropping.
	tooMuchLeft := false
	if stream := context.RequestBody(); stream != nil {
		tooMuchLeft = stream.abandon()
	}
	closing := tooMuchLeft || context.closing || context.Request.Close
	if closing {
		parser.spent = true
		parser.stream = nil
		parser.live.Store(nil)
	}
	// The connection is done with the request. Its body and whatever the
	// Reuse options recycle go back once the handler is done with them too,
	// which may be already or may be at a Release still to come.
	context.drop(ctxConn)
	return closing
}

// newContext returns the Context a request is served through: the one in
// the request's block when the two are recycled together or not at all, one
// from contextPool when only Contexts are recycled, and a fresh one when only
// requests are, since a handler may then keep the Context past the request.
func (h *ServerHandler) newContext(block *requestBlock) *Context {
	reuse := h.config.ReuseContexts
	var context *Context
	switch {
	case block != nil && block.pooled == reuse:
		context = &block.context
	case reuse:
		context = contextPool.Get().(*Context)
	default:
		return new(Context)
	}
	if reuse {
		context.reopen()
	}
	return context
}

// recycle gives back what serving a request took from the reuse pools; see
// Config.ReuseRequests. It runs once nothing but the server can reach the
// Context any more — the handler has returned and released every Retain, and
// the connection has done with the request — and nothing touches context
// afterwards.
func (h *ServerHandler) recycle(context *Context) {
	if context.streamed {
		// A streamed body can still be finishing after the response has
		// gone, and it writes the request's trailer and calls back into the
		// Context when it does. Such a request and its Context are left to
		// the collector.
		return
	}
	if block := context.block; block != nil {
		// The block takes with it the Context it holds, when that is the
		// one the request was served through.
		block.recycle()
	}
	if context.pooled {
		context.recycle()
		contextPool.Put(context)
	}
}

// finishRequest carries the connection on after a response written away from
// its worker: the streamed request's own goroutine, or whichever goroutine
// released the last hold on a retained one.
func (h *ServerHandler) finishRequest(context *Context) {
	c, parser := context.Conn, context.parser
	parser.mu.Lock()
	closing := h.endRequestLocked(context)
	parser.mu.Unlock()
	if closing {
		c.CloseAfterSend()
		return
	}
	h.drive(c, parser, nil)
}

// failStream ends a connection whose streamed body could not be framed or grew
// past what the server accepts. The handler has the request already, so the
// failure reaches it through the body rather than as a status.
func (h *ServerHandler) failStream(c *fib.Connection, parser *Parser, err error) {
	parser.spent = true
	if errors.Is(err, ErrMalformed) || errors.Is(err, ErrHeaderTooLarge) || errors.Is(err, ErrBodyTooLarge) {
		// The handler may still be writing a response to a request it was
		// given; let what it has sent go out before the connection ends.
		c.CloseAfterSend()
		return
	}
	c.CloseWithError(err)
}

// refuse answers a request the server could not parse or would not serve, and
// ends the connection.
func (h *ServerHandler) refuse(c *fib.Connection, err error) {
	status := stdhttp.StatusBadRequest
	var statusErr requestError
	switch {
	case errors.As(err, &statusErr):
		status = int(statusErr)
	case errors.Is(err, ErrHeaderTooLarge):
		status = stdhttp.StatusRequestHeaderFieldsTooLarge
	case errors.Is(err, ErrBodyTooLarge):
		status = stdhttp.StatusRequestEntityTooLarge
	case errors.Is(err, ErrUnsupportedTransferEncoding):
		status = stdhttp.StatusNotImplemented
	}
	request := &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Header: make(stdhttp.Header)}
	context := &Context{Conn: c, Request: request}
	_ = context.WriteResponse(Response{
		StatusCode: status,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte(stdhttp.StatusText(status) + "\n"),
		Close:      true,
	})
}

// Serve runs handler for the request context carries and, once it returns,
// writes the response back: the response the handler began through the
// ResponseWriter methods is ended, as net/http ends one when a handler
// returns, and handed to the connection. A handler that retained the request
// keeps it open instead, and the release that gives back its last hold writes
// it back the same way.
//
// The HTTP/1 and HTTP/2 servers here dispatch through it. A protocol served
// outside this package, as HTTP/3 is by package http3, calls it in place of
// calling the handler itself, so that Retain, Release and OnBody work there
// too.
func Serve(handler Handler, context *Context) { serveRequest(handler, context) }

// serveRequest runs the handler for the request context carries, and gives
// back the hold that serving it took, which writes the response unless the
// handler retained it.
func serveRequest(handler Handler, context *Context) {
	context.begin(false)
	handler.ServeHTTP(context, context.Request)
	context.returned()
}

// requestError is a request the server refuses with its status.
type requestError int

func (e requestError) Error() string { return "http: " + stdhttp.StatusText(int(e)) }

// checkRequest returns the status to refuse a request that parsed but that
// the server cannot serve with, or 0. An HTTP/1.1 request has to name its
// host exactly once (RFC 9112 section 3.2), and an expectation other than
// 100-continue cannot be met (RFC 9110 section 10.1.1).
func checkRequest(request *stdhttp.Request) int {
	if request.ProtoAtLeast(1, 1) && request.Method != stdhttp.MethodConnect {
		if hosts, ok := request.Header["Host"]; !ok && request.Host == "" || len(hosts) > 1 {
			return stdhttp.StatusBadRequest
		}
	}
	if expect, ok := request.Header["Expect"]; ok {
		if len(expect) != 1 || !strings.EqualFold(strings.TrimSpace(expect[0]), "100-continue") {
			return stdhttp.StatusExpectationFailed
		}
	}
	return 0
}

// sniff tells HTTP/2 from HTTP/1 by whether the connection opens with the
// HTTP/2 preface. It returns the bytes to parse as HTTP/1, or nil when they
// are HTTP/2 or too few to tell yet.
//
// A connection whose ALPN chose "h2", and any connection at all when
// Config.HTTP2Only is set, is HTTP/2 whatever it sends: what does not start
// with the preface is a connection error rather than an HTTP/1 request.
func (h *ServerHandler) sniff(c *fib.Connection, parser *Parser, data []byte) []byte {
	parser.appendBuffer(data)
	state := h.tlsState(c, parser)
	if h.config.HTTP2Only || state != nil && state.NegotiatedProtocol == "h2" {
		h.startH2(c, parser, state)
		return nil
	}
	n := min(len(parser.buffer), len(h2Preface))
	if string(parser.buffer[:n]) == h2Preface[:n] {
		if n < len(h2Preface) {
			return nil
		}
		h.startH2(c, parser, state)
		return nil
	}
	parser.sniffed = true
	return parser.TakeBuffered()
}

// startH2 hands the connection, and whatever it has sent so far, to HTTP/2.
func (h *ServerHandler) startH2(c *fib.Connection, parser *Parser, state *stdtls.ConnectionState) {
	sc := newH2ServerConn(h, c, parser.remoteAddr)
	sc.tlsState = state
	h.releaseTimeouts(c, parser)
	c.SetAttachment(sc)
	sc.start()
	// feed copies what it is given into the connection's own buffer, so the
	// parser's goes back to the pool rather than to the collector.
	buffered := parser.TakeBuffered()
	sc.feed(buffered)
	bufferpool.Put(buffered)
}

// tlsState is the connection's TLS state, looked up once the handshake has
// delivered the first bytes, or nil for a connection without TLS.
func (h *ServerHandler) tlsState(c *fib.Connection, parser *Parser) *stdtls.ConnectionState {
	if !parser.tlsChecked {
		parser.tlsChecked = true
		if state, ok := fibtls.ConnectionState(c); ok {
			parser.tls = &state
		}
	}
	return parser.tls
}

func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h *ServerHandler) OnClose(c *fib.Connection, err error) {
	switch state := c.Attachment().(type) {
	case *h2ServerConn:
		state.shutdown()
	case *Parser:
		// A handler waiting on the rest of a body has to hear that the rest
		// will never come, rather than take a truncated upload for a complete
		// one, and one still working on a request it retained has to hear
		// that there is nothing left to answer. Both are reported from a
		// goroutine, since this runs on the event loop, which a handler's
		// cleanup must never hold up; the parser's own lock is not taken
		// there either, for the same reason.
		stream, context := state.live.Load(), state.liveContext.Load()
		var gen uint64
		if context != nil {
			// A recycled Context may be serving another request by the
			// time the cancellation reaches it, and the generation is how
			// it tells. It is read while the Context is still this
			// connection's, which the second load confirms.
			gen = context.generation()
			if state.liveContext.Load() != context {
				context = nil
			}
		}
		if stream != nil || context != nil {
			go func() {
				if stream != nil {
					stream.fail(err)
				}
				if context != nil {
					context.cancelWith(err, gen)
				}
			}()
		}
	}
	c.SetAttachment(nil)
}

// sendResponse writes a whole HTTP/1 response to the connection.
//
// The head is built in a pooled buffer and goes to the connection with the
// body through SendParts, which copies only what the socket does not take at
// once. A response written from inside a read round, which is where nearly
// every one is written, is corked, so both parts are copied into the round's
// send buffer behind the responses before it and reach the socket with them
// in one write: answering a request allocates nothing for its response, and
// the round's replies share one buffer that goes back to the pool once they
// are sent, rather than each holding an array of its own until then.
//
// A chunked response, which carries trailers, is rare enough to be built
// whole instead.
func (c *Context) sendResponse(response Response, contentType string, closeConnection bool) error {
	framing, body, trailer := responseFraming(c.Request, response, closeConnection)
	framing.contentType = contentType
	if framing.chunked {
		data, err := marshalChunked(c.Request, framing, body, trailer)
		if err != nil {
			return err
		}
		return c.Conn.SendOwned(data)
	}
	pooled := bufferpool.Get(headCapacity(response.Header) + len(contentType))
	head, err := appendResponseHead(pooled[:0], c.Request, framing)
	if err == nil {
		err = c.Conn.SendParts(head, body)
		pooled = head
	}
	bufferpool.Put(pooled)
	return err
}

// marshalResponse builds a whole response into one array, which it hands to
// the caller to give to the connection.
func marshalResponse(request *stdhttp.Request, response Response, closeConnection bool) ([]byte, error) {
	framing, body, trailer := responseFraming(request, response, closeConnection)
	if framing.chunked {
		return marshalChunked(request, framing, body, trailer)
	}
	out, err := appendResponseHead(make([]byte, 0, len(body)+headCapacity(response.Header)), request, framing)
	if err != nil {
		return nil, err
	}
	return append(out, body...), nil
}

// headCapacity is room enough for the head of a response with header in
// nearly every case: its fields, and the status line, Date and framing
// fields the server adds.
func headCapacity(header stdhttp.Header) int {
	capacity := 128
	for key, values := range header {
		capacity += len(key) + 4
		for _, value := range values {
			capacity += len(value) + 2
		}
	}
	return capacity
}

// marshalChunked builds a chunked response, which ends with trailer.
func marshalChunked(request *stdhttp.Request, framing responseHead, body []byte, trailer stdhttp.Header) ([]byte, error) {
	out, err := appendResponseHead(make([]byte, 0, len(body)+headCapacity(framing.header)+32), request, framing)
	if err != nil {
		return nil, err
	}
	if len(body) > 0 {
		out = appendChunk(out, body)
	}
	out = append(out, "0\r\n"...)
	if out, err = appendHeaderLines(out, trailer, nil); err != nil {
		return nil, err
	}
	return append(out, crlf...), nil
}

// responseFraming works out how an HTTP/1 response to request is framed,
// which part of its body is sent, and for a chunked one, its trailer.
func responseFraming(request *stdhttp.Request, response Response, closeConnection bool) (framing responseHead, body []byte, trailer stdhttp.Header) {
	status := response.StatusCode
	framing = responseHead{status: status, header: response.Header, contentLength: -1, close: closeConnection}
	hasBody := statusHasBody(status)
	isHead := request.Method == stdhttp.MethodHead
	switch {
	case status == stdhttp.StatusNotModified:
		// A 304 may repeat the length of what it stands for, and only a
		// length the handler gives knows that (RFC 9110 section 8.6).
		framing.contentLength = declaredLength(response.Header)
	case !hasBody:
		// 1xx and 204 never carry Content-Length.
	case isHead:
		framing.contentLength = declaredLength(response.Header)
		if framing.contentLength < 0 {
			framing.contentLength = int64(len(response.Body))
		}
	case len(response.Trailer) > 0 && request.ProtoAtLeast(1, 1):
		framing.chunked = true
		trailer = make(stdhttp.Header, len(response.Trailer))
		for key, values := range response.Trailer {
			if !forbiddenTrailer(key) {
				key = stdhttp.CanonicalHeaderKey(key)
				trailer[key] = values
				framing.trailers = append(framing.trailers, key)
			}
		}
		sort.Strings(framing.trailers)
	default:
		framing.contentLength = int64(len(response.Body))
	}
	if isHead || !hasBody {
		return framing, nil, trailer
	}
	return framing, response.Body, trailer
}

// declaredLength is the Content-Length header holds, or -1.
func declaredLength(header stdhttp.Header) int64 {
	if values := header["Content-Length"]; len(values) == 1 {
		if n, err := strconv.ParseInt(strings.TrimSpace(values[0]), 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return -1
}

// appendHeaderLines appends header as HTTP/1 lines in key order, leaving out
// the keys skip reports.
func appendHeaderLines(out []byte, header stdhttp.Header, skip func(string) bool) ([]byte, error) {
	// Room on the stack for the keys of any ordinary header.
	var stack [16]string
	keys := stack[:0]
	for key := range header {
		if skip == nil || !skip(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" || textproto.CanonicalMIMEHeaderKey(key) == "" {
			return nil, errors.New("http: invalid response header name")
		}
		for _, value := range header[key] {
			if !validHeaderValue(value) {
				return nil, errors.New("http: invalid response header value")
			}
			out = append(out, key...)
			out = append(out, ':', ' ')
			out = append(out, value...)
			out = append(out, '\r', '\n')
		}
	}
	return out, nil
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if (value[i] < ' ' && value[i] != '\t') || value[i] == 0x7f {
			return false
		}
	}
	return true
}

var _ fib.Handler = (*ServerHandler)(nil)
