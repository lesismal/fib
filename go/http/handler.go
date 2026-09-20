//go:build linux || darwin || windows

package http

import (
	stdtls "crypto/tls"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"sort"
	"strconv"
	"strings"
	"time"

	fib "github.com/lesismal/fib/go"
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

// RequestBody is the request's body when it is still arriving, or nil when
// the whole body was read before the handler ran. Reading Request.Body needs
// no such test: it is an io.ReadCloser either way.
func (c *Context) RequestBody() *BodyStream {
	stream, _ := c.Request.Body.(*BodyStream)
	return stream
}

func (c *Context) Respond(status int, contentType string, body []byte) error {
	header := make(stdhttp.Header)
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return c.WriteResponse(Response{StatusCode: status, Header: header, Body: body})
}

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
	closeConnection := response.Close || c.Request.Close || headerHasToken(response.Header, "Connection", "close")
	data, err := marshalResponse(c.Request, response, closeConnection)
	if err != nil {
		return err
	}
	if err = c.Conn.SendOwned(data); err != nil {
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
	return &ServerHandler{
		handler: handler,
		config:  config,
		timed:   config.ReadHeaderTimeout > 0 || config.ReadTimeout > 0 || config.IdleTimeout > 0,
	}
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

// drive turns whatever the connection has into requests and serves them. It
// runs on the connection's worker for data that has just been read, and on the
// goroutine of a streamed request once its handler returns, so it holds the
// parser's lock throughout: a connection is never parsed by two goroutines at
// once, and a request pipelined behind a streamed one waits its turn.
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
			var done bool
			if done, err = parser.pumpBody(data); err != nil {
				h.failStream(c, parser, err)
				return
			} else if !done {
				return
			}
			continue
		}
		if parser.busy {
			// The handler of a streamed request has not returned. Keep what
			// arrives; its return drives the connection on from here.
			parser.buffer = append(parser.buffer, data...)
			return
		}
		var request *stdhttp.Request
		var complete bool
		request, complete, err = parser.FeedOne(data)
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
		context := &Context{Conn: c, Request: request}
		if parser.stream != nil {
			// The body has not all arrived. Serve the request on its own
			// goroutine, since reading the body blocks, and go on feeding
			// that body from here.
			parser.busy = true
			go h.serveStreamed(c, parser, context, parser.stream)
			continue
		}
		if h.timed {
			// The request has all arrived, so nothing is waiting on the peer
			// any more: the handler's time is its own, and the deadline for
			// whatever comes next is set when this round ends.
			_ = c.SetReadDeadline(time.Time{})
		}
		serveRequest(h.handler, context)
		if request.Close || context.closing {
			parser.spent = true
			return
		}
		if len(parser.buffer) == 0 {
			break
		}
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

// serveStreamed runs the handler of a request whose body is still arriving,
// and drives the connection on once it returns.
func (h *ServerHandler) serveStreamed(c *fib.Connection, parser *Parser, context *Context, stream *BodyStream) {
	recovered := runStreamed(h.handler, context)
	// Whatever the handler left of the body has to come off the connection
	// before the next request on it can be parsed, and abandon says when
	// there is too much of it left to be worth reading and dropping. It runs
	// before whatever else decides the connection's fate, so that a body the
	// handler walked away from is always retired.
	tooMuchLeft := stream.abandon()
	closing := recovered != nil || tooMuchLeft || context.closing || context.Request.Close
	parser.mu.Lock()
	parser.busy = false
	if closing {
		parser.spent = true
		parser.stream = nil
		parser.live.Store(nil)
	}
	parser.mu.Unlock()
	switch {
	case recovered != nil:
		c.CloseWithError(fmt.Errorf("handler panic: %v", recovered))
	case closing:
		c.CloseAfterSend()
	default:
		_ = c.Flush()
		h.drive(c, parser, nil)
	}
}

// runStreamed serves one request and returns what its handler panicked with,
// if anything. A panic on this goroutine would take the process with it, where
// one on a worker reaches the task pool that isolates it, so it is caught here
// and ends the connection the same way a worker's does.
func runStreamed(handler Handler, context *Context) (recovered any) {
	defer func() { recovered = recover() }()
	serveRequest(handler, context)
	return nil
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

// serveRequest runs the handler for the request context carries, and then ends the
// response the handler wrote through the ResponseWriter methods, if it began
// one, as net/http does when a handler returns.
func serveRequest(handler Handler, context *Context) {
	handler.ServeHTTP(context, context.Request)
	_ = context.Finish()
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
	parser.buffer = append(parser.buffer, data...)
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
	sc.feed(parser.TakeBuffered())
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
		if stream := state.live.Load(); stream != nil {
			// A handler waiting on the rest of a body has to hear that the
			// rest will never come, rather than take a truncated upload for a
			// complete one. The parser's own lock is not taken: this runs on
			// the event-loop goroutine, which a handler must never block.
			stream.fail(err)
		}
	}
	c.SetAttachment(nil)
}

func marshalResponse(request *stdhttp.Request, response Response, closeConnection bool) ([]byte, error) {
	status := response.StatusCode
	head := responseHead{status: status, header: response.Header, contentLength: -1, close: closeConnection}
	hasBody := statusHasBody(status)
	isHead := request.Method == stdhttp.MethodHead
	var trailer stdhttp.Header
	switch {
	case status == stdhttp.StatusNotModified:
		// A 304 may repeat the length of what it stands for, and only a
		// length the handler gives knows that (RFC 9110 section 8.6).
		head.contentLength = declaredLength(response.Header)
	case !hasBody:
		// 1xx and 204 never carry Content-Length.
	case isHead:
		head.contentLength = declaredLength(response.Header)
		if head.contentLength < 0 {
			head.contentLength = int64(len(response.Body))
		}
	case len(response.Trailer) > 0 && request.ProtoAtLeast(1, 1):
		head.chunked = true
		trailer = make(stdhttp.Header, len(response.Trailer))
		for key, values := range response.Trailer {
			if !forbiddenTrailer(key) {
				key = stdhttp.CanonicalHeaderKey(key)
				trailer[key] = values
				head.trailers = append(head.trailers, key)
			}
		}
		sort.Strings(head.trailers)
	default:
		head.contentLength = int64(len(response.Body))
	}
	capacity := len(response.Body) + 128
	for key, values := range response.Header {
		capacity += len(key) + 4
		for _, value := range values {
			capacity += len(value) + 2
		}
	}
	out, err := appendResponseHead(make([]byte, 0, capacity), request, head)
	if err != nil {
		return nil, err
	}
	if isHead || !hasBody {
		return out, nil
	}
	if !head.chunked {
		return append(out, response.Body...), nil
	}
	if len(response.Body) > 0 {
		out = appendChunk(out, response.Body)
	}
	out = append(out, "0\r\n"...)
	if out, err = appendHeaderLines(out, trailer, nil); err != nil {
		return nil, err
	}
	return append(out, crlf...), nil
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
	keys := make([]string, 0, len(header))
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
