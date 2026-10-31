//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	fibtls "github.com/lesismal/fib/tls"
)

type Handler interface {
	ServeHTTP(*Context)
}

type HandlerFunc func(*Context)

func (f HandlerFunc) ServeHTTP(c *Context) { f(c) }

type Response struct {
	StatusCode int
	Header     stdhttp.Header
	// Body is the response body. The response may keep the slice itself until
	// it is finished: sent on HTTP/1 and HTTP/2, acknowledged by the peer on
	// HTTP/3, which can be after WriteResponse or Respond returns. Whatever
	// its size, it must not be changed until then.
	//
	// RespondCopy and WriteResponseCopy are the calls that copy it, at the
	// call: a handler that wants its buffer back as soon as the call returns,
	// to fill again or to change, writes the response with one of them, and
	// the copy is what the response sends. Nothing else copies a body, except
	// a protocol's copy of the request's own buffer, which the server
	// recycles: a body that is Context.Body, or a part of it, is copied by
	// every protocol's send path, HTTP/1's, HTTP/2's and HTTP/3's alike, so
	// that echoing the request body is safe however long the response takes.
	Body []byte
	// Trailer is sent after the body. HTTP/1.1 carries it by sending the
	// body chunked, and announces its names in a Trailer header; HTTP/2 and
	// HTTP/3 send it as a trailing header block. An HTTP/1.0 client, and a
	// response that has no body, gets none of it. Like Body, it is kept until
	// the response is finished, so it must not be changed until then;
	// WriteResponseCopy copies it, map and values, at the call, as it does
	// the body. Unlike Body, it has no request-buffer exception to allow for:
	// a header's values are strings, so a trailer filled from the request's
	// header, or from Request.Trailer, holds no byte of the buffer the server
	// recycles.
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
	// ends holds endClosing, that the response ends the HTTP/1 connection,
	// so that no request pipelined behind it is served, and endLingers, that
	// the close is to linger for input still arriving; see
	// closeAfterResponse. They share a byte so that status fits the word.
	ends uint8
	// pooled records that the Context came from contextPool, and streamed
	// that its request's body was still arriving when the handler ran.
	// bodyDone records that OnBody's callback has had its last call, and
	// bodyHeld that OnBody holds the response open until then. The flags sit
	// together so that they share a word.
	pooled   bool
	streamed bool
	bodyDone bool
	bodyHeld bool
	// status is the status of the response written whole or the tunnel
	// opened, for Status; a response written through the ResponseWriter
	// methods keeps its own in w. It sits with the flags, in their word.
	status uint16
	// w is the response being written through the ResponseWriter methods,
	// or holding the hooks registered on it. Once a request has needed one,
	// the Context keeps it, cleared, for the requests it serves after,
	// rather than going to writerPool for each: an access log's OnFinish,
	// on every request, needs one. A cleared writer reads as unused: no
	// status, no hooks, nothing committed.
	w *responseWriter
	// stream is the HTTP/2 stream the request arrived on, or nil for HTTP/1.
	stream *h2ServerStream
	// external is the stream of a protocol served outside this package.
	external Stream

	// word is the response's and the Context's lifetime: the holds on each,
	// whether the response has been written or cancelled, and the generation
	// a recycled Context is serving; see ctxHolds. mu guards what hands a
	// callback between goroutines, and err, why the response was cancelled.
	word atomic.Uint64
	mu   sync.Mutex
	err  error
	// body is OnBody's callback. handover is the body OnBody was asked for
	// while the handler ran, owed once it returns; see later. cancel holds
	// OnCancel's, what Context handed out and SetValue's value; it is set
	// under mu, and loaded without it by SetValue and Value. deliverMu keeps
	// body callbacks one at a time.
	body      BodyFunc
	handover  func(worker bool)
	cancel    atomic.Pointer[cancelHooks]
	deliverMu sync.Mutex
	// server and parser are the HTTP/1 connection this request arrived on,
	// which a release away from the worker needs to carry on with. They are
	// nil for HTTP/2, HTTP/3 and anything else multiplexed, where a response
	// holds up nothing else.
	server *ServerHandler
	parser *Parser
	// whole is the request's body when it arrived whole, whose buffer goes
	// back to the pool once the response is finished. block is where the
	// request was allocated, when the simple parser took it.
	whole *wholeBody
	block *requestBlock
	// route is the route a Router served the request by, and the values its
	// parameters matched; see Param.
	route *routeState
}

// The bits of Context.ends.
const (
	endClosing uint8 = 1 << iota
	endLingers
)

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

// ResponseStreamer is implemented by a Stream that can send a response as
// its handler writes it through Context's ResponseWriter methods, rather than
// whole once the handler is done: a response that outgrows what those
// methods hold back, or is flushed, begins with BeginResponse, its body goes
// through WriteBody, and EndResponse ends it. A Stream that does not
// implement it has every such response held and sent with WriteResponse.
// Its methods are called one at a time, from the goroutines the handler
// writes on.
type ResponseStreamer interface {
	// BeginResponse sends req's final status and header, leaving the stream
	// open for the body. A Content-Length in header is the length the body
	// is declared to have, which the body written is held to.
	BeginResponse(req *stdhttp.Request, status int, header stdhttp.Header) error
	// WriteBody sends p, which the caller may reuse once it returns. With
	// wait set it may wait for the peer to take what was sent before, so
	// that a handler that writes faster than its client reads is held back
	// rather than buffered without bound. wait is false for a handler the
	// stream pool runs on the goroutine that reads the connection, which
	// would be waiting for itself.
	WriteBody(p []byte, wait bool) error
	// EndResponse ends the response, sending trailer after the body if it
	// holds anything the protocol can send. With complete false the body is
	// shorter than declared, and the stream is reset rather than ended.
	EndResponse(trailer stdhttp.Header, complete bool) error
}

// ResponseNotifier is a ResponseStreamer that can also be written to
// without waiting, for Context.WriteNoWait and Context.OnWritable.
type ResponseNotifier interface {
	ResponseStreamer
	// WriteBodyNoWait sends p as WriteBody does without wait, keeping what
	// the stream cannot send yet, and reports whether more than the
	// stream's limit is now held back.
	WriteBodyNoWait(p []byte) (above bool, err error)
	// NotifyWritable arranges for fn to run, once, on another goroutine,
	// when what the stream holds back falls to half its limit, or with an
	// error when the stream ends first, and reports true; it reports false,
	// and never runs fn, when the stream holds no more than half already.
	NotifyWritable(fn func(error)) bool
}

// OwnedResponseWriter is implemented by a Stream that can take over the body
// of a response written with Context.RespondCopy or Context.WriteResponseCopy:
// a buffer from package bufferpool, which the stream returns to the pool once
// it is done with it, rather than the caller's buffer, which the handler may
// reuse as soon as the call returns.
//
// A Stream that does not implement it is written to with a copy of the body
// that the collector frees instead, so every Stream serves the *Copy methods;
// this is what makes them cost no allocation where the stream can take a
// pooled buffer, as HTTP/3 does with a quic.Part{Owned: true}.
type OwnedResponseWriter interface {
	// WriteResponseOwned sends the final response to req, as WriteResponse
	// does, for a body that is response.Body, a buffer from package
	// bufferpool. The stream takes the buffer over, and returns it to the
	// pool once it is done with it: sent, acknowledged, reset, failed, or
	// never handed to the connection at all. It must not read the buffer
	// after that.
	WriteResponseOwned(req *stdhttp.Request, response Response, body []byte) error
}

// NewStreamContext returns the Context through which a handler answers req,
// which arrived on conn and is answered through stream. conn may be nil, for
// a stream that is not carried by a fib connection, such as one a test
// records responses with: the Context then never touches a connection, and a
// handler that retains the request and answers it later, from another
// goroutine, finishes it through stream alone.
func NewStreamContext(conn *fib.Connection, req *stdhttp.Request, stream Stream) *Context {
	return &Context{Conn: conn, Request: req, external: stream}
}

// StreamRequest is what serving one request that arrived over a protocol
// served outside this package takes from this package — the request, its
// URL, the Context that answers it, its body and its place on a StreamPool —
// for that protocol to allocate together with what it keeps of its own for
// the request, rather than each on its own. This package does not reuse a
// StreamRequest it is handed: like a request and its Context, all of it
// lives for as long as anything refers to any of it, unless the protocol
// recycles it, as this package's HTTP/2 server recycles its streams' under
// the Reuse options. The zero value is ready to use, for one request.
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
	c := r.bind(conn, body)
	c.external = stream
	return c
}

// bind readies the Context for Request, which arrived on conn with body, as
// Context does, leaving the caller to say what answers it: an external
// stream, or this package's own HTTP/2 stream.
func (r *StreamRequest) bind(conn *fib.Connection, body []byte) *Context {
	c := &r.context
	// A recycled Context waits for its next request reading as finished.
	c.reopen()
	c.Conn, c.Request = conn, &r.Request
	if len(body) == 0 {
		bufferpool.Put(body)
		return c
	}
	r.body = wholeBody{data: body, pooled: true}
	r.Request.Body, r.Request.ContentLength = &r.body, int64(len(body))
	c.whole = &r.body
	return c
}

// BodyWithin reports whether p is the body the request r holds arrived with,
// or a part of it: bytes in the server's buffer for that request, which it
// takes back once the response is finished. A sender that goes on reading them
// past that, as one framing a whole response does until the peer has
// acknowledged it, must copy them instead.
func (r *StreamRequest) BodyWithin(p []byte) bool {
	return within(p, r.body.data)
}

// within reports whether p is a part of b: bytes a server buffer holds, which
// goes back once the response is finished. A whole response built from them,
// as an echo is from Context.Body, must copy them rather than keep them, since
// it reads its body until it has been sent.
func within(p, b []byte) bool {
	if len(p) == 0 || len(p) > len(b) {
		return false
	}
	lo := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	p0 := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
	return p0 >= lo && p0+uintptr(len(p)) <= lo+uintptr(len(b))
}

// Recycle readies r for another request, once nothing can reach the one it
// held but the protocol that allocated r: its Context has ended, which a
// Stream that implements StreamEnder is told, and the protocol has done with
// it. The Context's generation moves on, so that a handler that wrongly kept
// it finds it finished, as a recycled HTTP/1 Context reads, and
// Context or StreamContext opens it for the next request.
func (r *StreamRequest) Recycle() {
	r.context.recycle()
	r.Request, r.URL = stdhttp.Request{}, url.URL{}
	r.body, r.task = wholeBody{}, streamTask{}
}

// Cancel ends the request r holds because its stream failed before its
// response was written: the client reset it, or the connection went. The
// handler hears of it as one on an HTTP/1 connection that closed does,
// through OnCancel, OnBody and Err, and nothing more is written; the request
// stays the handler's until it returns and releases what it retained. Cancel
// does nothing for a request whose response is finished, or one that has
// not been handed a Context yet.
//
// The protocol calls it before it lets go of r, from the goroutine that
// readies and recycles r, and only for a response it has not written whole:
// a handler that answered and goes on working is not told, since its
// response is not lost. The cancellation runs on the handler pool, since an
// OnCancel callback may answer, which may take what the caller holds; a
// Context recycled for another request by then is left alone.
func (r *StreamRequest) Cancel(err error) {
	c := &r.context
	if c.Request == nil {
		return
	}
	if w := c.word.Load(); ctxState(w) == ctxOpen {
		cancelElsewhere(c.Conn, func() { c.cancelWith(err, w>>ctxGenShift) })
	}
}

// StreamEnder is implemented by a Stream that is told when the request it
// answers has ended: the handler has returned, every Retain has been
// released, and the response, or the cancellation, has finished, so that
// nothing but the protocol can reach the request, its Context or the
// StreamRequest holding them any more. A protocol that recycles its streams
// recycles them from there, once it has done with them itself; see
// StreamRequest.Recycle.
type StreamEnder interface {
	RequestEnded()
}

// StreamAbandoner is implemented by a Stream that can be given up on when
// the response its handler began cannot be sent at all: a status or a header
// the protocol cannot carry, found once the handler is done. Abandon ends the
// stream, by resetting it, so that the client is not left waiting for an
// answer that will not come, as a connection of HTTP/1 is closed then. A
// Stream that does not implement it is left to end some other way.
type StreamAbandoner interface {
	Abandon()
}

// StreamDeadliner is implemented by a Stream whose response, and request
// body, can be given deadlines of their own, which is what Context's
// SetWriteDeadline and SetReadDeadline, and so http.ResponseController's,
// hand on to it once the Context has checked the response is not finished.
// A Stream that does not implement it has them report
// http.ErrNotSupported.
type StreamDeadliner interface {
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
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

// HoldBody pauses, with hold set, the body that OnBody delivers, and resumes it
// without: while it is held, the callback is not called, and what arrives
// waits in the server's buffer, which paces the client as an unread body
// does — an HTTP/1 connection stops reading its socket once
// Config.StreamRequestBodyBuffer waits, and HTTP/2 and HTTP/3 give the peer
// no room for more. That is how a callback that passes the body on to a slower
// consumer, such as a BodyWriter, waits for it without waiting: it holds the
// body, and resumes it once the consumer has room. A body that fails or is
// given up is still reported while held. Resuming hands what waits to the
// callback on the engine's handler pool, never on the caller's goroutine, so
// HoldBody may be called from inside the callback and from anywhere else. It
// does nothing to a body that arrived whole.
func (c *Context) HoldBody(hold bool) {
	if stream := c.RequestBody(); stream != nil {
		stream.holdSink(hold)
	}
}

// LimitBody bounds the request's body to n bytes, for a handler or
// middleware that takes less than the server's limits allow: the lower of n
// and the server's own limit applies, MaxBodyBytes for a body read whole
// before the handler ran and MaxStreamedBodyBytes for one that streams. Call
// it before the body is taken, on the goroutine that serves the request and
// before anything answers it. A negative n does nothing.
//
// A body known to be longer already is refused at once: one that declared a
// Content-Length past n, one that arrived whole and is longer, and one that
// streams and has brought more than n bytes so far. LimitBody then answers
// the request with 413 Request Entity Too Large, unless a response has been
// begun, ends the body with ErrBodyTooLarge, which a reader or an OnBody
// callback finds, and returns ErrBodyTooLarge; an HTTP/1 connection closes
// after the response, since the rest of the body is still on its way, and
// HTTP/2 and HTTP/3 refuse the rest of the stream. Otherwise it returns nil.
//
// A body of no declared length, HTTP/1's chunked and HTTP/2's and HTTP/3's
// without the field, is ended once it grows past n, with ErrBodyTooLarge,
// which Read reports and OnBody's callback is told, as for one past
// MaxStreamedBodyBytes. A request whose body ended that way and that is let
// go unanswered is answered with 413 rather than the empty 200 an unanswered
// request gets.
func (c *Context) LimitBody(n int64) error {
	if n < 0 || c.Request == nil {
		return nil
	}
	stream := c.RequestBody()
	var over bool
	switch {
	case stream != nil:
		// Both are checked: a declared length is refused whole, before any
		// of it arrives, and what has arrived already of one that is not
		// declared counts towards n.
		over = stream.limitTo(n) || c.Request.ContentLength > n
	case c.whole != nil && !c.whole.released:
		over = int64(len(c.whole.data)) > n
	default:
		over = c.Request.ContentLength > n
	}
	if !over {
		return nil
	}
	if stream != nil {
		// The body fails first, so that the handler hears why it ended
		// rather than that the response, written before it arrived, gave it
		// up.
		stream.failElsewhere(ErrBodyTooLarge)
	}
	if !c.begun() {
		// The rest of a streamed body is still on its way, and an HTTP/1
		// connection ends rather than read it, lingering so that the client
		// reads the refusal. HTTP/2 and HTTP/3 refuse the rest of the stream
		// alone.
		closing := stream != nil && c.isHTTP1()
		if closing {
			c.ends |= endLingers
		}
		_ = c.WriteResponse(Response{
			StatusCode: stdhttp.StatusRequestEntityTooLarge,
			Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
			Body:       []byte(stdhttp.StatusText(stdhttp.StatusRequestEntityTooLarge) + "\n"),
			Close:      closing,
		})
	}
	return ErrBodyTooLarge
}

// bodyTooLarge reports whether the request's body streamed and was ended for
// growing past its limit.
func (c *Context) bodyTooLarge() bool {
	stream := c.RequestBody()
	return stream != nil && errors.Is(stream.failure(), ErrBodyTooLarge)
}

// bodyFailed reports whether the request's body streamed and failed: grew
// past its limit, was misframed, held too long or cut. The rest of it was
// never read, so an HTTP/1 response says the connection ends, as it does once
// the response is out; a client told otherwise would send its next request on
// a connection that is closing.
func (c *Context) bodyFailed() bool {
	stream := c.RequestBody()
	return stream != nil && stream.failure() != nil
}

// bodyCutShort reports whether a whole response, sent while the request's
// body is still arriving, ends the connection: more of the body is left than
// the server will read and throw away once the request is done, or the client
// waits for a 100 Continue it will not get, so the connection closes after
// the response. Its head says so, so that the client does not send its next
// request on a connection about to close.
func (c *Context) bodyCutShort() bool {
	stream := c.RequestBody()
	return stream != nil && stream.abandonCloses()
}

// closeAfterResponse ends the HTTP/1 connection behind the response, with a
// lingering close if the request's body is still arriving.
func (c *Context) closeAfterResponse() {
	stream := c.RequestBody()
	closeAfterResponse(c.Conn, c.ends&endLingers != 0 || stream != nil && stream.unfinished())
}

// draining reports whether the HTTP/1 connection is to finish, its engine
// shutting down or its binding drained, in which case the response closes
// the connection, as net/http's do once Shutdown has begun, rather than keep
// it for a request it would not wait for; see fib.Engine.Shutdown and
// fib.Engine.ShutdownBinding.
func (c *Context) draining() bool { return c.Conn != nil && c.Conn.ShuttingDown() }

func (c *Context) Respond(status int, contentType string, body []byte) error {
	return c.respond(status, contentType, body, nil)
}

// RespondCopy sends a whole response, as Respond does, with a body the
// handler may reuse or change as soon as it returns: the body is copied at
// the call, and the copy is what the response sends. It is for a body that is a
// scratch buffer — one the handler fills again for the next request, or one
// from a buffer pool — where Respond, which keeps the slice until the
// response is finished, would not do; see Response.Body.
//
// A body the handler will not touch again costs nothing to hand over: Respond
// sends it without a copy, and so does RespondCopy for an empty one. A
// trailer is not a parameter of either call; WriteResponseCopy copies one.
func (c *Context) RespondCopy(status int, contentType string, body []byte) error {
	if c.begun() {
		return ErrResponseWritten
	}
	body, copied := c.copyResponseBody(body)
	return c.respond(status, contentType, body, copied)
}

// respond sends a whole response. copied, when it is not nil, is body's copy
// from the pool, which this call gives back once the send path has done with
// it.
func (c *Context) respond(status int, contentType string, body, copied []byte) error {
	if contentType == "" {
		return c.writeWhole(Response{StatusCode: status, Body: body}, copied)
	}
	if c.isHTTP1() && c.hooked() == nil {
		// HTTP/1 writes the one field straight into the response's head,
		// without a header map to build and walk.
		if c.w != nil && c.w.status != 0 {
			releaseBody(copied)
			return ErrResponseWritten
		}
		return c.writeHTTP1(Response{StatusCode: status, Body: body}, contentType, copied)
	}
	// Every path a response takes — HTTP/1's marshalResponse, HTTP/2's
	// encoder, an external stream's — has read the header by the time it
	// returns, and this one is not the caller's to keep, so it goes back to
	// be the next reply's. That is a map and a value slice per response,
	// which for a handler that answers with Respond is most of what
	// answering allocates.
	header := contentTypeHeaders.Get().(stdhttp.Header)
	header["Content-Type"] = append(header["Content-Type"][:0], contentType)
	var err error
	if c.w != nil && c.w.status != 0 {
		releaseBody(copied)
		err = ErrResponseWritten
	} else {
		// The header is this call's own, so the hooks may change it without
		// the copy WriteResponse makes of a caller's. Whatever fields they
		// added go before it serves another reply, including when they took
		// Content-Type out, as a 304 does, and left one field of their own.
		err = c.writeResponse(Response{StatusCode: status, Header: header, Body: body}, copied)
		if _, kept := header["Content-Type"]; len(header) > 1 || !kept {
			for key := range header {
				if key != "Content-Type" {
					delete(header, key)
				}
			}
		}
	}
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
func (c *Context) WriteResponse(response Response) error { return c.writeWhole(response, nil) }

// writeWhole is WriteResponse for a response whose body may be a copy this
// call made. copied, when it is not nil, is that copy, from the pool, which
// this call gives back once the send path has done with it.
func (c *Context) writeWhole(response Response, copied []byte) error {
	if c.w != nil && c.w.status != 0 {
		releaseBody(copied)
		return ErrResponseWritten
	}
	if hooks := c.hooked(); hooks != nil && hooks.changes && response.Header != nil && !c.wrote {
		// The OnHeader and OnResponse hooks may change the header, which is
		// the caller's and may be shared with other responses. OnFinish's
		// may not, so a response only they hook is sent with it as it is.
		response.Header = response.Header.Clone()
	}
	return c.writeResponse(response, copied)
}

// WriteResponseCopy sends the whole response at once, as WriteResponse does,
// with its Body and Trailer copied at the call: the handler may reuse or
// change its own body and trailer as soon as it returns, since the copy is
// what the response sends. Header is not copied, as for WriteResponse.
//
// It is WriteResponse for a body or a trailer the handler owns as a scratch
// buffer — one it fills again for the next response. A response whose body
// the handler will not touch again goes through WriteResponse, which hands
// the slice over without a copy; see Response.Body and Response.Trailer for
// the rule they follow until the response is finished.
func (c *Context) WriteResponseCopy(response Response) error {
	if c.begun() {
		return ErrResponseWritten
	}
	body, copied := c.copyResponseBody(response.Body)
	response.Body = body
	response.Trailer = response.Trailer.Clone()
	return c.writeWhole(response, copied)
}

// WriteResponseOwned sends the whole response at once, as WriteResponse does,
// for a Body that is a buffer from package bufferpool, which the response
// takes over: it gives it back to the pool once the send path is done with
// it, or leaves it to the collector where giving it back would cost more than
// it saves: HTTP/1 sends a body larger than a small reply's by reference
// rather than copying it into its queue, and HTTP/2 keeps by reference what
// its windows hold back. The caller must not touch the buffer
// after the call, whatever it returns. It is how a proxy sends on a
// response's body from ClientResponse.TakeBody without a copy.
func (c *Context) WriteResponseOwned(response Response) error {
	owned := response.Body
	if len(owned) == 0 || !c.senderTakesCopiedBody() || c.external == nil && c.stream == nil && len(owned) > maxCopiedResponseBody {
		owned = nil
	}
	if c.begun() {
		releaseBody(owned)
		return ErrResponseWritten
	}
	return c.writeWhole(response, owned)
}

// copyResponseBody copies body for a *Copy call, reporting the copy and, when
// a send path can give it back once it has done with it, the buffer from the
// pool the copy is. A body the sender cannot take over is copied plainly
// instead, for the collector to free. An empty body is not copied at all, so
// it costs what it costs Respond.
func (c *Context) copyResponseBody(body []byte) (copied, pooled []byte) {
	if len(body) == 0 {
		return body, nil
	}
	if !c.senderTakesCopiedBody() {
		return bytes.Clone(body), nil
	}
	pooled = bufferpool.Get(len(body))
	copy(pooled, body)
	return pooled, pooled
}

// senderTakesCopiedBody reports whether the sender of the response this
// Context is writing can take over a buffer from the pool, and give it back
// once it is done with it.
//
// HTTP/1's connection can: sendResponse sends a body from a *Copy call with
// SendParts, which copies what the socket does not take into the round's
// buffer or the queue, and is done with the body when it returns, so the pool
// gets the copy back and the wire gets the queue's own copy. HTTP/2's
// streams can, as framing copies the body: the copy goes back once framed,
// and only a tail the windows keep back is left to the collector; see
// h2ServerStream.respond. An external Stream that implements
// OwnedResponseWriter can too, as HTTP/3's does.
func (c *Context) senderTakesCopiedBody() bool {
	if c.external != nil {
		_, ok := c.external.(OwnedResponseWriter)
		return ok
	}
	return true
}

// releaseBody gives a *Copy call's buffer back to the pool, if it is one.
func releaseBody(copied []byte) {
	if copied != nil {
		bufferpool.Put(copied)
	}
}

// sameBody reports whether a and b are the same bytes of the same array.
func sameBody(a, b []byte) bool {
	return len(a) == len(b) && len(a) > 0 && unsafe.SliceData(a) == unsafe.SliceData(b)
}

func (c *Context) writeResponse(response Response, copied []byte) error {
	if c.wrote {
		releaseBody(copied)
		return ErrResponseWritten
	}
	if response.StatusCode == 0 {
		response.StatusCode = stdhttp.StatusOK
	}
	hooks := c.hooked()
	if hooks != nil {
		// The hooks are handed a copy, so that a response nothing hooks
		// stays off the heap: taking its own address would move it there.
		// The copy lives with the hooks, which are recycled with the
		// Context.
		hooks.response = response
		if !hooks.ran {
			hooks.ran = true
			if at := hooks.beforeFrom(&hooks.response, len(hooks.list)-1); at >= 0 && c.holdWhole(hooks, at) {
				// An OnHeaderAsync hook holds it, as a copy of its own, and
				// its resume sends it.
				releaseBody(copied)
				return nil
			}
		}
		if c.Request.Method != stdhttp.MethodHead && hooks.splitAsync() {
			// An asynchronous filter sends it from its done, from a copy of
			// its own.
			releaseBody(copied)
			response = hooks.response
			hooks.response = Response{}
			return c.holdBodyWhole(hooks, response)
		}
		err := hooks.filterWhole(&hooks.response, c.Request.Method == stdhttp.MethodHead)
		response = hooks.response
		hooks.response = Response{}
		if err != nil {
			hooks.ran = false
			releaseBody(copied)
			return err
		}
		if copied != nil && !sameBody(response.Body, copied) {
			// A hook put a body of its own in its place.
			releaseBody(copied)
			copied = nil
		}
	}
	var err error
	switch {
	case c.external != nil:
		if owner, ok := c.external.(OwnedResponseWriter); ok && copied != nil {
			// The stream takes the pooled copy over, and gives it back when
			// it is done with it, so this call no longer holds it.
			err = owner.WriteResponseOwned(c.Request, response, copied)
			copied = nil
		} else {
			releaseBody(copied)
			copied = nil
			err = c.external.WriteResponse(c.Request, response)
		}
		if err == nil {
			c.wrote, c.status = true, uint16(response.StatusCode)
		}
	case c.stream != nil:
		// HTTP/2 multiplexes the connection, so Close retires it gracefully
		// with GOAWAY rather than cutting the other streams short. The
		// stream takes the copy over; see senderTakesCopiedBody.
		err = c.stream.respond(c.Request, response, copied)
		copied = nil
		if err == nil {
			c.wrote, c.status = true, uint16(response.StatusCode)
		}
	default:
		// writeHTTP1 gives the copy back once its send has done with it.
		err = c.writeHTTP1(response, "", copied)
		copied = nil
	}
	if err != nil && hooks != nil && !c.wrote {
		// Nothing was sent, so the response the handler answers with
		// instead still has its hooks to run.
		hooks.ran = false
	}
	if err == nil && hooks != nil {
		hooks.finished(response.StatusCode, response.Header, wholeSize(c.Request, &response))
	}
	return err
}

// wholeSize is the length of the body a response sent whole carries.
func wholeSize(req *stdhttp.Request, response *Response) int64 {
	if req.Method == stdhttp.MethodHead || !statusHasBody(response.StatusCode) {
		return 0
	}
	return int64(len(response.Body))
}

// sendHeld is writeResponse's sending for a response an OnHeaderAsync hook
// held, once its hooks have run: it reads as written already, since the hold
// began, so writeResponse would refuse it. It is kept apart from
// writeResponse so that a response no hook holds goes no further than it.
func (c *Context) sendHeld(response *Response, hooks *responseHooks) error {
	var err error
	switch {
	case c.external != nil:
		err = c.external.WriteResponse(c.Request, *response)
	case c.stream != nil:
		err = c.stream.respond(c.Request, *response, nil)
	default:
		err = c.sendHTTP1(*response, "", nil)
	}
	if err == nil {
		hooks.finished(response.StatusCode, response.Header, wholeSize(c.Request, response))
	}
	return err
}

// writeHTTP1 is writeResponse for HTTP/1. contentType, when it is not empty,
// is sent as a Content-Type field besides those of response.Header, which
// then has none of its own. copied is a *Copy call's body, which this call
// gives back once its send has done with it.
func (c *Context) writeHTTP1(response Response, contentType string, copied []byte) error {
	if c.wrote {
		releaseBody(copied)
		return ErrResponseWritten
	}
	return c.sendHTTP1(response, contentType, copied)
}

// sendHTTP1 is writeHTTP1 for a response that may read as written already,
// having been held by an OnHeaderAsync hook; such a response's body is a copy
// of its own, so copied is nil then.
func (c *Context) sendHTTP1(response Response, contentType string, copied []byte) error {
	if response.StatusCode == 0 {
		response.StatusCode = stdhttp.StatusOK
	}
	closeConnection := response.Close || c.Request.Close || headerHasToken(response.Header, "Connection", "close") ||
		c.draining() || c.bodyFailed() || c.bodyCutShort()
	if err := c.sendResponse(response, contentType, closeConnection, copied); err != nil {
		return err
	}
	if !c.wrote {
		c.wrote, c.status = true, uint16(response.StatusCode)
	}
	if closeConnection {
		c.ends |= endClosing
		c.closeAfterResponse()
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
	return c.stream.push(c.Request, target, opts, c.onReader())
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
		handler = HandlerFunc(func(c *Context) {
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		})
	}
	h := &ServerHandler{
		handler: handler,
		config:  config,
		timed: config.ReadHeaderTimeout > 0 || config.ReadTimeout > 0 || config.ReadBodyTimeout > 0 ||
			config.IdleTimeout > 0,
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
	if parser.spent {
		return
	}
	if parser.bodyArmed {
		h.armBodyRead(c, parser)
		return
	}
	if h.timed {
		_ = c.SetReadDeadline(h.readDeadline(parser))
	}
}

// armBodyRead is armRead while a body that streams is arriving, or a
// deadline its handler set may still be the connection's: the handler's
// deadline, set with SetReadDeadline, stands in for Config.ReadTimeout's
// while its body is still arriving and its response still open, and gives
// way to whatever the connection waits for next after that.
func (h *ServerHandler) armBodyRead(c *fib.Connection, parser *Parser) {
	var at time.Time
	if h.timed {
		at = h.readDeadline(parser)
	}
	b := parser.bodyDeadline
	b.mu.Lock()
	b.open = nil
	if parser.stream != nil {
		if context := parser.liveContext.Load(); context != nil && ctxState(context.word.Load()) == ctxOpen {
			b.open = context
		}
	}
	own := b.open != nil && b.owner == b.open
	if own {
		at = b.at
	}
	if h.timed || own || b.applied {
		_ = c.SetReadDeadline(at)
	}
	b.applied = own
	parser.bodyArmed = b.open != nil
	b.mu.Unlock()
}

// readDeadline is the read deadline Config's timeouts put on what the
// connection is waiting for.
func (h *ServerHandler) readDeadline(parser *Parser) time.Time {
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
		if state == waitBody {
			if parser.bodyStart.IsZero() {
				parser.bodyStart = time.Now()
			}
			at = earliest(at, h.config.ReadBodyTimeout, parser.bodyStart)
		}
	}
	return at
}

// earliest is at, or start plus d when that comes first, or when at is zero;
// a d of zero or less leaves at as it is.
func earliest(at time.Time, d time.Duration, start time.Time) time.Time {
	if d <= 0 {
		return at
	}
	if bound := start.Add(d); at.IsZero() || bound.Before(at) {
		return bound
	}
	return at
}

// streamBodyDeadline is the read deadline Config's timeouts put on a body
// that streams on a multiplexed protocol, whose request began at start.
func streamBodyDeadline(config *Config, start time.Time) time.Time {
	return earliest(earliest(time.Time{}, config.ReadTimeout, start), config.ReadBodyTimeout, start)
}

// releaseTimeouts hands the connection over to another protocol: this parser
// stops timing it, and the deadline it had goes with it, whether the
// Config's or one the handler set.
func (h *ServerHandler) releaseTimeouts(c *fib.Connection, parser *Parser) {
	parser.spent = true
	applied := false
	if b := parser.bodyDeadline; b != nil {
		b.mu.Lock()
		applied = b.applied
		b.open, b.owner, b.applied = nil, nil, false
		b.mu.Unlock()
	}
	if h.timed || applied {
		_ = c.SetReadDeadline(time.Time{})
	}
	if h.config.WriteTimeout > 0 || !parser.writeDeadline.IsZero() {
		parser.writeDeadline = time.Time{}
		_ = c.SetWriteDeadline(time.Time{})
	}
}

func (h *ServerHandler) OnOpen(c *fib.Connection) {
	parser := h.attachParser(c)
	if fibtls.Handshaking(c) {
		// The handshake is bounded by the TLS handler's HandshakeTimeout,
		// and the connection is idle from when it can carry a request; see
		// OnHandshake. Timing it from here gave a slow handshake less of
		// IdleTimeout, or none, as net/http's never does.
		return
	}
	// A connection that opens and then says nothing is idle, and bounded like
	// any other idle connection.
	parser.mu.Lock()
	h.armRead(c, parser)
	parser.mu.Unlock()
}

// OnHandshake starts timing a TLS connection's idleness once its handshake
// has completed, as OnOpen does for one without TLS.
func (h *ServerHandler) OnHandshake(c *fib.Connection, _ stdtls.ConnectionState) {
	parser, ok := c.Attachment().(*Parser)
	if !ok {
		return
	}
	parser.mu.Lock()
	h.armRead(c, parser)
	parser.mu.Unlock()
}

// attachParser gives the connection its parser. Its requests are read and
// served in the connection's rounds, which run on the engine's workers, a
// handler from its request's first byte read to its response.
func (h *ServerHandler) attachParser(c *fib.Connection) *Parser {
	parser := h.newParser(c)
	c.SetAttachment(parser)
	return parser
}

// newParser starts a connection's parser, recording the peer's address so
// that each request carries it in RemoteAddr as net/http's do.
func (h *ServerHandler) newParser(c *fib.Connection) *Parser {
	parser := NewParser(h.config)
	parser.conn = c
	// The address the engine accepted the connection from is at hand, with
	// no system call to ask for it and no net.Addr to build; RemoteAddr is
	// left for the peers it does not cover.
	if ap := c.RemoteAddrPort(); ap.IsValid() {
		parser.remoteAddr = ap.String()
	} else if addr := c.RemoteAddr(); addr != nil {
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
	case *TunnelFeed:
		// An HTTP/1 tunnel's feed has no limit, so Write refuses nothing.
		_ = state.Write(data)
		return
	case *Parser:
		parser = state
	default:
		parser = h.attachParser(c)
	}
	h.drive(c, parser, data)
}

// EarlyDataEnabled reports Config.EarlyData, which turns early data on for the
// TLS handler this server is wrapped in; see tls.EarlyDataOptIn.
func (h *ServerHandler) EarlyDataEnabled() bool { return h.config.EarlyData }

// OnEarlyData serves what a client sent with its first flight, before its TLS
// handshake was over: 0-RTT data (RFC 8446 section 2.3), whose requests carry
// Early-Data (RFC 8470) and are held to the safe methods unless
// Config.EarlyDataUnsafe says otherwise. It is how Config.EarlyData reaches
// the TLS layer, which accepts a ticket's early data once and bounds how much
// of it there is, as it does for HTTP/3's QUIC connections.
func (h *ServerHandler) OnEarlyData(c *fib.Connection, data []byte) {
	if state, ok := c.Attachment().(*h2ServerConn); ok {
		// The h2 connection is built out of the preface in this data, and
		// the streams it opens here are 0-RTT ones.
		state.early = true
		state.feed(data)
		state.early = false
		return
	}
	parser, ok := c.Attachment().(*Parser)
	if !ok {
		parser = h.attachParser(c)
	}
	parser.mu.Lock()
	parser.earlyPending += int64(len(data))
	parser.mu.Unlock()
	h.drive(c, parser, data)
}

// refuseEarly answers a request that arrived as early data and may not be
// served from it: 425 Too Early tells the client to send it again once its
// handshake is over (RFC 8470 section 5.2). The connection ends with the
// response, as an HTTP/1 connection that has answered 425 is not left to
// carry the client's later requests: the client dials afresh, and this time
// the request is 1-RTT.
func (h *ServerHandler) refuseEarly(c *fib.Connection, parser *Parser, request *stdhttp.Request) {
	parser.spent = true
	context := &Context{Conn: c, Request: request, ends: endClosing}
	_ = context.WriteResponse(Response{
		StatusCode: stdhttp.StatusTooEarly,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte(stdhttp.StatusText(stdhttp.StatusTooEarly) + "\n"),
		Close:      true,
	})
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
	if parser.upgraded {
		// A retained request switched the connection while this round waited
		// for the parser: what it read is the tunnel's.
		if feed, ok := c.Attachment().(*TunnelFeed); ok {
			_ = feed.Write(data)
		}
		return
	}
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
					closeAfterResponse(c, parser.lingers)
					return
				}
				resumed = true
			}
			if err != nil {
				h.failStream(c, parser)
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
		// Whether the request begins in early data is known before parsing
		// it, which consumes the bytes that tell it; see OnEarlyData.
		fromEarly := parser.earlyPending > 0
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
		if fromEarly {
			// Sent before the handshake was over, and possibly a replay
			// (RFC 8470): only what changes nothing is served from it.
			request.Header.Set("Early-Data", "1")
			if !h.config.EarlyDataUnsafe && !safeMethod(request.Method) {
				h.refuseEarly(c, parser, request)
				return
			}
		}
		if settings, ok := h.h2cUpgrade(c, request); ok && parser.stream == nil {
			h.upgradeH2C(c, parser, request, settings)
			return
		}
		// The block, when there is one, is the one request was parsed into:
		// feedOne sets both from the same frame.
		block := parser.block
		parser.block = nil
		context := h.newContext(block)
		context.Conn, context.Request, context.server, context.parser = c, request, h, parser
		context.whole, _ = request.Body.(*wholeBody)
		context.block, context.streamed = block, parser.stream != nil
		// Nothing behind this request is served until its response is
		// written, so that pipelined responses keep their order, and the
		// connection knows which request to cancel if it closes first.
		context.begin(true, false)
		parser.busy = true
		parser.liveContext.Store(context)
		if context.streamed {
			parser.openBody(context)
		}
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
		if d := h.config.WriteTimeout; d > 0 {
			// The response has until then to be written: the connection
			// closes if output is still queued for the client by then, and
			// sendBody refuses to stream more of it afterwards.
			parser.writeDeadline = time.Now().Add(d)
			_ = c.SetWriteDeadline(parser.writeDeadline)
		} else if !parser.writeDeadline.IsZero() {
			// The last response's handler set a deadline of its own, which
			// does not carry over to this one.
			parser.writeDeadline = time.Time{}
			_ = c.SetWriteDeadline(time.Time{})
		}
		// The handler runs here, on the connection's worker, whether or not
		// its body has all arrived: reading a streamed body never waits, so
		// there is nothing for a goroutine of its own to wait on.
		serveRequest(h.handler, context)
		if parser.upgraded {
			// The handler switched the connection to another protocol, which
			// has it, and whatever it sends, from here on.
			parser.busy = false
			context.drop(ctxConn)
			return
		}
		if bodyErr != nil {
			// The body was refused or misframed before the handler even saw
			// it, which its own read told it; the connection cannot go on.
			h.failStream(c, parser)
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
			closeAfterResponse(c, parser.lingers)
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
	tooMuchLeft, unread := false, false
	if stream := context.RequestBody(); stream != nil {
		tooMuchLeft, unread = stream.abandon()
	}
	if context.streamed {
		parser.closeBody(context)
	}
	// A request cancelled while its handler ran, by a hangup heard before the
	// round could read it, has no response for the client to read, and
	// answering what follows it would answer the wrong request.
	cancelled := ctxState(context.word.Load()) == ctxCancelled
	closing := tooMuchLeft || cancelled || context.ends&endClosing != 0 || context.Request.Close
	if closing {
		parser.lingers = unread
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
	if parser.upgraded {
		// Switched to a tunnel after its handler returned (see Upgrade): the
		// connection is the tunnel's, and there is nothing to carry on.
		parser.busy = false
		parser.mu.Unlock()
		context.drop(ctxConn)
		return
	}
	closing := h.endRequestLocked(context)
	lingers := parser.lingers
	parser.mu.Unlock()
	if closing {
		closeAfterResponse(c, lingers)
		return
	}
	h.drive(c, parser, nil)
}

// failStream ends a connection whose streamed body could not be framed or grew
// past what the server accepts. The handler has the request already, so the
// failure reaches it through the body rather than as a status. The handler
// may still be writing a response to a request it was given, so what it has
// sent goes out before the connection ends.
func (h *ServerHandler) failStream(c *fib.Connection, parser *Parser) {
	parser.spent = true
	c.CloseAfterSend()
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
	// Whatever the client sent past what was refused, the rest of a body too
	// large or a malformed request, is still arriving, and the close lingers
	// for it, as net/http's does, so that the client reads the refusal.
	context := &Context{Conn: c, Request: request, ends: endLingers}
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
func Serve(handler Handler, context *Context) { serveRequestOn(handler, context, false) }

// serveRequest runs the handler for the request context carries, hands over
// the body it asked for through OnBody, and gives back the hold that serving
// it took, which writes the response unless the handler, or OnBody, retained
// it.
func serveRequest(handler Handler, context *Context) { serveRequestOn(handler, context, false) }

// serveOnReader is serveRequest for a request served on the goroutine that
// reads its multiplexed connection, which the handler's response is not to
// wait on; see Context.onReader.
func serveOnReader(handler Handler, context *Context) { serveRequestOn(handler, context, true) }

func serveRequestOn(handler Handler, context *Context, reader bool) {
	if !context.begin(false, reader) {
		return
	}
	handler.ServeHTTP(context)
	context.handled()
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
	// The idle deadline HTTP/1 armed goes first: the bytes that end it are
	// already in hand, and building the connection, which on an engine's
	// first takes its stream pool, is the server's time, not the client's.
	// Lifted after, a slow build cut off a client whose preface and first
	// request the server held unread.
	h.releaseTimeouts(c, parser)
	sc := newH2ServerConn(h, c, parser.remoteAddr)
	sc.tlsState = state
	c.SetAttachment(sc)
	sc.start()
	// feed copies what it is given into the connection's own buffer, so the
	// parser's goes back to the pool rather than to the collector. The streams
	// the preface and the frames behind it open are 0-RTT ones when those
	// bytes arrived as early data.
	buffered := parser.TakeBuffered()
	sc.early = parser.earlyPending > 0
	sc.feed(buffered)
	sc.early = false
	bufferpool.Put(buffered)
}

// tlsState is the connection's TLS state, looked up once the handshake has
// delivered the first bytes, or nil for a connection without TLS.
func (h *ServerHandler) tlsState(c *fib.Connection, parser *Parser) *stdtls.ConnectionState {
	if !parser.tlsChecked {
		parser.tlsChecked = true
		if state, ok := fibtls.ConnectionState(c); ok {
			// A copy made here, not &state: that would move state to the
			// heap for every connection, TLS or not.
			parser.tls = new(stdtls.ConnectionState)
			*parser.tls = state
		}
	}
	return parser.tls
}

func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h *ServerHandler) OnClose(c *fib.Connection, err error) {
	switch state := c.Attachment().(type) {
	case *h2ServerConn:
		state.shutdown()
	case *TunnelFeed:
		state.End(err)
	case *Parser:
		// A handler waiting on the rest of a body has to hear that the rest
		// will never come, rather than take a truncated upload for a complete
		// one, and one still working on a request it retained has to hear
		// that there is nothing left to answer. Both are reported on the
		// engine's handler pool, since this runs in the connection's last
		// round, on an engine worker, and a handler's cleanup must never hold
		// that up; the parser's own lock is not taken there either, for the
		// same reason.
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
			runOnStreams(c, func() {
				if stream != nil {
					stream.fail(err, false)
				}
				if context != nil {
					context.cancelWith(err, gen)
				}
			})
		}
	}
	c.SetAttachment(nil)
}

// OnHangup cancels the HTTP/1 request whose handler is holding up the round
// that would read its peer's hangup. A handler that waits in place, on a
// database or another server, rather than retaining the request, would
// otherwise hear nothing: by the time the round read the hangup, the handler
// would have returned and its response been written, leaving nothing to
// cancel. It hears it through OnCancel, OnBody and Err instead, as one that
// retained its request hears a close, and the connection closes, since the
// response is lost and answering what is pipelined behind the request would
// answer the wrong one. A request whose body is still arriving is left to the
// round, which reads what the peer sent before it went and then fails the
// body.
//
// It runs on the event loop, so the cancellation runs on the handler pool, as
// OnClose's does; see fib.HangupHandler.
func (h *ServerHandler) OnHangup(c *fib.Connection) {
	state, ok := c.Attachment().(*Parser)
	if !ok || state.live.Load() != nil {
		return
	}
	context := state.liveContext.Load()
	if context == nil {
		return
	}
	// As in OnClose: the generation is read while the Context is still this
	// connection's, which the second load confirms.
	gen := context.generation()
	if state.liveContext.Load() != context || ctxState(context.word.Load()) != ctxOpen {
		return
	}
	// The connection closes here rather than when the round reads the
	// hangup, so that what the handler writes from here on is refused, as it
	// is on a request whose connection closed under it, rather than sent to a
	// peer that has gone. OnClose still comes after the round, and whichever
	// of the two cancellations gets there first is the one that counts.
	c.CloseWithError(io.EOF)
	cancelElsewhere(c, func() { context.cancelWith(io.EOF, gen) })
}

// shutdownQuietGrace is how long a connection that has sent nothing at all
// is given to send its first request once its engine starts shutting down,
// before OnShutdown takes it for idle: a client that has just connected may
// have its request on the way. net/http.Server.Shutdown gives a new
// connection five seconds from its accept. A variable, so that tests can
// shorten it.
var shutdownQuietGrace = 5 * time.Second

// OnShutdown ends the connection as its engine shuts down, the way
// net/http.Server.Shutdown ends its own; see fib.Engine.Shutdown. An HTTP/1
// connection between requests closes now, and one serving a request closes
// once its response is written, which says Connection: close. One that has
// not sent a byte yet is given shutdownQuietGrace to send its first request.
// An HTTP/2 connection sends GOAWAY, finishes the streams it has, and closes
// after the last. A tunnel, which Upgrade switched a request to, is the
// protocol it carries' to end, which hears of the shutdown if its handler is
// a TunnelShutdownHandler: the HTTP/1 connection the tunnel took, or the
// HTTP/2 stream, then closes when it does.
//
// It runs on the event loop, and again on each round of the shutdown until
// the connection closes; see fib.ShutdownHandler.
func (h *ServerHandler) OnShutdown(c *fib.Connection) {
	switch state := c.Attachment().(type) {
	case *h2ServerConn:
		if state.retiring.CompareAndSwap(false, true) {
			// GOAWAY is sent under the connection's lock, which a stream
			// writing its response may hold.
			cancelElsewhere(c, state.retire)
		}
	case *TunnelFeed:
		if state.tunnel.markShutdown() {
			// The handler's call may wait, which the loop must not.
			cancelElsewhere(c, state.tunnel.tellShutdown)
		}
	case *Parser:
		if h.idleForShutdown(state) {
			// Closing goes through the connection's layer, whose lock a
			// round decrypting what just arrived may hold.
			cancelElsewhere(c, c.CloseAfterSend)
		}
	}
}

// OnEngineClose tells the client of an HTTP/2 connection that the server is
// going, with GOAWAY, as Engine.Close is about to end the connection; see
// fib.EngineCloseHandler. An HTTP/1 connection needs nothing: the end of its
// stream is the end of it, and a tunnel's protocol hears through its own
// handler. It runs on the goroutine that called Close, which must not wait,
// so the GOAWAY takes no lock of the connection and is written where it is
// built; see h2ServerConn.engineClose.
func (h *ServerHandler) OnEngineClose(c *fib.Connection) {
	if state, ok := c.Attachment().(*h2ServerConn); ok {
		state.engineClose()
	}
}

// idleForShutdown reports whether an HTTP/1 connection is between requests,
// and if it is marks it spent, so that a request arriving before it closes is
// not begun. A parser that is held is busy: it is held while a request is
// served, and a later round of the shutdown finds it idle if the response
// has not closed it by then.
func (h *ServerHandler) idleForShutdown(parser *Parser) bool {
	if !parser.mu.TryLock() {
		return false
	}
	defer parser.mu.Unlock()
	if parser.spent || parser.waitState() != waitIdle {
		return false
	}
	if !parser.sniffed {
		now := time.Now().UnixNano()
		if parser.quietSince == 0 {
			parser.quietSince = now
		}
		if time.Duration(now-parser.quietSince) < shutdownQuietGrace {
			return false
		}
	}
	parser.spent = true
	return true
}

// sendResponse writes a whole HTTP/1 response to the connection.
//
// The head is built in a pooled buffer and goes to the connection with the
// body through SendParts, or through SendPartsShared for a body above
// maxCopiedResponseBody, which the connection then keeps as the handler gave
// it rather than copying the part the socket does not take at once. A response
// written from inside a read round, which is where nearly every one is
// written, is corked, so the head is copied into the round's send buffer
// behind the responses before it, and a small body with it, and both reach the
// socket with them in one write: answering a request with a small response
// allocates nothing for its response, and the round's replies share one buffer
// that goes back to the pool once they are sent, rather than each holding an
// array of its own until then. A large body waits beside that buffer, so that
// a stalled reader of a whole response costs its body's memory once, as it
// does on HTTP/2 and HTTP/3, rather than a copy of it in a pool class larger
// than the body.
//
// A body that is part of the request's own, as Context.Body's is, is copied
// instead: the server takes that buffer back once the handler returns, which a
// response still sending it would outlive. A chunked response, which carries
// trailers, is rare enough to be built whole instead. A body copied for a
// *Copy call goes through SendParts whatever its size, so that the copy, a
// buffer from the pool, is the send's to be done with when it returns.
func (c *Context) sendResponse(response Response, contentType string, closeConnection bool, copied []byte) error {
	// A body copied for a *Copy call, from the pool, is this call's to give
	// back: the send below copies it, as it copies the head, so nothing reads
	// it once this returns.
	defer releaseBody(copied)
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
		// A body from a *Copy call is a buffer of this call's, from the pool,
		// which SendParts copies into the round's buffer or the queue as it
		// copies the head; SendPartsShared would keep it, and the pool could
		// not have it back when this returns.
		if len(body) > maxCopiedResponseBody && copied == nil &&
			!(c.whole != nil && within(body, c.whole.data)) {
			err = c.Conn.SendPartsShared(head, body)
		} else {
			err = c.Conn.SendParts(head, body)
		}
		pooled = head
	}
	bufferpool.Put(pooled)
	return err
}

// maxCopiedResponseBody is the largest whole-response body an HTTP/1 send
// copies into the queue rather than keeping as the handler gave it. A corked
// round's replies share one buffer and reach the socket in one write, which is
// what an ordinary small reply wants, and a message of several parts would
// cost the socket more than the copy does; above it the body's own memory is
// cheaper than a copy of it. HTTP/3 used the same bound while it still copied
// a small whole response into its frames, before it kept every size.
const maxCopiedResponseBody = 4 << 10

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

// responseLength is the Content-Length a stream reports for response to
// req: the length of its body, except that a response to HEAD, which sends
// no body, reports the one its header declares when it declares one, as
// HTTP/1 does.
func responseLength(req *stdhttp.Request, response Response) int64 {
	if req.Method == stdhttp.MethodHead {
		if n := declaredLength(response.Header); n >= 0 {
			return n
		}
	}
	return int64(len(response.Body))
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
		if !validHeaderName(key) {
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

// validHeaderName reports whether name is a token (RFC 9110 section 5.1),
// which a field name has to be. Canonicalising leaves a name that is not one,
// such as "Bad Name", as it is, so that does not tell.
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !tokenByte[name[i]] {
			return false
		}
	}
	return true
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if (value[i] < ' ' && value[i] != '\t') || value[i] == 0x7f {
			return false
		}
	}
	return true
}

var (
	_ fib.Handler            = (*ServerHandler)(nil)
	_ fib.EngineCloseHandler = (*ServerHandler)(nil)
)
