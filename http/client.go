//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/textproto"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/internal/reqbody"
	"github.com/lesismal/fib/sidepool"
	fibtls "github.com/lesismal/fib/tls"
)

var (
	// ErrClientClosed is what requests still waiting for a connection get when
	// the client is closed, and what new requests get afterwards.
	ErrClientClosed = errors.New("http: client closed")
	// ErrTooManyPending is what a request gets at once when
	// ClientConfig.MaxPendingPerHost requests to its host are already
	// waiting for a connection.
	ErrTooManyPending = errors.New("http: too many requests waiting for a connection to the host")
	// ErrUnsupportedScheme is what a request for anything but http:// or
	// https:// gets.
	ErrUnsupportedScheme = errors.New("http: unsupported URL scheme")
	// ErrResponseHeaderTimeout is what a request gets when
	// ClientConfig.ResponseHeaderTimeout runs out. errors.Is matches it
	// against os.ErrDeadlineExceeded too.
	ErrResponseHeaderTimeout = fmt.Errorf("http: timed out awaiting the response header: %w", os.ErrDeadlineExceeded)
	// errRequestTimeout is what a request gets when ClientConfig.Timeout runs
	// out. errors.Is matches it against os.ErrDeadlineExceeded.
	errRequestTimeout = fmt.Errorf("http: request timed out: %w", os.ErrDeadlineExceeded)
)

// ClientConfig bounds what a Client waits for and how many connections it
// keeps.
type ClientConfig struct {
	// Timeout bounds a request from Do until its response is complete:
	// waiting for a connection, dialing, sending and reading all count. Zero
	// means no limit beyond the request's context.
	Timeout time.Duration
	// ResponseHeaderTimeout bounds the wait for a response's header once the
	// request, its body included, has been handed to the connection; interim
	// 1xx responses do not end it. A request past it fails with
	// ErrResponseHeaderTimeout. Zero means no limit beyond Timeout. The two
	// share one timer per request.
	ResponseHeaderTimeout time.Duration
	// ExpectContinueTimeout is how long a request with a body and
	// "Expect: 100-continue" waits for the server's 100 (Continue) before
	// sending the body anyway, once its header has been sent, as net/http's
	// Transport.ExpectContinueTimeout does. A final response that comes first
	// is the answer, and the body is not sent at all: an HTTP/1 connection,
	// which then cannot tell where the next request starts, is closed, and an
	// HTTP/2 stream reset once the response is complete. Zero sends the body
	// with the header, without waiting, as before.
	ExpectContinueTimeout time.Duration
	// ReadIdleTimeout is how long an HTTP/2 connection may hear nothing from
	// the server before the client sends it a PING, and PingTimeout how long
	// it then waits for the server, the PING's acknowledgement or any other
	// frame, before taking the connection for dead: it is closed, and its
	// requests fail with ErrPingTimeout, or are sent again on another
	// connection when the server had answered nothing of them and they are
	// safe to repeat. This finds a connection that died without a word, as
	// one through a NAT that forgot it does, which TCP would otherwise only
	// give up on after many minutes. As with x/net/http2, ReadIdleTimeout's
	// zero leaves the check off, and PingTimeout's means 15 seconds. HTTP/1
	// connections are not checked: one carries a request at a time, which
	// its Timeout or ResponseHeaderTimeout bounds.
	ReadIdleTimeout time.Duration
	PingTimeout     time.Duration
	// DialTimeout bounds one connect. Zero leaves it to the operating system.
	DialTimeout time.Duration
	// Dial opens the client's connections, in place of the engine's
	// DialWithHandler, with the same arguments and the same contract: a
	// socks.Dialer's Dial sends every request through a SOCKS proxy. Nil
	// dials directly.
	Dial func(engine *fib.Engine, network, addr string, timeout time.Duration,
		handler fib.Handler, done func(*fib.Connection, error)) error
	// MaxConnsPerHost caps the connections open or dialing to one host:port.
	// Requests beyond it wait for one of them. Zero or less means no cap.
	MaxConnsPerHost int
	// MaxPendingPerHost caps the requests to one host:port waiting for a
	// connection that MaxConnsPerHost keeps the client from dialing for
	// them, as nginx's queue does: a request beyond it fails at once with
	// ErrTooManyPending, so that a host that has stopped answering costs a
	// bounded queue, of at most MaxConnsPerHost plus this, rather than one
	// that grows until the requests' timeouts. Requests a connection is
	// being dialed for, or could still be within MaxConnsPerHost, do not
	// count, so a burst to a cold host is not turned away while the
	// connections it is allowed open; for a host that may speak HTTP/2,
	// which the client dials once for until it knows, that is the dial and
	// the connections MaxConnsPerHost still leaves room for. It has no
	// effect without MaxConnsPerHost. Zero or less means no cap.
	MaxPendingPerHost int
	// MaxIdleConnsPerHost is how many finished connections to one host:port
	// are kept for later requests. Zero or less keeps none, so every request
	// dials afresh.
	MaxIdleConnsPerHost int
	// IdleConnTimeout closes a kept connection that has not been reused for
	// this long. Zero keeps it until the server closes it.
	IdleConnTimeout time.Duration
	// MaxResponseHeaderBytes and MaxResponseBodyBytes bound what one response
	// may hold. The body is buffered whole before the callback runs, so the
	// second is also the memory one response can take. A body that streams
	// (see StreamResponseBody) is bounded by MaxStreamedBodyBytes instead.
	MaxResponseHeaderBytes int
	MaxResponseBodyBytes   int64
	// StreamResponseBody runs the callback of a response before its body has
	// all arrived, on HTTP/1 and HTTP/2 connections: once the header has, for
	// a body with a Content-Length, and once some of the body has, for one
	// without a length (chunked, or ended by the server's close, or an HTTP/2
	// body that declares none). The body is then taken inside the callback
	// with ClientResponse.OnBody, which delivers it piece by piece, or read
	// from ClientResponse.Body without waiting. A callback that does neither
	// leaves the rest of the body to be thrown away. Off, the default, the
	// callback runs once the response is complete and its Body holds all of
	// it. Client.Go always waits for the whole response. HTTP/1 holds the
	// connection's reads while a body waits for a reader; HTTP/2 gives a
	// stream's window back only as its body is consumed, so a stalled reader
	// of Body never holds up the other streams on the connection. A function
	// given to OnBody is different: it runs on the worker that reads the
	// connection, so one that waits holds up the reading of every stream on
	// it.
	StreamResponseBody bool
	// StreamResponseBodyThreshold keeps the smaller bodies buffered whole when
	// StreamResponseBody is set: only a body whose Content-Length is larger
	// than this, or one without a length of which more than this has arrived,
	// streams, and its callback is not run until that much of it has arrived.
	// Zero streams every body as soon as its header has arrived. It has no
	// effect without StreamResponseBody.
	StreamResponseBodyThreshold int64
	// StreamUnknownLengthAtHeader runs the callback of a body of no declared
	// length (chunked, ended by the server's close, or an HTTP/2 body that
	// declares none) as soon as its header has arrived, rather than once
	// StreamResponseBodyThreshold of it has, so that a body that trickles,
	// as server-sent events and long polls do, is handed on as it comes.
	// Bodies with a Content-Length are kept whole or streamed by the
	// threshold as before. It has no effect without StreamResponseBody.
	StreamUnknownLengthAtHeader bool
	// MaxStreamedBodyBytes bounds a streamed body, which MaxResponseBodyBytes
	// does not. Zero, the default, leaves a streamed body unbounded.
	MaxStreamedBodyBytes int64
	// StreamResponseBodyBuffer is how many bytes of a streamed body may wait
	// for a reader before the connection stops reading its socket, so that a
	// callback slower than the server is paid for by TCP flow control rather
	// than by memory here. Zero means DefaultStreamResponseBodyBuffer. HTTP/2
	// is bounded by the stream window (1MB) instead.
	StreamResponseBodyBuffer int
	// TLSConfig is used for https:// requests. Nil means the defaults; either
	// way a config naming no server gets the request's host. Unless it sets
	// NextProtos itself, ALPN offers HTTP/2 and HTTP/1.1.
	TLSConfig *tls.Config
	// BlockingTLSHandshake runs the TLS handshakes of https:// connections
	// through crypto/tls on a worker of the handshake pool, each waiting there
	// for its peer. By default they run as state machines that take no
	// goroutine while they wait, where the TLSConfig allows it (see package
	// tls); this is the switch back, and what to compare the two with.
	BlockingTLSHandshake bool
	// DisableHTTP2 keeps https:// requests on HTTP/1.1. Otherwise they speak
	// HTTP/2 to any server that chooses it through ALPN.
	DisableHTTP2 bool
	// UnencryptedHTTP2 sends http:// requests as HTTP/2 with prior knowledge
	// (h2c), for servers known to speak it. It is off by default, since an
	// HTTP/1-only server cannot answer it.
	UnencryptedHTTP2 bool
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Timeout:                30 * time.Second,
		ExpectContinueTimeout:  time.Second,
		DialTimeout:            10 * time.Second,
		MaxConnsPerHost:        64,
		MaxIdleConnsPerHost:    16,
		IdleConnTimeout:        90 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		MaxResponseBodyBytes:   64 << 20,
	}
}

// Client sends HTTP/1.x and HTTP/2 requests over connections an engine dials
// and serves, without blocking the caller. Responses are read by the engine's
// workers and handed to a callback, by default with their body already
// buffered, so a callback never waits on the network; see
// ClientConfig.StreamResponseBody for handing it a large body as it arrives.
//
// A client keeps its connections alive between requests. An HTTP/1.1
// connection carries one request at a time; it does not pipeline. A request
// whose ProtoMinor is 0 is sent as HTTP/1.0 over an HTTP/1 connection, which
// is kept only if the request asks for it with "Connection: keep-alive" and
// the server agrees. An HTTP/2 connection carries as many at once as the
// server allows. http:// and https:// are supported; https speaks HTTP/2 when the server chooses it
// through ALPN and HTTP/1.1 over TLS otherwise, and http:// speaks HTTP/1.1
// unless ClientConfig.UnencryptedHTTP2 is set.
//
// The engine may be one fib.New made for clients alone, with no bindings, or a server's
// own engine: client connections carry their own handler, so the two never
// see each other's traffic. The engine has to be running. Close the client
// before closing the engine: closing the engine drops its connections without
// telling the client, so requests on them are only answered by their timeout.
type Client struct {
	engine *fib.Engine
	config ClientConfig
	// tlsConfig is config.TLSConfig with the client's ALPN offer.
	tlsConfig *tls.Config
	mu        sync.Mutex
	hosts     map[hostTarget]*hostPool
	closed    bool
}

// NewClient returns a client that dials through engine. Zero limits in config
// are taken as written, so start from DefaultClientConfig to change only some.
func NewClient(engine *fib.Engine, config ClientConfig) *Client {
	defaults := DefaultClientConfig()
	if config.MaxResponseHeaderBytes <= 0 {
		config.MaxResponseHeaderBytes = defaults.MaxResponseHeaderBytes
	}
	if config.MaxResponseBodyBytes <= 0 {
		config.MaxResponseBodyBytes = defaults.MaxResponseBodyBytes
	}
	if config.StreamResponseBodyBuffer <= 0 {
		config.StreamResponseBodyBuffer = DefaultStreamResponseBodyBuffer
	}
	if config.PingTimeout <= 0 {
		config.PingTimeout = DefaultPingTimeout
	}
	tlsConfig := config.TLSConfig
	if !config.DisableHTTP2 && (tlsConfig == nil || len(tlsConfig.NextProtos) == 0) {
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		} else {
			tlsConfig = tlsConfig.Clone()
		}
		tlsConfig.NextProtos = []string{"h2", "http/1.1"}
	}
	return &Client{engine: engine, config: config, tlsConfig: tlsConfig, hosts: make(map[hostTarget]*hostPool)}
}

// Do sends req and calls callback exactly once with its response or the
// reason there is none. It does not wait for the network.
//
// A body with a ContentLength of up to 1MB, which is what net/http.NewRequest
// sets for a bytes.Reader, bytes.Buffer or strings.Reader that small, is read
// whole by Do, so it must not block for long, and goes out with the header.
// Any other body is sent as it is read, by a goroutine of its own that reads
// it a piece at a time and waits whenever the connection has 64KB of it
// unsent, so that a large or slow body is paced by the server rather than
// held in memory: by TCP on HTTP/1, which frames it with Content-Length when
// ContentLength is set and chunked otherwise, and by the stream's flow control
// on HTTP/2. The body is closed once it has been sent, or once the request
// is over, which may be while a Read on it is waiting. A response that
// arrives before the body has all been sent ends the sending: the server has
// answered without it. A body that fails to read, or is not as long as its
// ContentLength, fails the request. A body that has been sent, wholly or in
// part, can only be sent again, as a request that a closed connection or the
// server turned away is, through req.GetBody; without it such a request fails.
// A request with a body and "Expect: 100-continue" sends its header first and
// its body once the server answers 100 (Continue), or once
// ClientConfig.ExpectContinueTimeout has passed; a final response before
// either means the body is not sent at all.
//
// The response body is buffered whole and may be read after the callback
// returns; closing it is optional. With ClientConfig.StreamResponseBody it
// may instead still be arriving, and is taken with ClientResponse.OnBody
// inside the callback. Cancelling req's context abandons the request, as does
// the client's Timeout, which then also ends a body that is streaming.
// Start sends a request as Do does and returns a Call that abandons it
// without a context.
//
// callback may run on any goroutine: an engine worker for a response, which
// the callback then holds until it returns, a timer for a timeout or a
// cancellation, the caller's own for a request Do rejects outright, or one of
// its own for a connection that failed. It must not block the engine for long,
// and when the engine runs handlers inline it runs on the event loop itself.
func (c *Client) Do(req *stdhttp.Request, callback func(*ClientResponse, error)) {
	c.do(req, callback, false, nil)
}

// Start is Do, and returns a Call that abandons the request, for a caller
// whose only reason to give req a context it can cancel would be to cancel
// the request: Do watches such a context with a context.AfterFunc, which
// costs a few allocations a request, and a Call costs none. A context that
// can be cancelled is still watched as Do watches it.
func (c *Client) Start(req *stdhttp.Request, callback func(*ClientResponse, error)) Call {
	return Call{r: c.do(req, callback, false, nil)}
}

// StartUpgrade is Start for a request that asks the server to switch to
// another protocol, as a WebSocket handshake does, with its Connection and
// Upgrade fields. If the server answers 101 Switching Protocols, the
// connection leaves the client, no longer counted against
// MaxConnsPerHost, and becomes a tunnel: ClientResponse.Tunnel, in the
// callback, is it, and handler takes what the server sends through it,
// beginning with OnTunnelOpen once the callback has returned and what
// arrived behind the 101 queued till then. Any other answer is an ordinary
// response, with no tunnel, and handler is never called.
//
// The switch needs an HTTP/1.1 connection: a request that would go out on a
// multiplexed one, HTTP/2's, fails with ErrNotUpgradable.
func (c *Client) StartUpgrade(req *stdhttp.Request, handler TunnelHandler, callback func(*ClientResponse, error)) Call {
	if handler == nil {
		if callback != nil {
			callback(nil, errors.New("http: invalid upgrade"))
		}
		return Call{}
	}
	return Call{r: c.do(req, callback, true, handler)}
}

// Call is a request Start sent, for abandoning it while it is in flight.
type Call struct {
	r *clientRequest
}

// Cancel abandons the request, as cancelling its context would: the callback
// hears err, or context.Canceled when err is nil, if it has not heard the
// outcome already, and a body that is streaming fails with it. It may be
// called from any goroutine, any number of times; once the request has been
// settled it does nothing, as does the zero Call and one for a request Start
// rejected outright.
func (c Call) Cancel(err error) {
	if c.r == nil {
		return
	}
	if err == nil {
		err = context.Canceled
	}
	c.r.abort(err)
}

// do is Do, with whole set when the response must arrive whole whatever the
// client's configuration says. It returns the request, or nil for one it
// rejected outright.
func (c *Client) do(req *stdhttp.Request, callback func(*ClientResponse, error), whole bool, tunnel TunnelHandler) *clientRequest {
	if callback == nil {
		callback = func(*ClientResponse, error) {}
	}
	target, err := requestTarget(req)
	if err != nil {
		closeRequestBody(req)
		callback(nil, err)
		return nil
	}
	// A small body is read once and kept, since whether it travels in
	// HTTP/1.1 or HTTP/2 framing is only known once a connection has been
	// chosen. Others are read as they are sent.
	var body []byte
	var upload *reqbody.Source
	if reqbody.Streams(req) {
		upload = reqbody.NewSource(req)
	} else if req.Body != nil && req.Body != stdhttp.NoBody {
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			callback(nil, err)
			return nil
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	var data []byte
	declared, chunked := int64(-1), false
	if upload != nil {
		declared = reqbody.Declared(req)
		chunked = declared < 0 || len(req.Trailer) > 0
		data, err = marshalStreamedHead(req, declared, chunked)
	} else {
		data, err = marshalRequest(req, body)
	}
	if err != nil {
		if upload != nil {
			// The request goes no further, and a transport closes its body
			// whatever becomes of it.
			upload.Close()
		}
		callback(nil, err)
		return nil
	}
	r := &clientRequest{req: req, data: data, body: body, upload: upload, declared: declared, chunked: chunked,
		callback: callback, whole: whole, tunnel: tunnel, headerWait: c.config.ResponseHeaderTimeout}
	r.headLen = len(data)
	if c.config.ExpectContinueTimeout > 0 && reqbody.ExpectsContinue(req, len(body) > 0 || upload != nil) {
		r.expect = true
		// HTTP/1 frames a body read whole by its Content-Length, so it is
		// what ends data, and the header is what comes before it.
		if upload == nil && bytes.HasSuffix(data, body) {
			r.headLen = len(data) - len(body)
		}
	}
	// Either hook can fire before it has been stored, so both are stored under
	// the lock finish reads them under.
	r.mu.Lock()
	if c.config.Timeout > 0 {
		r.timeoutAt = time.Now().Add(c.config.Timeout)
		r.timer = time.AfterFunc(c.config.Timeout, r.expire)
	}
	if ctx := req.Context(); ctx.Done() != nil {
		r.stopContext = context.AfterFunc(ctx, func() { r.abort(ctx.Err()) })
	}
	r.mu.Unlock()
	c.enqueue(target, r, false)
	return r
}

// marshalRequest frames req, whose body has already been read into body, for
// an HTTP/1 connection. net/http writes every request as HTTP/1.1; one whose
// ProtoMajor and ProtoMinor ask for HTTP/1.0 is written as that instead, with
// its body framed by Content-Length, since HTTP/1.0 has no chunks, and so
// without trailers.
func marshalRequest(req *stdhttp.Request, body []byte) ([]byte, error) {
	http10 := req.ProtoMajor == 1 && req.ProtoMinor == 0
	if data, ok := fastRequest(req, body, http10); ok {
		return data, nil
	}
	out := req
	if http10 {
		clone := *req
		clone.ContentLength = int64(len(body))
		clone.TransferEncoding = nil
		clone.Trailer = nil
		if len(body) == 0 {
			clone.Body = nil
		}
		out = &clone
	}
	var buf bytes.Buffer
	if err := out.Write(&buf); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	if http10 {
		lineEnd := bytes.Index(data, []byte("\r\n"))
		if lineEnd < 0 || !bytes.HasSuffix(data[:lineEnd], []byte(" HTTP/1.1")) {
			return nil, fmt.Errorf("http: cannot write request line %q as HTTP/1.0", data[:max(lineEnd, 0)])
		}
		data[lineEnd-1] = '0'
	}
	return data, nil
}

// marshalStreamedHead frames the header of req, whose body streams, for an
// HTTP/1 connection: with a Content-Length of declared, or as chunked, with
// the names of its trailers.
// closeRequestBody closes the body of a request that goes no further than
// Do, as a transport closes it whatever becomes of the request.
func closeRequestBody(req *stdhttp.Request) {
	if push := pushOf(req); push != nil {
		// Its producer learns from its next Write that nothing is sent.
		push.Stop()
		return
	}
	if req.Body != nil && req.Body != stdhttp.NoBody {
		_ = req.Body.Close()
	}
}

func marshalStreamedHead(req *stdhttp.Request, declared int64, chunked bool) ([]byte, error) {
	head, ok := fastStreamedHead(req)
	if !ok {
		clone := *req
		clone.Body, clone.GetBody = nil, nil
		clone.ContentLength = 0
		clone.TransferEncoding = nil
		clone.Trailer = nil
		var buf bytes.Buffer
		if err := clone.Write(&buf); err != nil {
			return nil, err
		}
		data := buf.Bytes()
		// net/http declares an empty body for a POST, PUT or PATCH without one.
		if i := bytes.Index(data, []byte("\r\nContent-Length: 0\r\n")); i >= 0 {
			data = append(data[:i+2], data[i+len("\r\nContent-Length: 0\r\n"):]...)
		}
		head = data[:len(data)-2]
	}
	end := []byte("\r\n")
	if !chunked {
		head = append(head, "Content-Length: "...)
		head = strconv.AppendInt(head, declared, 10)
		head = append(head, "\r\n"...)
		return append(head, end...), nil
	}
	head = append(head, "Transfer-Encoding: chunked\r\n"...)
	if len(req.Trailer) > 0 {
		names := make([]string, 0, len(req.Trailer))
		for name := range req.Trailer {
			names = append(names, textproto.CanonicalMIMEHeaderKey(name))
		}
		slices.Sort(names)
		head = append(head, "Trailer: "...)
		head = append(head, strings.Join(names, ",")...)
		head = append(head, "\r\n"...)
	}
	return append(head, end...), nil
}

// lastChunk ends a chunked body, with the trailer fields as they are now
// that the body has been read, which is when net/http reads them too.
func lastChunk(trailer stdhttp.Header) []byte {
	var buf bytes.Buffer
	buf.WriteString("0\r\n")
	_ = trailer.Write(&buf)
	buf.WriteString("\r\n")
	return buf.Bytes()
}

// Future is a request in flight, for callers that would rather wait on it than
// be called back.
type Future struct {
	done chan struct{}
	resp *stdhttp.Response
	err  error
}

// Go sends req like Do and returns a Future for its outcome.
func (c *Client) Go(req *stdhttp.Request) *Future {
	f := &Future{done: make(chan struct{})}
	c.do(req, func(resp *ClientResponse, err error) {
		if resp != nil {
			f.resp = resp.Response
		}
		f.err = err
		close(f.done)
	}, true, nil)
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
// connection fail with ErrClientClosed and idle connections are closed.
// Requests already on a connection finish as they would have.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	var idle []*clientConn
	var waiting []*clientRequest
	for _, h := range c.hosts {
		idle = append(idle, h.idle...)
		h.idle = nil
		waiting = append(waiting, h.waiting...)
		h.waiting = nil
		for _, cc := range h.h2 {
			if cc.streams == 0 {
				idle = append(idle, cc)
			}
		}
	}
	c.mu.Unlock()
	for _, cc := range idle {
		cc.discard()
	}
	for _, r := range waiting {
		r.finish(nil, ErrClientClosed)
	}
}

// hostTarget is where a request goes: the host:port to dial, and whether to
// speak TLS to it. Connections are pooled per target, so http and https to the
// same address never share one.
type hostTarget struct {
	addr   string
	secure bool
}

// requestTarget is where to send req.
func requestTarget(req *stdhttp.Request) (hostTarget, error) {
	if req.URL == nil {
		return hostTarget{}, errors.New("http: request has no URL")
	}
	var target hostTarget
	port := req.URL.Port()
	switch req.URL.Scheme {
	case "http":
		if port == "" {
			port = "80"
		}
	case "https":
		target.secure = true
		if port == "" {
			port = "443"
		}
	default:
		return hostTarget{}, fmt.Errorf("%w %q", ErrUnsupportedScheme, req.URL.Scheme)
	}
	host := req.URL.Hostname()
	if host == "" {
		return hostTarget{}, errors.New("http: request URL has no host")
	}
	target.addr = net.JoinHostPort(host, port)
	return target, nil
}

// hostPool is the client's state for one target. Guarded by Client.mu.
type hostPool struct {
	target hostTarget
	// open counts connections open or dialing, and dialing the second kind.
	open    int
	dialing int
	idle    []*clientConn
	waiting []*clientRequest
	// ended counts the requests in waiting that ended while they waited,
	// which stay there until they reach its head or are swept out, so that
	// the requests still waiting are counted without walking the queue.
	// Requests end without the client's lock, so it is atomic.
	ended atomic.Int64
	// h2 holds the HTTP/2 connections that take new streams, busy or not.
	// multiplexed records that the host has spoken HTTP/2, and probed that a
	// connection to it has told which protocol it speaks. Until then, and
	// for good when it is HTTP/2, one dial at a time is enough.
	h2          []*clientConn
	multiplexed bool
	probed      bool
}

// assignment pairs a request with the connection that will carry it. They are
// collected under the client's lock and carried out after it is released.
type assignment struct {
	conn *clientConn
	req  *clientRequest
}

// enqueue queues r for target and starts whatever that makes possible. A
// retried request goes to the front, since it has already waited its turn once.
func (c *Client) enqueue(target hostTarget, r *clientRequest, retry bool) {
	if retry {
		r.requeued()
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		r.finish(nil, ErrClientClosed)
		return
	}
	h := c.hosts[target]
	if h == nil {
		h = &hostPool{target: target}
		c.hosts[target] = h
	}
	if retry {
		// A request sent again was not waiting when it was let in, so the
		// cap does not turn it away now.
		h.waiting = append([]*clientRequest{r}, h.waiting...)
	} else {
		h.waiting = append(h.waiting, r)
	}
	r.queued(h)
	work, dials := c.dispatchLocked(h)
	refused := !retry && c.overPendingLocked(h, r)
	c.mu.Unlock()
	c.carryOut(h, work, dials)
	if refused {
		r.finish(nil, ErrTooManyPending)
	}
}

// overPendingLocked reports whether r, just queued and dispatched, waits
// beyond MaxPendingPerHost, and takes it out of the queue if so. What
// counts is the requests no connection is being dialed for, nor could be
// within MaxConnsPerHost: dispatchLocked has turned the room for
// connections into dials already, except for a host that may speak HTTP/2,
// which it dials once for, and whose room is counted here as the dials it
// would be were the host to answer in HTTP/1.
//
// It costs the same however long the queue: past the cap, every request a
// host sends is turned away here, under the client's lock, which the
// connections' responses need too.
func (c *Client) overPendingLocked(h *hostPool, r *clientRequest) bool {
	limit, conns := c.config.MaxPendingPerHost, c.config.MaxConnsPerHost
	if limit <= 0 || conns <= 0 {
		return false
	}
	covered := h.dialing + max(conns-h.open, 0)
	if len(h.waiting)-covered <= limit {
		return false
	}
	if ended := int(h.ended.Load()); 2*ended > len(h.waiting) {
		// Most of the queue has ended: sweep it, which takes as long as
		// the requests it drops, so that it does not hold them for long.
		sweepLocked(h)
	}
	if len(h.waiting)-int(h.ended.Load())-covered <= limit {
		return false
	}
	// r is the last of the queue, unless dispatchLocked handed it a
	// connection, in which case it waits for nothing.
	if n := len(h.waiting); n > 0 && h.waiting[n-1] == r {
		h.waiting[n-1] = nil
		h.waiting = h.waiting[:n-1]
		h.dequeued(r)
		return true
	}
	return false
}

// sweepLocked drops the requests that ended while they waited, which the
// queue otherwise keeps until they reach its head.
func sweepLocked(h *hostPool) {
	kept := h.waiting[:0]
	for _, r := range h.waiting {
		if r.done.Load() {
			h.dequeued(r)
		} else {
			kept = append(kept, r)
		}
	}
	clear(h.waiting[len(kept):])
	h.waiting = kept
}

// dequeued settles the count of ended requests for r, just taken out of
// h's queue. The client's lock is held.
func (h *hostPool) dequeued(r *clientRequest) {
	if r.waitingIn.Swap(nil) == nil {
		// It ended while it waited, and markDone counted it.
		h.ended.Add(-1)
	}
}

// popLocked takes the request at the head of h's queue out.
func (h *hostPool) popLocked() *clientRequest {
	r := h.waiting[0]
	h.waiting = h.waiting[1:]
	h.dequeued(r)
	return r
}

// dispatchLocked matches waiting requests with idle connections, and reports
// how many connections to dial for the requests left over.
func (c *Client) dispatchLocked(h *hostPool) (work []assignment, dials int) {
	for len(h.waiting) > 0 {
		r := h.waiting[0]
		if r.done.Load() {
			h.popLocked()
			continue
		}
		cc := leastLoadedLocked(h.h2)
		if cc != nil {
			cc.streams++
		} else if n := len(h.idle); n > 0 {
			cc = h.idle[n-1]
			h.idle = h.idle[:n-1]
		} else {
			break
		}
		cc.takeIdleLocked()
		h.popLocked()
		work = append(work, assignment{conn: cc, req: r})
	}
	need := len(h.waiting) - h.dialing
	if h.multiplexed || !h.probed && c.mayMultiplex(h.target) {
		// The next connection may well be HTTP/2, and carry all of them.
		need = min(need, 1-h.dialing)
	}
	for ; need > 0; need-- {
		if c.config.MaxConnsPerHost > 0 && h.open >= c.config.MaxConnsPerHost {
			break
		}
		h.open++
		h.dialing++
		dials++
	}
	if len(h.waiting) == 0 {
		// Let the queue's array go instead of keeping the longest it has been.
		h.waiting = nil
	}
	return work, dials
}

// leastLoadedLocked picks the HTTP/2 connection to open the next stream on:
// the one with the most of its streams to spare, so that a host's requests
// spread over the connections it has rather than piling onto the first until
// the server's limit, where a connection slowed by its socket, or by large
// bodies holding its window, would carry most of them while the next sat
// nearly idle. Room is counted against what each connection's server
// allows, which may differ, and a tie goes to the earlier connection, so a
// lone connection is the one used as before. A second connection is only
// dialed once every one is full, so spreading over them costs nothing while
// one suffices; once there are several, they all stay in use while the
// requests keep them busy, and close once the host goes quiet for
// IdleConnTimeout. It returns nil when none has room. Callers hold
// Client.mu.
func leastLoadedLocked(conns []*clientConn) *clientConn {
	var best *clientConn
	room := 0
	for _, m := range conns {
		if spare := m.maxStreams - m.streams; spare > room {
			best, room = m, spare
		}
	}
	return best
}

// mayMultiplex reports whether a connection to target might speak HTTP/2.
func (c *Client) mayMultiplex(target hostTarget) bool {
	if !target.secure {
		return c.config.UnencryptedHTTP2
	}
	return c.tlsConfig != nil && slices.Contains(c.tlsConfig.NextProtos, "h2")
}

func (c *Client) carryOut(h *hostPool, work []assignment, dials int) {
	for _, a := range work {
		a.conn.send(a.req)
	}
	for ; dials > 0; dials-- {
		c.dial(h)
	}
}

func (c *Client) dial(h *hostPool) {
	cc := &clientConn{client: c, host: h}
	cc.parser.maxHeader = c.config.MaxResponseHeaderBytes
	cc.parser.maxBody = c.config.MaxResponseBodyBytes
	cc.parser.threshold = c.config.StreamResponseBodyThreshold
	cc.parser.maxStreamed = c.config.MaxStreamedBodyBytes
	cc.parser.sink = cc
	done := func(_ *fib.Connection, err error) { c.dialed(cc, err) }
	var err error
	if h.target.secure {
		// The dial is settled once the handshake has told which protocol to
		// speak, in OnHandshake, or failed, in OnClose.
		done = func(_ *fib.Connection, err error) {
			if err != nil {
				c.dialed(cc, err)
			}
		}
		err = fibtls.DialWithOptions(c.engine, "tcp", h.target.addr, c.config.DialTimeout, c.tlsConfig,
			fibtls.DialOptions{Blocking: c.config.BlockingTLSHandshake, Dial: c.config.Dial}, cc, done)
	} else {
		if c.config.UnencryptedHTTP2 {
			done = func(_ *fib.Connection, err error) {
				if err == nil {
					cc.startH2()
				}
				c.dialed(cc, err)
			}
		}
		dial := c.config.Dial
		if dial == nil {
			dial = (*fib.Engine).DialWithHandler
		}
		err = dial(c.engine, "tcp", h.target.addr, c.config.DialTimeout, cc, done)
	}
	if err != nil {
		c.dialed(cc, err)
	}
}

// dialed settles a dial. A new connection goes to work straight away; a failed
// dial fails the request that has waited longest, since it is the one most
// likely to have caused it, and the rest try again.
func (c *Client) dialed(cc *clientConn, err error) {
	h := cc.host
	cc.mu.Lock()
	settled := cc.settled
	cc.settled = true
	cc.mu.Unlock()
	if settled {
		return
	}
	c.mu.Lock()
	h.dialing--
	h.probed = h.probed || err == nil
	if err == nil && cc.h2 != nil {
		h.multiplexed = true
		if !cc.isClosed() {
			h.h2 = append(h.h2, cc)
		}
		work, dials := c.dispatchLocked(h)
		discard := c.idleH2Locked(cc)
		c.mu.Unlock()
		if discard {
			cc.discard()
		}
		c.carryOut(h, work, dials)
		return
	}
	if err == nil {
		c.mu.Unlock()
		c.release(cc)
		return
	}
	h.open--
	var failed *clientRequest
	for len(h.waiting) > 0 && failed == nil {
		if r := h.popLocked(); !r.done.Load() {
			failed = r
		}
	}
	work, dials := c.dispatchLocked(h)
	c.mu.Unlock()
	if failed != nil {
		// Dial callbacks run on the event loop, which a request's callback must
		// never be allowed to hold up, so it runs on the client pool.
		sidepool.Go(func() { failed.finish(nil, err) })
	}
	c.carryOut(h, work, dials)
}

// release hands a connection with nothing in flight to the next waiting
// request, or keeps it idle, or closes it if there is room for neither.
func (c *Client) release(cc *clientConn) {
	h := cc.host
	c.mu.Lock()
	if cc.isClosed() {
		c.mu.Unlock()
		return
	}
	for len(h.waiting) > 0 {
		r := h.popLocked()
		if !r.done.Load() {
			c.mu.Unlock()
			cc.send(r)
			return
		}
	}
	keep := !c.closed && len(h.idle) < c.config.MaxIdleConnsPerHost
	if keep {
		h.idle = append(h.idle, cc)
		c.idleLocked(cc)
	}
	c.mu.Unlock()
	if !keep {
		cc.discard()
	}
}

// idleLocked starts the idle clock on a connection gone idle: it closes once
// it has been idle for IdleConnTimeout. The clock is idleUntil; the timer is
// set only when none is pending, so that a connection taken and given back on
// every request, as a busy one is, does not reset it each time: one pending
// fires, finds the connection taken or idle until later, and is set again for
// what is left then. Callers hold c.mu.
func (c *Client) idleLocked(cc *clientConn) {
	d := c.config.IdleConnTimeout
	if d <= 0 {
		return
	}
	cc.idleUntil = time.Now().Add(d)
	switch {
	case cc.idleTimer == nil:
		cc.idleTimer = time.AfterFunc(d, func() { c.expireIdle(cc) })
	case !cc.idlePending:
		cc.idleTimer.Reset(d)
	}
	cc.idlePending = true
}

// takeIdleLocked stops the idle clock on a connection a request has taken,
// leaving a pending timer to find it taken when it fires. Callers hold
// Client.mu.
func (cc *clientConn) takeIdleLocked() {
	cc.idleUntil = time.Time{}
}

// stopIdleLocked stops the idle clock and its timer on a connection that has
// closed. Callers hold Client.mu.
func (cc *clientConn) stopIdleLocked() {
	cc.idleUntil = time.Time{}
	if cc.idlePending {
		cc.idlePending = false
		cc.idleTimer.Stop()
	}
}

// expireIdle closes a connection that sat idle for IdleConnTimeout, unless a
// request took it in the meantime. One that went idle again after the timer
// was set is idle until later, and the timer is set again for then.
func (c *Client) expireIdle(cc *clientConn) {
	c.mu.Lock()
	if !cc.idlePending {
		// Stopped as the connection closed, after this call had begun.
		c.mu.Unlock()
		return
	}
	cc.idlePending = false
	now := time.Now()
	if !cc.idleUntil.IsZero() && now.Before(cc.idleUntil) {
		cc.idlePending = true
		cc.idleTimer.Reset(cc.idleUntil.Sub(now))
		c.mu.Unlock()
		return
	}
	idle := !cc.idleUntil.IsZero()
	if idle && cc.h2 != nil {
		idle = cc.streams == 0 && removeConn(&cc.host.h2, cc)
	} else if idle {
		idle = removeConn(&cc.host.idle, cc)
	}
	if idle {
		cc.idleUntil = time.Time{}
	}
	c.mu.Unlock()
	if idle {
		cc.discard()
	}
}

// streamDone gives back the stream an HTTP/2 connection carried, and hands
// the room to a waiting request.
func (c *Client) streamDone(cc *clientConn) {
	h := cc.host
	c.mu.Lock()
	cc.streams--
	work, dials := c.dispatchLocked(h)
	discard := c.idleH2Locked(cc)
	c.mu.Unlock()
	if discard {
		cc.discard()
	}
	c.carryOut(h, work, dials)
}

// idleH2Locked starts the idle clock on an HTTP/2 connection left with no
// streams, or reports that it should be closed because the client keeps no
// more idle connections than MaxIdleConnsPerHost.
func (c *Client) idleH2Locked(cc *clientConn) bool {
	h := cc.host
	if cc.streams > 0 || !cc.idleUntil.IsZero() {
		return false
	}
	idle := 0
	for _, m := range h.h2 {
		if m.streams == 0 {
			idle++
		}
	}
	if c.closed || idle > c.config.MaxIdleConnsPerHost {
		return removeConn(&h.h2, cc)
	}
	c.idleLocked(cc)
	return false
}

// retire stops an HTTP/2 connection taking new streams, once the server has
// said it is going away.
func (c *Client) retire(cc *clientConn) {
	c.mu.Lock()
	removeConn(&cc.host.h2, cc)
	c.mu.Unlock()
}

// forget drops a closed connection from its host and lets waiting requests
// have its place.
func (c *Client) forget(cc *clientConn) {
	h := cc.host
	c.mu.Lock()
	removeConn(&h.idle, cc)
	removeConn(&h.h2, cc)
	cc.stopIdleLocked()
	h.open--
	work, dials := c.dispatchLocked(h)
	c.mu.Unlock()
	c.carryOut(h, work, dials)
}

func removeConn(conns *[]*clientConn, cc *clientConn) bool {
	for i, other := range *conns {
		if other == cc {
			*conns = append((*conns)[:i], (*conns)[i+1:]...)
			return true
		}
	}
	return false
}

// clientRequest is one call to Do, from the queue to its callback.
type clientRequest struct {
	req *stdhttp.Request
	// data is the request in HTTP/1.1 framing, and body its body alone, which
	// is what HTTP/2 sends after the header block.
	data []byte
	body []byte
	// upload is the body of a request whose body streams, sent after data,
	// which then holds only the header, declared its length or -1, and
	// chunked whether HTTP/1 frames it in chunks.
	upload   *reqbody.Source
	declared int64
	chunked  bool
	// expect says the request asked for 100-continue, and has a body to hold
	// back until the server answers it (see ClientConfig.
	// ExpectContinueTimeout). headLen is how much of data is the header,
	// which goes first: all of it, but for a body read whole.
	expect   bool
	headLen  int
	callback func(*ClientResponse, error)
	// whole asks for the response to arrive whole even when the client would
	// stream it. tunnel is StartUpgrade's handler, for a 101 to switch the
	// connection to.
	whole  bool
	tunnel TunnelHandler
	done   atomic.Bool
	// waitingIn is the host whose queue the request waits in, or nil: set
	// as it is queued, and cleared by whichever comes first of its leaving
	// the queue and its ending, which then counts it in the host's ended.
	waitingIn atomic.Pointer[hostPool]
	// retried records that the request has already been sent again once, after
	// a reused connection turned out to be closed.
	retried bool
	// streamID is the HTTP/2 stream carrying the request, guarded by that
	// connection's lock.
	streamID uint32
	mu       sync.Mutex
	// conn is the connection carrying the request, so that abandoning the
	// request can also abandon its half-finished exchange. timer and
	// stopContext are the hooks that abandon it. Guarded by mu.
	conn        *clientConn
	timer       *time.Timer
	stopContext func() bool
	// timeoutAt is when ClientConfig.Timeout runs out, and headerAt when
	// ResponseHeaderTimeout, headerWait, does, from the request's end until
	// its header has arrived, which headerDone records; zero when there is
	// none. timer fires at the earlier of the two, or at one passed since.
	// Guarded by mu.
	timeoutAt  time.Time
	headerAt   time.Time
	headerWait time.Duration
	headerDone bool
	// stream is the body stream of the response, once the callback has run on
	// it, which fired says it has. Guarded by mu.
	stream *ResponseStream
	fired  bool
}

// finish reports the outcome, once: whichever of response, failure, timeout
// and cancellation comes first is the one the callback hears.
//
// A response that streams has its callback run by the stream, before it is
// complete, and finish then only reports how it ended: an error goes to the
// stream, and a completion is the stream's own to report.
func (r *clientRequest) finish(resp *stdhttp.Response, err error) bool {
	if !r.markDone() {
		return false
	}
	r.mu.Lock()
	stream, fired := r.stream, r.fired
	r.mu.Unlock()
	if fired {
		if err != nil {
			stream.Fail(err)
		}
		return true
	}
	if err != nil {
		r.callback(nil, err)
		return true
	}
	DeliverResponse(resp, r.abort, r.callback)
	return true
}

// markDone settles the request, once, and stops what would abandon it.
func (r *clientRequest) markDone() bool {
	if !r.done.CompareAndSwap(false, true) {
		return false
	}
	if h := r.waitingIn.Swap(nil); h != nil {
		// It ends in the queue, where it stays until taken out.
		h.ended.Add(1)
	}
	r.mu.Lock()
	timer, stopContext := r.timer, r.stopContext
	r.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if stopContext != nil {
		stopContext()
	}
	if r.upload != nil {
		// Whatever of the body is still being sent is not wanted now, and
		// closing it ends a Read on it that waits.
		r.upload.Close()
	}
	return true
}

// requestSent starts the wait for the response's header, now that the whole
// request has been handed to the connection. A request sent again, after a
// reused connection turned out to be closed, starts it again.
func (r *clientRequest) requestSent() {
	if r.headerWait <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.headerDone || r.done.Load() {
		return
	}
	r.headerAt = time.Now().Add(r.headerWait)
	switch {
	case !r.timeoutAt.IsZero() && !r.headerAt.Before(r.timeoutAt):
		// Timeout ends the request first.
	case r.timer == nil:
		r.timer = time.AfterFunc(r.headerWait, r.expire)
	default:
		r.timer.Reset(r.headerWait)
	}
}

// queued records that r waits in h's queue, where enqueue has just put it.
// One that ended before it got there is counted as ended at once, by this
// or by markDone, whichever sees the other.
func (r *clientRequest) queued(h *hostPool) {
	r.waitingIn.Store(h)
	if r.done.Load() && r.waitingIn.CompareAndSwap(h, nil) {
		h.ended.Add(1)
	}
}

// requeued stops the wait for the response's header of a request that goes
// back to the queue, so that waiting for another connection does not count
// against ResponseHeaderTimeout; requestSent starts it again. The timer, if
// set for the header, is left to fire and find nothing due, as in
// headerArrived.
func (r *clientRequest) requeued() {
	if r.headerWait <= 0 {
		return
	}
	r.mu.Lock()
	r.headerAt = time.Time{}
	r.mu.Unlock()
}

// headerArrived ends the wait for the response's header. The timer, if it
// was set for the header, is left to fire then and find Timeout still ahead,
// which costs less than moving it on every request.
func (r *clientRequest) headerArrived() {
	if r.headerWait <= 0 {
		return
	}
	r.mu.Lock()
	r.headerDone = true
	r.headerAt = time.Time{}
	if r.timeoutAt.IsZero() && r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
}

// expire fails the request whose Timeout or ResponseHeaderTimeout has run
// out, or sets the timer again for whichever of them is still ahead.
func (r *clientRequest) expire() {
	r.mu.Lock()
	now := time.Now()
	var err error
	next := r.timeoutAt
	switch {
	case !r.headerAt.IsZero() && !now.Before(r.headerAt):
		err = ErrResponseHeaderTimeout
	case !r.timeoutAt.IsZero() && !now.Before(r.timeoutAt):
		err = errRequestTimeout
	case next.IsZero() || !r.headerAt.IsZero() && r.headerAt.Before(next):
		next = r.headerAt
	}
	if err == nil && !next.IsZero() && !r.done.Load() {
		r.timer.Reset(next.Sub(now))
	}
	r.mu.Unlock()
	if err != nil {
		r.abort(err)
	}
}

// begin records that the stream is about to run the callback. It reports false
// if the request has been settled, in which case there is nothing to call.
func (r *clientRequest) begin(s *ResponseStream) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done.Load() {
		return false
	}
	r.stream, r.fired = s, true
	return true
}

// abort fails the request from outside the exchange. A connection left in the
// middle of it cannot carry another request, so it is closed.
func (r *clientRequest) abort(err error) {
	if !r.finish(nil, err) {
		return
	}
	r.mu.Lock()
	cc := r.conn
	r.conn = nil
	r.mu.Unlock()
	if cc == nil {
		return
	}
	if cc.h2 != nil {
		// Only this request's stream is abandoned, not the connection.
		cc.h2.cancel(r)
		return
	}
	cc.discard()
}

// attach records the connection carrying the request. It reports false if the
// request has already been settled, in which case it must not be sent.
func (r *clientRequest) attach(cc *clientConn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done.Load() {
		return false
	}
	r.conn = cc
	return true
}

func (r *clientRequest) detach() {
	r.mu.Lock()
	r.conn = nil
	r.mu.Unlock()
}

// retryable reports whether a request that may have reached the server can be
// sent again without the server acting on it twice.
func (r *clientRequest) retryable() bool {
	if r.upload != nil && r.req.GetBody == nil {
		// Its body has gone, at least in part.
		return false
	}
	switch r.req.Method {
	case "", stdhttp.MethodGet, stdhttp.MethodHead, stdhttp.MethodOptions, stdhttp.MethodTrace:
		return !r.retried
	}
	return false
}

// clientConn is one connection the client dialed. It is the connection's
// handler, so everything the engine reports about it arrives here.
type clientConn struct {
	client *Client
	host   *hostPool
	conn   *fib.Connection
	// feed is the tunnel the connection became after a 101; see
	// switchTunnel. Set and read only by the worker reading the connection.
	feed *TunnelFeed
	// parser is used only by the worker reading the connection, and so are
	// feeding, which says OnData is in the parser, and deferred, the stream
	// whose end was held back until it is out.
	parser   responseParser
	feeding  bool
	deferred *ResponseStream
	mu       sync.Mutex
	// current is the request in flight, if any. sent records that the
	// connection has carried a request before this one, and received that the
	// server has answered this one with at least a byte. uploading is the
	// request whose body is still being sent, which leaves the connection
	// unfit to carry another.
	current   *clientRequest
	uploading *clientRequest
	// gate holds back the body of current, which expects 100-continue.
	gate     *reqbody.Gate
	sent     bool
	reused   bool
	received bool
	closed   bool
	// idleUntil is when the connection, idle, is to close, and zero while it
	// is not idle; idleTimer, made the first time it goes idle, fires then or
	// earlier, and idlePending says it is set (see idleLocked). Guarded by
	// Client.mu.
	idleUntil   time.Time
	idleTimer   *time.Timer
	idlePending bool
	// settled records that the dial has been reported to the client, which
	// for TLS happens once the handshake is done. Guarded by mu.
	settled bool
	// stream is the body stream of the response being read, if it streams. It
	// is used only by the worker reading the connection.
	stream *ResponseStream
	// h2 is set once the connection is known to speak HTTP/2, before it
	// carries anything, and never changes after. streams counts the streams
	// it carries or has been given, and maxStreams how many the server
	// allows. Both are guarded by Client.mu.
	h2         *h2ClientConn
	streams    int
	maxStreams int
}

func (cc *clientConn) isClosed() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.closed
}

// send puts r on the connection. A connection that closed in the meantime
// hands r back to the queue.
func (cc *clientConn) send(r *clientRequest) {
	if cc.h2 != nil {
		cc.h2.send(r)
		return
	}
	head := r.data
	var gate *reqbody.Gate
	if r.expect && (r.upload != nil || r.headLen < len(r.data)) {
		head = r.data[:r.headLen]
		gate = reqbody.NewGate(func() { cc.sendHeld(r) })
	}
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		cc.client.enqueue(cc.host.target, r, true)
		return
	}
	cc.current = r
	cc.gate = gate
	cc.reused = cc.sent
	cc.sent = true
	cc.received = false
	cc.parser.reset()
	cc.parser.streamOK = cc.client.config.StreamResponseBody && !r.whole
	cc.stream = nil
	cc.mu.Unlock()
	if !r.attach(cc) {
		// Settled while it waited: give the connection to the next request.
		cc.mu.Lock()
		cc.current = nil
		cc.mu.Unlock()
		cc.client.release(cc)
		return
	}
	if r.upload != nil {
		cc.mu.Lock()
		cc.uploading = r
		cc.mu.Unlock()
	}
	// A failed send closes the connection, and OnClose then settles r.
	if cc.conn.Send(head) != nil {
		return
	}
	switch {
	case gate != nil:
		// The body waits for the server's 100, or for the wait to run out.
		gate.Start(cc.client.config.ExpectContinueTimeout)
	case r.upload == nil:
		r.requestSent()
	default:
		cc.startUpload(r)
	}
}

// startUpload starts sending the body of r, which streams.
func (cc *clientConn) startUpload(r *clientRequest) {
	if push := r.upload.Pusher(); push != nil {
		// A pushed body is sent as it is written and as the connection
		// drains, with nothing waiting for either.
		attachPush(r, push, &h1Sink{cc: cc, r: r, push: push})
		return
	}
	// The body is read by a goroutine of its own, since a Read may wait
	// as long as the body's producer likes, and so may the sending, for
	// the server; neither may hold up a worker or a pool.
	go cc.sendBody(r)
}

// sendHeld sends the body that r's gate held back, once the server has
// answered 100 (Continue) or the wait has run out, unless the connection has
// gone on without r meanwhile.
func (cc *clientConn) sendHeld(r *clientRequest) {
	cc.mu.Lock()
	live := cc.current == r && !cc.closed
	cc.mu.Unlock()
	if !live {
		return
	}
	if r.upload != nil {
		cc.startUpload(r)
		return
	}
	if cc.conn.Send(r.data[r.headLen:]) == nil {
		r.requestSent()
	}
}

// uploadQueued is how much of a streamed body a client connection may hold
// unsent before the next piece waits for some of it to go. With the piece
// that follows, at most reqbody.ChunkSize, the backlog stays under fib's
// default write watermark (64KB), at which a connection stops reading until
// its queue drains: a server that answers before it has read the body, and
// then reads no more of it, may never let the queue drain, and its answer
// would then go unread. On Windows, where an overlapped send holds what it
// carries until it completes and the pump keeps queueing behind it, a body
// allowed up to the watermark left that answer unread until the server reset
// the connection.
const uploadQueued = 16 << 10

// sendBody sends the body of r, which streams, and waits whenever
// uploadQueued of it is queued for the socket, so that the server's pace is
// the body's. It ends early when the connection closes, which a response
// that arrives before the body has gone does (see OnData).
func (cc *clientConn) sendBody(r *clientRequest) {
	rc, err := r.upload.Take()
	if err == nil {
		err = reqbody.Pump(rc, r.declared, func(p []byte) error {
			var err error
			if r.chunked {
				out := appendChunk(bufferpool.Get(len(p) + 20)[:0], p)
				err = cc.conn.Send(out)
				bufferpool.Put(out)
			} else {
				err = cc.conn.Send(p)
			}
			if err == nil {
				err = outputwait.Wait(cc.conn, uploadQueued)
			}
			return err
		})
		if err == nil && r.chunked {
			err = cc.conn.Send(lastChunk(r.req.Trailer))
		}
	}
	if err == nil {
		r.requestSent()
	}
	cc.mu.Lock()
	if cc.uploading == r {
		cc.uploading = nil
	}
	cc.mu.Unlock()
	if err == nil {
		return
	}
	var bodyErr *reqbody.BodyError
	if errors.As(err, &bodyErr) {
		err = bodyErr.Err
	} else if !errors.Is(err, reqbody.ErrNotReplayable) {
		// The connection failed, which OnClose reports.
		return
	}
	r.abort(err)
}

// discard closes the connection without handing it back.
func (cc *clientConn) discard() {
	cc.mu.Lock()
	cc.closed = true
	cc.current = nil
	cc.uploading = nil
	gate := cc.gate
	cc.gate = nil
	cc.mu.Unlock()
	gate.Shut()
	cc.conn.Close()
}

func (cc *clientConn) OnOpen(conn *fib.Connection) { cc.conn = conn }

func (cc *clientConn) OnPriorityData(*fib.Connection, []byte) {}

// OnHandshake settles a TLS dial, with the protocol ALPN chose.
func (cc *clientConn) OnHandshake(_ *fib.Connection, state tls.ConnectionState) {
	if state.NegotiatedProtocol == "h2" {
		cc.startH2()
	}
	cc.client.dialed(cc, nil)
}

func (cc *clientConn) OnData(conn *fib.Connection, data []byte) {
	if cc.h2 != nil {
		cc.h2.feed(data)
		return
	}
	if cc.feed != nil {
		// The connection switched protocols, and is a tunnel's.
		_ = cc.feed.Write(data)
		return
	}
	cc.mu.Lock()
	r := cc.current
	gate := cc.gate
	closed := cc.closed
	cc.received = true
	cc.mu.Unlock()
	if closed {
		return
	}
	if r == nil {
		// Nothing was asked on this connection, so these bytes answer nothing
		// and the stream can no longer be trusted.
		cc.discard()
		return
	}
	// The end of a streamed body reaches its callback after the connection is
	// released or discarded (see bodyEnd).
	cc.feeding = true
	resp, err := cc.parser.feed(data, r.req)
	cc.feeding = false
	if err != nil {
		r.detach()
		cc.discard()
		cc.endDeferred()
		r.finish(nil, err)
		return
	}
	// A final response is the answer whether or not the body has gone, and
	// one that comes before the body has is the server turning it down; a
	// 100 before any is the server asking for it.
	held := false
	if resp != nil || cc.parser.head != nil {
		r.headerArrived()
		held = gate.Shut()
	} else if cc.parser.continued {
		gate.Open()
	}
	cc.parser.continued = false
	if resp == nil {
		cc.endDeferred()
		return
	}
	if r.tunnel != nil && resp.StatusCode == stdhttp.StatusSwitchingProtocols {
		cc.switchTunnel(conn, r, resp)
		return
	}
	// Bytes past the response, a protocol switch, or either side asking to
	// close all mean the connection cannot carry another request.
	// An HTTP/1.0 request only keeps its connection if it asked to.
	reusable := !resp.Close && !r.req.Close && !cc.parser.buffered() &&
		resp.StatusCode != stdhttp.StatusSwitchingProtocols &&
		(r.req.ProtoAtLeast(1, 1) || headerHasToken(r.req.Header, "Connection", "keep-alive"))
	cc.mu.Lock()
	cc.current = nil
	cc.gate = nil
	if held {
		// The header went without the body it announced, which the server
		// would take the next request for.
		reusable = false
	}
	if cc.uploading == r {
		// The server answered before the body had all gone: it does not want
		// the rest, and the connection, part way through the body, cannot
		// carry another request. Closing it ends the sending.
		cc.uploading = nil
		reusable = false
	}
	cc.mu.Unlock()
	r.detach()
	// Read before the connection is released: its next request resets it.
	streamed := cc.parser.streamedDone
	if cc.isClosed() {
		// The callback of a streaming response gave up on it already.
		reusable = false
	}
	if reusable {
		cc.client.release(cc)
	} else {
		cc.discard()
	}
	cc.endDeferred()
	if !streamed {
		r.finish(resp, nil)
	}
}

// switchTunnel makes cc, whose request r StartUpgrade sent and the server
// answered with 101, r's tunnel: it leaves the client, what follows the 101
// is the tunnel's first data, and r's callback gets the response with the
// tunnel before handler hears from it. It runs on the worker reading the
// connection, which holds every OnData until it returns.
func (cc *clientConn) switchTunnel(conn *fib.Connection, r *clientRequest, resp *stdhttp.Response) {
	rest := cc.parser.takeBuffered()
	feed := NewTunnelFeed(conn, conn, 0)
	cc.mu.Lock()
	cc.current = nil
	cc.feed = feed
	cc.mu.Unlock()
	r.detach()
	cc.client.forget(cc)
	_ = feed.Write(rest)
	bufferpool.Put(rest)
	if !r.markDone() {
		// Abandoned as the answer came: the tunnel goes with it.
		conn.Close()
		return
	}
	t := feed.Tunnel()
	r.callback(&ClientResponse{Response: resp, tunnel: t}, nil)
	t.open(r.tunnel)
}

// endDeferred ends the streamed body whose end bodyEnd held back.
func (cc *clientConn) endDeferred() {
	if s := cc.deferred; s != nil {
		cc.deferred = nil
		s.End()
	}
}

// newClientStream returns the stream of r's response, whose body is arriving
// on cc. A transport that paces its sender by window passes credit; otherwise
// the connection's reads are held.
func newClientStream(cc *clientConn, r *clientRequest, head *stdhttp.Response, credit func(int64)) *ResponseStream {
	config := &cc.client.config
	hooks := StreamHooks{
		Begin:    r.begin,
		Callback: func(cr *ClientResponse) { r.callback(cr, nil) },
		Whole:    func(resp *stdhttp.Response) { r.finish(resp, nil) },
		Done:     r.markDone,
		Abort:    r.abort,
		Credit:   credit,
	}
	if credit == nil {
		hooks.Hold = cc.conn.HoldReads
	}
	return NewResponseStream(head, r.req, StreamOptions{
		Threshold: streamThreshold(config.StreamResponseBodyThreshold, config.StreamUnknownLengthAtHeader, head),
		Buffer:    config.StreamResponseBodyBuffer,
	}, hooks)
}

// streamThreshold is how much of head's body a stream collects before its
// callback runs: none for a body of no declared length when atHeader says
// so, and threshold otherwise.
func streamThreshold(threshold int64, atHeader bool, head *stdhttp.Response) int64 {
	if atHeader && head.ContentLength < 0 {
		return 0
	}
	return threshold
}

// bodyStart, bodyData and bodyEnd take a streamed body from the parser.
func (cc *clientConn) bodyStart(head *stdhttp.Response) {
	cc.mu.Lock()
	r := cc.current
	cc.mu.Unlock()
	if r == nil {
		return
	}
	cc.stream = newClientStream(cc, r, head, nil)
	cc.stream.Start()
}

func (cc *clientConn) bodyData(data []byte) {
	if s := cc.stream; s != nil {
		s.Data(data)
	}
}

// bodyEnd ends the stream. While OnData is feeding the parser it is held until
// OnData has released the connection, since the callback that learns of the
// end may send the next request at once, and that one should find the
// connection idle rather than dial another.
func (cc *clientConn) bodyEnd() {
	s := cc.stream
	cc.stream = nil
	if s == nil {
		return
	}
	if cc.feeding {
		cc.deferred = s
		return
	}
	s.End()
}

func (cc *clientConn) OnClose(_ *fib.Connection, err error) {
	if cc.feed != nil {
		// A tunnel's connection, which the client forgot when it switched.
		cc.feed.End(err)
		return
	}
	cc.mu.Lock()
	settled := cc.settled
	cc.mu.Unlock()
	if !settled {
		// A TLS handshake that failed: the dial failed.
		cc.mu.Lock()
		cc.closed = true
		cc.mu.Unlock()
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		cc.client.dialed(cc, err)
		return
	}
	if cc.h2 != nil {
		cc.mu.Lock()
		cc.closed = true
		cc.mu.Unlock()
		cc.client.forget(cc)
		cc.h2.shutdown(err)
		return
	}
	cc.mu.Lock()
	r := cc.current
	cc.current = nil
	cc.closed = true
	retry := r != nil && cc.reused && !cc.received
	cc.mu.Unlock()
	cc.client.forget(cc)
	if r == nil {
		return
	}
	r.detach()
	if cc.parser.untilClose() && (err == nil || err == io.EOF) {
		// The close ends the body. A streaming one tells its callback, which
		// must not hold up the event loop OnClose may be on.
		sidepool.Go(func() {
			if resp := cc.parser.finish(); resp != nil && !cc.parser.streamedDone {
				r.finish(resp, nil)
			}
		})
		return
	}
	if retry && r.retryable() {
		// A kept connection the server had already closed. Nothing came back,
		// so a request that is safe to repeat is sent again on another.
		r.retried = true
		cc.client.enqueue(cc.host.target, r, true)
		return
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	// OnClose runs in the connection's last round, which may be on the event
	// loop, and the callback must not hold that up, so it runs on the client
	// pool.
	sidepool.Go(func() { r.finish(nil, err) })
}
