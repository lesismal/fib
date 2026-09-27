// Package http provides incremental HTTP/1.x request parsing and response
// handling for the parent epoll package.
package http

import (
	"bufio"
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

var (
	ErrHeaderTooLarge = errors.New("http: request header too large")
	ErrBodyTooLarge   = errors.New("http: request body too large")
	ErrMalformed      = errors.New("http: malformed request")
	// ErrUnsupportedTransferEncoding is a request whose body is framed by a
	// transfer coding other than chunked, which the server answers with 501
	// (RFC 9112 section 6.1). errors.Is matches it against ErrMalformed too.
	ErrUnsupportedTransferEncoding = fmt.Errorf("%w: unsupported transfer encoding", ErrMalformed)
)

// requestReader is the pair every request is parsed through: a bytes.Reader
// over the buffered bytes, and the bufio.Reader net/http reads it with. They
// are pooled together because the bufio.Reader keeps a pointer to the other,
// so a bytes.Reader in a local variable escapes to the heap on every request,
// and pointing the bufio.Reader back at something harmless at the end needs a
// reader to point it at. Pooled as a pair, parsing a request borrows both and
// allocates neither.
type requestReader struct {
	source bytes.Reader
	buf    *bufio.Reader
}

var requestReaderPool = sync.Pool{New: func() any {
	r := new(requestReader)
	r.buf = bufio.NewReaderSize(&r.source, 1024)
	return r
}}

func acquireRequestReader(data []byte) *requestReader {
	r := requestReaderPool.Get().(*requestReader)
	r.source.Reset(data)
	r.buf.Reset(&r.source)
	return r
}

// releaseRequestReader hands the pair back, with neither of them left holding
// the request's bytes.
func releaseRequestReader(r *requestReader) {
	r.source.Reset(nil)
	r.buf.Reset(&r.source)
	requestReaderPool.Put(r)
}

const maxRetainedBuffer = 64 << 10

var crlf = []byte("\r\n")

// DefaultStreamRequestBodyBuffer is Config.StreamRequestBodyBuffer's default.
const DefaultStreamRequestBodyBuffer = 256 << 10

type Config struct {
	// MaxHeaderBytes bounds a request's header: its bytes in HTTP/1, and in
	// HTTP/2 both its encoded block and its decoded size as RFC 9113
	// counts it.
	MaxHeaderBytes int
	MaxBodyBytes   int64
	// StreamRequestBody hands a request to its handler as soon as its body
	// has begun, on HTTP/1 and HTTP/2 alike: once its header has arrived,
	// for a body with a Content-Length, and once some of it has, for a
	// chunked one or an HTTP/2 one without a content-length. The body
	// arrives as a *BodyStream in Request.Body, which reads what the
	// connection has delivered without waiting for the rest, and which
	// Context.OnBody takes as it arrives; Context.BodyComplete tells the two
	// cases apart. HTTP/1 paces the client by holding the connection's reads
	// (see StreamRequestBodyBuffer); HTTP/2, which cannot stop reading for
	// one stream, by giving the stream's flow-control window back only as
	// the handler consumes the body. Off, the default, a handler runs only
	// once its request has arrived whole, body and all, so that Request.Body
	// holds every byte of it. Package http3 has the same option for HTTP/3.
	StreamRequestBody bool
	// StreamRequestBodyThreshold keeps the smaller bodies buffered whole when
	// StreamRequestBody is set: only a body whose Content-Length is larger
	// than this, or one without a length of which more than this has
	// arrived, streams. Zero streams every body. It has no effect without
	// StreamRequestBody.
	StreamRequestBodyThreshold int64
	// MaxStreamedBodyBytes bounds a streamed body, which MaxBodyBytes does
	// not: the point of streaming is to accept an upload larger than the
	// server is willing to hold in memory. Zero, the default, leaves a
	// streamed body unbounded.
	MaxStreamedBodyBytes int64
	// ReadHeaderTimeout bounds how long a request's header may take to
	// arrive, measured from the first byte of the request. Zero means
	// ReadTimeout, as net/http.Server resolves it; both zero means no limit.
	// A request that outstays it has its connection closed.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds how long a whole request, header and body, may take
	// to arrive, measured from its first byte. It bounds a streamed body for
	// as long as it is still arriving, and stops once the request has all
	// arrived, so a handler is never cut off by it. Zero means no limit.
	ReadTimeout time.Duration
	// IdleTimeout bounds how long a kept-alive connection may sit between
	// requests. Zero means ReadTimeout; both zero means no limit.
	IdleTimeout time.Duration
	// StreamRequestBodyBuffer is how many bytes of a streamed HTTP/1 body may
	// wait unread before the connection stops reading its socket, so that a
	// handler slower than its client is paid for by TCP flow control rather
	// than by memory here. Zero means DefaultStreamRequestBodyBuffer. An
	// HTTP/2 body is bounded by its stream's window instead.
	StreamRequestBodyBuffer int
	// DisableHTTP2 serves HTTP/1 only. Otherwise a connection that opens with
	// the HTTP/2 preface, over TLS after ALPN chose "h2" or in cleartext with
	// prior knowledge, is served as HTTP/2.
	DisableHTTP2 bool
	// HTTP2Only serves HTTP/2 only: every connection is HTTP/2 whatever it
	// sends, and one that does not start with the HTTP/2 preface is ended
	// with GOAWAY rather than answered in HTTP/1. A connection whose ALPN
	// chose "h2" is treated this way in any case. It has no effect when
	// DisableHTTP2 is set.
	HTTP2Only bool
	// MaxConcurrentStreams is how many requests an HTTP/2 client may have
	// open on one connection. Zero means DefaultMaxConcurrentStreams.
	MaxConcurrentStreams uint32
	// StreamPool runs HTTP/2 request handlers on a pool of their own, so
	// that the requests a client has open on one connection are served
	// concurrently rather than one after another on the goroutine that
	// reads it. Its zero value is the default, which does that on a shared
	// pool with no per-connection limit; see StreamPoolConfig. It has no
	// effect on HTTP/1, where a connection carries one request at a time
	// anyway.
	StreamPool StreamPoolConfig
	// ReuseRequests, ReuseHeaders, ReuseURLs and ReuseContexts recycle the
	// objects an HTTP/1 request is served with once its response is finished,
	// rather than leaving each request's to the collector: the
	// *http.Request, its Header, its URL, and the *Context it is answered
	// through. A busy server makes a request's worth of them for every
	// request, and at a high rate collecting them is what holds it back.
	//
	// The price is that a recycled object is only the handler's until the
	// request is done with: the handler has returned and has released every
	// Retain it took — a request retained past its response, or past its
	// connection going, stays its own until the last Release — after which
	// the next request, on this connection or another, is given it. A handler that keeps one
	// longer, or hands it to a goroutine that outlives the response, reads
	// or writes another request's: keep a copy of what is needed instead,
	// as http.Request.Clone and http.Header.Clone make. That is fasthttp's
	// rule for its RequestCtx, and the reason these are off by default.
	//
	// A request whose body streams (see StreamRequestBody), and one
	// net/http parsed because it is outside the shape parsed here, keep
	// their Request, Header and URL whatever these say.
	ReuseRequests bool
	ReuseHeaders  bool
	ReuseURLs     bool
	ReuseContexts bool
}

// reuse is which of a request's objects the parser takes from the pools.
func (c *Config) reuse() reuseOptions {
	return reuseOptions{requests: c.ReuseRequests, headers: c.ReuseHeaders, urls: c.ReuseURLs}
}

func DefaultConfig() Config {
	return Config{MaxHeaderBytes: 1 << 20, MaxBodyBytes: 16 << 20}
}

// Parser incrementally turns arbitrary TCP chunks into complete HTTP requests.
type Parser struct {
	config Config
	// buffer is what has arrived and not been parsed yet. It is a window on
	// base, the pooled array the parser received it into: consuming a request
	// moves the window's start rather than copying what follows it to the
	// front, so that a read carrying many pipelined requests is copied once
	// rather than once per request. When borrowed is set, buffer is instead a
	// window on the bytes the connection is delivering, which are only valid
	// until the read round ends; own copies what is left of them before then.
	// An empty buffer holds no array at all, so that an idle connection keeps
	// none.
	buffer []byte
	// block is where the request FeedOne returned last was allocated, when
	// the simple parser took it, for the server handler to take the rest of
	// what serving it needs from.
	block *requestBlock
	// base is always the whole array, len and cap alike.
	base     []byte
	borrowed bool
	// awaiting is the frame of a request whose header has been parsed and
	// whose body is still arriving, so that the header is parsed once rather
	// than again on every read until the body is complete.
	awaiting   frameInfo
	headerScan int
	// conn is the connection being parsed, which a streamed body reads from
	// and holds back; the server handler fills it in. remoteAddr is the
	// peer's address, which every request carries as its RemoteAddr.
	conn       *fib.Connection
	remoteAddr string
	// sniffed records that the server handler has seen enough of the
	// connection to know it is not HTTP/2.
	sniffed bool
	// tls is the connection's TLS state for every request's TLS, looked up
	// once, which tlsChecked records.
	tls        *stdtls.ConnectionState
	tlsChecked bool
	// wantContinue asks the server handler to send 100 Continue for a request
	// whose header has arrived with Expect: 100-continue and whose body has
	// not; continued records that it was asked for the request in progress.
	wantContinue bool
	continued    bool
	// mu serializes everything the server handler does with the parser. It is
	// held while a request is served, so that the connection's worker and the
	// goroutine serving a streamed request never parse at once.
	mu sync.Mutex
	// stream is the streamed body being fed from buffer, and busy records
	// that the handler of a streamed request has not returned: nothing that
	// follows it on the connection is parsed until it has, so pipelined
	// requests keep their order.
	stream *BodyStream
	busy   bool
	// spent marks a connection that is ending, or that has become HTTP/2,
	// whose remaining bytes this parser will not answer.
	spent bool
	// headerComplete records that the request being parsed has its header and
	// is waiting for its body, which is what tells ReadHeaderTimeout from
	// ReadTimeout. requestStart is when that request's first byte arrived,
	// which both are measured from.
	headerComplete bool
	requestStart   time.Time
	// live is stream, or the last one, reachable without mu so that a close
	// on the event-loop goroutine can fail a body whose reader is waiting.
	live atomic.Pointer[BodyStream]
	// serverState is what only a server keeps, which is built only on the
	// platforms that have one.
	serverState
}

type frameInfo struct {
	end       int
	headerEnd int
	chunked   bool
	// stream marks a body too big to wait for: the request is complete at
	// headerEnd and the body that follows is delivered as it arrives.
	stream  bool
	request *stdhttp.Request
	// block is where request was allocated, when the simple parser took it.
	block *requestBlock
}

func NewParser(config Config) *Parser {
	defaults := DefaultConfig()
	if config.MaxHeaderBytes <= 0 {
		config.MaxHeaderBytes = defaults.MaxHeaderBytes
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = defaults.MaxBodyBytes
	}
	return &Parser{config: config}
}

// Reset clears buffered state so the parser can be reused for another
// connection with the same configuration.
func (p *Parser) Reset() {
	// The parser is about to serve another connection, so its buffer belongs
	// back in the pool rather than attached to it through the wait in between.
	p.discard()
	p.headerScan = 0
	p.continued, p.wantContinue = false, false
	p.stream, p.busy, p.spent = nil, false, false
	p.headerComplete, p.requestStart = false, time.Time{}
	p.live.Store(nil)
	p.resetServerState()
}

// Feed may return zero, one, or several pipelined requests.
func (p *Parser) Feed(data []byte) ([]*stdhttp.Request, error) {
	var requests []*stdhttp.Request
	for {
		req, complete, err := p.FeedOne(data)
		data = nil
		if err != nil {
			return requests, err
		}
		if !complete {
			return requests, nil
		}
		requests = append(requests, req)
		if len(p.buffer) == 0 {
			return requests, nil
		}
	}
}

// FeedOne parses at most one request. Any bytes following that request remain
// buffered and can be retrieved with TakeBuffered. This is useful for protocol
// upgrades whose first frame may arrive in the same TCP read as the request.
//
// A request whose body streams (see Config.StreamRequestBody) is
// returned as soon as its header has arrived, with the rest of the body still
// to come in Request.Body. Until that body ends, the bytes that follow are the
// body rather than another request, so FeedOne buffers them and returns
// nothing; only the server handler, which feeds the body, goes on from there.
func (p *Parser) FeedOne(data []byte) (*stdhttp.Request, bool, error) {
	p.appendBuffer(data)
	return p.feedOne()
}

// feedBorrowed is FeedOne for the server handler, which parses the bytes a
// read round delivers where they are rather than copying them into the
// parser's own buffer first: only what is left of them once the round's
// requests are parsed is copied, by own, which the caller runs before the
// round ends.
func (p *Parser) feedBorrowed(data []byte) (*stdhttp.Request, bool, error) {
	if len(p.buffer) == 0 && len(data) > 0 {
		p.discard()
		p.buffer, p.borrowed = data, true
	} else {
		p.appendBuffer(data)
	}
	return p.feedOne()
}

func (p *Parser) feedOne() (*stdhttp.Request, bool, error) {
	if p.stream != nil {
		return nil, false, nil
	}
	frame, complete, err := p.frameLength()
	if err != nil {
		p.discard()
		p.headerScan = 0
		return nil, false, err
	}
	if !complete {
		return nil, false, nil
	}
	req := frame.request
	if frame.stream {
		// The header is complete and the body is not; the bytes that follow
		// it in the buffer are the start of the body, which pumpBody hands to
		// the reader.
		p.consume(frame.headerEnd)
		var decoder *chunkedDecoder
		if frame.chunked {
			decoder = &chunkedDecoder{maxTrailer: p.config.MaxHeaderBytes}
		}
		wantContinue := req.ProtoAtLeast(1, 1) && strings.EqualFold(req.Header.Get("Expect"), "100-continue")
		p.stream = newBodyStream(p.conn, req, p.config, decoder, wantContinue)
		p.live.Store(p.stream)
		req.Body = p.stream
		p.block = frame.block
		return req, true, nil
	}
	if frame.chunked && frame.block != nil && decodePlainChunks(p.buffer[frame.headerEnd:frame.end], &frame.block.body) {
		// A body of plain chunks decodes here, into the block the header
		// was parsed into; see decodePlainChunks.
		req.Body = &frame.block.body
	} else if frame.chunked {
		// net/http owns the chunk decoder; only chunked requests need this
		// second parse. Content-Length requests reuse the header parse below.
		// The request it makes replaces the one parsed into the block.
		frame.block = nil
		var reader *requestReader
		req, reader, err = readRequest(p.buffer[:frame.end], true)
		if err != nil {
			if reader != nil {
				releaseRequestReader(reader)
			}
			p.discard()
			return nil, false, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		req.Close = req.Close || frame.request.Close
		body, readErr := io.ReadAll(io.LimitReader(req.Body, p.config.MaxBodyBytes+1))
		_ = req.Body.Close()
		releaseRequestReader(reader)
		if readErr != nil || int64(len(body)) > p.config.MaxBodyBytes {
			p.discard()
			if int64(len(body)) > p.config.MaxBodyBytes {
				return nil, false, ErrBodyTooLarge
			}
			return nil, false, fmt.Errorf("%w: %v", ErrMalformed, readErr)
		}
		req.Body = &wholeBody{data: body}
	} else if req.ContentLength > 0 {
		var body *wholeBody
		if frame.block != nil {
			body = &frame.block.body
		} else {
			body = new(wholeBody)
		}
		body.fill(p.buffer[frame.headerEnd:frame.end])
		req.Body = body
	}
	p.consume(frame.end)
	p.requestEnded()
	p.block = frame.block
	return req, true, nil
}

// requestEnded marks the request that was arriving as arrived, so the next one
// is timed from its own first byte.
func (p *Parser) requestEnded() {
	p.headerComplete, p.requestStart = false, time.Time{}
}

// waitState is what the connection is waiting for, which says which of the
// read timeouts bounds it.
type waitState uint8

const (
	// waitIdle is a connection between requests, waitHeader one whose
	// request has begun without its header being complete, waitBody one
	// whose body is still arriving, and waitServing one whose request has
	// all arrived and whose handler is running.
	waitIdle waitState = iota
	waitHeader
	waitBody
	waitServing
)

func (p *Parser) waitState() waitState {
	switch {
	case p.stream != nil:
		return waitBody
	case p.busy:
		return waitServing
	case len(p.buffer) == 0:
		return waitIdle
	case p.headerComplete:
		return waitBody
	default:
		return waitHeader
	}
}

// pumpBody hands a streamed body the bytes it is owed, from what the parser
// has buffered and from data, and reports whether the body ended there. What
// the body took leaves the buffer, so that whatever follows it on the
// connection is parsed as the next request; when the buffer is empty the body
// takes its bytes straight from data, so an upload is copied once rather than
// twice.
func (p *Parser) pumpBody(data []byte) (bool, error) {
	stream := p.stream
	if len(p.buffer) == 0 {
		used, done, err := stream.absorb(data)
		p.appendBuffer(data[used:])
		return p.bodyEnded(done, err)
	}
	p.appendBuffer(data)
	used, done, err := stream.absorb(p.buffer)
	if used > 0 {
		p.consume(used)
	}
	return p.bodyEnded(done, err)
}

// bodyEnded retires a streamed body that has arrived whole, or failed.
func (p *Parser) bodyEnded(done bool, err error) (bool, error) {
	if err == nil && !done {
		return false, nil
	}
	stream := p.stream
	p.stream = nil
	p.live.Store(nil)
	p.requestEnded()
	if err != nil {
		stream.fail(err, true)
		return false, err
	}
	return true, nil
}

// TakeBuffered returns and clears bytes read beyond the last parsed request.
// The array it returns starts with them, so that the caller may hand it back
// to the buffer pool once it is done with them.
func (p *Parser) TakeBuffered() []byte {
	p.own()
	data := p.buffer
	if len(data) > 0 && cap(data) != cap(p.base) {
		// The window has moved up its array; bring it back to the front.
		data = p.base[:copy(p.base, data)]
	}
	p.buffer, p.base = nil, nil
	p.awaiting = frameInfo{}
	p.headerScan = 0
	p.continued, p.wantContinue = false, false
	return data
}

// appendBuffer adds data behind what is buffered, in the parser's own array.
// Room that consuming left at the front of the array is reused before a
// larger one is taken from the pool.
func (p *Parser) appendBuffer(data []byte) {
	if len(data) == 0 {
		return
	}
	p.own()
	n := len(p.buffer)
	need := n + len(data)
	if need <= cap(p.buffer) {
		p.buffer = append(p.buffer, data...)
		return
	}
	if need <= cap(p.base) {
		p.buffer = append(p.base[:copy(p.base, p.buffer)], data...)
		return
	}
	grown := bufferpool.Get(need)
	grown = grown[:cap(grown)]
	copy(grown, p.buffer)
	bufferpool.Put(p.base)
	p.base = grown
	p.buffer = append(grown[:n], data...)
}

// own ends a borrow: what is left of the bytes the round delivered is copied
// into an array of the parser's own, sized to them, since the round's buffer
// goes back to the connection once it ends.
func (p *Parser) own() {
	if !p.borrowed {
		return
	}
	left := p.buffer
	p.buffer, p.borrowed = nil, false
	if len(left) > 0 {
		p.base = bufferpool.Get(len(left))
		p.base = p.base[:cap(p.base)]
		p.buffer = p.base[:copy(p.base, left)]
	}
}

// discard drops what is buffered, returning the array to the pool. The caller
// must have established that nothing parsed out of it is still referring to it.
func (p *Parser) discard() {
	if !p.borrowed {
		bufferpool.Put(p.base)
	}
	p.buffer, p.base, p.borrowed = nil, nil, false
	p.awaiting = frameInfo{}
}

func (p *Parser) consume(n int) {
	if n == len(p.buffer) {
		// Nothing is left, so the array goes back to the pool rather than
		// staying with a connection that may say nothing more for a while.
		p.discard()
	} else {
		p.buffer = p.buffer[n:]
	}
	p.headerScan = 0
	p.continued = false
}

func (p *Parser) frameLength() (frameInfo, bool, error) {
	if frame := p.awaiting; frame.request != nil {
		if frame.end > len(p.buffer) {
			return frameInfo{}, false, nil
		}
		p.awaiting = frameInfo{}
		return frame, true, nil
	}
	headerAt := bytes.Index(p.buffer[p.headerScan:], []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(p.buffer) > p.config.MaxHeaderBytes {
			return frameInfo{}, false, ErrHeaderTooLarge
		}
		p.headerScan = len(p.buffer) - 3
		if p.headerScan < 0 {
			p.headerScan = 0
		}
		return frameInfo{}, false, nil
	}
	headerAt += p.headerScan
	headerEnd := headerAt + 4
	if headerEnd > p.config.MaxHeaderBytes {
		return frameInfo{}, false, ErrHeaderTooLarge
	}
	req, block, err := parseRequestHead(p.buffer[:headerEnd], p.config.reuse())
	if err != nil {
		if strings.Contains(err.Error(), "unsupported transfer encoding") {
			// net/http refuses transfer codings other than chunked, without
			// an error type to tell that from a malformed request.
			return frameInfo{}, false, ErrUnsupportedTransferEncoding
		}
		return frameInfo{}, false, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	_ = req.Body.Close()
	// The header is in; from here the request is waiting for its body, which
	// ReadTimeout rather than ReadHeaderTimeout bounds.
	p.headerComplete = true
	if !req.ProtoAtLeast(1, 1) && hasHeaderField(p.buffer[:headerEnd], "transfer-encoding") {
		// HTTP/1.0 has no transfer codings, so a body framed by one cannot
		// be trusted to end where either side thinks (RFC 9112 section 6.1).
		// net/http drops the field, so it is looked for in the raw header.
		return frameInfo{}, false, fmt.Errorf("%w: Transfer-Encoding in an HTTP/1.0 request", ErrMalformed)
	}
	if len(req.TransferEncoding) != 0 {
		if len(req.TransferEncoding) != 1 || !strings.EqualFold(req.TransferEncoding[0], "chunked") {
			return frameInfo{}, false, ErrUnsupportedTransferEncoding
		}
		if hasHeaderField(p.buffer[:headerEnd], "content-length") {
			// Framed two ways, a request may be read differently by whatever
			// passed it on; chunked wins, and the connection ends after it
			// (RFC 9112 section 6.3).
			req.Close = true
		}
		if p.config.StreamRequestBody && int64(len(p.buffer)-headerEnd) > p.config.StreamRequestBodyThreshold {
			// More has been sent than this server is willing to hold, and a
			// chunked body never says how much more is coming. Hand the
			// request over and decode the rest as it arrives.
			return frameInfo{end: headerEnd, headerEnd: headerEnd, chunked: true, stream: true, request: req, block: block}, true, nil
		}
		end, complete, err := chunkedEnd(p.buffer, headerEnd, p.config.MaxHeaderBytes, p.config.MaxBodyBytes)
		if err == nil && !complete {
			p.expectContinue(req)
		}
		if err != nil || !complete {
			return frameInfo{end: end, headerEnd: headerEnd, chunked: true, request: req, block: block}, complete, err
		}
		if int64(end-headerEnd) > p.config.MaxBodyBytes+int64(p.config.MaxHeaderBytes) {
			return frameInfo{}, false, ErrBodyTooLarge
		}
		return frameInfo{end: end, headerEnd: headerEnd, chunked: true, request: req, block: block}, true, nil
	}
	if req.ContentLength < 0 {
		return frameInfo{end: headerEnd, headerEnd: headerEnd, request: req, block: block}, true, nil
	}
	if p.config.StreamRequestBody && req.ContentLength > p.config.StreamRequestBodyThreshold {
		if limit := p.config.MaxStreamedBodyBytes; limit > 0 && req.ContentLength > limit {
			return frameInfo{}, false, ErrBodyTooLarge
		}
		return frameInfo{end: headerEnd, headerEnd: headerEnd, stream: true, request: req, block: block}, true, nil
	}
	if req.ContentLength > p.config.MaxBodyBytes {
		return frameInfo{}, false, ErrBodyTooLarge
	}
	end64 := int64(headerEnd) + req.ContentLength
	frame := frameInfo{end: int(end64), headerEnd: headerEnd, request: req, block: block}
	if end64 > int64(len(p.buffer)) {
		p.expectContinue(req)
		p.awaiting = frame
		return frameInfo{}, false, nil
	}
	return frame, true, nil
}

// expectContinue notes a request, complete but for its body, that waits for
// 100 Continue before sending it.
func (p *Parser) expectContinue(req *stdhttp.Request) {
	if !p.continued && req.ProtoAtLeast(1, 1) && strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
		p.continued = true
		p.wantContinue = true
	}
}

// readRequest parses the request at the front of data. With keepBody set, the
// request's Body reads the rest of data through the reader that comes back
// with it, which the caller releases once it has finished with the body;
// otherwise the body is detached here and the reader goes straight back.
func readRequest(data []byte, keepBody bool) (*stdhttp.Request, *requestReader, error) {
	reader := acquireRequestReader(data)
	req, err := stdhttp.ReadRequest(reader.buf)
	if keepBody {
		return req, reader, err
	}
	if req != nil {
		// Detach the request from the pooled reader. FeedOne installs the real
		// Content-Length body after the complete frame has arrived.
		req.Body = stdhttp.NoBody
	}
	releaseRequestReader(reader)
	return req, nil, err
}

func chunkedEnd(data []byte, offset, maxTrailer int, maxBody int64) (int, bool, error) {
	var decoded uint64
	for {
		lineEnd := bytes.Index(data[offset:], []byte("\r\n"))
		if lineEnd < 0 {
			return 0, false, nil
		}
		line := string(data[offset : offset+lineEnd])
		if semicolon := strings.IndexByte(line, ';'); semicolon >= 0 {
			line = line[:semicolon]
		}
		size, err := strconv.ParseUint(strings.TrimSpace(line), 16, 63)
		if err != nil {
			return 0, false, fmt.Errorf("%w: invalid chunk size", ErrMalformed)
		}
		offset += lineEnd + 2
		if size == 0 {
			if len(data[offset:]) >= 2 && bytes.Equal(data[offset:offset+2], []byte("\r\n")) {
				return offset + 2, true, nil
			}
			trailerEnd := bytes.Index(data[offset:], []byte("\r\n\r\n"))
			if trailerEnd >= 0 {
				if trailerEnd+4 > maxTrailer {
					return 0, false, ErrHeaderTooLarge
				}
				return offset + trailerEnd + 4, true, nil
			}
			if len(data)-offset > maxTrailer {
				return 0, false, ErrHeaderTooLarge
			}
			return 0, false, nil
		}
		if size > uint64(maxBody)-decoded {
			return 0, false, ErrBodyTooLarge
		}
		decoded += size
		if size > uint64(len(data)-offset) {
			return 0, false, nil
		}
		chunkEnd := offset + int(size)
		if chunkEnd+2 > len(data) {
			return 0, false, nil
		}
		if !bytes.Equal(data[chunkEnd:chunkEnd+2], []byte("\r\n")) {
			return 0, false, fmt.Errorf("%w: invalid chunk terminator", ErrMalformed)
		}
		offset = chunkEnd + 2
	}
}

// maxPlainChunkDigits is the longest chunk size decodePlainChunks takes,
// which keeps every size it parses within a uint64.
const maxPlainChunkDigits = 15

// decodePlainChunks decodes body, a whole chunked body chunkedEnd has
// framed, into a pooled buffer for into, when every chunk-size line is bare
// hex digits and nothing follows the last chunk but the blank line: the shape
// chunked bodies ordinarily take, which net/http decodes to the same bytes.
// It reports false and fills nothing for any other, such as one with chunk
// extensions, padded sizes or a trailer, which is left to net/http to decode,
// or to refuse.
func decodePlainChunks(body []byte, into *wholeBody) bool {
	total := 0
	for at := 0; ; {
		size, n := plainChunkSize(body[at:])
		if n == 0 {
			return false
		}
		at += n
		if size == 0 {
			if len(body)-at != 2 || body[at] != '\r' || body[at+1] != '\n' {
				return false
			}
			break
		}
		if size > len(body)-at-2 || body[at+size] != '\r' || body[at+size+1] != '\n' {
			return false
		}
		at += size + 2
		total += size
	}
	if total == 0 {
		*into = wholeBody{}
		return true
	}
	data := bufferpool.Get(total)[:0]
	for at := 0; ; {
		size, n := plainChunkSize(body[at:])
		if size == 0 {
			break
		}
		at += n
		data = append(data, body[at:at+size]...)
		at += size + 2
	}
	*into = wholeBody{data: data, pooled: true}
	return true
}

// plainChunkSize parses a chunk-size line of bare hex digits at the front
// of b, reporting the size and the length of the line with its CRLF, or a
// zero length for any other line, and for a size larger than b, which no
// chunk in b can have.
func plainChunkSize(b []byte) (size, n int) {
	var v uint64
	for i, c := range b {
		switch {
		case '0' <= c && c <= '9':
			v = v<<4 | uint64(c-'0')
		case 'a' <= c && c <= 'f':
			v = v<<4 | uint64(c-'a'+10)
		case 'A' <= c && c <= 'F':
			v = v<<4 | uint64(c-'A'+10)
		case c == '\r' && i > 0 && i+1 < len(b) && b[i+1] == '\n':
			if v > uint64(len(b)) {
				return 0, 0
			}
			return int(v), i + 2
		default:
			return 0, 0
		}
		if i == maxPlainChunkDigits {
			return 0, 0
		}
	}
	return 0, 0
}

// hasHeaderField reports whether a raw header block has a field named name,
// which is given in lower case.
func hasHeaderField(header []byte, name string) bool {
	for line := range bytes.SplitSeq(header, []byte("\n")) {
		if len(line) > len(name) && line[len(name)] == ':' && strings.EqualFold(string(line[:len(name)]), name) {
			return true
		}
	}
	return false
}
