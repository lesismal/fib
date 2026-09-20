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

	fib "github.com/lesismal/fib/go"
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

var requestReaderPool = sync.Pool{New: func() any {
	return bufio.NewReaderSize(bytes.NewReader(nil), 1024)
}}

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
	// StreamRequestBodyThreshold, when positive, hands a request to the
	// handler before its whole body has arrived: a body whose Content-Length
	// is larger than this, or a chunked body that has already sent more than
	// this, arrives as a *BodyStream in Request.Body, which reads the rest as
	// the connection delivers it. Such a handler runs on its own goroutine,
	// since reading the body blocks. Zero, the default, buffers every body
	// whole and runs every handler on the connection's worker.
	StreamRequestBodyThreshold int64
	// MaxStreamedBodyBytes bounds a streamed body, which MaxBodyBytes does
	// not: the point of streaming is to accept an upload larger than the
	// server is willing to hold in memory. Zero, the default, leaves a
	// streamed body unbounded.
	MaxStreamedBodyBytes int64
	// StreamRequestBodyBuffer is how many bytes of a streamed body may wait
	// unread before the connection stops reading its socket, so that a
	// handler slower than its client is paid for by TCP flow control rather
	// than by memory here. Zero means DefaultStreamRequestBodyBuffer.
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
}

func DefaultConfig() Config {
	return Config{MaxHeaderBytes: 1 << 20, MaxBodyBytes: 16 << 20}
}

// Parser incrementally turns arbitrary TCP chunks into complete HTTP requests.
type Parser struct {
	config     Config
	buffer     []byte
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
	// spent marks a connection that is ending, whose remaining bytes belong
	// to a message nobody will answer.
	spent bool
	// live is stream, or the last one, reachable without mu so that a close
	// on the event-loop goroutine can fail a body whose reader is waiting.
	live atomic.Pointer[BodyStream]
}

type frameInfo struct {
	end       int
	headerEnd int
	chunked   bool
	// stream marks a body too big to wait for: the request is complete at
	// headerEnd and the body that follows is delivered as it arrives.
	stream  bool
	request *stdhttp.Request
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
	if cap(p.buffer) > maxRetainedBuffer {
		p.buffer = nil
	} else {
		p.buffer = p.buffer[:0]
	}
	p.headerScan = 0
	p.continued, p.wantContinue = false, false
	p.stream, p.busy, p.spent = nil, false, false
	p.live.Store(nil)
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
// A request whose body streams (see Config.StreamRequestBodyThreshold) is
// returned as soon as its header has arrived, with the rest of the body still
// to come in Request.Body. Until that body ends, the bytes that follow are the
// body rather than another request, so FeedOne buffers them and returns
// nothing; only the server handler, which feeds the body, goes on from there.
func (p *Parser) FeedOne(data []byte) (*stdhttp.Request, bool, error) {
	if p.stream != nil {
		p.buffer = append(p.buffer, data...)
		return nil, false, nil
	}
	p.buffer = append(p.buffer, data...)
	frame, complete, err := p.frameLength()
	if err != nil {
		p.buffer = nil
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
		return req, true, nil
	}
	if frame.chunked {
		// net/http owns the chunk decoder; only chunked requests need this
		// second parse. Content-Length requests reuse the header parse below.
		req, err = readRequest(p.buffer[:frame.end], true)
		if err != nil {
			p.buffer = nil
			return nil, false, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		req.Close = req.Close || frame.request.Close
		body, readErr := io.ReadAll(io.LimitReader(req.Body, p.config.MaxBodyBytes+1))
		_ = req.Body.Close()
		if readErr != nil || int64(len(body)) > p.config.MaxBodyBytes {
			p.buffer = nil
			if int64(len(body)) > p.config.MaxBodyBytes {
				return nil, false, ErrBodyTooLarge
			}
			return nil, false, fmt.Errorf("%w: %v", ErrMalformed, readErr)
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	} else if req.ContentLength > 0 {
		body := append([]byte(nil), p.buffer[frame.headerEnd:frame.end]...)
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	p.consume(frame.end)
	return req, true, nil
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
		p.buffer = append(p.buffer, data[used:]...)
		return p.bodyEnded(done, err)
	}
	p.buffer = append(p.buffer, data...)
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
	if err != nil {
		stream.fail(err)
		return false, err
	}
	return true, nil
}

// TakeBuffered returns and clears bytes read beyond the last parsed request.
func (p *Parser) TakeBuffered() []byte {
	data := p.buffer
	p.buffer = nil
	p.headerScan = 0
	p.continued, p.wantContinue = false, false
	return data
}

func (p *Parser) consume(n int) {
	if n == len(p.buffer) {
		if cap(p.buffer) > maxRetainedBuffer {
			p.buffer = nil
		} else {
			p.buffer = p.buffer[:0]
		}
	} else {
		copy(p.buffer, p.buffer[n:])
		p.buffer = p.buffer[:len(p.buffer)-n]
	}
	p.headerScan = 0
	p.continued = false
}

func (p *Parser) frameLength() (frameInfo, bool, error) {
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
	req, err := readRequest(p.buffer[:headerEnd], false)
	if err != nil {
		if strings.Contains(err.Error(), "unsupported transfer encoding") {
			// net/http refuses transfer codings other than chunked, without
			// an error type to tell that from a malformed request.
			return frameInfo{}, false, ErrUnsupportedTransferEncoding
		}
		return frameInfo{}, false, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	_ = req.Body.Close()
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
		if threshold := p.config.StreamRequestBodyThreshold; threshold > 0 &&
			int64(len(p.buffer)-headerEnd) > threshold {
			// More has been sent than this server is willing to hold, and a
			// chunked body never says how much more is coming. Hand the
			// request over and decode the rest as it arrives.
			return frameInfo{end: headerEnd, headerEnd: headerEnd, chunked: true, stream: true, request: req}, true, nil
		}
		end, complete, err := chunkedEnd(p.buffer, headerEnd, p.config.MaxHeaderBytes, p.config.MaxBodyBytes)
		if err == nil && !complete {
			p.expectContinue(req)
		}
		if err != nil || !complete {
			return frameInfo{end: end, headerEnd: headerEnd, chunked: true, request: req}, complete, err
		}
		if int64(end-headerEnd) > p.config.MaxBodyBytes+int64(p.config.MaxHeaderBytes) {
			return frameInfo{}, false, ErrBodyTooLarge
		}
		return frameInfo{end: end, headerEnd: headerEnd, chunked: true, request: req}, true, nil
	}
	if req.ContentLength < 0 {
		return frameInfo{end: headerEnd, headerEnd: headerEnd, request: req}, true, nil
	}
	if threshold := p.config.StreamRequestBodyThreshold; threshold > 0 && req.ContentLength > threshold {
		if limit := p.config.MaxStreamedBodyBytes; limit > 0 && req.ContentLength > limit {
			return frameInfo{}, false, ErrBodyTooLarge
		}
		return frameInfo{end: headerEnd, headerEnd: headerEnd, stream: true, request: req}, true, nil
	}
	if req.ContentLength > p.config.MaxBodyBytes {
		return frameInfo{}, false, ErrBodyTooLarge
	}
	end64 := int64(headerEnd) + req.ContentLength
	if end64 > int64(len(p.buffer)) {
		p.expectContinue(req)
		return frameInfo{}, false, nil
	}
	return frameInfo{end: int(end64), headerEnd: headerEnd, request: req}, true, nil
}

// expectContinue notes a request, complete but for its body, that waits for
// 100 Continue before sending it.
func (p *Parser) expectContinue(req *stdhttp.Request) {
	if !p.continued && req.ProtoAtLeast(1, 1) && strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
		p.continued = true
		p.wantContinue = true
	}
}

func readRequest(data []byte, keepBody bool) (*stdhttp.Request, error) {
	if keepBody {
		return stdhttp.ReadRequest(bufio.NewReaderSize(bytes.NewReader(data), 1024))
	}
	var source bytes.Reader
	source.Reset(data)
	reader := requestReaderPool.Get().(*bufio.Reader)
	reader.Reset(&source)
	req, err := stdhttp.ReadRequest(reader)
	if req != nil && !keepBody {
		// Detach the request from the pooled reader. FeedOne installs the real
		// Content-Length body after the complete frame has arrived.
		req.Body = stdhttp.NoBody
	}
	reader.Reset(bytes.NewReader(nil))
	requestReaderPool.Put(reader)
	return req, err
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
