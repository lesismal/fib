package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/lesismal/fib/bufferpool"
)

var (
	ErrResponseHeaderTooLarge = errors.New("http: response header too large")
	ErrResponseBodyTooLarge   = errors.New("http: response body too large")
	ErrMalformedResponse      = errors.New("http: malformed response")
)

// bodyFraming is how the end of a response body is found.
type bodyFraming uint8

const (
	// framingNone: the response has no body, whatever its headers say.
	framingNone bodyFraming = iota
	framingLength
	framingChunked
	// framingUntilClose: the body is everything until the server closes.
	framingUntilClose
)

// responseParser frames HTTP/1.x responses on a client connection. The client
// sends one request at a time on a connection, so there is at most one
// response in progress and the request it answers is always known.
type responseParser struct {
	maxHeader  int
	maxBody    int64
	buffer     []byte
	headerScan int
	// head is the parsed header of the response in progress, headerEnd where
	// its body starts, and framing how that body ends. head is nil until the
	// header is complete.
	head      *stdhttp.Response
	headerEnd int
	framing   bodyFraming

	// sink receives bodies that stream instead of being buffered, when
	// streamOK says this exchange may stream: those larger than threshold, by
	// Content-Length, or of unknown length. maxStreamed bounds them in place of
	// maxBody; zero leaves them unbounded.
	sink        bodySink
	streamOK    bool
	threshold   int64
	maxStreamed int64
	// streaming is set once the header of the response in progress has been
	// found to stream, started once the sink has been told, remaining is what
	// a Content-Length body still owes, passed what has gone to the sink, and
	// dec and scratch decode a chunked one. streamedDone records that the
	// last feed or finish completed a streamed response, whose end the sink
	// has already been told.
	streaming    bool
	started      bool
	remaining    int64
	passed       int64
	dec          chunkedDecoder
	scratch      []byte
	streamedDone bool
}

// bodySink is where a streamed response body goes: start once its header is
// known, data for each piece of the body in order, and end when it is
// complete. data is only valid for the duration of the call.
type bodySink interface {
	bodyStart(head *stdhttp.Response)
	bodyData(data []byte)
	bodyEnd()
}

func (p *responseParser) reset() {
	bufferpool.Put(p.buffer)
	p.buffer = nil
	p.headerScan = 0
	p.head = nil
	p.headerEnd = 0
	p.streaming, p.started, p.streamedDone = false, false, false
	p.remaining, p.passed = 0, 0
	if cap(p.scratch) > maxRetainedBuffer {
		p.scratch = nil
	}
}

// buffered reports whether bytes are held beyond the last complete response.
func (p *responseParser) buffered() bool { return len(p.buffer) > 0 }

// feed adds bytes read from the connection and returns the response to req
// once it is complete. Interim 1xx responses are skipped: they are not the
// answer, and the final response follows them on the same connection.
//
// A response whose body streams is reported through the sink instead: feed
// returns its header, with streamedDone set, once the body has ended and the
// sink has been told so.
func (p *responseParser) feed(data []byte, req *stdhttp.Request) (*stdhttp.Response, error) {
	p.buffer = bufferpool.Append(p.buffer, data)
	p.streamedDone = false
	for {
		if p.head == nil {
			done, err := p.parseHead(req)
			if err != nil || !done {
				return nil, err
			}
			if p.head == nil {
				// An interim response was dropped; look for the next header.
				continue
			}
		}
		if p.streaming {
			return p.feedStream()
		}
		switch p.framing {
		case framingNone:
			return p.complete(p.headerEnd, nil), nil
		case framingLength:
			end := int64(p.headerEnd) + p.head.ContentLength
			if int64(len(p.buffer)) < end {
				return nil, nil
			}
			return p.complete(int(end), p.buffer[p.headerEnd:end]), nil
		case framingChunked:
			end, complete, err := chunkedEnd(p.buffer, p.headerEnd, p.maxHeader, p.maxBody)
			if err != nil {
				return nil, clientParseError(err)
			}
			if !complete {
				return nil, nil
			}
			return p.completeChunked(end, req)
		default:
			if int64(len(p.buffer)-p.headerEnd) > p.maxBody {
				return nil, ErrResponseBodyTooLarge
			}
			return nil, nil
		}
	}
}

// finish is called when the server closes the connection. A body delimited by
// the close is complete at that point; anything else is cut short, and finish
// returns nil.
func (p *responseParser) finish() *stdhttp.Response {
	if !p.untilClose() {
		return nil
	}
	if p.streaming {
		return p.endStream(nil)
	}
	return p.complete(len(p.buffer), p.buffer[p.headerEnd:])
}

// untilClose reports whether the response in progress ends when the server
// closes the connection.
func (p *responseParser) untilClose() bool {
	return p.head != nil && p.framing == framingUntilClose
}

// feedStream passes on the body that has arrived, once the sink has been told
// of the header, and reports the response when its body has ended.
func (p *responseParser) feedStream() (*stdhttp.Response, error) {
	if !p.started {
		p.started = true
		p.consume(p.headerEnd)
		p.headerEnd = 0
		p.remaining = p.head.ContentLength
		p.dec = chunkedDecoder{maxTrailer: p.maxHeader}
		p.sink.bodyStart(p.head)
	}
	switch p.framing {
	case framingLength:
		n := min(int64(len(p.buffer)), p.remaining)
		if n > 0 {
			if err := p.pass(p.buffer[:n]); err != nil {
				return nil, err
			}
			p.consume(int(n))
			p.remaining -= n
		}
		if p.remaining > 0 {
			return nil, nil
		}
		return p.endStream(nil), nil
	case framingChunked:
		out, used, trailer, done, err := p.dec.decode(p.scratch[:0], p.buffer)
		p.scratch = out
		if err != nil {
			return nil, clientParseError(err)
		}
		if len(out) > 0 {
			if err := p.pass(out); err != nil {
				return nil, err
			}
		}
		p.consume(used)
		if !done {
			return nil, nil
		}
		fields, err := parseTrailer(trailer)
		if err != nil {
			return nil, clientParseError(err)
		}
		return p.endStream(fields), nil
	default:
		if len(p.buffer) > 0 {
			if err := p.pass(p.buffer); err != nil {
				return nil, err
			}
			p.consume(len(p.buffer))
		}
		return nil, nil
	}
}

// pass gives the sink a piece of the body, within the limit on streamed ones.
func (p *responseParser) pass(data []byte) error {
	p.passed += int64(len(data))
	if p.maxStreamed > 0 && p.passed > p.maxStreamed {
		return ErrResponseBodyTooLarge
	}
	p.sink.bodyData(data)
	return nil
}

// endStream closes the streamed response in progress, which is handed back for
// the caller's decision about the connection. The parser is done with it
// before the sink hears of the end, since the sink may run a callback.
func (p *responseParser) endStream(trailer stdhttp.Header) *stdhttp.Response {
	resp := p.head
	if trailer != nil {
		resp.Trailer = trailer
	}
	p.head = nil
	p.streaming, p.started = false, false
	p.streamedDone = true
	p.sink.bodyEnd()
	return resp
}

// parseHead looks for a complete header and, when it finds one, works out how
// the body that follows it is framed. It reports false while the header is
// still arriving.
func (p *responseParser) parseHead(req *stdhttp.Request) (bool, error) {
	headerAt := bytes.Index(p.buffer[p.headerScan:], []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(p.buffer) > p.maxHeader {
			return false, ErrResponseHeaderTooLarge
		}
		p.headerScan = max(len(p.buffer)-3, 0)
		return false, nil
	}
	headerEnd := p.headerScan + headerAt + 4
	if headerEnd > p.maxHeader {
		return false, ErrResponseHeaderTooLarge
	}
	head, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(p.buffer[:headerEnd])), req)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	_ = head.Body.Close()
	if head.StatusCode >= 100 && head.StatusCode < 200 && head.StatusCode != stdhttp.StatusSwitchingProtocols {
		p.consume(headerEnd)
		return true, nil
	}
	p.head, p.headerEnd = head, headerEnd
	switch {
	case !bodyAllowed(req, head.StatusCode):
		p.framing = framingNone
	case len(head.TransferEncoding) != 0:
		if len(head.TransferEncoding) != 1 || !strings.EqualFold(head.TransferEncoding[0], "chunked") {
			return false, fmt.Errorf("%w: unsupported transfer encoding %q", ErrMalformedResponse, head.TransferEncoding)
		}
		p.framing = framingChunked
	case head.ContentLength >= 0:
		p.framing = framingLength
	default:
		p.framing = framingUntilClose
	}
	p.streaming = p.sink != nil && p.streamOK && p.framing != framingNone &&
		!(p.framing == framingLength && head.ContentLength <= max(p.threshold, 0))
	if p.framing == framingLength {
		limit := p.maxBody
		if p.streaming {
			limit = p.maxStreamed
		}
		if limit > 0 && head.ContentLength > limit {
			return false, ErrResponseBodyTooLarge
		}
	}
	return true, nil
}

// bodyAllowed reports whether a response may carry a body at all. The answer
// to HEAD never does, and neither do 1xx, 204 and 304 responses, whatever
// Content-Length they declare.
func bodyAllowed(req *stdhttp.Request, status int) bool {
	if req.Method == stdhttp.MethodHead {
		return false
	}
	return status >= 200 && status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
}

// completeChunked decodes a chunked body, through net/http's own decoder so
// that trailers land where callers expect them.
func (p *responseParser) completeChunked(end int, req *stdhttp.Request) (*stdhttp.Response, error) {
	full, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(p.buffer[:end])), req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	body, err := io.ReadAll(io.LimitReader(full.Body, p.maxBody+1))
	_ = full.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if int64(len(body)) > p.maxBody {
		return nil, ErrResponseBodyTooLarge
	}
	p.head = full
	return p.complete(end, body), nil
}

// complete hands out the response in progress with its body and removes it
// from the buffer. The body is copied: the buffer is reused for the next
// response on the connection.
func (p *responseParser) complete(end int, body []byte) *stdhttp.Response {
	resp := p.head
	if len(body) == 0 {
		resp.Body = stdhttp.NoBody
	} else {
		resp.Body = &bufferedBody{data: append([]byte(nil), body...)}
	}
	p.head = nil
	p.consume(end)
	return resp
}

func (p *responseParser) consume(n int) {
	if n == len(p.buffer) {
		if cap(p.buffer) > maxRetainedBuffer {
			// One large response is not a reason for this connection to hold
			// the buffer it needed; the pool keeps it in its own class.
			bufferpool.Put(p.buffer)
			p.buffer = nil
		} else {
			p.buffer = p.buffer[:0]
		}
	} else {
		p.buffer = append(p.buffer[:0], p.buffer[n:]...)
	}
	p.headerScan = 0
}

// clientParseError restates the shared chunk scanner's errors, which speak of
// requests, in terms of the response being read.
func clientParseError(err error) error {
	switch {
	case errors.Is(err, ErrHeaderTooLarge):
		return ErrResponseHeaderTooLarge
	case errors.Is(err, ErrBodyTooLarge):
		return ErrResponseBodyTooLarge
	case errors.Is(err, ErrMalformed):
		return fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	return err
}
