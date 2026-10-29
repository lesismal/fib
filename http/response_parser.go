//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"

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
	// continued records that a 100 (Continue) response has been skipped,
	// for the client to send a body it held back for it.
	continued bool
	// coding decodes a body sent in a transfer coding besides chunked (see
	// splitTransferCodings); codingDec is its decoder and piece what it
	// decodes into for the sink. codings is the Transfer-Encoding as it was
	// listed, which the response reports as net/http reports chunked, and
	// inflated counts what the body decoded to, which maxBody or maxStreamed
	// bounds as they bound what arrived.
	coding     transferCoding
	codingDec  *codingDecoder
	codings    []string
	inflated   int64
	codingDone bool
	piece      []byte
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
	p.continued = false
	p.remaining, p.passed = 0, 0
	p.coding, p.codingDec, p.codings = "", nil, nil
	p.inflated, p.codingDone = 0, false
	if cap(p.scratch) > maxRetainedBuffer {
		p.scratch = nil
	}
	if cap(p.piece) > maxRetainedBuffer {
		p.piece = nil
	}
}

// takeBuffered returns the bytes held beyond the last complete response, in a
// pooled buffer that is the caller's, and leaves the parser empty.
func (p *responseParser) takeBuffered() []byte {
	data := bufferpool.Append(nil, p.buffer)
	p.reset()
	return data
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
	p.streamedDone = false
	if len(p.buffer) == 0 && p.started && p.framing == framingLength {
		return p.feedLength(data), nil
	}
	p.buffer = bufferpool.Append(p.buffer, data)
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
// returns nothing. A coded body is decoded here when it was buffered whole,
// and checked for a complete coded stream when it was not.
func (p *responseParser) finish() (*stdhttp.Response, error) {
	if !p.untilClose() {
		return nil, nil
	}
	if p.streaming {
		if err := p.finishCoding(); err != nil {
			return nil, err
		}
		return p.endStream(nil), nil
	}
	body := p.buffer[p.headerEnd:]
	if p.coding == "" {
		return p.complete(len(p.buffer), body), nil
	}
	decoded, err := decodeWhole(p.coding, body, p.maxBody, ErrResponseBodyTooLarge, ErrMalformedResponse)
	if err != nil {
		return nil, err
	}
	return p.completeOwned(len(p.buffer), decoded), nil
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
			// Within the limit: parseHead refused a Content-Length past it.
			_ = p.pass(p.buffer[:n])
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
		if len(out) > 0 || done {
			if err := p.passBody(out, done); err != nil {
				return nil, err
			}
		}
		p.consume(used)
		if !done {
			return nil, nil
		}
		if err := p.finishCoding(); err != nil {
			return nil, err
		}
		fields, err := parseTrailer(trailer)
		if err != nil {
			return nil, clientParseError(err)
		}
		return p.endStream(fields), nil
	default:
		if len(p.buffer) > 0 {
			if err := p.passBody(p.buffer, false); err != nil {
				return nil, err
			}
			p.consume(len(p.buffer))
		}
		return nil, nil
	}
}

// feedLength is feed for a Content-Length body that streams with nothing
// held back from the last read: what was read goes to the sink from where it
// was read, rather than through the parser's buffer, which would copy every
// byte of the body and grow on every connection to the size of its reads.
// Only what follows the body, the next response's start, is kept.
func (p *responseParser) feedLength(data []byte) *stdhttp.Response {
	n := min(int64(len(data)), p.remaining)
	if n > 0 {
		// Within the limit: parseHead refused a Content-Length past it.
		_ = p.pass(data[:n])
		p.remaining -= n
	}
	if p.remaining > 0 {
		return nil
	}
	if int64(len(data)) > n {
		p.buffer = bufferpool.Append(p.buffer, data[n:])
	}
	return p.endStream(nil)
}

// passBody gives the sink a piece of the body, decoding it first when it
// arrived in a transfer coding. final says it is the last of the coded body.
func (p *responseParser) passBody(data []byte, final bool) error {
	if p.coding == "" {
		if len(data) == 0 {
			return nil
		}
		return p.pass(data)
	}
	for {
		n, used, done, err := p.codingDec.decode(p.piece, data, final)
		data = data[used:]
		p.inflated += int64(n)
		p.codingDone = done
		if n > 0 {
			if err := p.pass(p.piece[:n]); err != nil {
				return err
			}
		}
		switch {
		case err != nil:
			return fmt.Errorf("%w: %s transfer coding: %v", ErrMalformedResponse, p.coding, err)
		case p.maxStreamed > 0 && p.inflated > p.maxStreamed:
			return ErrResponseBodyTooLarge
		case done:
			return nil
		case n == 0 && used == 0:
			// The rest of the coded body has not arrived.
			return nil
		}
	}
}

// finishCoding checks that a coded body that streams has ended in a complete
// coded stream, as its framing or the connection's close said it had.
func (p *responseParser) finishCoding() error {
	if p.coding == "" || p.codingDone {
		return nil
	}
	if err := p.passBody(nil, true); err != nil {
		return err
	}
	if !p.codingDone {
		return fmt.Errorf("%w: %s transfer coding: %v", ErrMalformedResponse, p.coding, io.ErrUnexpectedEOF)
	}
	return nil
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
	// net/http's readers take chunked alone, so a head sent in another
	// transfer coding is rewritten for them here, and the body decoded from
	// it (see splitTransferCodings). An HTTP/1.0 response's Transfer-Encoding
	// is left to net/http, which ignores it, as a 1.0 message has none.
	raw := p.buffer[:headerEnd]
	var coding transferCoding
	var codings []string
	chunked := false
	if len(raw) < 8 || string(raw[:8]) != "HTTP/1.0" {
		if _, ok := transferEncodingFields(raw); ok && !plainChunked(raw) {
			rewritten, c, list, err := splitTransferCodings(raw, false)
			if err != nil {
				if errors.Is(err, ErrUnsupportedTransferEncoding) {
					return false, fmt.Errorf("%w: unsupported transfer coding", ErrMalformedResponse)
				}
				return false, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
			}
			coding, codings, raw = c, list, rewritten
			chunked = list[len(list)-1] == "chunked"
		}
	}
	head, ok := parseResponseHead(raw, req)
	if !ok {
		var err error
		head, err = stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), req)
		if err != nil {
			return false, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
		}
		_ = head.Body.Close()
	}
	if head.StatusCode >= 100 && head.StatusCode < 200 && head.StatusCode != stdhttp.StatusSwitchingProtocols {
		p.continued = p.continued || head.StatusCode == stdhttp.StatusContinue
		p.consume(headerEnd)
		return true, nil
	}
	if codings != nil {
		// The codings as they were listed, where net/http would report the
		// chunked one alone.
		head.TransferEncoding = codings
		p.codings = codings
	}
	p.head, p.headerEnd = head, headerEnd
	switch {
	case !bodyAllowed(req, head.StatusCode):
		p.framing = framingNone
	case codings != nil && chunked:
		p.framing = framingChunked
	case codings != nil:
		// Without chunked last, a response's body ends with the connection
		// (RFC 9112 section 6.3).
		p.framing = framingUntilClose
	case len(head.TransferEncoding) != 0:
		p.framing = framingChunked
	case head.ContentLength >= 0:
		p.framing = framingLength
	default:
		p.framing = framingUntilClose
	}
	if coding != "" {
		p.coding, p.codingDec = coding, newCodingDecoder(coding)
		p.piece = make([]byte, codingPiece)
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

// completeChunked decodes a chunked body. One with no trailer, announced or
// sent, and nothing net/http's chunked reader would refuse, is decoded here;
// any other goes through net/http's own decoder, so that its trailer lands
// where callers expect it and what it refuses is refused. The body needs no
// limit here: chunkedEnd has framed it and refused one that decodes past
// maxBody. A body sent in another transfer coding is decoded from what the
// chunks carry, within maxBody.
func (p *responseParser) completeChunked(end int, req *stdhttp.Request) (*stdhttp.Response, error) {
	if p.codings != nil {
		// The head was rewritten for net/http's reader, which reads chunked
		// alone, so the framing is decoded here instead, as the streamed path
		// does; the trailer lands where callers expect it either way.
		body, used, trailerBlock, done, err := (&chunkedDecoder{maxTrailer: p.maxHeader}).decode(
			p.scratch[:0], p.buffer[p.headerEnd:end])
		p.scratch = body
		if err == nil && (!done || p.headerEnd+used != end) {
			err = fmt.Errorf("%w: chunked body not framed as it was scanned", ErrMalformed)
		}
		if err != nil {
			return nil, clientParseError(err)
		}
		fields, err := parseTrailer(trailerBlock)
		if err != nil {
			return nil, clientParseError(err)
		}
		if fields != nil {
			p.head.Trailer = fields
		}
		if p.coding == "" {
			return p.complete(end, body), nil
		}
		decoded, err := decodeWhole(p.coding, body, p.maxBody, ErrResponseBodyTooLarge, ErrMalformedResponse)
		if err != nil {
			return nil, err
		}
		return p.completeOwned(end, decoded), nil
	}
	var body []byte
	if p.head.Trailer == nil {
		if decoded, ok := decodeChunks(p.scratch[:0], p.buffer[p.headerEnd:end]); ok {
			p.scratch = decoded
			body = decoded
		}
	}
	if body == nil {
		full, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(p.buffer[:end])), req)
		if err == nil {
			body, err = io.ReadAll(full.Body)
			_ = full.Body.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
		}
		p.head = full
	}
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

// completeOwned is complete for a body the parser owns and hands over as it
// is: the caller gets the array itself, not a copy of it.
func (p *responseParser) completeOwned(end int, body []byte) *stdhttp.Response {
	resp := p.head
	if len(body) == 0 {
		resp.Body = stdhttp.NoBody
	} else {
		resp.Body = &bufferedBody{data: body}
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
