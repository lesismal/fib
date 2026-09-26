package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/textproto"
	"strconv"
	"sync"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// ErrBodyAbandoned is what a streamed body reports to a reader that comes back
// to it after Close, after OnBody took the body over, or after the handler it
// belongs to has returned.
var ErrBodyAbandoned = errors.New("http: request body no longer being read")

// ErrWouldBlock is what reading a streamed body reports when none of it has
// arrived yet. It is the engine's own, so a handler may test either for it.
var ErrWouldBlock = fib.ErrWouldBlock

// discardAfterHandler bounds what the server will still take off a connection
// to keep it alive when the handler returned without reading the whole body.
// Past it the connection closes instead: draining an upload nobody wants is
// work the client, not the server, should pay for.
const discardAfterHandler = 256 << 10

// BodyFunc receives a request's body as it arrives, piece by piece; see
// Context.OnBody. fin marks the last call, and err a body that will not be
// finished, which is also a last call. data is only valid for the duration of
// the call.
type BodyFunc func(data []byte, fin bool, err error)

// sinkFunc is what a BodyStream hands its body to once OnBody has taken it
// over: a BodyFunc, told as well whether it is being called on the
// connection's worker, which holds the parser while it delivers.
type sinkFunc func(data []byte, fin bool, err error, worker bool)

// emptyBody is a Body with nothing in it, for a request whose body has been
// handed to a callback instead.
func emptyBody() io.ReadCloser { return stdhttp.NoBody }

// BodyStream is the Body of a request whose handler ran before the whole body
// had arrived; see Config.StreamRequestBody.
//
// It never waits. The handler of a streamed request runs on the connection's
// worker like any other, and a worker that waited for the peer would be
// waiting on itself, so Read takes what has arrived and reports
// ErrWouldBlock for the rest. io.EOF means the whole body has been read, and
// any other error that the rest of it will never arrive — a connection that
// closed mid-body, a body past MaxStreamedBodyBytes, broken framing — so a
// truncated upload is never mistaken for a complete one.
//
// A handler that meets ErrWouldBlock and wants the rest — or that finds
// Context.BodyComplete false before reading at all — asks for the rest
// through Context.OnBody, which keeps the request open and delivers the body
// as it arrives. Until it does, what has arrived and not been
// read is buffered, and once that buffer reaches
// Config.StreamRequestBodyBuffer the connection stops reading its socket,
// which slows the client down through TCP flow control rather than growing
// memory here.
//
// Reading after Close, after OnBody has taken the body over, or after the
// handler has returned without retaining the request, returns
// ErrBodyAbandoned.
type BodyStream struct {
	conn *fib.Connection
	mu   sync.Mutex
	// buf holds body bytes that have arrived and not been read, from read
	// onwards. Bytes before read have been handed out already.
	buf  []byte
	read int
	// remaining counts down a Content-Length body; chunked leaves it at -1
	// and decoder frames the body instead.
	remaining int64
	decoder   *chunkedDecoder
	// total is how much of the body has been taken off the connection, which
	// limit bounds; zero means unbounded.
	total int64
	limit int64
	// highWater is how many unread bytes make the connection hold its reads,
	// and held records that it is holding them for this body.
	highWater int
	held      bool
	// ended is set once the whole body has been taken off the connection,
	// err once it never will be. Both are reported only after the reader has
	// drained what did arrive.
	ended bool
	err   error
	// abandoned is set when the reader is done with the body before the body
	// is done: Close, or a handler that returned without reading it all.
	// What still arrives is consumed to keep the connection framed, and
	// dropped.
	abandoned bool
	// trailer is the request's Trailer, filled in from a chunked body's
	// trailer section when the body ends.
	trailer stdhttp.Header
	request *stdhttp.Request
	// wantContinue is a body the client will not send until it is told to.
	// The telling is left until the first Read, so that a handler which
	// refuses the request answers it without the upload ever starting.
	wantContinue bool
	continueSent bool
	// scratch is the buffer the chunked decoder decodes into, kept between
	// reads so that a chunked upload does not allocate one per read.
	scratch []byte
	// sink, when set, is given the body as it arrives instead of it being
	// buffered for a reader; Context.OnBody installs it. sinkBusy is set
	// while one goroutine is handing the sink what has arrived, and nothing
	// waits for it: whatever arrives meanwhile is buffered, and that
	// goroutine hands it on before it lets go. delivered records the sink's
	// last call.
	sink      sinkFunc
	sinkBusy  bool
	delivered bool
	// sendContinue sends the 100 Continue the body waits for, on a protocol
	// other than HTTP/1, which conn sends it on. credit is told of every
	// byte of the body the handler has taken, by reading it or through
	// OnBody, and of every byte thrown away unread, so that a multiplexed
	// protocol gives its peer room for more only as the body is consumed;
	// see BodyFeed. HTTP/1 holds the connection's reads instead.
	sendContinue func()
	credit       func(n int)
}

// setSink hands the body to fn as it arrives rather than buffering it for
// Read. It never calls fn itself: it reports whether fn is owed something
// already — body taken off the connection and not yet read, or the body's
// end — in which case the caller has to hand it over with drain, and nothing
// else will until it has. reserve asks for that whether or not anything is
// owed yet, for a handler still running: on a multiplexed connection the body
// goes on arriving on another goroutine meanwhile, and has to wait for the
// handler to return rather than reach fn under its feet.
func (s *BodyStream) setSink(fn sinkFunc, reserve bool) bool {
	// Asking for the body as a callback is asking for it, so a client waiting
	// for permission to send it is given that here as a read would.
	s.grantContinue()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sink = fn
	if s.sinkBusy || !reserve && !s.owedLocked() {
		// Either another goroutine is handing the body on and picks the new
		// sink up from here, or there is nothing to hand on until the
		// connection delivers more.
		return false
	}
	s.sinkBusy = true
	return true
}

// owedLocked reports whether the sink has something to be given: body not
// yet handed on, or its last call.
func (s *BodyStream) owedLocked() bool {
	if s.delivered {
		return false
	}
	return s.read < len(s.buf) || s.ended || s.err != nil || s.abandoned
}

// drain hands the sink whatever it is owed, until nothing is left, and then
// lets another goroutine deliver again. The caller has set sinkBusy. worker
// says drain runs on the connection's worker, holding its parser.
//
// Nothing is held back while the sink has the body, so the buffered bytes and
// any hold on the connection's reads go to it together: what paces the peer
// from here is the sink itself.
func (s *BodyStream) drain(worker bool) {
	for {
		s.mu.Lock()
		sink := s.sink
		if sink == nil || !s.owedLocked() {
			s.sinkBusy = false
			s.mu.Unlock()
			return
		}
		pending := s.buf[s.read:]
		s.buf, s.read = nil, 0
		s.releaseLocked()
		var fin bool
		var err error
		switch {
		case len(pending) > 0:
			// The body's end goes with its last bytes, but an error is
			// reported on a call of its own once the bytes before it are in.
			fin = s.ended && s.err == nil
		case s.abandoned && !s.ended && s.err == nil:
			fin, err = true, ErrBodyAbandoned
		default:
			fin, err = true, s.err
		}
		s.delivered = fin
		s.mu.Unlock()

		sink(pending, fin, err, worker)
		s.credited(len(pending))

		if len(pending) > 0 {
			s.mu.Lock()
			if s.buf == nil && !s.abandoned {
				// The buffer is the stream's again, for what arrives next.
				s.buf = pending[:0]
			}
			s.mu.Unlock()
		}
	}
}

func newBodyStream(conn *fib.Connection, request *stdhttp.Request, config Config, decoder *chunkedDecoder, wantContinue bool) *BodyStream {
	highWater := config.StreamRequestBodyBuffer
	if highWater <= 0 {
		highWater = DefaultStreamRequestBodyBuffer
	}
	s := &BodyStream{
		conn:         conn,
		request:      request,
		remaining:    -1,
		decoder:      decoder,
		limit:        config.MaxStreamedBodyBytes,
		highWater:    highWater,
		wantContinue: wantContinue,
	}
	if decoder == nil {
		s.remaining = request.ContentLength
	}
	return s
}

// Read takes what of the body has arrived, and never waits for the rest: it
// reports ErrWouldBlock when none of it has, io.EOF when all of it has been
// read, and the reason the body ended short when it did. See BodyStream.
func (s *BodyStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.grantContinue()
	s.mu.Lock()
	if s.sink != nil {
		// What is buffered is the sink's now.
		s.mu.Unlock()
		return 0, ErrBodyAbandoned
	}
	if s.read < len(s.buf) {
		n := copy(p, s.buf[s.read:])
		s.read += n
		s.compactLocked()
		s.mu.Unlock()
		s.credited(n)
		return n, nil
	}
	defer s.mu.Unlock()
	switch {
	case s.abandoned:
		return 0, ErrBodyAbandoned
	case s.ended:
		return 0, io.EOF
	case s.err != nil:
		return 0, s.err
	}
	return 0, ErrWouldBlock
}

// Close gives up the rest of the body. The connection consumes and drops what
// is still on its way, so that a request whose body the handler did not want
// does not leave the next request on the connection misframed; past
// discardAfterHandler unread bytes the connection closes instead. A body that
// OnBody has taken over ends there too, with ErrBodyAbandoned.
func (s *BodyStream) Close() error {
	s.mu.Lock()
	dropped := 0
	if !s.abandoned {
		s.abandoned = true
		dropped = s.dropLocked()
		s.releaseLocked()
	}
	s.deliverLocked(false)
	s.credited(dropped)
	return nil
}

// credited passes n bytes the handler is done with on to credit.
func (s *BodyStream) credited(n int) {
	if n > 0 && s.credit != nil {
		s.credit(n)
	}
}

// deliverLocked unlocks the stream and, when the sink is owed something and
// no goroutine is handing it on already, hands it on from this one.
func (s *BodyStream) deliverLocked(worker bool) {
	if s.sink == nil || s.sinkBusy || !s.owedLocked() {
		s.mu.Unlock()
		return
	}
	s.sinkBusy = true
	s.mu.Unlock()
	s.drain(worker)
}

// Trailer is the trailer section of a chunked body, or nil. It is complete
// only once Read has returned io.EOF; the request's Trailer is the same map.
func (s *BodyStream) Trailer() stdhttp.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer
}

// Consumed is how many bytes of the body have been taken off the connection so
// far, decoded, which is at least what Read has returned.
func (s *BodyStream) Consumed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// Complete reports whether the whole body has arrived.
func (s *BodyStream) Complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

// grantContinue sends the 100 Continue a body waits for, once, when the
// handler first asks for the body, by reading it or by taking it as a
// callback.
func (s *BodyStream) grantContinue() {
	s.mu.Lock()
	send := s.wantContinue && !s.continueSent && !s.ended && s.err == nil
	s.continueSent = s.continueSent || send
	s.mu.Unlock()
	switch {
	case !send:
	case s.sendContinue != nil:
		s.sendContinue()
	case s.conn != nil:
		_ = s.conn.Send([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
		_ = s.conn.Flush()
	}
}

// compactLocked drops bytes the reader has taken and lets the connection read
// again once the buffer is half empty. Waiting for half rather than resuming
// at the watermark keeps a reader that takes a few bytes at a time from
// pausing and resuming the socket for each of them, the same hysteresis the
// engine's write watermarks use.
func (s *BodyStream) compactLocked() {
	if s.read == len(s.buf) {
		// The buffer is kept whatever it grew to: a body never buffers more
		// than the watermark plus one read, and it is thrown away with the
		// request anyway, so re-allocating it every time it empties would
		// only cost an allocation per read round.
		s.buf, s.read = s.buf[:0], 0
	}
	if s.held && len(s.buf)-s.read <= s.highWater/2 {
		s.releaseLocked()
	}
}

// dropLocked throws away what the reader will not take, and reports how much
// that was.
func (s *BodyStream) dropLocked() int {
	n := len(s.buf) - s.read
	s.buf, s.read = nil, 0
	return n
}

// push hands decoded body bytes to the reader, holding the connection's reads
// when they pile up faster than the reader takes them.
func (s *BodyStream) push(data []byte) {
	s.mu.Lock()
	if s.abandoned || s.err != nil || s.ended {
		// Nobody will take it: the body was given up or has already ended.
		s.mu.Unlock()
		s.credited(len(data))
		return
	}
	if sink := s.sink; sink != nil && !s.sinkBusy && s.read == len(s.buf) {
		// Nothing is waiting ahead of data, so it goes to the sink straight
		// from the connection's buffer.
		s.sinkBusy = true
		s.mu.Unlock()
		sink(data, false, nil, true)
		s.credited(len(data))
		s.drain(true)
		return
	}
	if s.read > 0 && s.read == len(s.buf) {
		s.buf, s.read = s.buf[:0], 0
	}
	// A sink that is busy on another goroutine takes this once it is done
	// with what it has; until then it is buffered, and bounded, as it is for
	// a reader.
	s.buf = append(s.buf, data...)
	if !s.held && len(s.buf)-s.read >= s.highWater {
		s.held = true
		s.holdReads(true)
	}
	s.deliverLocked(true)
}

// finish marks the body complete and publishes its trailer.
func (s *BodyStream) finish(trailer stdhttp.Header) {
	s.mu.Lock()
	s.ended = true
	if trailer != nil {
		s.trailer = trailer
		if s.request != nil {
			s.request.Trailer = trailer
		}
	}
	s.releaseLocked()
	s.deliverLocked(true)
}

// fail ends the body short, with the reason the rest will never arrive. A
// peer that simply closed reports io.EOF, which becomes io.ErrUnexpectedEOF
// here: the body is not over, so a reader must not take the close for its end.
// worker says fail runs on the connection's worker, holding its parser.
func (s *BodyStream) fail(err error, worker bool) {
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	if s.err == nil && !s.ended {
		s.err = err
	}
	s.releaseLocked()
	s.deliverLocked(worker)
}

// releaseLocked ends any hold this body has on the connection's reads. The
// engine call is safe under the lock: it records the change and wakes the
// event loop rather than waiting for it.
func (s *BodyStream) releaseLocked() {
	if s.held {
		s.held = false
		s.holdReads(false)
	}
}

// holdReads passes a hold on to the connection, if there is one: a Parser used
// on its own, outside a server, has no connection to hold.
func (s *BodyStream) holdReads(hold bool) {
	if s.conn != nil {
		s.conn.HoldReads(hold)
	}
}

// abandon is what the handler's return does to a body it has not finished
// reading. It reports whether the connection has to close: either the client
// is still waiting for the 100 Continue that now will never come, or what is
// left of the body is more than the server is willing to read and throw away.
func (s *BodyStream) abandon() (closing bool) {
	s.mu.Lock()
	s.abandoned = true
	dropped := s.dropLocked()
	s.releaseLocked()
	defer s.credited(dropped)
	switch {
	case s.ended || s.err != nil:
	case s.wantContinue && !s.continueSent:
		closing = true
	default:
		closing = s.remaining < 0 || s.remaining > discardAfterHandler
	}
	// A sink still waiting for the rest hears that it is not coming.
	s.deliverLocked(true)
	return closing
}

// absorb takes as much of src as the body's framing accounts for, hands the
// decoded bytes to the reader, and reports how much of src it used and whether
// the body ended there.
func (s *BodyStream) absorb(src []byte) (used int, done bool, err error) {
	if s.decoder == nil {
		used, done, err = s.reserve(len(src))
		if err != nil {
			return used, false, err
		}
		s.push(src[:used])
		if !done {
			return used, false, nil
		}
		s.finish(nil)
		return used, true, nil
	}
	decoded, used, trailerBlock, done, err := s.decoder.decode(s.scratch[:0], src)
	if len(decoded) > 0 {
		if limitErr := s.account(int64(len(decoded))); limitErr != nil {
			s.keepScratch(decoded)
			return used, false, limitErr
		}
		s.push(decoded)
	}
	s.keepScratch(decoded)
	if err != nil || !done {
		return used, false, err
	}
	trailer, err := parseTrailer(trailerBlock)
	if err != nil {
		return used, false, err
	}
	s.finish(trailer)
	return used, true, nil
}

// keepScratch takes the decoder's buffer back for the next read. Like buf it
// is bounded by the watermark and dies with the request, so it is kept rather
// than re-allocated.
func (s *BodyStream) keepScratch(b []byte) { s.scratch = b }

// reserve claims up to n bytes of a Content-Length body: it returns how many
// of them the body is still owed and whether that is the last of it. The count
// lives under the lock because abandon reads it from the handler's goroutine
// while the connection's worker is still delivering.
func (s *BodyStream) reserve(n int) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if int64(n) > s.remaining {
		n = int(s.remaining)
	}
	if s.limit > 0 && s.total+int64(n) > s.limit {
		return n, false, ErrBodyTooLarge
	}
	s.total += int64(n)
	s.remaining -= int64(n)
	return n, s.remaining == 0, nil
}

// account adds n bytes to a chunked body and reports whether it has outgrown
// the configured limit.
func (s *BodyStream) account(n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total += n
	if s.limit > 0 && s.total > s.limit {
		return ErrBodyTooLarge
	}
	return nil
}

// forbiddenTrailer reports fields a trailer may not carry, since they frame,
// route or authenticate the message (RFC 9110 section 6.5.1).
func forbiddenTrailer(name string) bool {
	switch stdhttp.CanonicalHeaderKey(name) {
	case "Content-Length", "Transfer-Encoding", "Trailer", "Host", "Content-Type", "Content-Encoding",
		"Content-Range", "Cache-Control", "Expect", "Max-Forwards", "Pragma", "Range", "Te",
		"Authorization", "Set-Cookie", "Connection", "Keep-Alive", "Upgrade":
		return true
	}
	return false
}

func parseTrailer(block []byte) (stdhttp.Header, error) {
	if len(block) == 0 {
		return nil, nil
	}
	fields, err := textproto.NewReader(bufio.NewReader(bytes.NewReader(block))).ReadMIMEHeader()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	trailer := make(stdhttp.Header, len(fields))
	for key, values := range fields {
		if !forbiddenTrailer(key) {
			trailer[key] = values
		}
	}
	return trailer, nil
}

// chunkedDecoder decodes a chunked body a read at a time. Unlike chunkedEnd,
// which waits for the whole body and lets net/http decode it, it consumes what
// it has and keeps enough state to carry on from the next read. A partial
// chunk line, a partial terminator and a partial trailer section are left
// unconsumed rather than held here, since the parser buffer keeps them anyway.
type chunkedDecoder struct {
	state      uint8
	remaining  int64
	maxTrailer int
}

const (
	chunkStateSize uint8 = iota
	chunkStateData
	chunkStateTerminator
	chunkStateTrailer
)

var crlfcrlf = []byte("\r\n\r\n")

// decode appends the body bytes framed in src to dst. It returns the extended
// dst, how much of src it consumed, the trailer section once the body ends,
// and whether the body ended.
func (d *chunkedDecoder) decode(dst, src []byte) (out []byte, used int, trailer []byte, done bool, err error) {
	for {
		switch d.state {
		case chunkStateSize:
			at := bytes.Index(src[used:], crlf)
			if at < 0 {
				if len(src)-used > d.maxTrailer {
					return dst, used, nil, false, ErrHeaderTooLarge
				}
				return dst, used, nil, false, nil
			}
			line := src[used : used+at]
			if semicolon := bytes.IndexByte(line, ';'); semicolon >= 0 {
				line = line[:semicolon]
			}
			size, parseErr := strconv.ParseUint(string(bytes.TrimSpace(line)), 16, 63)
			if parseErr != nil {
				return dst, used, nil, false, fmt.Errorf("%w: invalid chunk size", ErrMalformed)
			}
			used += at + 2
			if size == 0 {
				d.state = chunkStateTrailer
				continue
			}
			d.remaining = int64(size)
			d.state = chunkStateData
		case chunkStateData:
			n := int64(len(src) - used)
			if n == 0 {
				return dst, used, nil, false, nil
			}
			if n > d.remaining {
				n = d.remaining
			}
			dst = append(dst, src[used:used+int(n)]...)
			used += int(n)
			d.remaining -= n
			if d.remaining == 0 {
				d.state = chunkStateTerminator
			}
		case chunkStateTerminator:
			if len(src)-used < 2 {
				return dst, used, nil, false, nil
			}
			if !bytes.Equal(src[used:used+2], crlf) {
				return dst, used, nil, false, fmt.Errorf("%w: invalid chunk terminator", ErrMalformed)
			}
			used += 2
			d.state = chunkStateSize
		default:
			rest := src[used:]
			if len(rest) >= 2 && bytes.Equal(rest[:2], crlf) {
				return dst, used + 2, nil, true, nil
			}
			at := bytes.Index(rest, crlfcrlf)
			if at < 0 {
				if len(rest) > d.maxTrailer {
					return dst, used, nil, false, ErrHeaderTooLarge
				}
				return dst, used, nil, false, nil
			}
			if at+4 > d.maxTrailer {
				return dst, used, nil, false, ErrHeaderTooLarge
			}
			return dst, used + at + 4, rest[:at+4], true, nil
		}
	}
}

// ErrBodyReleased is what the body of a request that arrived whole reports
// to a reader that comes back to it after the response was finished, by
// which point the server has taken its buffer back.
var ErrBodyReleased = errors.New("http: read of a request body after its response was finished")

// wholeBody is the Body of a request that arrived whole before its handler
// ran. Its bytes are in a pooled buffer, which goes back to the pool once the
// response is finished, as net/http closes a request's body once its handler
// is done: a handler reads the body while it answers the request, or, if it
// retained the request, until it releases it, and a read after that reports
// ErrBodyReleased. Close does nothing, so a handler that closes the body may
// still read it until then.
type wholeBody struct {
	data []byte
	read int
	// pooled says data came from the pool, and released that it has gone
	// back there.
	pooled   bool
	released bool
}

// fill copies data into a pooled buffer for the body to read.
func (b *wholeBody) fill(data []byte) {
	b.data = bufferpool.Get(len(data))
	copy(b.data, data)
	b.pooled = true
}

func (b *wholeBody) Read(p []byte) (int, error) {
	if b.released {
		return 0, ErrBodyReleased
	}
	if b.read >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.read:])
	b.read += n
	return n, nil
}

// WriteTo hands what is left of the body to w in one write, which is what
// io.Copy and io.ReadAll's callers would otherwise do a piece at a time.
func (b *wholeBody) WriteTo(w io.Writer) (int64, error) {
	if b.released {
		return 0, ErrBodyReleased
	}
	if b.read >= len(b.data) {
		return 0, nil
	}
	n, err := w.Write(b.data[b.read:])
	b.read += n
	return int64(n), err
}

// Len is how much of the body has not been read yet.
func (b *wholeBody) Len() int {
	if b.released {
		return 0
	}
	return len(b.data) - b.read
}

func (b *wholeBody) Close() error { return nil }

// release takes the buffer back once the response is finished.
func (b *wholeBody) release() {
	if b.released {
		return
	}
	b.released = true
	if b.pooled {
		bufferpool.Put(b.data)
	}
	b.data = nil
}
