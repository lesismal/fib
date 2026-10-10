//go:build linux || darwin || windows

package http

import (
	"errors"
	"io"
	stdhttp "net/http"
	"sync"

	"github.com/lesismal/fib/sidepool"
)

// DefaultStreamResponseBodyBuffer is ClientConfig.StreamResponseBodyBuffer's
// default.
const DefaultStreamResponseBodyBuffer = 256 << 10

var (
	// ErrResponseAbandoned is what a streamed response body reports when it
	// was given up: by ClientResponse.Close, or because the callback returned
	// without taking the body, and the rest of it was more than the client
	// would read to keep the connection.
	ErrResponseAbandoned = errors.New("http: response body no longer being read")
	// ErrBodyTaken is what reading a streamed response's Body reports once
	// OnBody has taken the body over.
	ErrBodyTaken = errors.New("http: read of a response body that OnBody has taken")
)

// ClientResponse is what a Client hands its callback: the response, with
// ClientResponse.Response being the net/http value, embedded so that its fields
// read directly, and a way to take its body as it arrives.
//
// By default the callback runs once the response is complete, Body holds all
// of it and stays readable after the callback returns, and OnBody delivers it
// in one call. With ClientConfig.StreamResponseBody the callback of a large
// response runs before the body has arrived, and the body is taken with OnBody
// or read from Body inside the callback; see that option.
type ClientResponse struct {
	*stdhttp.Response

	mu sync.Mutex
	// whole, fn, inCallback and delivered serve a response that arrived
	// whole: OnBody delivers its body once the callback has returned.
	whole      *bufferedBody
	fn         BodyFunc
	inCallback bool
	delivered  bool
	// s is the stream of a response whose body was still arriving when the
	// callback ran, and nil for one that arrived whole.
	s *respStream
	// r is the request it answers.
	r *clientRequest
}

// newWholeResponse wraps a response whose body has arrived.
func newWholeResponse(resp *stdhttp.Response, r *clientRequest) *ClientResponse {
	cr := &ClientResponse{Response: resp, r: r}
	cr.whole, _ = resp.Body.(*bufferedBody)
	return cr
}

// OnBody delivers the response's body to fn, piece by piece as it arrives.
// fin marks the last call, and err a body that will not be finished — the
// connection went, the request timed out or was cancelled, the body outgrew
// ClientConfig.MaxStreamedBodyBytes, its framing was broken, or Close was
// called — which is also a last call. data is only valid for the duration of
// the call, and the trailer, if any, is in Trailer once fin arrives.
//
// OnBody never calls fn itself and never waits: it registers fn and returns.
// What of the body has already arrived is handed to fn once the callback
// returns, on the goroutine that ran it, and the rest follows as the
// connection is read, on the worker that reads it. The calls are one at a time
// and in order, so a body of any size can be taken without a goroutine and
// without it being buffered: what holds the connection back is fn itself.
// A response that arrived whole is delivered in one call with fin set.
//
// Call OnBody from inside the callback. Called after the callback has
// returned, it still gets a body that is being kept, but not one the client
// gave up on because nothing took it, which fn then hears of as
// ErrResponseAbandoned. Calling it twice replaces fn, and Body is not
// readable once it has been called.
func (c *ClientResponse) OnBody(fn BodyFunc) {
	if fn == nil {
		return
	}
	if c.s != nil {
		c.s.setFn(fn)
		return
	}
	c.mu.Lock()
	if c.delivered {
		c.mu.Unlock()
		return
	}
	c.fn = fn
	late := !c.inCallback
	c.mu.Unlock()
	if late {
		sidepool.Go(c.deliverWhole)
	}
}

// deliverWhole hands a body that arrived whole to the function waiting for it.
func (c *ClientResponse) deliverWhole() {
	c.mu.Lock()
	fn := c.fn
	if fn == nil || c.delivered {
		c.mu.Unlock()
		return
	}
	c.delivered, c.fn = true, nil
	c.mu.Unlock()
	var data []byte
	if c.whole != nil {
		data = c.whole.data[c.whole.off:]
		c.whole.off = len(c.whole.data)
	} else if body := c.Response.Body; body != nil && body != stdhttp.NoBody {
		data, _ = io.ReadAll(body)
	}
	fn(data, true, nil)
}

// BodyComplete reports whether all of the body has arrived, which is always so
// unless the response streams. It is the way to tell, inside the callback,
// whether Body holds the whole of it.
func (c *ClientResponse) BodyComplete() bool {
	if c.s == nil {
		return true
	}
	return c.s.complete()
}

// Trailer returns the trailer fields that followed the body, which are known
// once it has ended, and nil before that and for a body without any.
func (c *ClientResponse) Trailer() stdhttp.Header {
	if c.s == nil {
		return c.Response.Trailer
	}
	return c.s.trailerFields()
}

// Consumed returns how many bytes of the body have gone to OnBody's function
// or been read from Body so far.
func (c *ClientResponse) Consumed() int64 {
	if c.s == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.delivered {
			return int64(len(c.whole.data))
		}
		return 0
	}
	return c.s.consumedBytes()
}

// Close gives up on the rest of a body that is still arriving: the connection
// is closed and OnBody's function, if it has one, hears ErrResponseAbandoned.
// It does nothing once the body has all arrived.
func (c *ClientResponse) Close() {
	if c.r != nil {
		c.r.abort(ErrResponseAbandoned)
	}
}

// bufferedBody is the Body of a response that arrived whole.
type bufferedBody struct {
	data []byte
	off  int
}

func (b *bufferedBody) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

func (b *bufferedBody) Close() error { return nil }

// respStream carries the body of a response that streams, from the worker that
// reads the connection to whoever takes it: OnBody's function, or Read.
type respStream struct {
	cc *clientConn
	r  *clientRequest
	cr *ClientResponse
	// threshold is how much body to collect before running the callback, and
	// highWater how much may wait unread, after that, before reads stop.
	threshold int64
	highWater int

	mu sync.Mutex
	// buf holds body that has arrived and not been taken.
	buf []byte
	fn  BodyFunc
	// fired says the callback has been started, inCallback that it is running,
	// delivering that a goroutine is inside fn, touched that something took or
	// read the body, and discarding that the callback returned without any
	// of that, so the rest is thrown away.
	fired, inCallback, delivering, touched, discarding bool
	dropped                                            int
	// ended says all of the body has arrived, err that it never will, and
	// finSent that fn has been told so.
	ended, finSent bool
	err            error
	// held says the connection's reads are stopped for want of a reader.
	held     bool
	consumed int64
	trailer  stdhttp.Header
	// credit, when set, is how a body that cannot stop the connection's reads
	// — an HTTP/2 stream — paces its sender instead: it is told how many
	// bytes have left the stream's buffer, and gives the sender that much
	// window. Body that arrives before the callback runs is credited at once,
	// so that a threshold larger than the window cannot stall the stream, and
	// preCredited counts what of buf has been credited already.
	credit      func(n int64)
	preCredited int64
}

// takeLocked accounts n bytes leaving buf, read, delivered or thrown away, and
// returns how many of them the sender is still owed window for.
func (s *respStream) takeLocked(n int) int64 {
	if s.credit == nil {
		return 0
	}
	c := int64(n)
	d := min(c, s.preCredited)
	s.preCredited -= d
	return c - d
}

// give passes credit on to the sender. It must be called without s.mu held.
func (s *respStream) give(n int64) {
	if n > 0 && s.credit != nil {
		s.credit(n)
	}
}

func newRespStream(cc *clientConn, r *clientRequest, head *stdhttp.Response) *respStream {
	config := &cc.client.config
	s := &respStream{cc: cc, r: r, threshold: config.StreamResponseBodyThreshold, highWater: config.StreamResponseBodyBuffer}
	if s.highWater <= 0 {
		s.highWater = DefaultStreamResponseBodyBuffer
	}
	s.cr = &ClientResponse{Response: head, s: s, r: r}
	head.Body = &streamBody{s: s}
	return s
}

// start runs the callback at once if no body need be collected first.
func (s *respStream) start() {
	if s.threshold <= 0 {
		s.fire()
	}
}

// fire runs the callback, on the worker that reads the connection.
func (s *respStream) fire() {
	if !s.r.begin(s) {
		return
	}
	s.mu.Lock()
	s.fired, s.inCallback = true, true
	s.mu.Unlock()
	s.r.callback(s.cr, nil)
	s.afterCallback()
}

// afterCallback throws the body away if the callback left it with no one, and
// otherwise hands over what it has.
func (s *respStream) afterCallback() {
	s.mu.Lock()
	s.inCallback = false
	if s.fn == nil && !s.touched {
		s.discarding = true
		c := s.takeLocked(len(s.buf))
		s.buf = nil
		s.unholdLocked()
		s.mu.Unlock()
		s.give(c)
		return
	}
	s.mu.Unlock()
	s.drain()
	s.mu.Lock()
	s.holdLocked()
	s.mu.Unlock()
}

// data takes a piece of the body from the worker that read it.
func (s *respStream) data(b []byte) {
	s.mu.Lock()
	if s.err != nil || s.ended {
		s.mu.Unlock()
		return
	}
	if s.discarding {
		s.dropped += len(b)
		over := s.dropped > discardAfterHandler
		s.mu.Unlock()
		s.give(int64(len(b)))
		if over {
			s.r.abort(ErrResponseAbandoned)
		}
		return
	}
	if s.fn != nil && s.fired && !s.inCallback && !s.delivering && len(s.buf) == 0 {
		// Nobody is behind: hand it over from where it was read.
		fn := s.fn
		s.delivering = true
		s.consumed += int64(len(b))
		s.mu.Unlock()
		fn(b, false, nil)
		s.give(int64(len(b)))
		s.mu.Lock()
		s.delivering = false
		more := len(s.buf) > 0 || s.err != nil
		s.mu.Unlock()
		if more {
			s.drain()
		}
		return
	}
	s.buf = append(s.buf, b...)
	var early int64
	if !s.fired && s.credit != nil {
		early = int64(len(b))
		s.preCredited += early
	}
	fire := !s.fired && int64(len(s.buf)) >= s.threshold
	if !fire {
		s.holdLocked()
	}
	s.mu.Unlock()
	s.give(early)
	if fire {
		s.fire()
	}
}

// end is told that all of the body has arrived. A response that never ran its
// callback does so now, as one that arrived whole.
func (s *respStream) end() {
	s.mu.Lock()
	s.ended = true
	s.trailer = s.cr.Response.Trailer
	s.unholdLocked()
	if !s.fired {
		body := s.buf
		s.buf = nil
		s.mu.Unlock()
		resp := s.cr.Response
		if len(body) == 0 {
			resp.Body = stdhttp.NoBody
		} else {
			resp.Body = &bufferedBody{data: body}
		}
		if s.credit != nil && resp.ContentLength < 0 && s.r.req.Method != stdhttp.MethodHead {
			// What an HTTP/2 response that arrived whole reports.
			resp.ContentLength = int64(len(body))
		}
		s.r.finish(resp, nil)
		return
	}
	s.mu.Unlock()
	if s.r.markDone() {
		s.drain()
	}
}

// fail ends the body with err, which fn hears of and Read reports.
func (s *respStream) fail(err error) {
	s.mu.Lock()
	if s.err == nil && !s.finSent {
		s.err = err
	}
	s.buf = nil
	s.unholdLocked()
	s.mu.Unlock()
	s.drain()
}

func (s *respStream) setFn(fn BodyFunc) {
	s.mu.Lock()
	if s.discarding {
		s.mu.Unlock()
		sidepool.Go(func() { fn(nil, true, ErrResponseAbandoned) })
		return
	}
	s.fn, s.touched = fn, true
	late := !s.inCallback
	s.mu.Unlock()
	if late {
		sidepool.Go(s.drain)
	}
}

// drain hands fn what has arrived, then the end, one call at a time. Whoever
// finds nobody inside fn does it, and the one inside fn carries on with
// whatever arrives meanwhile.
func (s *respStream) drain() {
	s.mu.Lock()
	if s.delivering || s.inCallback || s.fn == nil {
		s.mu.Unlock()
		return
	}
	s.delivering = true
	for {
		fn := s.fn
		switch {
		case len(s.buf) > 0 && s.err == nil:
			data := s.buf
			s.buf = nil
			s.consumed += int64(len(data))
			c := s.takeLocked(len(data))
			s.mu.Unlock()
			fn(data, false, nil)
			s.give(c)
			s.mu.Lock()
			continue
		case s.err != nil && !s.finSent:
			s.finSent = true
			err := s.err
			s.mu.Unlock()
			fn(nil, true, err)
			s.mu.Lock()
			continue
		case s.ended && !s.finSent && s.err == nil:
			s.finSent = true
			s.mu.Unlock()
			fn(nil, true, nil)
			s.mu.Lock()
			continue
		}
		break
	}
	s.delivering = false
	s.holdLocked()
	s.mu.Unlock()
}

// holdLocked stops the connection's reads while more waits for a reader than
// the buffer allows, and starts them again once it has gone down by half.
func (s *respStream) holdLocked() {
	if s.credit != nil {
		return
	}
	switch {
	case !s.held && s.fired && !s.ended && s.err == nil && len(s.buf) > s.highWater:
		s.held = true
		s.cc.conn.HoldReads(true)
	case s.held && len(s.buf) <= s.highWater/2:
		s.held = false
		s.cc.conn.HoldReads(false)
	}
}

func (s *respStream) unholdLocked() {
	if s.held {
		s.held = false
		s.cc.conn.HoldReads(false)
	}
}

func (s *respStream) complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

func (s *respStream) trailerFields() stdhttp.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer
}

func (s *respStream) consumedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumed
}

// streamBody is the Body of a response that streams. It reads what has
// arrived and never waits for more: with nothing to read and the body not
// over, it returns ErrWouldBlock, and the data is better taken with OnBody.
// Closing it does nothing; ClientResponse.Close gives the response up.
type streamBody struct{ s *respStream }

func (b *streamBody) Read(p []byte) (int, error) {
	s := b.s
	s.mu.Lock()
	switch {
	case s.fn != nil:
		s.mu.Unlock()
		return 0, ErrBodyTaken
	case s.discarding:
		s.mu.Unlock()
		return 0, ErrResponseAbandoned
	}
	s.touched = true
	if len(s.buf) > 0 {
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		s.consumed += int64(n)
		c := s.takeLocked(n)
		s.holdLocked()
		s.mu.Unlock()
		s.give(c)
		return n, nil
	}
	defer s.mu.Unlock()
	switch {
	case s.err != nil:
		return 0, s.err
	case s.ended:
		return 0, io.EOF
	}
	return 0, ErrWouldBlock
}

func (b *streamBody) Close() error { return nil }
