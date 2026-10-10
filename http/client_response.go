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
	s *ResponseStream
	// abort gives the response up; see Close.
	abort func(error)
}

// NewBufferedBody returns the Body for a response that arrived whole, which
// DeliverResponse can hand to OnBody without copying it. The body does not
// copy data either.
func NewBufferedBody(data []byte) io.ReadCloser {
	if len(data) == 0 {
		return stdhttp.NoBody
	}
	return &bufferedBody{data: data}
}

// DeliverResponse runs callback with resp, a response whose body has arrived
// whole, as a ClientResponse that OnBody serves once callback returns. abort
// is what ClientResponse.Close does: it fails the request and abandons the
// exchange. It is for the clients of other transports, such as package http3,
// to deliver their responses the way this package's Client does.
func DeliverResponse(resp *stdhttp.Response, abort func(error), callback func(*ClientResponse, error)) {
	cr := &ClientResponse{Response: resp, abort: abort}
	cr.whole, _ = resp.Body.(*bufferedBody)
	cr.mu.Lock()
	cr.inCallback = true
	cr.mu.Unlock()
	callback(cr, nil)
	cr.mu.Lock()
	cr.inCallback = false
	taken := cr.fn != nil
	cr.mu.Unlock()
	if taken {
		cr.deliverWhole()
	}
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
	if c.abort != nil {
		c.abort(ErrResponseAbandoned)
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

// StreamHooks ties a ResponseStream to the request it answers, which is the
// transport's own.
type StreamHooks struct {
	// Begin is called just before the callback runs, with the stream, which
	// the request is to tell of a later failure by Fail. It reports false if
	// the request has already ended, and then nothing is called.
	Begin func(*ResponseStream) bool
	// Callback runs the callback with the response.
	Callback func(*ClientResponse)
	// Whole is called with the response if its body ended before Begin, so
	// that it is delivered as one that arrived whole: with DeliverResponse.
	Whole func(*stdhttp.Response)
	// Done settles the request once a response that streamed has ended. It
	// reports false if the request had ended already.
	Done func() bool
	// Abort fails the request with err and abandons the exchange.
	Abort func(err error)
	// Hold stops or resumes the transport's reading, for a transport that
	// can stop reading just this response, by holding the connection's.
	// Transports that pace the sender by flow control instead leave it nil
	// and set Credit.
	Hold func(hold bool)
	// Credit is told how many bytes of body have left the stream's buffer,
	// and gives the sender that much window. Body that arrives before the
	// callback runs is credited at once, so that a threshold larger than the
	// window cannot stall the sender. It may be called from any goroutine,
	// but never while the stream holds a lock the transport's Data and Fail
	// calls hold.
	Credit func(n int64)
}

// StreamOptions are the limits on a ResponseStream, from the client's
// configuration.
type StreamOptions struct {
	// Threshold is how much body to collect before running the callback.
	Threshold int64
	// Buffer is how many bytes may wait for a reader before Hold stops
	// reads; zero means DefaultStreamResponseBodyBuffer.
	Buffer int
}

// ResponseStream carries the body of a response that streams, from the worker that
// reads the connection to whoever takes it: OnBody's function, or Read.
//
// It is for clients: package http's own, and those of other transports, which
// build one for a response whose body is still arriving, call Start, then Data
// for each piece of the body, in order, and End when it is complete or Fail
// when it will not be. Everything it needs of the request it answers comes
// through StreamHooks.
type ResponseStream struct {
	hooks StreamHooks
	req   *stdhttp.Request
	cr    *ClientResponse
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
	// preCredited counts what of buf has been credited already, when
	// hooks.Credit paces the sender.
	preCredited int64
}

// takeLocked accounts n bytes leaving buf, read, delivered or thrown away, and
// returns how many of them the sender is still owed window for.
func (s *ResponseStream) takeLocked(n int) int64 {
	if s.hooks.Credit == nil {
		return 0
	}
	c := int64(n)
	d := min(c, s.preCredited)
	s.preCredited -= d
	return c - d
}

// give passes credit on to the sender. It must be called without s.mu held.
func (s *ResponseStream) give(n int64) {
	if n > 0 && s.hooks.Credit != nil {
		s.hooks.Credit(n)
	}
}

// NewResponseStream returns the stream for a response, head, whose body is
// still arriving, to the request req. head.Body is replaced.
func NewResponseStream(head *stdhttp.Response, req *stdhttp.Request, options StreamOptions, hooks StreamHooks) *ResponseStream {
	s := &ResponseStream{hooks: hooks, req: req, threshold: options.Threshold, highWater: options.Buffer}
	if s.highWater <= 0 {
		s.highWater = DefaultStreamResponseBodyBuffer
	}
	s.cr = &ClientResponse{Response: head, s: s, abort: hooks.Abort}
	head.Body = &streamBody{s: s}
	return s
}

// Start runs the callback at once if no body need be collected first.
func (s *ResponseStream) Start() {
	if s.threshold <= 0 {
		s.fire()
	}
}

// fire runs the callback, on the goroutine that delivers the body.
func (s *ResponseStream) fire() {
	if !s.hooks.Begin(s) {
		return
	}
	s.mu.Lock()
	s.fired, s.inCallback = true, true
	s.mu.Unlock()
	s.hooks.Callback(s.cr)
	s.afterCallback()
}

// afterCallback throws the body away if the callback left it with no one, and
// otherwise hands over what it has.
func (s *ResponseStream) afterCallback() {
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
func (s *ResponseStream) Data(b []byte) {
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
			s.hooks.Abort(ErrResponseAbandoned)
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
	if !s.fired && s.hooks.Credit != nil {
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
func (s *ResponseStream) End() {
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
		if s.hooks.Credit != nil && resp.ContentLength < 0 && s.req.Method != stdhttp.MethodHead {
			// What an HTTP/2 response that arrived whole reports.
			resp.ContentLength = int64(len(body))
		}
		s.hooks.Whole(resp)
		return
	}
	s.mu.Unlock()
	if s.hooks.Done() {
		s.drain()
	}
}

// fail ends the body with err, which fn hears of and Read reports.
func (s *ResponseStream) Fail(err error) {
	s.mu.Lock()
	if s.err == nil && !s.finSent {
		s.err = err
	}
	s.buf = nil
	s.unholdLocked()
	s.mu.Unlock()
	s.drain()
}

func (s *ResponseStream) setFn(fn BodyFunc) {
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
func (s *ResponseStream) drain() {
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

// holdLocked stops the transport's reads while more waits for a reader than
// the buffer allows, and starts them again once it has gone down by half.
func (s *ResponseStream) holdLocked() {
	if s.hooks.Hold == nil {
		return
	}
	switch {
	case !s.held && s.fired && !s.ended && s.err == nil && len(s.buf) > s.highWater:
		s.held = true
		s.hooks.Hold(true)
	case s.held && len(s.buf) <= s.highWater/2:
		s.held = false
		s.hooks.Hold(false)
	}
}

func (s *ResponseStream) unholdLocked() {
	if s.held {
		s.held = false
		s.hooks.Hold(false)
	}
}

func (s *ResponseStream) complete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

func (s *ResponseStream) trailerFields() stdhttp.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer
}

func (s *ResponseStream) consumedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumed
}

// streamBody is the Body of a response that streams. It reads what has
// arrived and never waits for more: with nothing to read and the body not
// over, it returns ErrWouldBlock, and the data is better taken with OnBody.
// Closing it does nothing; ClientResponse.Close gives the response up.
type streamBody struct{ s *ResponseStream }

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
