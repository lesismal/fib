//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"io"
	stdhttp "net/http"
	"sync"

	"github.com/lesismal/fib/bufferpool"
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
	// whole, fn, inCallback, delivered and taken serve a response that
	// arrived whole: OnBody delivers its body once the callback has returned,
	// and taken is how much of it that was.
	whole      *bufferedBody
	fn         BodyFunc
	inCallback bool
	delivered  bool
	paused     bool
	taken      int64
	// s is the stream of a response whose body was still arriving when the
	// callback ran, and nil for one that arrived whole.
	s *ResponseStream
	// abort gives the response up; see Close.
	abort func(error)
	// tunnel is what a 101 to StartUpgrade's request switched to.
	tunnel *Tunnel
}

// Tunnel returns the tunnel the connection became when the server answered
// a request sent with Client.StartUpgrade with 101 Switching Protocols, and
// nil for any other response.
func (c *ClientResponse) Tunnel() *Tunnel { return c.tunnel }

// NewBufferedBody returns the Body for a response that arrived whole, which
// DeliverResponse can hand to OnBody without copying it. The body does not
// copy data either.
//
// It is also the Body for a request whose body is bytes already: Client
// takes data as it is rather than reading it into a copy, so data must stay
// as it is until the request's callback has run.
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
// On HTTP/2 that worker reads every stream of the connection, so fn that
// waits holds them all up, not only its own; a reader of Body that stalls
// holds up only its stream.
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

// HoldBody pauses, with hold set, the delivery of the body to OnBody's
// function, and resumes it without. While it is held, what arrives waits in
// the client's buffer, as it does for a function that is slow to return:
// HTTP/1 stops reading the connection once StreamResponseBodyBuffer waits,
// and HTTP/2 and HTTP/3 give back no window for this stream, so the other
// streams on the connection go on. The end of the body waits too, but an
// error, which ends the body without the rest of it, is delivered at once.
// Resuming delivers what waits on the client's side pool, never on the
// caller's goroutine, so HoldBody may be called from inside the function
// and from anywhere else. It is how a caller that passes the body on to a
// writer that must not wait, such as Context.WriteNoWait, pauses it until
// Context.OnWritable says there is room.
//
// A response that arrived whole is held the same way, before its one call.
func (c *ClientResponse) HoldBody(hold bool) {
	if c.s != nil {
		c.s.hold(hold)
		return
	}
	c.mu.Lock()
	resume := c.paused && !hold && c.fn != nil && !c.delivered && !c.inCallback
	c.paused = hold
	c.mu.Unlock()
	if resume {
		sidepool.Go(c.deliverWhole)
	}
}

// deliverWhole hands a body that arrived whole to the function waiting for it.
func (c *ClientResponse) deliverWhole() {
	c.mu.Lock()
	fn := c.fn
	if fn == nil || c.delivered || c.paused {
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
	c.mu.Lock()
	c.taken = int64(len(data))
	c.mu.Unlock()
	fn(data, true, nil)
}

// BodyBytes returns the body of a response that arrived whole, what of it
// Body has yet to read, and true; it reports false for a body that streams.
// The bytes are the client's own copy, which it never reuses, so they stay
// valid for as long as they are referenced and may be handed on without a
// copy, as Context.WriteResponse's Body, say, even past the callback. They
// must not be changed. Taking them reads nothing: Body and OnBody still
// deliver them.
func (c *ClientResponse) BodyBytes() ([]byte, bool) {
	if c.s != nil {
		return nil, false
	}
	switch body := c.Response.Body; {
	case c.whole != nil:
		return c.whole.data[c.whole.off:], true
	case body == nil || body == stdhttp.NoBody:
		return nil, true
	}
	return nil, false
}

// TakeBody hands over the body of a response that arrived whole: what of it
// Body has yet to read, and true; it reports false for a body that streams.
// From then on the bytes are the caller's, and Body, BodyBytes and OnBody
// find the body empty.
//
// pooled says the bytes are a buffer from package bufferpool, which the
// caller may give back, with bufferpool.Put or by sending it with
// Context.WriteResponseOwned, once it has done with them; it need not, since
// the collector frees a buffer that is not given back. It is how a proxy
// passes a response on without a copy and without the allocation each
// response's body otherwise costs: TakeBody, then WriteResponseOwned.
func (c *ClientResponse) TakeBody() (body []byte, pooled, ok bool) {
	if c.s != nil {
		return nil, false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.whole
	if w == nil {
		if b := c.Response.Body; b == nil || b == stdhttp.NoBody {
			return nil, false, true
		}
		return nil, false, false
	}
	body, pooled = w.data[w.off:], w.pooled && w.off == 0
	w.data, w.off, w.pooled = nil, 0, false
	return body, pooled, true
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
		return c.taken
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

// bufferedBody is the Body of a response that arrived whole, or of a request
// whose body is bytes already.
type bufferedBody struct {
	data []byte
	off  int
	// pooled says data is a buffer from package bufferpool, which TakeBody
	// can hand over for the caller to give back. Left to the collector
	// otherwise, as Body and BodyBytes leave it.
	pooled bool
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
	// Detached keeps OnBody's function off the goroutine that delivers the
	// body: it runs on one of its own, and the transport's goroutine only
	// queues the data. It is for transports that must go on reading while
	// the callback takes its time, as QUIC must, to acknowledge what
	// arrives: they pace the sender with Credit, which bounds what waits.
	Detached bool
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
	detached  bool

	mu sync.Mutex
	// chunks hold body that has arrived and not been taken, oldest first, in
	// buffers of streamChunk bytes from the pool; off is how much of the
	// first one Read has taken, and buffered how many bytes they hold in all.
	// Each chunk is handed over whole and goes back to the pool as soon as
	// it has been, so the body waiting costs what is waiting, rather than a
	// buffer grown to it a copy at a time and left to the collector.
	chunks   [][]byte
	off      int
	buffered int
	fn       BodyFunc
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
	// held says the connection's reads are stopped for want of a reader, and
	// paused that HoldBody holds the body back from fn.
	held     bool
	paused   bool
	consumed int64
	trailer  stdhttp.Header
	// preCredited counts what of chunks has been credited already, when
	// hooks.Credit paces the sender.
	preCredited int64
}

// pushLocked adds b to the body waiting, filling the last chunk first.
func (s *ResponseStream) pushLocked(b []byte) {
	s.buffered += len(b)
	for len(b) > 0 {
		last := len(s.chunks) - 1
		if last < 0 || len(s.chunks[last]) == cap(s.chunks[last]) {
			s.chunks = append(s.chunks, bufferpool.Get(streamChunk)[:0])
			last++
		}
		tail := s.chunks[last]
		n := min(cap(tail)-len(tail), len(b))
		s.chunks[last] = append(tail, b[:n]...)
		b = b[n:]
	}
}

// popLocked takes the oldest chunk off the body waiting, and returns what of
// it is unread, data, and the chunk, which goes back to the pool once data
// has been handed on.
func (s *ResponseStream) popLocked() (data, chunk []byte) {
	chunk = s.chunks[0]
	data = chunk[s.off:]
	n := copy(s.chunks, s.chunks[1:])
	s.chunks[n] = nil
	s.chunks = s.chunks[:n]
	s.off = 0
	s.buffered -= len(data)
	return data, chunk
}

// dropLocked throws the body waiting away.
func (s *ResponseStream) dropLocked() {
	for i, chunk := range s.chunks {
		bufferpool.Put(chunk)
		s.chunks[i] = nil
	}
	s.chunks, s.off, s.buffered = s.chunks[:0], 0, 0
}

// takeLocked accounts n bytes leaving chunks, read, delivered or thrown away, and
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
	s := &ResponseStream{hooks: hooks, req: req, threshold: options.Threshold, highWater: options.Buffer, detached: options.Detached}
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
		c := s.takeLocked(s.buffered)
		s.dropLocked()
		s.unholdLocked()
		s.mu.Unlock()
		s.give(c)
		return
	}
	s.mu.Unlock()
	if s.detached {
		s.kick()
		return
	}
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
	if s.fn != nil && s.fired && !s.inCallback && !s.delivering && s.buffered == 0 && !s.detached && !s.paused {
		// Nobody is behind: hand it over from where it was read.
		fn := s.fn
		s.delivering = true
		s.consumed += int64(len(b))
		s.mu.Unlock()
		fn(b, false, nil)
		s.give(int64(len(b)))
		s.mu.Lock()
		s.delivering = false
		more := s.buffered > 0 || s.err != nil
		s.mu.Unlock()
		if more {
			s.drain()
		}
		return
	}
	s.pushLocked(b)
	var early int64
	if !s.fired && s.hooks.Credit != nil {
		early = int64(len(b))
		s.preCredited += early
	}
	fire := !s.fired && int64(s.buffered) >= s.threshold
	if !fire {
		s.holdLocked()
	}
	s.mu.Unlock()
	s.give(early)
	if fire {
		s.fire()
	} else if s.detached {
		s.kick()
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
		var body []byte
		if s.buffered > 0 {
			// The body keeps no more than its size, and the chunks go back.
			body = make([]byte, 0, s.buffered)
			for i, chunk := range s.chunks {
				if i == 0 {
					chunk = chunk[s.off:]
				}
				body = append(body, chunk...)
			}
		}
		s.dropLocked()
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
		s.deliver()
	}
}

// fail ends the body with err, which fn hears of and Read reports.
func (s *ResponseStream) Fail(err error) {
	s.mu.Lock()
	if s.err == nil && !s.finSent {
		s.err = err
	}
	s.dropLocked()
	s.unholdLocked()
	s.mu.Unlock()
	s.deliver()
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
	switch {
	case late && s.detached:
		s.kick()
	case late:
		sidepool.Go(s.drain)
	}
}

// hold is ClientResponse.HoldBody for a body that streams.
func (s *ResponseStream) hold(hold bool) {
	s.mu.Lock()
	resume := s.paused && !hold
	s.paused = hold
	s.mu.Unlock()
	switch {
	case !resume:
	case s.detached:
		s.kick()
	default:
		sidepool.Go(s.drain)
	}
}

// deliver hands fn what has arrived: on this goroutine, or, for a stream
// that is detached, on one of its own.
func (s *ResponseStream) deliver() {
	if s.detached {
		s.kick()
	} else {
		s.drain()
	}
}

// kick starts a goroutine to drain the stream, unless one is in fn already,
// or the callback is still running, which carries on with it when it
// returns.
func (s *ResponseStream) kick() {
	s.mu.Lock()
	if s.delivering || s.inCallback || s.fn == nil || s.paused && s.err == nil {
		s.mu.Unlock()
		return
	}
	s.delivering = true
	s.mu.Unlock()
	// Not a pool's goroutine: fn may take as long as it likes.
	go func() {
		s.mu.Lock()
		s.drainLocked()
	}()
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
	s.drainLocked()
}

// drainLocked is drain's loop, entered with s.mu held and delivering set,
// and left with s.mu released.
func (s *ResponseStream) drainLocked() {
	for {
		fn := s.fn
		switch {
		case s.buffered > 0 && s.err == nil && !s.paused:
			data, chunk := s.popLocked()
			s.consumed += int64(len(data))
			c := s.takeLocked(len(data))
			s.mu.Unlock()
			fn(data, false, nil)
			bufferpool.Put(chunk)
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
		case s.ended && !s.finSent && s.err == nil && !s.paused:
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
	case !s.held && s.fired && !s.ended && s.err == nil && s.buffered > s.highWater:
		s.held = true
		s.hooks.Hold(true)
	case s.held && s.buffered <= s.highWater/2:
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
	if s.buffered > 0 {
		n := 0
		for n < len(p) && s.buffered > 0 {
			k := copy(p[n:], s.chunks[0][s.off:])
			n += k
			s.off += k
			s.buffered -= k
			if s.off == len(s.chunks[0]) {
				// Read all of it: the chunk goes back.
				_, chunk := s.popLocked()
				bufferpool.Put(chunk)
			}
		}
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
