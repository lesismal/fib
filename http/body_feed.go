//go:build linux || darwin || windows

package http

import (
	"fmt"
	"math"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
)

// BodyFeed is the producing side of a request body that streams to its
// handler on a multiplexed protocol: HTTP/2 here, and HTTP/3 in package
// http3. The protocol writes the body into it as it arrives, and the handler
// reads it from Request.Body, a *BodyStream, or takes it through
// Context.OnBody, as it would an HTTP/1 body.
//
// A multiplexed connection cannot stop reading for the sake of one request,
// so a streamed body is paced by the protocol's flow control instead: the
// protocol gives the peer room for more of it only as BodyFeedConfig.Credit
// is told the handler has consumed what it has.
type BodyFeed struct {
	s        *BodyStream
	declared int64
}

// BodyFeedConfig says what a BodyFeed needs of the protocol it serves.
type BodyFeedConfig struct {
	// Declared is the body's length when its request declared one, or -1.
	// A body that turns out longer or shorter fails with ErrMalformed.
	Declared int64
	// Limit bounds the body, as Config.MaxStreamedBodyBytes does; zero
	// leaves it unbounded.
	Limit int64
	// Continue, when set, sends the 100 Continue the client is waiting for
	// before it sends the body. It is called once, when the handler first
	// asks for the body, so that a handler which refuses the request answers
	// it before the upload starts.
	Continue func()
	// Credit is told of every byte of the body the handler has consumed, by
	// reading it or through OnBody, and of every byte thrown away unread, for
	// the protocol to give the peer room for as much again. It is called
	// from whichever goroutine consumed them, and never with the feed's own
	// lock held.
	Credit func(n int)
}

// StreamContext returns the Context through which a handler answers Request,
// which arrived on conn and is answered through stream, as Context does, for
// a request whose body is still arriving: the returned BodyFeed is where the
// protocol writes it, and it is the request's Body, with ContentLength set to
// config.Declared.
func (r *StreamRequest) StreamContext(conn *fib.Connection, stream Stream, config BodyFeedConfig) (*Context, *BodyFeed) {
	c := r.bind(conn, nil)
	c.external = stream
	return c, r.streamBody(c, config)
}

// streamBody makes the request's body a stream fed through the returned
// BodyFeed.
func (r *StreamRequest) streamBody(c *Context, config BodyFeedConfig) *BodyFeed {
	s := &BodyStream{
		request:      &r.Request,
		remaining:    -1,
		limit:        config.Limit,
		highWater:    math.MaxInt,
		wantContinue: config.Continue != nil,
		sendContinue: config.Continue,
		credit:       config.Credit,
	}
	r.Request.Body, r.Request.ContentLength = s, config.Declared
	c.streamed = true
	return &BodyFeed{s: s, declared: config.Declared}
}

// Write hands the handler the next piece of the body, which it may not keep
// past the call. It fails, and fails the body with the same error, when the
// body goes past its declared length (ErrMalformed) or past the limit
// (ErrBodyTooLarge). What is written once the body has failed, has been
// given up by the handler or has ended is dropped, and credited.
func (f *BodyFeed) Write(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	s := f.s
	s.mu.Lock()
	if s.abandoned || s.err != nil || s.ended {
		s.mu.Unlock()
		s.credited(len(data))
		return nil
	}
	s.total += int64(len(data))
	var err error
	switch {
	case f.declared >= 0 && s.total > f.declared:
		err = fmt.Errorf("%w: body longer than its declared length", ErrMalformed)
	case s.limit > 0 && s.total > s.limit:
		err = ErrBodyTooLarge
	}
	s.mu.Unlock()
	if err != nil {
		s.credited(len(data))
		s.fail(err, true)
		return err
	}
	s.push(data)
	return nil
}

// End ends the body, with the trailer that followed it, if any, which becomes
// the request's Trailer. It fails, and fails the body, when the body is
// shorter than its declared length.
func (f *BodyFeed) End(trailer stdhttp.Header) error {
	s := f.s
	s.mu.Lock()
	short := f.declared >= 0 && s.total != f.declared && s.err == nil
	s.mu.Unlock()
	if short {
		err := fmt.Errorf("%w: body shorter than its declared length", ErrMalformed)
		s.fail(err, true)
		return err
	}
	s.finish(trailer)
	return nil
}

// Fail ends the body short: the request was reset, or its connection went.
// The handler hears err through Read or OnBody once it has what arrived
// before it.
func (f *BodyFeed) Fail(err error) { f.s.fail(err, false) }
