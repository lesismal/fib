//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdhttp "net/http"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/internal/reqbody"
	"github.com/lesismal/fib/sidepool"
)

// BodyWriter is a request body that its producer pushes, a piece at a time,
// rather than one the client reads. Sending it takes no goroutine that waits,
// as reading a body does: Write copies what it is given and returns at once,
// and the client sends it as the connection allows, from whichever goroutine
// makes room, the producer's Write or the connection's own event — the
// socket draining on HTTP/1, a WINDOW_UPDATE on HTTP/2, the stream's flow
// control on HTTP/3.
//
// Set it as a request's Body, with ContentLength when the length is known,
// and call Write as the body is produced, then Close, which sends the end
// with the request's Trailer as it is then, so that trailer values known
// only once the body has been produced go with it, or CloseWithTrailer,
// which sends the trailer it is given instead, or CloseWithError, which
// fails the request. A trailer goes over HTTP/1 only with a chunked body,
// which a request has when its ContentLength is unknown or its Trailer
// names fields, and servers commonly keep only the trailer fields the
// request named in its Trailer when it was sent, so name them there, with
// nil values, before Do. What waits to be sent is bounded by the producer: once more
// than the writer's limit waits, OnDrain says when to go on. A proxy forwards
// an upload with Context.OnBody and Context.HoldBody this way:
//
//	body := fibhttp.NewBodyWriter(0)
//	req.Body, req.ContentLength = body, c.Request.ContentLength
//	client.Do(req, onResponse)
//	c.OnBody(func(data []byte, fin bool, err error) {
//		switch {
//		case err != nil:
//			body.CloseWithError(err)
//			return
//		case len(data) > 0:
//			if _, werr := body.Write(data); werr != nil {
//				return // the request is over
//			}
//		}
//		if fin {
//			body.Close()
//			return
//		}
//		c.HoldBody(true)
//		if !body.OnDrain(func() { c.HoldBody(false) }) {
//			c.HoldBody(false)
//		}
//	})
//
// Write reports an error once the body is closed, or once the request is
// over — answered, failed or abandoned — and OnDrain's callback runs then
// too. The body is sent once: a request whose connection fails after some of
// it has gone fails rather than being sent again. Another transport, such as
// net/http's, reads it, with a Read that waits for what is written, and
// sends the request's Trailer, never what CloseWithTrailer was given.
type BodyWriter = reqbody.Pusher

// NewBodyWriter returns an empty BodyWriter that lets limit bytes wait to be
// sent before OnDrain asks its producer to wait; zero means 256KB.
func NewBodyWriter(limit int) *BodyWriter { return reqbody.NewPusher(limit) }

// goKick runs push.Kick off the goroutine that has room to report, which
// holds a lock the sending takes.
func goKick(push *reqbody.Pusher) { sidepool.Post(push.Kick) }

// kickOnDrain is the callback a sink arms with outputwait.Notify: a
// connection that closed instead settles the request where it closes.
func kickOnDrain(push *reqbody.Pusher) func(error) {
	return func(err error) {
		if err == nil {
			push.Kick()
		}
	}
}

// connRoom reports whether a connection has room for more of a pushed body,
// arming push's Kick for when it has when it does not.
func connRoom(cc *clientConn, push *reqbody.Pusher) bool {
	return outputwait.Notify == nil || !outputwait.Notify(cc.conn, uploadQueued, kickOnDrain(push))
}

// h1Sink sends a pushed body on an HTTP/1 connection: framed by its
// Content-Length, or in chunks.
type h1Sink struct {
	cc   *clientConn
	r    *clientRequest
	push *reqbody.Pusher
}

func (s *h1Sink) Room() bool { return connRoom(s.cc, s.push) }

func (s *h1Sink) Send(p []byte) error {
	if !s.r.chunked {
		return s.cc.conn.Send(p)
	}
	out := appendChunk(bufferpool.Get(len(p) + 20)[:0], p)
	err := s.cc.conn.Send(out)
	bufferpool.Put(out)
	return err
}

func (s *h1Sink) End() error {
	var err error
	if s.r.chunked {
		err = s.cc.conn.Send(lastChunk(s.push.Trailer(s.r.req.Trailer)))
	}
	s.done()
	if err == nil {
		s.r.requestSent()
	}
	return err
}

func (s *h1Sink) Fail(err error) {
	s.done()
	s.r.abort(err)
}

// done records that the body is no longer being sent, which leaves the
// connection fit to carry another request once the response is in.
func (s *h1Sink) done() {
	s.cc.mu.Lock()
	if s.cc.uploading == s.r {
		s.cc.uploading = nil
	}
	s.cc.mu.Unlock()
}

// h2Sink sends a pushed body on an HTTP/2 stream, as its windows allow.
type h2Sink struct {
	hc   *h2ClientConn
	st   *h2ClientStream
	push *reqbody.Pusher
}

func (s *h2Sink) Room() bool {
	hc := s.hc
	hc.mu.Lock()
	if hc.liveLocked(s.st) && len(s.st.pending) > 0 {
		// The windows have not let the last piece all go; a WINDOW_UPDATE
		// that does kicks the body (see flushLocked).
		s.st.pushRoom = s.push
		hc.mu.Unlock()
		return false
	}
	hc.mu.Unlock()
	return connRoom(hc.cc, s.push)
}

func (s *h2Sink) Send(p []byte) error {
	hc := s.hc
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if !hc.liveLocked(s.st) {
		return errStreamGone
	}
	s.st.pending = p
	hc.sendLocked(hc.flushStreamLocked(nil, s.st))
	if len(s.st.pending) > 0 {
		// p is the caller's again once Send returns.
		s.st.pending = append([]byte(nil), s.st.pending...)
	}
	return nil
}

func (s *h2Sink) End() error {
	hc, st := s.hc, s.st
	hc.mu.Lock()
	if !hc.liveLocked(st) {
		hc.mu.Unlock()
		return errStreamGone
	}
	st.streaming = false
	// The trailer's values are those of when the body has ended.
	st.trailer = h2TrailerFields(s.push.Trailer(st.r.req.Trailer))
	if h2PairsSize(st.trailer) > hc.peerMaxHeaderList {
		// As in h2ClientConn.sendBody: the request is not whole without it.
		st.trailer = nil
		hc.mu.Unlock()
		st.r.abort(errH2HeaderListTooLarge)
		return errH2HeaderListTooLarge
	}
	defer hc.mu.Unlock()
	var out []byte
	if len(st.trailer) > 0 {
		out = hc.appendTrailerLocked(nil, st)
	} else {
		out = h2AppendFrameHeader(nil, h2FrameData, h2FlagEndStream, st.id, 0)
		st.endLocalLocked()
	}
	hc.sendLocked(out)
	return nil
}

func (s *h2Sink) Fail(err error) { s.st.r.abort(err) }

// attachPush starts sending r's pushed body through sink, or fails r when it
// cannot be sent again.
func attachPush(r *clientRequest, push *reqbody.Pusher, sink reqbody.Sink) {
	if err := push.Attach(sink, r.declared); err != nil {
		r.abort(err)
	}
}

// pushOf is the body of req when its producer pushes it.
func pushOf(req *stdhttp.Request) *reqbody.Pusher {
	push, _ := req.Body.(*reqbody.Pusher)
	return push
}
