//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// SpliceBody offers the rest of the request's body to the request whose body
// w is, for the client to move it from this connection to its own with
// splice(2), rather than through OnBody and w's writes: from the client's
// socket to a pipe and on to the upstream's socket, the kernel moving its
// pages by reference, with no copy into user space and no fresh pages for the
// upstream's send buffer. A proxy that passes an upload on as it arrives
// offers it so, and the client takes it up when it sends the request on an
// HTTP/1 connection without TLS on Linux.
//
// It reports false, and offers nothing, for a body it cannot move so: one
// that arrived whole, one that came over HTTP/2 or HTTP/3, or through TLS,
// one framed in chunks or sent in a transfer coding, one the handler has
// begun to take through OnBody, one that has ended or failed already, and
// any body on a system without splice(2). The caller then takes the body as
// it would have.
//
// When it reports true, w must be the Body of the request the caller sends
// next, with the request's ContentLength, and nothing else may take the body:
// either the client splices it, writing what had arrived before the splice
// began and moving exactly the rest, and fallback is never called; or it
// cannot, as on a connection with TLS or one that is HTTP/2, and calls
// fallback, once, before it wants any of the body, on a goroutine of its own,
// and the caller then pushes the body into w as it would have, through
// OnBody. A request that ends before either, as one cancelled while it waits
// for a connection does, calls neither, and the body is left to the server,
// which ends it with the response as it ends any body the handler did not
// take.
//
// A spliced body is the client's from there on: its bytes never reach
// OnBody, Read or a hold, and the connection reads past it again once the
// last of it has gone to the upstream's socket. A splice that fails because
// the upstream's connection failed hands the rest of the body back to the
// server, which reads and drops it, or closes the connection, as it does a
// body nothing takes; one that fails because the client's connection failed
// closes the upstream's, whose request it has cut short, and the request
// fails as it would have.
func (c *Context) SpliceBody(w *BodyWriter, fallback func()) bool {
	if w == nil || fallback == nil || c.server == nil || c.parser == nil {
		return false
	}
	s := c.RequestBody()
	if s == nil || s.conn != c.Conn || c.Conn == nil || !c.Conn.Spliceable() {
		return false
	}
	s.mu.Lock()
	ok := s.spliceableLocked()
	s.mu.Unlock()
	if !ok {
		return false
	}
	w.OfferSplice(&bodySplice{h: c.server, parser: c.parser, conn: c.Conn, stream: s, fallback: fallback})
	return true
}

// spliceableLocked reports whether the body is one SpliceBody can offer: an
// HTTP/1 body framed by its Content-Length, as it arrives, with some of it
// still to arrive and nothing taking it yet. s.mu is held.
func (s *BodyStream) spliceableLocked() bool {
	return s.conn != nil && !s.offload && s.decoder == nil && s.coding == nil && s.sink == nil &&
		!s.splicing && !s.abandoned && !s.ended && s.err == nil && s.remaining > 0
}

// bodySplice is a body SpliceBody offered, as the client takes it: the
// connection it arrives on and its parser, and the server that drives them.
type bodySplice struct {
	h        *ServerHandler
	parser   *Parser
	conn     *fib.Connection
	stream   *BodyStream
	fallback func()
	// dst is the connection the body goes to, once SpliceTo has it.
	dst *fib.Connection
}

func (b *bodySplice) Decline() { b.fallback() }

// SpliceTo starts moving the body, n bytes in all, to dst.
func (b *bodySplice) SpliceTo(dst *fib.Connection, n int64, written func(error)) bool {
	s := b.stream
	s.mu.Lock()
	ok := s.request != nil && n == s.request.ContentLength && s.spliceableLocked()
	s.splicing = ok
	s.mu.Unlock()
	if !ok {
		return false
	}
	b.dst = dst
	if err := b.conn.SpliceTo(dst, fib.Splice{Begin: b.begin, Read: b.read, Written: written}); err != nil {
		s.mu.Lock()
		s.splicing = false
		s.mu.Unlock()
		return false
	}
	return true
}

// begin hands the destination what of the body has arrived, and returns how
// much is still to come, which the splice moves; see fib.Splice.Begin. It
// runs on the connection's round, between two reads, where nothing adds to
// the body.
func (b *bodySplice) begin() int64 {
	s := b.stream
	// A client waiting for 100 Continue is sent it, as a read would send it.
	s.grantContinue()
	s.mu.Lock()
	if s.sink != nil || s.abandoned || s.err != nil {
		// Taken or ended some other way meanwhile.
		s.splicing = false
		s.mu.Unlock()
		return -1
	}
	buf, pending, spare := s.buf, s.buf[s.read:], s.spare
	s.buf, s.read, s.spare = nil, 0, nil
	// The buffer is empty now: the hold it had on the connection's reads,
	// which the splice paces from here, goes.
	s.releaseLocked()
	n := s.remaining
	s.mu.Unlock()
	if len(pending) > 0 {
		// A failed send closes the destination, which the splice hears.
		_ = b.dst.Send(pending)
	}
	bufferpool.Put(buf)
	bufferpool.Put(spare)
	return n
}

// read is told what the splice took off the connection, and how it ended;
// see fib.Splice.Read.
func (b *bodySplice) read(n int64, err error) {
	s := b.stream
	s.mu.Lock()
	s.total += n
	s.remaining -= n
	s.splicing = false
	held := s.held
	s.mu.Unlock()
	if err == nil {
		// The whole body is off the connection: the parser ends it, as it
		// ends one it took itself, and goes on to what follows it.
		b.h.drive(b.conn, b.parser, nil)
		return
	}
	// The rest of the body is the server's again, to read and drop as it
	// does a body nothing takes, or the connection is closing.
	if !held {
		b.conn.HoldReads(false)
	}
}
