//go:build linux || darwin || windows

package http

import (
	"fmt"
	stdhttp "net/http"
	"sync"

	"github.com/lesismal/fib/bufferpool"
)

// A response written through Context's ResponseWriter methods streams on
// HTTP/2 once it outgrows what those methods hold back, or is flushed: its
// header goes out in HEADERS that leave the stream open, its body in DATA
// frames as it is written and as flow control lets it go, and its end in
// END_STREAM, or in the trailing HEADERS that carry its trailers.

// h2MaxPendingOutput is how much of a streaming response's body a stream
// holds while the client's window keeps it back, before the handler writing
// it waits for the client to take some. A writer that waits is let go once
// half of it has drained, rather than as each frame does.
const h2MaxPendingOutput = 64 << 10

// h2Output is what a stream keeps while its response streams, apart from the
// stream so that the streams of responses sent whole stay small. buf is the
// array from the pool the stream's pending bytes are the tail of, ending is
// set once the handler is done, after which END_STREAM, or the trailer, goes
// out once nothing is pending, and room is what writers waiting for pending
// bytes to drain wait on, waiting counting them. All of it is guarded by the
// connection's mu, which room waits with.
type h2Output struct {
	buf     []byte
	ending  bool
	room    sync.Cond
	waiting int
}

// h2Responder is a stream as what a response that streams is sent through.
type h2Responder h2ServerStream

func (r *h2Responder) BeginResponse(req *stdhttp.Request, status int, header stdhttp.Header) error {
	return (*h2ServerStream)(r).beginResponse(req, status, header)
}

func (r *h2Responder) WriteBody(p []byte, wait bool) error {
	return (*h2ServerStream)(r).writeBody(p, wait)
}

func (r *h2Responder) EndResponse(trailer stdhttp.Header, complete bool) error {
	return (*h2ServerStream)(r).endResponse(trailer, complete)
}

// beginResponse sends the header of a response whose body follows through
// writeBody, leaving the stream open for it.
func (st *h2ServerStream) beginResponse(req *stdhttp.Request, status int, header stdhttp.Header) error {
	if status < 200 || status > 999 {
		return fmt.Errorf("http: invalid status code %d", status)
	}
	if err := h2CheckHeader(header); err != nil {
		return err
	}
	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || st.reset {
		return errH2StreamClosed
	}
	if st.responded {
		return ErrResponseWritten
	}
	st.responded = true
	st.out = &h2Output{}
	st.out.room.L = &sc.mu
	block := sc.enc.Begin(sc.headerBlock[:0])
	block = sc.enc.AppendField(block, ":status", h2Status(status), false)
	if n := declaredLength(header); n >= 0 && statusHasBody(status) {
		block = sc.enc.AppendField(block, "content-length", sc.lengthLocked(n), false)
	}
	block = sc.appendHeaderLocked(block, header)
	out := bufferpool.Get(len(block) + h2FrameHeaderLen*(1+len(block)/sc.peerMaxFrame))[:0]
	out = h2AppendHeaderBlock(out, st.id, block, false, sc.peerMaxFrame)
	sc.keepHeaderBlockLocked(block)
	sc.sendPooledLocked(out)
	return nil
}

// writeBody sends p on a stream whose response streams, as far as the
// windows let it go now, and keeps the rest until they let it. With wait set,
// a handler that writes faster than the client takes its body waits here once
// h2MaxPendingOutput is pending, so that what the stream holds stays bounded
// whatever the handler writes; it is let go when the client has taken enough,
// or the stream or its connection has ended, which it is then told. A handler
// running on the connection's own reader cannot wait, since nothing else
// would read the WINDOW_UPDATE that ends the wait: the stream holds what it
// writes until the client's window lets it go.
func (st *h2ServerStream) writeBody(p []byte, wait bool) error {
	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for {
		if sc.closed || st.reset || st.out == nil || st.out.ending {
			return errH2StreamClosed
		}
		if len(p) == 0 {
			break
		}
		o := st.out
		chunk := p
		if wait {
			// What the windows let go now never waits in the stream.
			room := h2MaxPendingOutput - len(st.pending)
			if len(st.pending) == 0 {
				room += int(max(min(sc.sendWindow, st.sendWindow), 0))
			}
			if room <= 0 {
				o.waiting++
				o.room.Wait()
				o.waiting--
				continue
			}
			chunk = p[:min(len(p), room)]
		}
		p = p[len(chunk):]
		if len(st.pending) > 0 {
			// The windows are shut, or the bytes ahead of these would have
			// gone: they wait behind what is pending.
			buf := o.buf
			if len(st.pending) != len(buf) {
				// pending is the tail of buf; bring it back to the front.
				buf = buf[:copy(buf, st.pending)]
			}
			buf = bufferpool.Append(buf, chunk)
			o.buf, st.pending = buf, buf
			continue
		}
		// Nothing is ahead of these bytes, so what the windows let go is
		// framed straight from them, and only the rest is copied to wait.
		st.pending = chunk
		sc.sendPooledLocked(sc.frameOutputLocked(st))
		if rest := st.pending; len(rest) > 0 {
			o.buf = bufferpool.Append(o.buf[:0], rest)
			st.pending = o.buf
		}
	}
	if o := st.out; len(st.pending) == 0 && o.buf != nil {
		// Nothing is waiting, so the array goes back to the pool rather than
		// staying with a response that may say nothing more for a while.
		bufferpool.Put(o.buf)
		o.buf = nil
	}
	return nil
}

// endResponse ends a response that streams once what is pending has gone:
// with trailer, if HTTP/2 can send any of it, or else END_STREAM. A response
// that is not complete, its body shorter than its content-length, is reset
// instead, since ending it would leave the client a body it cannot tell from
// a whole one; the stream is gone either way once this returns.
func (st *h2ServerStream) endResponse(trailer stdhttp.Header, complete bool) error {
	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || st.reset || st.out == nil || st.out.ending {
		return errH2StreamClosed
	}
	if !complete {
		if feed := st.feed; feed != nil && !st.remoteDone {
			feed.Fail(ErrBodyAbandoned)
		}
		sc.sendLocked(h2AppendRSTStream(nil, st.id, H2InternalError))
		sc.forgetLocked(st)
		sc.closeFinishedLocked()
		return nil
	}
	st.out.ending = true
	st.trailer = h2Trailer(trailer)
	sc.sendPooledLocked(sc.frameOutputLocked(st))
	if o := st.out; len(st.pending) == 0 && o.buf != nil {
		bufferpool.Put(o.buf)
		o.buf = nil
	}
	sc.finishStreamLocked(st)
	return nil
}

// frameOutputLocked frames what of a streaming response's pending bytes the
// windows allow, in a buffer from the pool, and its end once nothing is
// pending if the response is ending.
func (sc *h2ServerConn) frameOutputLocked(st *h2ServerStream) []byte {
	size := h2FrameHeaderLen
	if len(st.pending) > 0 {
		sendable := max(min(int64(len(st.pending)), sc.sendWindow, st.sendWindow), 0)
		size += int(sendable) + h2FrameHeaderLen*(1+int(sendable)/sc.peerMaxFrame)
	}
	out := sc.flushStreamLocked(bufferpool.Get(size)[:0], st)
	if st.out.ending && len(st.pending) == 0 && !st.localDone {
		// The body ended with nothing left to carry END_STREAM: an empty
		// DATA frame does, or the trailer.
		if st.trailer == nil {
			out = h2AppendFrameHeader(out, h2FrameData, h2FlagEndStream, st.id, 0)
		}
		out = sc.endStreamLocked(out, st)
	}
	return out
}

// wakeWritersLocked lets the writers waiting on st's response go once enough
// of what is pending has drained.
func (st *h2ServerStream) wakeWritersLocked() {
	if o := st.out; o != nil && o.waiting > 0 && len(st.pending) <= h2MaxPendingOutput/2 {
		o.room.Broadcast()
	}
}
