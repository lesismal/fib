//go:build linux || darwin || windows

package http3

import (
	"fmt"
	stdhttp "net/http"
	"strconv"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
)

// A response written through Context's ResponseWriter methods streams once
// it outgrows what those methods hold back, or is flushed: requestStream is
// an http.ResponseStreamer, which sends its header in a HEADERS frame of its
// own, each piece of its body in a DATA frame as it is written, and its end
// as the stream's FIN, behind the trailers if it has any.

// maxUnsentOutput is how much of a streaming response's body may wait in its
// QUIC stream, held back by flow or congestion control, before the handler
// writing it waits for the client to take some. A write is cut into pieces of
// this size, so that the wait comes before more is copied in.
const maxUnsentOutput = 64 << 10

// BeginResponse sends the header of a response whose body follows through
// WriteBody, leaving the stream open for it.
func (rs *requestStream) BeginResponse(req *stdhttp.Request, status int, header stdhttp.Header) error {
	// The header is the write the connection was told to expect.
	defer rs.writeDone()
	if status < 200 || status > 999 {
		return fmt.Errorf("http3: invalid status code %d", status)
	}
	if err := checkHeader(header); err != nil {
		return err
	}
	rs.mu.Lock()
	switch {
	case rs.responded:
		rs.mu.Unlock()
		return fibhttp.ErrResponseWritten
	case rs.closed:
		rs.mu.Unlock()
		return errStreamClosed
	}
	rs.responded, rs.streaming = true, true
	rs.mu.Unlock()
	block := bufferpool.Append(nil, qpack.Prefix)
	var digits [20]byte
	block = qpack.AppendField(block, ":status", string(strconv.AppendInt(digits[:0], int64(status), 10)), false)
	if n := declaredLength(header); n >= 0 && status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified {
		block = qpack.AppendField(block, "content-length", string(strconv.AppendInt(digits[:0], n, 10)), false)
	}
	block = appendHeader(block, header, func(name string) bool { return name == "content-length" })
	// The stream takes out over, as the buffer it sends from until the
	// client has acknowledged it.
	out := appendHeadersFrame(bufferpool.Get(len(block) + 16)[:0], block)
	bufferpool.Put(block)
	if err := rs.s.WriteOwned(out, false); err != nil {
		return errStreamClosed
	}
	return nil
}

// WriteBody sends p in DATA frames. With wait set it waits, before each
// piece, for the stream to have sent all but maxUnsentOutput of what it was
// given, so that what a handler writing faster than its client reads leaves
// waiting in the stream stays bounded; without it, which is a handler on the
// goroutine the connection's datagrams arrive on, everything waits in the
// stream until flow control lets it go.
func (rs *requestStream) WriteBody(p []byte, wait bool) error {
	for len(p) > 0 {
		if wait {
			if err := rs.s.WaitSendable(maxUnsentOutput); err != nil {
				return errStreamClosed
			}
		}
		n := len(p)
		if wait {
			n = min(n, maxUnsentOutput)
		}
		out := bufferpool.Get(n + 16)[:0]
		out = appendFrameHeader(out, frameData, n)
		out = append(out, p[:n]...)
		if err := rs.s.WriteOwned(out, false); err != nil {
			return errStreamClosed
		}
		p = p[n:]
	}
	return nil
}

// EndResponse ends a response that streams with the stream's FIN, behind the
// trailers if there are any to send, or resets the stream when the body fell
// short of its content-length, since ending it would leave the client a body
// it cannot tell from a whole one.
func (rs *requestStream) EndResponse(trailer stdhttp.Header, complete bool) error {
	rs.mu.Lock()
	if rs.closed || !rs.streaming {
		rs.mu.Unlock()
		return errStreamClosed
	}
	rs.streaming = false
	remoteDone := rs.remoteDone
	if !complete {
		rs.closed = true
	}
	rs.mu.Unlock()
	var err error
	if complete {
		var out []byte
		if len(trailer) > 0 {
			// The stream takes out over, as WriteFinal has it, if the
			// trailer has anything a trailer may carry.
			if out = appendTrailer(bufferpool.Get(256)[:0], trailer); len(out) == 0 {
				bufferpool.Put(out)
				out = nil
			}
		}
		err = rs.s.WriteFinal(out, false)
	} else {
		rs.s.Reset(uint64(ErrCodeInternalError))
	}
	if !remoteDone {
		// Answered before the request finished arriving: the rest of it is
		// not wanted, as WriteResponse has it.
		rs.s.StopSending(uint64(ErrCodeNoError))
		rs.failBody(fibhttp.ErrBodyAbandoned)
	}
	if rs.req != nil {
		rs.sc.untrack(rs)
	}
	if err != nil {
		return errStreamClosed
	}
	return nil
}

// declaredLength is the Content-Length header holds, or -1.
func declaredLength(header stdhttp.Header) int64 {
	if values := header["Content-Length"]; len(values) == 1 {
		if n, err := strconv.ParseInt(values[0], 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return -1
}
