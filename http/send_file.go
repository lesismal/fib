//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strconv"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/sidepool"
)

// sendFilePiece is how much of a file SendFile reads from the disk at a time
// where it cannot hand the file to sendfile.
const sendFilePiece = 64 << 10

// sendFileQueuePiece is how much of a file SendFileStall queues for
// sendfile at a time, so that it can wait for the client between pieces:
// queued, not read, so it may be larger than a piece read.
const sendFileQueuePiece = 256 << 10

// SendFile sends n bytes of f, from offset, as the response's body, without
// the calling goroutine waiting for the disk or for the client, and runs
// done, once and on another goroutine, when all of it has been handed to the
// connection or the stream, or with the error that stopped it. It is how a
// handler serves a file of any size without a worker held up by either: on
// an HTTP/1 connection whose Connection.QueuesFiles, plain or over TLS, the
// range is queued as Connection.SendFile queues it, paced by the socket: it
// goes from the file to the socket by sendfile(2) or TransmitFile, or, over
// TLS, by sendfile with the kernel's TLS sealing it on Linux and read and
// sealed a chunk at a time as the socket takes it elsewhere; what of it is
// still queued counts against the response's limit as written bytes do, so
// WriteNoWait and OnWritable see a client that has stopped reading it.
// Elsewhere it is read a piece at a time on a pool of its own for the disk,
// package sidepool's File, and each piece is written with WriteNoWait, the
// next read waiting for OnWritable once the response is above its limit.
//
// The header is the one the handler set through Header, with a
// Content-Length of n added when it sets none and the response has not
// begun; a handler that wants another status, such as 206 with its
// Content-Range, calls WriteHeader first. SendFile keeps the response open
// until done has run, as a Retain does, and lets it go after: the handler
// writes nothing more until then, and may return at once. f must stay open
// until done runs; the range must lie within it, since a file that turns
// out shorter ends the response short, which closes an HTTP/1 connection or
// resets a stream. A response to HEAD sends none of the file.
//
// SendFile waits for the client without a bound; SendFileStall bounds it.
func (c *Context) SendFile(f *os.File, offset, n int64, done func(error)) {
	c.SendFileStall(f, offset, n, 0, done)
}

// SendFileStall is SendFile with every wait for the client bounded by
// stall: the range goes a piece at a time, a piece queued for sendfile or
// one read from the disk, and each time the response is above its limit,
// SendFileStall waits for room under a write deadline of stall from then,
// lifted once there is room. So a client that takes nothing of the response
// for stall loses it, which ends with os.ErrDeadlineExceeded, closing an
// HTTP/1 connection or resetting a stream, and one that reads slowly keeps
// it. The last piece, once queued, is the connection's to send, under what
// deadline the connection has. A stream that takes no write deadline, one
// served outside this package that does not implement StreamDeadliner, is
// waited for without one. A stall of 0 or less is SendFile's.
func (c *Context) SendFileStall(f *os.File, offset, n int64, stall time.Duration, done func(error)) {
	if done == nil {
		done = func(error) {}
	}
	switch {
	case f == nil || offset < 0 || n < 0:
		sidepool.Go(func() { done(errors.New("http: invalid SendFile range")) })
		return
	case c.wrote:
		sidepool.Go(func() { done(ErrResponseWritten) })
		return
	}
	w := c.writer()
	if w.status == 0 {
		if _, set := w.header["Content-Length"]; !set && w.written == 0 && !c.holdsWhole() {
			c.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		}
		c.WriteHeader(stdhttp.StatusOK)
	}
	c.Retain()
	s := &fileSend{c: c, f: f, offset: offset, left: n, stall: stall, done: done}
	if n == 0 || c.Request.Method == stdhttp.MethodHead || !statusHasBody(w.status) {
		var err error
		switch {
		case n == 0:
		case !statusHasBody(w.status):
			err = stdhttp.ErrBodyNotAllowed
		default:
			// Counted, as Write counts what it is given for HEAD.
			w.written += n
		}
		sidepool.Go(func() { s.finish(err) })
		return
	}
	if c.isHTTP1() && !c.holdsWhole() && c.Conn.QueuesFiles() && w.err == nil && n >= minSendFileSize &&
		(w.declared < 0 || w.written+n <= w.declared) {
		if stall <= 0 {
			err := c.sendFileHTTP1(f, offset, n)
			sidepool.Go(func() { s.finish(err) })
			return
		}
		s.queue()
		return
	}
	sidepool.File().Go(s.pump)
}

// sendFileHTTP1 is SendFile on an HTTP/1 connection that queues the file,
// to be read as the socket takes it.
func (c *Context) sendFileHTTP1(f *os.File, offset, n int64) error {
	w := c.w
	if !w.committed {
		if err := c.commit(false); err != nil {
			return w.fail(err)
		}
	} else if c.h1WriteExpired() {
		return w.fail(os.ErrDeadlineExceeded)
	}
	if w.chunked {
		if err := c.Conn.SendOwned(fmt.Appendf(nil, "%x\r\n", n)); err != nil {
			return w.fail(err)
		}
	}
	if err := c.Conn.SendFile(f, offset, n); err != nil {
		return w.fail(err)
	}
	if w.chunked {
		if err := c.Conn.Send(crlf); err != nil {
			return w.fail(err)
		}
	}
	w.written += n
	return nil
}

// fileSend is a SendFile reading its file a piece at a time, or queuing it
// for sendfile a piece at a time when it has a stall bound.
type fileSend struct {
	c      *Context
	f      *os.File
	offset int64
	left   int64
	stall  time.Duration
	done   func(error)
}

// queue hands the range to sendfile a piece at a time, waiting for room
// between pieces under the stall bound. It runs on the handler's goroutine
// first, and then on OnWritable's.
func (s *fileSend) queue() {
	for {
		k := min(s.left, sendFileQueuePiece)
		if err := s.c.sendFileHTTP1(s.f, s.offset, k); err != nil {
			sidepool.Go(func() { s.finish(err) })
			return
		}
		s.offset += k
		s.left -= k
		if s.left == 0 {
			sidepool.Go(func() { s.finish(nil) })
			return
		}
		waiting, err := s.wait(s.requeue)
		if err != nil {
			sidepool.Go(func() { s.finish(err) })
			return
		}
		if waiting {
			return
		}
	}
}

// requeue carries on queuing once the response has room, or ends it.
func (s *fileSend) requeue(err error) {
	if err == nil {
		err = s.lift()
	}
	if err != nil {
		s.finish(err)
		return
	}
	s.queue()
}

// wait arranges for fn to run once the response has room, under a write
// deadline of the stall bound when there is one, and reports whether it
// will; with room already, it reports false and fn does not run.
func (s *fileSend) wait(fn func(error)) (bool, error) {
	if s.stall > 0 {
		if err := s.deadline(time.Now().Add(s.stall)); err != nil {
			return false, err
		}
	}
	if s.c.OnWritable(fn) {
		return true, nil
	}
	return false, s.lift()
}

// lift takes the stall bound's write deadline off.
func (s *fileSend) lift() error {
	if s.stall <= 0 {
		return nil
	}
	return s.deadline(time.Time{})
}

// deadline sets the response's write deadline, going on without one on a
// stream that takes none.
func (s *fileSend) deadline(t time.Time) error {
	if err := s.c.SetWriteDeadline(t); err != nil && !errors.Is(err, stdhttp.ErrNotSupported) {
		return err
	}
	return nil
}

// pump reads and writes pieces, on the file pool, until the response is
// above its limit, when it goes on from OnWritable's callback, or the range
// has all gone.
func (s *fileSend) pump() {
	buf := bufferpool.Get(int(min(s.left, sendFilePiece)))
	defer bufferpool.Put(buf)
	for s.left > 0 {
		k, err := s.f.ReadAt(buf[:min(s.left, int64(len(buf)))], s.offset)
		if k == 0 {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			s.finish(err)
			return
		}
		s.offset += int64(k)
		s.left -= int64(k)
		above, werr := s.c.WriteNoWait(buf[:k])
		if werr != nil {
			s.finish(werr)
			return
		}
		if above && s.left > 0 {
			waiting, err := s.wait(s.resume)
			if err != nil {
				s.finish(err)
				return
			}
			if waiting {
				return
			}
		}
	}
	s.finish(nil)
}

// resume carries on once the response has room, or ends it.
func (s *fileSend) resume(err error) {
	if err == nil {
		err = s.lift()
	}
	if err != nil {
		s.finish(err)
		return
	}
	sidepool.File().Go(s.pump)
}

// finish runs done and lets the response go.
func (s *fileSend) finish(err error) {
	s.done(err)
	s.c.Release()
}
