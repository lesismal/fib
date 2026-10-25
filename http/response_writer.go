//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/outputwait"
)

// ErrResponseWritten is what writing to a response returns once the response
// has been sent whole, by WriteResponse, Respond or Finish.
var ErrResponseWritten = errors.New("http: response already written")

const (
	// writerBufferSize is how much body the ResponseWriter methods hold back
	// before sending the header. A handler that writes no more than this and
	// returns gets a Content-Length response rather than a chunked one.
	writerBufferSize = 4 << 10
	// minSendFileSize is the smallest file range ReadFrom hands to sendfile.
	// Below it, copying the bytes costs less than the extra system calls.
	minSendFileSize = 16 << 10
	// maxHeldBody is the longest declared body the ResponseWriter methods
	// hold back whole, and send with the header when it is all written,
	// rather than streaming it. A body whose Content-Length is set up front,
	// as http.ServeContent sets it, and no longer than this is one response
	// rather than a stream: streaming it sent the header and the body in
	// writes of their own, and over TLS sealed them into records of their
	// own. HttpArena's static-tls profile, files of a few kilobytes to a few
	// tens of them, made two writes a response where one does.
	maxHeldBody = 64 << 10
	// copyBufferSize is the buffer ReadFrom copies a reader it cannot hand to
	// sendfile through, taken from the pool rather than made by io.Copy for
	// every response.
	copyBufferSize = 32 << 10
	// h1MaxPendingOutput is how much of a streaming HTTP/1 response's body
	// may wait in its connection's queue for a client that is not taking it,
	// before the handler writing it waits for the client to take some. A
	// handler that waits is let go once half of it has gone, as on HTTP/2;
	// see h2MaxPendingOutput.
	h1MaxPendingOutput = 64 << 10
)

// responseWriter is a response written through Context's ResponseWriter
// methods, as net/http's handlers write theirs.
type responseWriter struct {
	// header is what Header returns, and sent the copy WriteHeader took of
	// it, which is what goes out: later changes only count for trailers.
	header stdhttp.Header
	sent   stdhttp.Header
	status int
	// declared is the Content-Length the handler set, or -1, and written how
	// much body it has written so far.
	declared int64
	written  int64
	// buf holds body not yet sent, until the header goes out, or until the
	// handler is done when the response is held whole.
	buf []byte
	// committed records that the header has been sent, and chunked and
	// closeAfter how an HTTP/1 body is framed and whether the connection
	// ends with it. trailers are the names the header announced.
	committed  bool
	chunked    bool
	closeAfter bool
	trailers   []string
	finished   bool
	// err is the first error sending the response met, which every Write
	// and Flush after it reports, as net/http's do: the response it broke
	// cannot be carried on.
	err error
	// hooks are what OnHeader, OnResponse and OnFinish registered, which
	// live here rather than in Context so that a Context, which HTTP/3
	// allocates with each request, costs no more for a feature few use.
	hooks *responseHooks
	// spareHooks is the storage for hooks a request this writer served
	// before registered, which addHook takes rather than allocating.
	spareHooks *responseHooks
}

// holdsDeclared reports whether a body is held back whole because its declared
// length is short; see maxHeldBody.
func (w *responseWriter) holdsDeclared() bool {
	return w.declared >= 0 && w.declared <= maxHeldBody
}

// Header returns the header of the response written through Write, as
// http.ResponseWriter's does. Changing it after WriteHeader or the first
// Write has no effect, except for trailers: values for names announced in a
// "Trailer" header, and keys prefixed with http.TrailerPrefix, are sent
// after the body.
func (c *Context) Header() stdhttp.Header {
	w := c.writer()
	if w.header == nil {
		w.header = make(stdhttp.Header)
	}
	return w.header
}

func (c *Context) writer() *responseWriter {
	if c.w == nil {
		c.w = writerPool.Get().(*responseWriter)
	}
	return c.w
}

// writerPool holds the writers of recycled Contexts, cleared, for the
// requests that need one next.
var writerPool = sync.Pool{New: func() any { return &responseWriter{declared: -1} }}

// fail records err as what broke the response, unless something did
// before, and returns it.
func (w *responseWriter) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return err
}

// reset clears the writer for its Context's next request. Of what it held
// it keeps only its hooks' storage, emptied, for that request's hooks.
func (w *responseWriter) reset() {
	spare := w.spareHooks
	if h := w.hooks; h != nil {
		clear(h.list)
		*h = responseHooks{list: h.list[:0]}
		spare = h
	}
	*w = responseWriter{declared: -1, spareHooks: spare}
}

// WriteHeader sends the response's status with the header Header returned,
// as http.ResponseWriter's does. A 1xx status other than 101 is sent at once
// as an interim response, and may be followed by others; the final status
// may only be set once, and Write sets 200 if it has not been set.
//
// The header goes out with the first part of the body. A handler whose body
// is no longer than a few kilobytes, or that set a Content-Length of no more
// than 64 kilobytes itself, and that does not flush, gets its response sent
// whole once it is done, with Content-Length. A longer body, or a flushed
// one, streams: on HTTP/1 it is chunked, which is also what carries trailers,
// unless Content-Length was set, and an HTTP/1.0 client, which does not know
// chunked framing, gets a body that ends when the connection closes. On
// HTTP/2, and on HTTP/3, the header goes out in a HEADERS frame of its own,
// the body in DATA frames as it is written and as the client's flow control
// lets it go, and trailers in HEADERS after it.
func (c *Context) WriteHeader(status int) {
	if status < 100 || status > 999 {
		panic(fmt.Sprintf("http: invalid WriteHeader code %d", status))
	}
	w := c.writer()
	if c.wrote || w.status != 0 {
		return
	}
	if status < 200 && status != stdhttp.StatusSwitchingProtocols {
		_ = c.WriteInterim(status, w.header)
		return
	}
	w.status = status
	w.sent = w.header.Clone()
	if w.sent == nil {
		w.sent = make(stdhttp.Header)
	}
	if w.hooks != nil && !w.hooks.whole {
		// The header is settled from here on, so this is the last moment the
		// hooks can change it. A response held whole runs them when it is
		// sent instead, with its body. An OnHeaderAsync hook holds it, and
		// its resume settles it.
		if at := w.hooks.beforeHeader(status, w.sent, false); at >= 0 && c.holdHeader(at) {
			return
		}
	}
	c.settleHeader()
}

// settleHeader is the header of a response written through the
// ResponseWriter methods settled, once its hooks have run: the length it
// declares, and what filtering its body changes.
func (c *Context) settleHeader() {
	w := c.w
	if w.hooks != nil && !w.hooks.whole {
		if w.hooks.filters != nil {
			if c.Request.Method == stdhttp.MethodHead {
				abortFilters(w.hooks.filters, errBodyUnended)
				w.hooks.filters = nil
			} else {
				// The length declared is the body's before the filters.
				delete(w.sent, "Content-Length")
			}
		}
	}
	if values := w.sent["Content-Length"]; len(values) == 1 {
		if n, err := strconv.ParseInt(strings.TrimSpace(values[0]), 10, 64); err == nil && n >= 0 {
			w.declared = n
		}
	}
	if w.declared < 0 {
		delete(w.sent, "Content-Length")
	}
}

// Write adds p to the response body, as http.ResponseWriter's does, sending
// the header first if it has not gone yet. A response to HEAD counts what is
// written but sends none of it.
//
// A response that streams is paced by the client. On HTTP/1, once 64
// kilobytes of it are queued on the connection because the client has not
// taken them, Write waits for the client to take some, or for the connection
// to end, which it then reports. On HTTP/2 and HTTP/3 Write waits the same
// way once 64 kilobytes are held back by flow control, for the client to take
// some or for the stream to end. Config.WriteTimeout, and HTTP/3's, bound the
// wait. A handler its multiplexed connection runs on its own reader, which
// StreamPoolConfig describes, is the exception, since the client's go-ahead
// would arrive on the goroutine that is waiting: what it writes is held
// until it can go.
func (c *Context) Write(p []byte) (int, error) {
	if c.wrote {
		return 0, ErrResponseWritten
	}
	w := c.writer()
	if w.err != nil {
		return 0, w.err
	}
	if w.status == 0 {
		c.WriteHeader(stdhttp.StatusOK)
	}
	if a := w.asyncHeader(); a != nil {
		held, _, err := a.hold(c, p, true)
		if held {
			return len(p), nil
		}
		if err != nil {
			// The client went while the Write waited for the resume.
			return 0, err
		}
		if w.err != nil {
			// The hook's resume failed the response.
			return 0, w.err
		}
	}
	return c.writeBody(p)
}

// writeBody is Write once the header is settled.
func (c *Context) writeBody(p []byte) (int, error) {
	w := c.w
	if w.filtered() && !w.finished && len(p) > 0 {
		out, err := w.hooks.filter(p)
		if err != nil {
			return 0, w.fail(err)
		}
		if _, err := c.write(out); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	return c.write(p)
}

// filtered reports whether the body goes through OnBodyFilter's filters.
func (w *responseWriter) filtered() bool { return w.hooks != nil && w.hooks.filters != nil }

// write is Write for p as it is sent, after any filters.
func (c *Context) write(p []byte) (int, error) {
	if c.wrote {
		return 0, ErrResponseWritten
	}
	w := c.w
	if w.err != nil {
		return 0, w.err
	}
	if w.finished {
		return 0, ErrResponseWritten
	}
	if !statusHasBody(w.status) {
		return 0, stdhttp.ErrBodyNotAllowed
	}
	if w.declared >= 0 && w.written+int64(len(p)) > w.declared {
		return 0, stdhttp.ErrContentLength
	}
	w.written += int64(len(p))
	if c.Request.Method == stdhttp.MethodHead || len(p) == 0 {
		return len(p), nil
	}
	if c.holdsWhole() {
		w.buf = append(w.buf, p...)
		return len(p), nil
	}
	if !w.committed && (len(w.buf)+len(p) <= writerBufferSize || w.holdsDeclared()) {
		if c.isHTTP1() {
			// Held back in a pooled buffer, which commit gives back once the
			// header has gone out with it.
			w.buf = bufferpool.Append(w.buf, p)
		} else {
			// A body sent whole on a stream may stay with the stream after
			// the response is finished, waiting for the client's window, so
			// it is not the pool's to have back.
			w.buf = append(w.buf, p...)
		}
		return len(p), nil
	}
	if !w.committed {
		if err := c.commit(false); err != nil {
			return 0, w.fail(err)
		}
	}
	if err := c.sendBody(p); err != nil {
		return 0, w.fail(err)
	}
	return len(p), nil
}

// WriteNoWait adds p to the response body as Write does, but never waits for
// the client: it takes all of p, and reports above when more than the
// response's limit of 64 kilobytes is now queued on the HTTP/1 connection or
// held back by HTTP/2's or HTTP/3's flow control. A caller that gets above
// stops writing until a function it gives OnWritable runs, which is how a
// response is paced by its client without a goroutine waiting on it, as a
// proxy passing on a body from elsewhere wants. What is written past the
// limit is queued as it is, so a caller that goes on writing regardless is
// not held back.
//
// What Write holds back, a small first write or a response an OnResponse
// hook needs whole, is held back here too, and never above. A stream served
// outside this package that is not a ResponseNotifier is written to as
// Write writes it.
func (c *Context) WriteNoWait(p []byte) (above bool, err error) {
	if c.wrote {
		return false, ErrResponseWritten
	}
	w := c.writer()
	if w.err != nil {
		return false, w.err
	}
	if w.status == 0 {
		c.WriteHeader(stdhttp.StatusOK)
	}
	if a := w.asyncHeader(); a != nil {
		if held, above, _ := a.hold(c, p, false); held {
			return above, nil
		}
		if w.err != nil {
			return false, w.err
		}
	}
	return c.writeNoWait(p)
}

// writeNoWait is WriteNoWait once the header is settled.
func (c *Context) writeNoWait(p []byte) (above bool, err error) {
	w := c.w
	if c.external != nil && !c.holdsWhole() {
		if _, ok := c.external.(ResponseNotifier); !ok {
			_, err := c.writeBody(p)
			return false, err
		}
	}
	if w.finished {
		return false, ErrResponseWritten
	}
	if !statusHasBody(w.status) {
		return false, stdhttp.ErrBodyNotAllowed
	}
	if w.declared >= 0 && w.written+int64(len(p)) > w.declared {
		return false, stdhttp.ErrContentLength
	}
	if w.filtered() && len(p) > 0 {
		if p, err = w.hooks.filter(p); err != nil {
			return false, w.fail(err)
		}
	}
	if c.Request.Method == stdhttp.MethodHead || len(p) == 0 || c.holdsWhole() ||
		!w.committed && (len(w.buf)+len(p) <= writerBufferSize || w.holdsDeclared()) {
		// Write only holds these back, never waits on them.
		_, err := c.write(p)
		return false, err
	}
	w.written += int64(len(p))
	if !w.committed {
		if err := c.commit(false); err != nil {
			return false, w.fail(err)
		}
	}
	switch {
	case c.isHTTP1():
		if c.h1WriteExpired() {
			return false, w.fail(os.ErrDeadlineExceeded)
		}
		if w.chunked {
			out := appendChunk(bufferpool.Get(len(p) + 20)[:0], p)
			err = c.Conn.Send(out)
			bufferpool.Put(out)
		} else {
			err = c.Conn.Send(p)
		}
		if err != nil {
			return false, w.fail(err)
		}
		return outputwait.Unsent(c.Conn) > h1MaxPendingOutput, nil
	case c.stream != nil:
		above, err = c.stream.writeBodyNoWait(p)
	default:
		above, err = c.external.(ResponseNotifier).WriteBodyNoWait(p)
	}
	if err != nil {
		return false, w.fail(err)
	}
	return above, nil
}

// OnWritable arranges for fn to run, once, on another goroutine, when the
// response's queued output, which WriteNoWait reported above its limit, has
// fallen to half of it, or with an error when the connection or the stream
// ends first, which a write deadline passing does too; and reports true. It
// reports false, and never runs fn, when the output is at or below half
// already, or the response has not begun to stream, or is finished. A
// response has room for one fn at a time: register the next once the last
// has run.
func (c *Context) OnWritable(fn func(error)) bool {
	if fn == nil || c.wrote || c.w == nil {
		return false
	}
	if a := c.w.asyncHeader(); a != nil && a.deferWritable(fn) {
		return true
	}
	return c.onWritable(fn)
}

// onWritable is OnWritable once the header is settled.
func (c *Context) onWritable(fn func(error)) bool {
	if c.wrote || !c.w.committed || c.w.finished || c.holdsWhole() {
		return false
	}
	switch {
	case c.isHTTP1():
		return c.Conn.NotifyOutput(h1MaxPendingOutput, fn)
	case c.stream != nil:
		return c.stream.onWritable(fn)
	}
	if n, ok := c.external.(ResponseNotifier); ok {
		return n.NotifyWritable(fn)
	}
	return false
}

// WriteString is Write for a string.
func (c *Context) WriteString(s string) (int, error) { return c.Write([]byte(s)) }

// Flush sends the header and whatever body is held back, as http.Flusher's
// does, and hands it to the socket at once rather than when the handler
// returns; the response streams from then on. On HTTP/2 and HTTP/3 what the
// client's flow control holds back goes as it lets it.
func (c *Context) Flush() { _ = c.FlushError() }

// FlushError is Flush reporting what went wrong, which is what
// http.ResponseController looks for: once sending the response has failed,
// it reports that, as every Write does, and once the connection or the
// stream has gone under the response, why it went (see Err), so that a
// handler streaming in a loop stops when its client can no longer be
// reached, whichever of the two it checks. A response an OnResponse hook
// needs whole is held until the handler is done, so Flush does nothing for
// it, nor on a stream served outside this package that cannot send a
// response as it is written (see ResponseStreamer).
func (c *Context) FlushError() error {
	if c.wrote {
		return ErrResponseWritten
	}
	w := c.writer()
	if w.err != nil {
		return w.err
	}
	if ctxState(c.word.Load()) == ctxCancelled {
		// The connection, or the stream, went under the response: a write
		// timeout, a reset, a hangup. Nothing flushed would reach the
		// client, and every Write and Flush from here reports it, as they
		// report a send that failed.
		if err := c.Err(); err != nil {
			return w.fail(err)
		}
	}
	if w.status == 0 {
		c.WriteHeader(stdhttp.StatusOK)
	}
	if w.finished || c.holdsWhole() {
		return nil
	}
	if a := w.asyncHeader(); a != nil {
		if a.held() {
			return nil
		}
		if w.err != nil {
			return w.err
		}
	}
	if w.filtered() {
		held, err := w.hooks.endFilters(true)
		if err != nil {
			return w.fail(err)
		}
		if len(held) > 0 {
			if _, err := c.write(held); err != nil {
				return err
			}
		}
	}
	if !w.committed {
		if err := c.commit(false); err != nil {
			return w.fail(err)
		}
	}
	if !c.isHTTP1() {
		// The stream sends what it is given without being asked.
		return nil
	}
	if err := c.Conn.Flush(); err != nil {
		return w.fail(err)
	}
	return nil
}

// SetWriteDeadline replaces the deadline Config.WriteTimeout puts on the
// response, for this response alone, as http.ResponseController's does: a
// zero t lifts it, which is how a handler that streams for as long as its
// client listens (server-sent events, a long download) keeps WriteTimeout
// from cutting it off. It sets one too when Config.WriteTimeout does not.
// What passing it does is what passing WriteTimeout does. On HTTP/1 the
// connection is closed if output is still queued for the client, and a
// streaming response fails with os.ErrDeadlineExceeded the next time it
// sends anything, its header and what Write held back with it included, so
// a t already past fails the first Flush; as net/http's buffered writes do,
// a Write that is only held back succeeds. A response sent whole as the
// handler finishes is not refused. On HTTP/2 and HTTP/3 the stream is
// reset, at once for a t already past. Once sending the response has
// failed, every Write and Flush after it reports the same error.
//
// It reports ErrResponseWritten once the response is finished, and
// http.ErrNotSupported on a stream served outside this package that is not
// a StreamDeadliner.
func (c *Context) SetWriteDeadline(t time.Time) error {
	if c.wrote {
		return ErrResponseWritten
	}
	switch {
	case c.isHTTP1():
		if c.parser != nil {
			c.parser.writeDeadline = t
		}
		return c.Conn.SetWriteDeadline(t)
	case c.stream != nil:
		return c.stream.setWriteDeadline(t)
	}
	if d, ok := c.external.(StreamDeadliner); ok {
		return d.SetWriteDeadline(t)
	}
	return stdhttp.ErrNotSupported
}

// SetReadDeadline gives the rest of the request's body until t to arrive,
// for this request alone, as http.ResponseController's does. On HTTP/1 it
// replaces the deadline Config.ReadTimeout puts on a body that streams: a
// zero t lifts it, which is how a handler taking a long upload keeps
// ReadTimeout from cutting it off, and the connection is closed if the body
// has not all arrived by t. HTTP/2 and HTTP/3 put no read deadline on a
// stream of their own; one set here fails a body still arriving at t with
// os.ErrDeadlineExceeded, as net/http's HTTP/2 server does, leaving the
// stream open for the handler to answer, and a zero t lifts it.
//
// A body that had all arrived before the handler ran, as every body does
// that does not stream (see Config.StreamRequestBody), has nothing left to
// bound, so setting it does nothing then, as it does once a streamed body
// has ended. It reports ErrResponseWritten once the response is finished.
func (c *Context) SetReadDeadline(t time.Time) error {
	switch {
	case c.wrote:
		return ErrResponseWritten
	case c.isHTTP1():
		if !c.streamed || c.parser == nil || c.parser.bodyDeadline == nil {
			return nil
		}
		return c.parser.bodyDeadline.set(c, t)
	case c.stream != nil:
		return c.stream.setReadDeadline(t)
	}
	if d, ok := c.external.(StreamDeadliner); ok {
		return d.SetReadDeadline(t)
	}
	return stdhttp.ErrNotSupported
}

// ReadFrom copies src into the response body, as io.Copy does through
// io.ReaderFrom. On an HTTP/1 connection a regular file, or an
// *io.LimitedReader over one as http.ServeContent passes, goes from the file
// to the socket by sendfile(2) where the platform has it, without being
// read into memory; so http.ServeFile and http.ServeContent, given the
// Context as their ResponseWriter, serve files that way. src is left
// positioned after what was sent.
func (c *Context) ReadFrom(src io.Reader) (int64, error) {
	if c.wrote {
		return 0, ErrResponseWritten
	}
	w := c.writer()
	file, ok := sendableFileOf(src)
	if !ok || c.holdsWhole() || w.asyncHeader() != nil || !c.isHTTP1() || c.Request.Method == stdhttp.MethodHead || w.finished ||
		w.status != 0 && !statusHasBody(w.status) || file.size < minSendFileSize ||
		w.declared >= 0 && w.written+file.size > w.declared {
		if w.status == 0 {
			// As Write does even for nothing, and net/http's ReadFrom does
			// before it copies: an empty src still begins the response.
			c.WriteHeader(stdhttp.StatusOK)
		}
		buf := bufferpool.Get(copyBufferSize)
		defer bufferpool.Put(buf)
		return io.CopyBuffer(writerOnly{c}, src, buf)
	}
	if w.status == 0 {
		if _, set := w.header["Content-Type"]; !set && w.written == 0 {
			// Sniff the type as a first Write would have.
			sniff := make([]byte, 512)
			n, _ := file.f.ReadAt(sniff, file.offset)
			c.Header().Set("Content-Type", stdhttp.DetectContentType(sniff[:n]))
		}
		c.WriteHeader(stdhttp.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.filtered() {
		// A filtered body is read and written, not sent from the file.
		buf := bufferpool.Get(copyBufferSize)
		defer bufferpool.Put(buf)
		return io.CopyBuffer(writerOnly{c}, src, buf)
	}
	if !w.committed {
		if err := c.commit(false); err != nil {
			return 0, w.fail(err)
		}
	} else if c.h1WriteExpired() {
		return 0, w.fail(os.ErrDeadlineExceeded)
	}
	if w.chunked {
		if err := c.Conn.SendOwned(fmt.Appendf(nil, "%x\r\n", file.size)); err != nil {
			return 0, w.fail(err)
		}
	}
	if err := c.Conn.SendFile(file.f, file.offset, file.size); err != nil {
		return 0, w.fail(err)
	}
	if w.chunked {
		if err := c.Conn.Send(crlf); err != nil {
			return 0, w.fail(err)
		}
	}
	w.written += file.size
	file.advance()
	return file.size, nil
}

// writerOnly hides a Context's ReadFrom from io.Copy, which would otherwise
// call it back.
type writerOnly struct{ io.Writer }

// sendableFile is a stretch of a regular file ReadFrom can hand to SendFile.
type sendableFile struct {
	f       fileLike
	offset  int64
	size    int64
	limited *io.LimitedReader
}

// fileLike is an *os.File, or a type wrapping one, such as the one
// os.File.WriteTo passes to io.Copy.
type fileLike interface {
	fib.File
	io.Seeker
	Stat() (fs.FileInfo, error)
}

func sendableFileOf(src io.Reader) (sendableFile, bool) {
	limit := int64(-1)
	limited, _ := src.(*io.LimitedReader)
	if limited != nil {
		src, limit = limited.R, limited.N
	}
	f, ok := src.(fileLike)
	if !ok {
		return sendableFile{}, false
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return sendableFile{}, false
	}
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return sendableFile{}, false
	}
	size := max(info.Size()-offset, 0)
	if limit >= 0 {
		size = min(size, limit)
	}
	return sendableFile{f: f, offset: offset, size: size, limited: limited}, true
}

// advance moves the source past what was sent, as reading it would have.
func (s sendableFile) advance() {
	_, _ = s.f.Seek(s.offset+s.size, io.SeekStart)
	if s.limited != nil {
		s.limited.N -= s.size
	}
}

// holdsWhole reports whether a response written through the ResponseWriter
// methods is held until the handler is done and then sent at once: when an
// OnResponse hook needs its body whole, and on a stream of a protocol served
// outside this package that cannot send it as it is written.
func (c *Context) holdsWhole() bool {
	if c.w != nil && c.w.hooks != nil && c.w.hooks.whole {
		return true
	}
	if c.external != nil {
		_, streams := c.external.(ResponseStreamer)
		return !streams
	}
	return false
}

// streamer is what the response to a request that arrived on a multiplexed
// stream is sent through as it is written.
func (c *Context) streamer() ResponseStreamer {
	if c.stream != nil {
		return (*h2Responder)(c.stream)
	}
	streamer, _ := c.external.(ResponseStreamer)
	return streamer
}

// isHTTP1 reports whether the response goes straight onto an HTTP/1
// connection, rather than onto a stream of a multiplexed one.
func (c *Context) isHTTP1() bool { return c.stream == nil && c.external == nil }

// statusHasBody reports whether a response with status may carry a body.
func statusHasBody(status int) bool {
	return status >= 200 && status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
}

// commit sends the header, with whatever body is held back. final says the
// handler is done, so that a body held back whole can be sent with its
// length; HTTP/2 and HTTP/3 send such a body whole instead.
func (c *Context) commit(final bool) error {
	if !c.isHTTP1() {
		return c.commitStream()
	}
	if !final && c.h1WriteExpired() {
		// A response that begins to stream after its write deadline is
		// refused from its first byte, as each part after it is; see
		// sendBody. One sent whole as the handler finishes is not.
		return os.ErrDeadlineExceeded
	}
	w := c.w
	req := c.Request
	head := responseHead{status: w.status, header: w.sent, contentLength: -1}
	head.close = c.Request.Close || headerHasToken(w.sent, "Connection", "close") || c.draining() || c.bodyFailed() ||
		final && c.bodyCutShort()
	http11 := req.ProtoAtLeast(1, 1)
	w.trailers = announcedTrailers(w.sent)
	w.sniff()
	switch {
	case !statusHasBody(w.status):
		if w.status == stdhttp.StatusNotModified && w.declared >= 0 {
			head.contentLength = w.declared
		}
	case w.declared >= 0:
		head.contentLength = w.declared
	case final && (len(w.trailers) == 0 && !hasPrefixedTrailers(w.header) || !http11):
		if req.Method != stdhttp.MethodHead || w.written > 0 {
			head.contentLength = w.written
		}
	case req.Method == stdhttp.MethodHead:
	case http11:
		head.chunked = true
		head.trailers = w.trailers
	default:
		// HTTP/1.0 has no chunks: the body runs until the connection closes.
		head.close = true
	}
	// The header and whatever body was held back go out together, built in a
	// pooled buffer that Send copies from, since the round's send buffer is
	// where they end up either way.
	pooled := bufferpool.Get(headCapacity(w.sent) + len(w.buf) + 32)
	out, err := appendResponseHead(pooled[:0], req, head)
	if err != nil {
		bufferpool.Put(pooled)
		return err
	}
	w.committed = true
	w.chunked = head.chunked
	w.closeAfter = head.close
	if head.close {
		c.ends |= endClosing
	} else {
		c.ends &^= endClosing
	}
	if len(w.buf) > 0 {
		if w.chunked {
			out = appendChunk(out, w.buf)
		} else {
			out = append(out, w.buf...)
		}
	}
	bufferpool.Put(w.buf)
	w.buf = nil
	err = c.Conn.Send(out)
	bufferpool.Put(out)
	if err != nil {
		return err
	}
	if final || w.holdsDeclared() {
		// The whole response is out, or short enough that it is one: it goes
		// to the socket with whatever else this read round answers.
		return nil
	}
	// A body too long to hold back is streamed: from here on it goes to the
	// socket as it is written, rather than piling up until the handler
	// returns.
	return c.Conn.Flush()
}

// commitStream begins a response that streams on an HTTP/2 or HTTP/3 stream:
// its header goes out, and whatever body is held back behind it.
func (c *Context) commitStream() error {
	w := c.w
	w.trailers = announcedTrailers(w.sent)
	w.sniff()
	streamer := c.streamer()
	if err := streamer.BeginResponse(c.Request, w.status, w.sent); err != nil {
		return err
	}
	w.committed = true
	held := w.buf
	w.buf = nil
	if len(held) == 0 {
		return nil
	}
	return streamer.WriteBody(held, !c.onReader())
}

// sendBody sends part of a committed body.
//
// On HTTP/1 what the client has not taken yet stays queued on the connection,
// and a handler that writes faster than the client reads waits here once
// h1MaxPendingOutput of it is queued, until half of that has gone, or the
// connection has ended, which it is then told. So a handler that copies a
// large body to a slow client holds no more than that of it in memory, as on
// HTTP/2 and HTTP/3, rather than all of it. A long p is sent in parts of that
// size, waiting between them. Only a body that streams gets here: one held
// back and sent whole, and the replies a round answers pipelined requests
// with, never wait. Config.WriteTimeout bounds the wait: the connection closes
// once it has passed with output still queued, and a part sent after it has
// passed is refused.
func (c *Context) sendBody(p []byte) error {
	if !c.isHTTP1() {
		return c.streamer().WriteBody(p, !c.onReader())
	}
	for len(p) > 0 {
		if c.h1WriteExpired() {
			return os.ErrDeadlineExceeded
		}
		part := p[:min(len(p), h1MaxPendingOutput)]
		p = p[len(part):]
		var err error
		if !c.w.chunked {
			err = c.Conn.Send(part)
		} else {
			out := appendChunk(bufferpool.Get(len(part) + 20)[:0], part)
			err = c.Conn.Send(out)
			bufferpool.Put(out)
		}
		if err == nil {
			err = outputwait.Wait(c.Conn, h1MaxPendingOutput)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// h1WriteExpired reports whether the HTTP/1 response's write deadline,
// Config.WriteTimeout's or the one its handler set, has passed, and closes
// the connection when it has: a response it cut short cannot be carried on.
func (c *Context) h1WriteExpired() bool {
	p := c.parser
	if p == nil || p.writeDeadline.IsZero() || time.Now().Before(p.writeDeadline) {
		return false
	}
	c.Conn.CloseWithError(os.ErrDeadlineExceeded)
	return true
}

func appendChunk(out, p []byte) []byte {
	out = strconv.AppendInt(out, int64(len(p)), 16)
	out = append(out, crlf...)
	out = append(out, p...)
	return append(out, crlf...)
}

// Finish ends a response written through Write: it sends the header if it
// has not gone yet, what body is held back, and the trailers. The server
// calls it when the last hold on the response goes, which for a handler that
// retained nothing is its own return, as net/http ends a response then, so a
// handler only calls it to end the response sooner. A response that was never
// begun is left alone here, since the handler may answer it later with
// WriteResponse; the server itself answers one with an empty 200 when the
// handler returns holding nothing, as net/http does.
func (c *Context) Finish() error {
	w := c.w
	if w == nil || w.status == 0 || w.finished || c.wrote {
		return nil
	}
	if a := w.asyncHeader(); a != nil && a.deferFinish() {
		return nil
	}
	return c.finishResponse()
}

// finishResponse is Finish once the header is settled.
func (c *Context) finishResponse() error {
	w := c.w
	if w.status == 0 || w.finished || c.wrote {
		return nil
	}
	var err error
	if w.filtered() && w.err == nil {
		// What the filters hold back goes before the end of the body.
		var tail []byte
		if tail, err = w.hooks.endFilters(false); err == nil && len(tail) > 0 {
			_, err = c.write(tail)
		}
		if err != nil {
			err = w.fail(err)
		}
	}
	w.finished = true
	if err == nil {
		// A body its filters failed cannot be ended as if it were whole.
		err = c.finish(w)
	}
	if err != nil && !c.wrote {
		c.abandon()
	}
	return err
}

// abandon ends a request nothing can answer now. A connection kept open
// would leave its client waiting, or reading the response to the next
// request it pipelined as this one's, and so would a stream.
func (c *Context) abandon() {
	switch {
	case c.isHTTP1():
		c.ends |= endClosing
		c.closeAfterResponse()
	case c.stream != nil:
		c.stream.abandon()
	default:
		if a, ok := c.external.(StreamAbandoner); ok {
			a.Abandon()
		}
	}
}

// finish is Finish once the response is marked finished.
func (c *Context) finish(w *responseWriter) error {
	if c.holdsWhole() || !c.isHTTP1() && !w.committed {
		// Held whole, or short enough to send whole now the handler is done.
		w.sniff()
		response := Response{StatusCode: w.status, Header: w.sent, Body: w.buf, Trailer: w.trailer()}
		switch {
		case c.Request.Method == stdhttp.MethodHead:
			// None of the body is held back, so the length the stream
			// reports is the one declared, or else what was written.
			if w.declared < 0 && w.written > 0 {
				response.Header.Set("Content-Length", strconv.FormatInt(w.written, 10))
			}
		case w.declared >= 0:
			// The stream's own framing reports the length.
			delete(response.Header, "Content-Length")
		}
		return c.writeResponse(response)
	}
	if !c.isHTTP1() {
		return c.endStream()
	}
	if !w.committed {
		if err := c.commit(true); err != nil {
			return err
		}
	}
	if w.chunked {
		out := append([]byte(nil), "0\r\n"...)
		out, err := appendHeaderLines(out, w.trailer(), nil)
		if err != nil {
			// The body is already out; end it without the bad trailer.
			out = append(out[:0], "0\r\n"...)
		}
		if err := c.Conn.SendOwned(append(out, crlf...)); err != nil {
			return err
		}
	}
	c.wrote = true
	if w.hooks != nil {
		size := w.written
		if c.Request.Method == stdhttp.MethodHead || !statusHasBody(w.status) {
			size = 0
		}
		w.hooks.finished(w.status, w.sent, size)
	}
	if w.closeAfter || w.declared >= 0 && w.written < w.declared && c.Request.Method != stdhttp.MethodHead &&
		statusHasBody(w.status) {
		// A body shorter than its Content-Length leaves the client waiting
		// for bytes that will never come, so the connection has to end.
		c.ends |= endClosing
		c.closeAfterResponse()
	}
	return nil
}

// endStream ends a response that streams on an HTTP/2 or HTTP/3 stream, with
// its trailers, or resets the stream when its body fell short of its
// Content-Length.
func (c *Context) endStream() error {
	w := c.w
	hasBody := statusHasBody(w.status) && c.Request.Method != stdhttp.MethodHead
	var trailer stdhttp.Header
	if hasBody {
		trailer = w.trailer()
	}
	complete := !hasBody || w.declared < 0 || w.written >= w.declared
	if err := c.streamer().EndResponse(trailer, complete); err != nil {
		return err
	}
	c.wrote = true
	if w.hooks != nil {
		size := w.written
		if !hasBody {
			size = 0
		}
		w.hooks.finished(w.status, w.sent, size)
	}
	return nil
}

// sniff sets the Content-Type of a response whose handler set none from the
// body it holds back, as net/http does.
func (w *responseWriter) sniff() {
	if _, set := w.sent["Content-Type"]; !set && len(w.buf) > 0 && statusHasBody(w.status) {
		w.sent.Set("Content-Type", stdhttp.DetectContentType(w.buf[:min(len(w.buf), 512)]))
	}
}

// trailer gathers the trailers the handler set: values for the names its
// header announced, and keys it prefixed with http.TrailerPrefix.
func (w *responseWriter) trailer() stdhttp.Header {
	var trailer stdhttp.Header
	add := func(key string, values []string) {
		if trailer == nil {
			trailer = make(stdhttp.Header)
		}
		trailer[key] = append(trailer[key], values...)
	}
	names := w.trailers
	if !w.committed {
		names = announcedTrailers(w.sent)
	}
	for _, name := range names {
		if values := w.header[name]; len(values) > 0 {
			add(name, values)
		}
	}
	for key, values := range w.header {
		if name := strings.TrimPrefix(key, stdhttp.TrailerPrefix); name != key && !forbiddenTrailer(name) {
			add(stdhttp.CanonicalHeaderKey(name), values)
		}
	}
	return trailer
}

// announcedTrailers lists the names a "Trailer" header announces.
func announcedTrailers(header stdhttp.Header) []string {
	var names []string
	for _, value := range header["Trailer"] {
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" && !forbiddenTrailer(name) {
				names = append(names, stdhttp.CanonicalHeaderKey(name))
			}
		}
	}
	return names
}

func hasPrefixedTrailers(header stdhttp.Header) bool {
	for key := range header {
		if strings.HasPrefix(key, stdhttp.TrailerPrefix) {
			return true
		}
	}
	return false
}

// responseHead is how an HTTP/1 response's header frames it.
type responseHead struct {
	status int
	header stdhttp.Header
	// contentLength is sent unless it is negative. chunked sends
	// Transfer-Encoding: chunked, with trailers announced in a Trailer
	// header. close says the connection ends after the response.
	contentLength int64
	chunked       bool
	trailers      []string
	close         bool
	// contentType, when it is not empty, is a Content-Type field header
	// does not hold.
	contentType string
}

// appendResponseHead appends the status line and header of an HTTP/1
// response to req. The framing fields it sets itself replace any the header
// holds, and a Date is added unless the header has one.
func appendResponseHead(out []byte, req *stdhttp.Request, head responseHead) ([]byte, error) {
	if head.status < 100 || head.status > 999 {
		return nil, fmt.Errorf("http: invalid status code %d", head.status)
	}
	http10 := req.ProtoMajor == 1 && req.ProtoMinor == 0
	connection := ""
	switch {
	case head.close && !http10:
		connection = "close"
	case !head.close && http10:
		connection = "keep-alive"
	}
	statusText := stdhttp.StatusText(head.status)
	if statusText == "" {
		statusText = "Status"
	}
	if http10 {
		out = append(out, "HTTP/1.0 "...)
	} else {
		out = append(out, "HTTP/1.1 "...)
	}
	out = strconv.AppendInt(out, int64(head.status), 10)
	out = append(out, ' ')
	out = append(out, statusText...)
	out = append(out, crlf...)
	if _, ok := head.header["Date"]; !ok {
		out = appendDate(out)
	}
	if head.contentLength >= 0 {
		out = append(out, "Content-Length: "...)
		out = strconv.AppendInt(out, head.contentLength, 10)
		out = append(out, crlf...)
	}
	if head.chunked {
		out = append(out, "Transfer-Encoding: chunked\r\n"...)
		if len(head.trailers) > 0 {
			out = append(out, "Trailer: "...)
			out = append(out, strings.Join(head.trailers, ", ")...)
			out = append(out, crlf...)
		}
	}
	if connection != "" {
		out = append(out, "Connection: "...)
		out = append(out, connection...)
		out = append(out, crlf...)
	}
	if head.contentType != "" {
		if !validHeaderValue(head.contentType) {
			return nil, errors.New("http: invalid response header value")
		}
		out = append(out, "Content-Type: "...)
		out = append(out, head.contentType...)
		out = append(out, crlf...)
	}
	if len(head.header) == 0 {
		return append(out, crlf...), nil
	}
	out, err := appendHeaderLines(out, head.header, func(key string) bool {
		switch {
		case strings.EqualFold(key, "Content-Length"), strings.EqualFold(key, "Transfer-Encoding"),
			strings.EqualFold(key, "Trailer"), strings.HasPrefix(key, stdhttp.TrailerPrefix):
			return true
		case strings.EqualFold(key, "Connection"):
			return connection != ""
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return append(out, crlf...), nil
}

// cachedDate is the Date header value for one second, formatted once.
type cachedDate struct{ value []byte }

func newCachedDate(now time.Time) *cachedDate {
	return &cachedDate{value: now.UTC().AppendFormat(nil, stdhttp.TimeFormat)}
}

// The Date every response carries comes from a clock of its own: a timer
// that fires at each second boundary and formats the new second into
// dateClock, so that a response reads the value rather than the time. Reading
// the time for every response cost more than the rest of its head together.
// Nothing waits on the clock between ticks: each tick arms the next.
//
// The clock runs only while responses are being written. dateUsed records
// that one was written since the clock last ticked, and a clock that ticks
// twice without one, which dateIdle counts, stops, clearing dateClock; the
// next response starts it again, and until it has, formats the time it reads
// itself.
var (
	dateClock   atomic.Pointer[cachedDate]
	dateRunning atomic.Bool
	dateUsed    atomic.Bool
	dateIdle    atomic.Int32
)

// appendDate appends a Date header for now, which RFC 9110 section 6.6.1 asks
// an origin server with a clock to send.
func appendDate(out []byte) []byte {
	date := dateClock.Load()
	if date == nil {
		date = startDateClock()
	}
	// A store only when the flag changes, since every response on every
	// core passes here and the line it sits on is shared.
	if !dateUsed.Load() {
		dateUsed.Store(true)
	}
	out = append(out, "Date: "...)
	out = append(out, date.value...)
	return append(out, crlf...)
}

// startDateClock starts the clock if it is not running, and returns the date
// for now.
func startDateClock() *cachedDate {
	now := time.Now()
	date := newCachedDate(now)
	if dateRunning.CompareAndSwap(false, true) {
		dateIdle.Store(0)
		dateClock.Store(date)
		time.AfterFunc(untilNextSecond(now), tickDateClock)
	}
	return date
}

// tickDateClock moves the clock on to the second that has just begun, and
// arms the next tick, or stops the clock once it has gone unused.
func tickDateClock() {
	if dateUsed.Swap(false) {
		dateIdle.Store(0)
	} else if dateIdle.Add(1) >= 2 {
		dateClock.Store(nil)
		dateRunning.Store(false)
		return
	}
	now := time.Now()
	dateClock.Store(newCachedDate(now))
	time.AfterFunc(untilNextSecond(now), tickDateClock)
}

// untilNextSecond is how long after now the next second begins.
func untilNextSecond(now time.Time) time.Duration {
	return time.Second - time.Duration(now.Nanosecond())
}

var (
	_ stdhttp.ResponseWriter = (*Context)(nil)
	_ stdhttp.Flusher        = (*Context)(nil)
	_ io.ReaderFrom          = (*Context)(nil)
	_ io.StringWriter        = (*Context)(nil)
)
