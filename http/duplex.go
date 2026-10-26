//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	"runtime/debug"
	"sync"

	"github.com/lesismal/fib/log"
)

// ErrDuplexClosed is what a Duplex reports once it has been closed, or once
// the request under it has been cancelled or reset.
var ErrDuplexClosed = errors.New("http: duplex closed")

// A DuplexHandler serves a request whose body and response are used at the
// same time: it reads the body through a Duplex.Read and writes the response
// through Duplex.Write, in either order, rather than taking the body whole
// before the handler runs and producing the response after it ends.
type DuplexHandler func(*Duplex)

// Duplex is one request's body and response at once. Its Read takes the
// request body as it arrives, and its Write sends response bytes as they are
// produced; Flush pushes what is written to the client, and Close ends both
// directions.
//
// Reads block the goroutine that calls them, which is the handler's own and
// not an engine worker, so a handler may wait for the client without holding
// the connection's event loop. At most duplexBodyLimit bytes of body that the
// handler has not read yet are held: past that the callback that delivers the
// body waits, which gives the client's flow control the chance to hold it
// back, so a client cannot make the server buffer a body it does not read.
//
// Writes do not block for the client: what the stream or connection cannot
// take yet is queued, as with ResponseWriter. A handler that writes faster
// than the client reads should Flush (or rely on the stream's own flow
// control), and the protocol decides whether that waits.
//
// A Duplex is not safe for concurrent Reads, nor for concurrent Writes; one
// goroutine may read while another writes, which is what the API is for. Its
// Close and Err may be called from any goroutine.
type Duplex struct {
	c    *Context
	r    io.Reader
	pipe *duplexBody

	mu     sync.Mutex
	closed bool
	err    error
}

// Context returns the request this duplex serves.
func (d *Duplex) Context() *Context { return d.c }

// Read reads the request body, waiting until some of it arrives. It returns
// io.EOF once the whole body has been read, and the request's error when the
// client went away, the body failed, or the duplex was closed.
func (d *Duplex) Read(p []byte) (int, error) {
	d.mu.Lock()
	closed, err := d.closed, d.err
	d.mu.Unlock()
	if closed {
		if err != nil {
			return 0, err
		}
		return 0, ErrDuplexClosed
	}
	n, err := d.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		d.setErr(err)
	}
	return n, err
}

// Write writes response body bytes. It reports ErrResponseWritten, an error
// from the stream, or ErrDuplexClosed once the duplex is closed.
func (d *Duplex) Write(p []byte) (int, error) {
	d.mu.Lock()
	closed, err := d.closed, d.err
	d.mu.Unlock()
	if closed {
		if err != nil {
			return 0, err
		}
		return 0, ErrDuplexClosed
	}
	n, err := d.c.Write(p)
	if err != nil {
		d.setErr(err)
	}
	return n, err
}

// Flush sends what has been written so far to the client, as
// ResponseWriter's Flush does.
func (d *Duplex) Flush() error {
	d.mu.Lock()
	closed, err := d.closed, d.err
	d.mu.Unlock()
	if closed {
		if err != nil {
			return err
		}
		return ErrDuplexClosed
	}
	if err := d.c.FlushError(); err != nil {
		d.setErr(err)
		return err
	}
	return nil
}

// Close ends the duplex: what was written is flushed, an empty 200 is begun
// when nothing was, the response is finished, the rest of the request body is
// dropped, and later Reads and Writes report ErrDuplexClosed. It is safe to
// call more than once; a handler that returns without calling it has its end
// done for it.
func (d *Duplex) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	if d.pipe != nil {
		d.pipe.close(ErrDuplexClosed)
	}
	if stream := d.c.RequestBody(); stream != nil {
		_ = stream.Close()
	}
	// FlushError begins a 200 when nothing was written, so Close ends the
	// response even for a handler that answers with nothing; Finish would
	// leave a status-0 response to the handler's return otherwise.
	if err := d.c.FlushError(); err != nil && !errors.Is(err, ErrResponseWritten) {
		d.setErr(err)
		_ = d.c.Finish()
		return err
	}
	err := d.c.Finish()
	if err != nil {
		d.setErr(err)
		return err
	}
	if d.c.Conn != nil {
		if ferr := d.c.Conn.Flush(); ferr != nil {
			d.setErr(ferr)
		}
	}
	return nil
}

// Err returns why the duplex ended: the request's error when the client went
// away or the stream failed, otherwise the error a Read or Write saw, or
// ErrDuplexClosed once it ended. It reads only the duplex's own state, so it
// is safe after the handler has returned and the request has been released.
func (d *Duplex) Err() error {
	d.mu.Lock()
	err := d.err
	closed := d.closed
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if closed {
		return ErrDuplexClosed
	}
	return nil
}

// setErr keeps the first error the duplex saw.
func (d *Duplex) setErr(err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	if d.err == nil {
		d.err = err
	}
	d.mu.Unlock()
}

// ServeDuplex hands the request to h on a goroutine of its own, with its body
// readable and its response writable at the same time. It retains the request
// and returns once that goroutine has started, not when h returns: the
// caller's handler returns, and the response stays open until h returns (or
// calls Close). h reads the body through Read, which waits for the client, and
// writes the response through Write and Flush.
//
// The body is the one the request has when the handler runs: with
// Config.StreamRequestBody and a body past its threshold, Read takes it as it
// arrives; a body that arrived whole is read from its buffer and never
// waits. On HTTP/2 and HTTP/3 the request's stream is full duplex, so this is
// how a handler speaks a protocol that talks both ways at once, such as gRPC
// or WebTransport. An HTTP/1.1 response may be written before the client has
// finished sending the request body, but a client that stops sending when it
// sees the response will leave Read waiting for bytes that never come; such a
// handler should answer only once it has the body it needs.
//
// ServeDuplex calls the request body's OnBody, so the handler must not read
// Request.Body or call OnBody itself. It reports ErrResponseWritten when the
// response has already begun or the request is no longer open. It is meant to
// be called once, from the request's handler, before anything is written.
func (c *Context) ServeDuplex(h DuplexHandler) error {
	if h == nil {
		return errors.New("http: nil duplex handler")
	}
	if c.Request == nil {
		return ErrResponseWritten
	}
	if c.begun() {
		return ErrResponseWritten
	}
	if !c.hold() {
		if err := c.Err(); err != nil {
			return err
		}
		return ErrResponseWritten
	}
	d := &Duplex{c: c}
	if stream := c.RequestBody(); stream != nil {
		p := newDuplexBody()
		d.r, d.pipe = p, p
		c.OnBody(p.write)
	} else if body := c.Body(); body != nil {
		// The body arrived whole and stays the handler's while the response
		// is held open, so the duplex reads it without copying or waiting.
		d.r = bytes.NewReader(body)
	} else if c.Request.Body != nil {
		d.r = c.Request.Body
	} else {
		d.r = bytes.NewReader(nil)
	}
	// The error is kept with the duplex, so that Err and a waiting Read
	// report it even after the request has been released and recycled.
	c.OnCancel(func(err error) {
		if err == nil {
			err = ErrDuplexClosed
		}
		d.setErr(err)
		if d.pipe != nil {
			d.pipe.close(err)
		}
	})
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("http: duplex handler panicked",
					"panic", recovered, "stack", log.Lines(debug.Stack()))
				d.setErr(ErrDuplexClosed)
				d.c.abandon()
			}
			d.finish()
			d.c.Release()
		}()
		h(d)
	}()
	return nil
}

// finish ends what the handler left open and returns the request's retain.
// The body comes first: closing the pipe wakes a body callback that may be
// waiting on a full buffer, and closing the stream drops a body the client is
// still sending instead of holding the request until it stops.
func (d *Duplex) finish() {
	// Keep the request's error before Release: Err must not read a Context
	// that may already be serving another request.
	if err := d.c.Err(); err != nil {
		d.setErr(err)
	}
	d.mu.Lock()
	closed := d.closed
	d.closed = true
	d.mu.Unlock()
	if d.pipe != nil {
		d.pipe.close(ErrDuplexClosed)
	}
	if stream := d.c.RequestBody(); stream != nil {
		_ = stream.Close()
	}
	if !closed {
		_ = d.c.Finish()
	}
}

// duplexBodyLimit is how much request body a Duplex holds unread before the
// callback that delivers it waits for the handler to catch up.
const duplexBodyLimit = 1 << 20

// duplexBody carries a streamed request body from OnBody's callback to
// Duplex.Read. The callback waits while duplexBodyLimit bytes are unread,
// which holds the stream's flow-control window, and so the client, back.
type duplexBody struct {
	mu   sync.Mutex
	cond sync.Cond
	// buf holds what has arrived, read what of it the reader has taken.
	buf  []byte
	read int
	// err is what a read meets once buf is drained: io.EOF after the whole
	// body, or why the rest will not come. closed is set once the reader is
	// gone, after which what arrives is dropped.
	err    error
	closed bool
}

func newDuplexBody() *duplexBody {
	p := &duplexBody{}
	p.cond.L = &p.mu
	return p
}

// write is OnBody's callback.
func (p *duplexBody) write(data []byte, fin bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if p.read > 0 && p.read >= len(p.buf)/2 {
		p.buf = p.buf[:copy(p.buf, p.buf[p.read:])]
		p.read = 0
	}
	p.buf = append(p.buf, data...)
	switch {
	case err != nil:
		p.err = err
	case fin:
		p.err = io.EOF
	}
	p.cond.Broadcast()
	for len(p.buf)-p.read >= duplexBodyLimit && !p.closed {
		p.cond.Wait()
	}
}

// Read takes what has arrived, waiting until there is some or the body ends.
func (p *duplexBody) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.read == len(p.buf) && p.err == nil && !p.closed {
		p.cond.Wait()
	}
	if p.read == len(p.buf) {
		if p.err != nil {
			return 0, p.err
		}
		return 0, ErrDuplexClosed
	}
	n := copy(b, p.buf[p.read:])
	p.read += n
	if p.read == len(p.buf) {
		p.buf, p.read = p.buf[:0], 0
	}
	p.cond.Broadcast()
	return n, nil
}

// close ends the pipe for the reader, with err for a read that finds it
// drained, and wakes a callback waiting on a full buffer.
func (p *duplexBody) close(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}
