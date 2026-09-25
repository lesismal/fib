package quic

import "github.com/lesismal/fib/bufferpool"

// eventKind is which handler call an event is.
type eventKind uint8

const (
	evStreamData eventKind = iota
	evStreamReset
	evStopSending
	evStreamsAvailable
	evHandshake
	evClose
	// evRecycle is no call: it gives a datagram back to the pool once the
	// calls queued ahead of it, which may refer to it, have run.
	evRecycle
	// evRelease is no call either: it drops a stream's Context, on the
	// goroutine that makes the calls, once the application is done with it
	// (see Stream.WriteFinal).
	evRelease
)

// event is a handler call waiting for dispatch. It is a value rather than a
// closure, so that queueing one allocates nothing.
type event struct {
	kind   eventKind
	fin    bool
	stream *Stream
	// data is OnStreamData's, or the datagram evRecycle gives back.
	data []byte
	code uint64
}

// run makes the call an event is. Callers hold no lock.
func (c *Conn) run(e *event) {
	if e.stream != nil && e.stream.released {
		return
	}
	switch e.kind {
	case evStreamData:
		c.handler.OnStreamData(e.stream, e.data, e.fin)
	case evStreamReset:
		c.handler.OnStreamReset(e.stream, e.code)
	case evStopSending:
		c.handler.OnStopSending(e.stream, e.code)
	case evStreamsAvailable:
		c.handler.OnStreamsAvailable(c)
	case evHandshake:
		c.handler.OnHandshake(c)
	case evClose:
		c.mu.Lock()
		t := c.tls
		c.mu.Unlock()
		if t != nil {
			t.Close()
		}
		_ = c.pc.Close()
		// closeErr is set once, before evClose is queued.
		c.handler.OnClose(c, c.closeErr)
	case evRecycle:
		bufferpool.Put(e.data)
	case evRelease:
		e.stream.Context = nil
		e.stream.released = true
	}
}
