//go:build !linux && !darwin && !windows

package fib

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/go/bufferpool"
)

type portableEvent struct {
	data     []byte
	closeErr error
	closing  bool
}

type connectionAttachment struct{ value any }

type Connection struct {
	engine                             *Engine
	handler                            Handler
	conn                               net.Conn
	fd                                 atomic.Int64
	mu                                 sync.Mutex
	events                             []portableEvent
	scheduled, closing, closeDelivered bool
	// readHeld is an application-driven read pause, set through HoldReads.
	// readWake is what the reader goroutine parks on while it is set.
	readHeld   bool
	readWake   *sync.Cond
	writeMu    sync.Mutex
	attachment atomic.Pointer[connectionAttachment]
	layer      Layer
	// udp marks a connection that exchanges datagrams: a peer of a UDP
	// listener, whose conn is a udpPeerConn, or a dialed UDP socket.
	udp bool
	// udpActive is when a listener's peer last sent or was sent a datagram,
	// in nanoseconds, for the idle timeout.
	udpActive atomic.Int64
}

// IsUDP reports whether the connection exchanges datagrams.
func (c *Connection) IsUDP() bool { return c.udp }

// RemoteAddr returns the peer's address.
func (c *Connection) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

func (c *Connection) FD() int { return int(c.fd.Load()) }

// Close ends the connection. The close is carried out asynchronously, so
// there is no error to report and Close always returns nil, including for a
// connection that is already closing. It satisfies net.Conn and io.Closer.
func (c *Connection) Close() error {
	c.closeWithError(nil)
	return nil
}

func (c *Connection) Attachment() any {
	if value := c.attachment.Load(); value != nil {
		return value.value
	}
	return nil
}

func (c *Connection) SetAttachment(value any) {
	if value == nil {
		c.attachment.Store(nil)
	} else {
		c.attachment.Store(&connectionAttachment{value: value})
	}
}

func (c *Connection) closeWithError(err error) {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true
	if c.readWake != nil {
		// A reader parked on a hold has to see the close.
		c.readWake.Broadcast()
	}
	c.events = append(c.events, portableEvent{closing: true, closeErr: err})
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	_ = c.conn.Close()
	if submit && !c.engine.submit(c) {
		c.engine.finishConnection(c, err)
	}
}

// HoldReads stops or resumes reading from this connection. See the native
// backends' HoldReads: here the connection's reader goroutine parks while the
// hold is set, which leaves the peer's bytes in the socket.
func (c *Connection) HoldReads(hold bool) {
	if c.udp {
		return
	}
	c.mu.Lock()
	if c.readHeld != hold {
		c.readHeld = hold
		if c.readWake == nil {
			c.readWake = sync.NewCond(&c.mu)
		}
		c.readWake.Broadcast()
	}
	c.mu.Unlock()
}

// ReadsHeld reports whether HoldReads is currently holding reads back.
func (c *Connection) ReadsHeld() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readHeld
}

// awaitReadable parks the reader goroutine while reads are held, and reports
// whether reading should go on.
func (c *Connection) awaitReadable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.readHeld && !c.closing {
		if c.readWake == nil {
			c.readWake = sync.NewCond(&c.mu)
		}
		c.readWake.Wait()
	}
	return !c.closing
}

func (c *Connection) Send(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.sendRaw(data)
}

// sendRaw writes bytes to the socket below any layer.
func (c *Connection) sendRaw(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return io.ErrClosedPipe
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			c.closeWithError(err)
			return err
		}
		if n == 0 {
			c.closeWithError(io.ErrNoProgress)
			return io.ErrNoProgress
		}
		data = data[n:]
	}
	if c.udp {
		c.udpActive.Store(time.Now().UnixNano())
	}
	return nil
}

// Flush does nothing on the portable backend, whose sends have reached the
// socket by the time they return.
func (c *Connection) Flush() error { return nil }

// SendOwned is equivalent to Send on the synchronous portable backend.
func (c *Connection) SendOwned(data []byte) error { return c.Send(data) }

func (c *Connection) SendParts(first, second []byte) error {
	if l := c.layer; l != nil {
		return l.Send(first, second)
	}
	// A send on this backend has reached the socket by the time it returns,
	// so the joined copy is done with as soon as Send is.
	data := bufferpool.Join(nil, first, second)
	defer bufferpool.Put(data)
	return c.Send(data)
}

// sendClosed reports whether the connection has stopped accepting sends.
func (c *Connection) sendClosed() bool {
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	return closing
}

// closeAfterSendRaw closes the connection. Sends on this backend have already
// reached the socket by the time they return, so nothing is left to wait for.
func (c *Connection) closeAfterSendRaw() { c.closeWithError(nil) }

func (c *Connection) enqueueData(data []byte) bool {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return false
	}
	c.events = append(c.events, portableEvent{data: append([]byte(nil), data...)})
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if submit && !c.engine.submit(c) {
		c.closeWithError(errors.New("task pool stopped"))
		return false
	}
	return true
}

func (c *Connection) process() {
	defer func() {
		if r := recover(); r != nil {
			c.engine.finishConnection(c, fmt.Errorf("handler panic: %v", r))
		}
	}()
	for {
		c.mu.Lock()
		if len(c.events) == 0 {
			c.scheduled = false
			c.mu.Unlock()
			return
		}
		event := c.events[0]
		c.events[0] = portableEvent{}
		c.events = c.events[1:]
		c.mu.Unlock()
		if event.closing {
			c.engine.finishConnection(c, event.closeErr)
			return
		}
		c.handler.OnData(c, event.data)
	}
}

func (c *Connection) RunTask() {
	defer c.engine.taskWG.Done()
	c.process()
}
