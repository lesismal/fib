//go:build linux || darwin || windows

package arpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// Stream is a two-way sequence of messages over a Client, bound to a method.
// Each side closes its sending and its receiving half on its own, and the
// Stream is gone from the Client once both are closed.
type Stream struct {
	id     uint64
	cli    *Client
	method string
	// local marks the side that opened the Stream.
	local  bool
	chData chan *Message
	// recvClosed is closed by CloseRecv.
	recvClosed chan struct{}
	recvOnce   sync.Once
	sendClosed atomic.Bool
	halves     atomic.Int32
}

// NewStream opens a Stream for method. The peer's handler for method, which
// Handler.HandleStream registers, runs when the first message arrives.
func (c *Client) NewStream(method string) *Stream {
	return c.newStream(method, 0, true)
}

// newStream makes a Stream and adds it to the Client. An id of 0 takes the
// next sequence number.
func (c *Client) newStream(method string, id uint64, local bool) *Stream {
	if id == 0 {
		id = c.seq.Add(1)
	}
	s := &Stream{
		id:         id,
		cli:        c,
		method:     method,
		local:      local,
		chData:     make(chan *Message, c.Handler.streamQueueSize),
		recvClosed: make(chan struct{}),
	}
	c.mu.Lock()
	if local {
		c.streamLocal[id] = s
	} else {
		c.streamRemote[id] = s
	}
	c.mu.Unlock()
	return s
}

// Id returns the Stream's id.
func (s *Stream) Id() uint64 { return s.id }

// Method returns the Stream's method.
func (s *Stream) Method() string { return s.method }

// onStreamMessage hands a stream message to its Stream, opening one for a
// method with a handler if the peer opened it.
func (c *Client) onStreamMessage(msg *Message) {
	id, eof := msg.Seq(), msg.IsStreamEOF()
	// The sender's local Stream is this side's remote one.
	local := !msg.IsStreamLocal()
	c.mu.Lock()
	streams := c.streamRemote
	if local {
		streams = c.streamLocal
	}
	s, ok := streams[id]
	c.mu.Unlock()
	if ok {
		s.onMessage(msg)
		if eof {
			s.CloseRecv()
		}
		return
	}
	sr, ok := c.Handler.streams[msg.method()]
	if !ok || local {
		logger().Warn("arpc: no stream handler for method", "method", msg.Method(), "remote", c.remoteAddr())
		c.Handler.OnMessageDone(c, msg)
		return
	}
	s = c.newStream(msg.Method(), id, false)
	s.onMessage(msg)
	if eof {
		s.CloseRecv()
	}
	task := &streamTask{handler: sr.handler, stream: s}
	if sr.async {
		c.runTask(task)
	} else {
		task.RunTask()
	}
}

type streamTask struct {
	handler StreamHandlerFunc
	stream  *Stream
}

func (t *streamTask) RunTask() {
	safeCall("stream handler of "+t.stream.method, func() { t.handler(t.stream) })
}

// onMessage queues msg for Recv. A message with no payload, which carries only
// the EOF, is not queued. While the queue is full, the connection waits, and
// reads nothing more, after handing what its round has sent to the socket.
func (s *Stream) onMessage(msg *Message) {
	c := s.cli
	if len(msg.Data()) == 0 {
		c.Handler.OnMessageDone(c, msg)
		return
	}
	select {
	case <-s.recvClosed:
		c.Handler.OnMessageDone(c, msg)
		return
	case s.chData <- msg:
		return
	default:
	}
	if conn := c.Conn(); conn != nil {
		_ = conn.Flush()
	}
	select {
	case s.chData <- msg:
	case <-s.recvClosed:
		c.Handler.OnMessageDone(c, msg)
	case <-c.chClose:
		c.Handler.OnMessageDone(c, msg)
	}
}

// CloseRecv closes the receiving half. Recv still returns the messages
// already queued, and then io.EOF.
func (s *Stream) CloseRecv() {
	s.recvOnce.Do(func() {
		close(s.recvClosed)
		s.halfClose()
	})
}

// CloseRecvContext is CloseRecv.
func (s *Stream) CloseRecvContext(ctx context.Context) { s.CloseRecv() }

// CloseSend closes the sending half, by sending the peer an EOF.
func (s *Stream) CloseSend() { s.CloseSendContext(context.Background()) }

// CloseSendContext is CloseSend. A message goes to the connection at once, so
// there is nothing for ctx to bound.
func (s *Stream) CloseSendContext(ctx context.Context) {
	if s.sendClosed.CompareAndSwap(false, true) {
		_ = s.send(nil, true, nil)
		s.halfClose()
	}
}

// abort closes both halves of a Stream whose connection was lost.
func (s *Stream) abort() {
	if s.sendClosed.CompareAndSwap(false, true) {
		s.halves.Add(1)
	}
	s.recvOnce.Do(func() {
		close(s.recvClosed)
		s.halves.Add(1)
	})
}

// Recv waits for the next message and decodes it into v, as Client.Call
// decodes a response. Once the receiving half is closed and drained it
// returns io.EOF, and once the Client stops ErrClientStopped.
func (s *Stream) Recv(v any) error {
	return s.recv(nil, v)
}

// RecvContext is Recv, returning ErrTimeout if ctx ends first.
func (s *Stream) RecvContext(ctx context.Context, v any) error {
	return s.recv(ctx.Done(), v)
}

// RecvWith is RecvContext.
func (s *Stream) RecvWith(ctx context.Context, v any) error { return s.RecvContext(ctx, v) }

func (s *Stream) recv(done <-chan struct{}, v any) error {
	var msg *Message
	select {
	case msg = <-s.chData:
	default:
		select {
		case msg = <-s.chData:
		case <-s.recvClosed:
			select {
			case msg = <-s.chData:
			default:
				return io.EOF
			}
		case <-done:
			return ErrTimeout
		case <-s.cli.chClose:
			return ErrClientStopped
		}
	}
	c := s.cli
	err := bytesToValue(c.Codec, msg.Data(), v)
	c.Handler.OnMessageDone(c, msg)
	return err
}

// Send sends v to the peer, encoded as Client.Call encodes a request. values,
// if given, are the Message's, for the coders. Once the sending half is
// closed it returns ErrStreamClosedSend.
func (s *Stream) Send(v any, values ...map[any]any) error {
	return s.checkStateAndSend(v, false, values)
}

// SendContext is Send. A message goes to the connection at once, so there is
// nothing for ctx to bound.
func (s *Stream) SendContext(ctx context.Context, v any, values ...map[any]any) error {
	return s.checkStateAndSend(v, false, values)
}

// SendWith is SendContext.
func (s *Stream) SendWith(ctx context.Context, v any, values ...map[any]any) error {
	return s.SendContext(ctx, v, values...)
}

// SendAndClose sends v as the last message, closing the sending half.
func (s *Stream) SendAndClose(v any, values ...map[any]any) error {
	return s.checkStateAndSend(v, true, values)
}

// SendAndCloseContext is SendAndClose.
func (s *Stream) SendAndCloseContext(ctx context.Context, v any, values ...map[any]any) error {
	return s.SendAndClose(v, values...)
}

// SendAndCloseWith is SendAndCloseContext.
func (s *Stream) SendAndCloseWith(ctx context.Context, v any, values ...map[any]any) error {
	return s.SendAndClose(v, values...)
}

func (s *Stream) checkStateAndSend(v any, eof bool, values []map[any]any) error {
	if eof {
		if !s.sendClosed.CompareAndSwap(false, true) {
			return ErrStreamClosedSend
		}
		err := s.send(v, true, values)
		s.halfClose()
		return err
	}
	if s.sendClosed.Load() {
		return ErrStreamClosedSend
	}
	return s.send(v, false, values)
}

func (s *Stream) send(v any, eof bool, values []map[any]any) error {
	h := header{cmd: CmdStream, method: s.method, seq: s.id}
	if s.local {
		h.cmd |= HeaderStreamLocalBit
	}
	if eof {
		h.cmd |= HeaderStreamEOFBit
	}
	return s.cli.sendValue(h, v, firstValues(values))
}

// halfClose counts a closed half, and forgets the Stream once both are.
func (s *Stream) halfClose() {
	if s.halves.Add(1) != 2 {
		return
	}
	c := s.cli
	c.mu.Lock()
	streams := c.streamRemote
	if s.local {
		streams = c.streamLocal
	}
	if streams[s.id] == s {
		delete(streams, s.id)
	}
	c.mu.Unlock()
}
