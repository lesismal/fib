//go:build linux || darwin || windows

package grpc

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

var (
	errMessageTooLarge = errors.New("grpc: message larger than the limit")
	errStreamDone      = errors.New("grpc: the stream is done")
)

// stream is one HTTP/2 stream of a call, on either side: the messages it has
// received, and the window this side is sending within.
type stream struct {
	t  *transport
	id uint32
	// ctx ends the call: the client's own context, or the one a server
	// makes for the handler.
	ctx context.Context
	ss  *serverStream
	cs  *clientStream

	mu sync.Mutex
	// buf[off:] is what has arrived and has not been read.
	buf []byte
	off int
	// remoteEnd is set once the peer has ended its side, err once the stream
	// failed.
	remoteEnd bool
	err       error
	// recvWindow is how much more the peer may send; unacked is what has
	// been read since the window was last given back.
	recvWindow int64
	unacked    int64
	// ready has a token once something has changed for a reader.
	ready chan struct{}
	// onEnd, if set, runs once the peer's side has ended, before readers
	// hear: a server dispatching a unary call then.
	onEnd func()

	// Guarded by t.mu: the window this side sends within, and whether this
	// side has ended.
	sendWindow int64
	localEnd   bool
}

func (s *stream) signal() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// onData takes a DATA frame's payload, which took length of the window.
func (s *stream) onData(payload []byte, length int64, end bool) error {
	s.mu.Lock()
	if length > s.recvWindow {
		s.mu.Unlock()
		return &streamError{id: s.id, code: errFlowControl}
	}
	s.recvWindow -= length
	// Padding is never read, so it is given back with what is.
	s.unacked += length - int64(len(payload))
	if s.err == nil {
		s.buf = append(s.buf, payload...)
	}
	// A unary call waiting for its request is served once the request has
	// arrived, or once the window is full and the handler has to make room
	// for the rest.
	onEnd := s.onEnd
	if end || s.recvWindow <= 0 {
		s.onEnd = nil
	} else {
		onEnd = nil
	}
	if end {
		s.remoteEnd = true
	}
	s.mu.Unlock()
	if onEnd != nil {
		onEnd()
	}
	s.signal()
	return nil
}

// abort fails the stream with err.
func (s *stream) abort(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.onEnd = nil
	s.mu.Unlock()
	s.signal()
	if s.ss != nil {
		s.ss.cancel()
	}
	if s.cs != nil {
		s.cs.ended()
	}
}

// readMsg waits for the next message and returns whether it is compressed
// and its payload, which is the caller's. It returns io.EOF once the peer
// has ended its side and everything has been read.
func (s *stream) readMsg(maxSize int) (bool, []byte, error) {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return false, nil, err
		}
		avail := len(s.buf) - s.off
		need := 5
		if avail >= 5 {
			length := binary.BigEndian.Uint32(s.buf[s.off+1:])
			if uint64(length) > uint64(maxSize) {
				s.mu.Unlock()
				return false, nil, status.Errorf(codes.ResourceExhausted,
					"grpc: received message larger than max (%d vs. %d)", length, maxSize)
			}
			need = 5 + int(length)
			if avail >= need {
				compressed := s.buf[s.off] == 1
				msg := make([]byte, length)
				copy(msg, s.buf[s.off+5:])
				s.off += need
				if s.off == len(s.buf) {
					s.buf, s.off = s.buf[:0], 0
				} else if s.off > cap(s.buf)/2 {
					s.buf = s.buf[:copy(s.buf, s.buf[s.off:])]
					s.off = 0
				}
				s.unacked += int64(need)
				inc := int64(0)
				if s.unacked >= streamWindow/4 && !s.remoteEnd {
					inc, s.unacked = s.unacked, 0
					s.recvWindow += inc
				}
				s.mu.Unlock()
				s.credit(inc)
				return compressed, msg, nil
			}
		}
		if s.remoteEnd {
			s.mu.Unlock()
			if avail == 0 {
				return false, nil, io.EOF
			}
			return false, nil, status.Error(codes.Internal, "grpc: the stream ended in the middle of a message")
		}
		// A message larger than the window could never arrive whole unless
		// the window makes room for it.
		inc := int64(0)
		if short := int64(need-avail) - s.recvWindow; short > 0 {
			inc = short + s.unacked
			s.unacked = 0
			s.recvWindow += inc
		}
		s.mu.Unlock()
		s.credit(inc)
		select {
		case <-s.ready:
		case <-s.ctx.Done():
			return false, nil, status.FromContextError(s.ctx.Err()).Err()
		}
	}
}

// credit gives inc back to the peer's window.
func (s *stream) credit(inc int64) {
	if inc <= 0 {
		return
	}
	t := s.t
	t.mu.Lock()
	if t.streams[s.id] == s {
		t.out = appendWindowUpdate(t.out, s.id, uint32(inc))
	}
	t.mu.Unlock()
	t.flush()
}

// writeMsg sends a message: the five byte prefix and payload in DATA frames
// as the windows let them go, waiting for room as long as ctx lets it.
func (s *stream) writeMsg(compressed bool, payload []byte) error {
	var prefix [5]byte
	if compressed {
		prefix[0] = 1
	}
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(payload)))
	parts := [2][]byte{prefix[:], payload}
	t := s.t
	t.mu.Lock()
	for i := 0; i < len(parts); {
		if err := s.sendableLocked(); err != nil {
			t.mu.Unlock()
			return err
		}
		part := parts[i]
		if len(part) == 0 {
			i++
			continue
		}
		n := int(min(int64(len(part)), t.sendWindow, s.sendWindow, int64(t.peerMaxFrame)))
		if n <= 0 {
			changed := t.changed
			t.mu.Unlock()
			t.flush()
			select {
			case <-changed:
			case <-s.ctx.Done():
				return status.FromContextError(s.ctx.Err()).Err()
			}
			t.mu.Lock()
			continue
		}
		// The prefix rides with the start of the payload when it can.
		if i == 0 && len(payload) > 0 && n == len(part) {
			m := int(min(int64(len(payload)), t.sendWindow-int64(n), s.sendWindow-int64(n), int64(t.peerMaxFrame-n)))
			if m > 0 {
				t.out = appendFrameHeader(t.out, frameData, 0, s.id, n+m)
				t.out = append(append(t.out, part...), payload[:m]...)
				t.sendWindow -= int64(n + m)
				s.sendWindow -= int64(n + m)
				parts[1] = payload[m:]
				i++
				continue
			}
		}
		t.out = appendFrameHeader(t.out, frameData, 0, s.id, n)
		t.out = append(t.out, part[:n]...)
		t.sendWindow -= int64(n)
		s.sendWindow -= int64(n)
		parts[i] = part[n:]
		if len(t.out) >= maxSpareOut {
			t.mu.Unlock()
			t.flush()
			t.mu.Lock()
		}
	}
	t.mu.Unlock()
	t.flush()
	return nil
}

// sendableLocked reports why the stream cannot send. Callers hold t.mu.
func (s *stream) sendableLocked() error {
	t := s.t
	if t.closed {
		return status.New(codes.Unavailable, "grpc: connection closed").Err()
	}
	if s.localEnd {
		return errStreamDone
	}
	if t.streams[s.id] != s {
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		if err == nil {
			err = errStreamDone
		}
		return err
	}
	return nil
}
