package kcp

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/lesismal/fib"
)

// Defaults of a Config.
const (
	// DefaultIdleTimeout is how long a session hears nothing from its peer
	// before it closes, when Config.IdleTimeout is zero. KCP has no
	// keepalive of its own: an application that may be quiet longer sends
	// its own, or sets a longer timeout.
	DefaultIdleTimeout = time.Minute
)

// Errors of a Session.
var (
	// ErrDeadLink closes a session whose segment went unacknowledged
	// through Config.DeadLink transmissions.
	ErrDeadLink = errors.New("kcp: peer stopped acknowledging")
	// ErrIdle closes a session that heard nothing for Config.IdleTimeout.
	ErrIdle = errors.New("kcp: peer idle")
	// ErrReplaced closes a session whose peer started a new conversation
	// from the same address.
	ErrReplaced = errors.New("kcp: peer started a new conversation")
)

// Config configures a session.
type Config struct {
	// NoDelay, Interval, Resend and NoCongestion are KCP.NoDelay's knobs;
	// the zero Config is the reference implementation's normal mode, and
	// kcptun's "fast3" is NoDelay 1, Interval 10ms, Resend 2,
	// NoCongestion true. Both sides should agree.
	NoDelay      int
	Interval     time.Duration
	Resend       int
	NoCongestion bool
	// SendWindow and RecvWindow are the windows, in segments;
	// DefaultSendWindow and DefaultRecvWindow when zero.
	SendWindow, RecvWindow int
	// MTU is the largest datagram the session sends, framing included;
	// DefaultMTU when zero.
	MTU int
	// StreamMode packs Write's bytes into whole segments (KCP stream
	// mode) instead of sending each Write as messages of its own.
	StreamMode bool
	// DeadLink is how many transmissions of a segment go unacknowledged
	// before the session closes with ErrDeadLink; DefaultDeadLink when
	// zero.
	DeadLink int
	// Crypt encrypts the datagrams, as kcp-go's BlockCrypt of the same
	// cipher and key; nil sends them in the clear.
	Crypt *Crypt
	// DataShards and ParityShards turn on kcp-go's forward error
	// correction, which must match the peer's: kcptun's default is 10 and
	// 3. Either at zero turns it off, as in kcp-go; FEC shards from a peer
	// that sends them are still read, their parity unused.
	DataShards, ParityShards int
	// IdleTimeout closes a session that hears nothing from its peer for
	// that long, with ErrIdle; DefaultIdleTimeout when zero, never when
	// negative.
	IdleTimeout time.Duration
	// WriteQueue is how many segments may be queued or in flight before
	// Write blocks; twice the send window when zero.
	WriteQueue int
	// OnMessage, when set, is given each message as it arrives, and Read
	// is not used: the session's flow control is then the application's,
	// for messages are taken from KCP as they complete. It runs on a fib
	// worker, never two at a time for a session, with the session's
	// mutex released, so it may Send; it must not Write, which may block
	// for acknowledgements only that worker reads. msg is valid until it
	// returns.
	OnMessage func(s *Session, msg []byte)
	// OnClose is told the session closed, and why: nil when Close closed
	// it.
	OnClose func(s *Session, err error)
}

func (c *Config) newKCP(conv uint32, overhead int, output func([]byte)) (*KCP, error) {
	mtu := c.MTU
	if mtu == 0 {
		mtu = DefaultMTU
	}
	k := New(conv, output)
	if !k.SetMTU(mtu - overhead) {
		return nil, errors.New("kcp: MTU too small for the framing")
	}
	k.NoDelay(c.NoDelay, int(c.Interval/time.Millisecond), c.Resend, c.NoCongestion)
	k.SetWindow(c.SendWindow, c.RecvWindow)
	k.SetStreamMode(c.StreamMode)
	k.SetDeadLink(c.DeadLink)
	return k, nil
}

// Session is a KCP conversation on a fib UDP connection, a Server's or
// Dial's. It is a net.Conn: Read and Write carry a stream of bytes, as a
// kcp-go UDPSession's do; with Config.OnMessage, the peer's messages come
// to it instead, and Send sends messages of the session's own. Its KCP is
// driven by the datagrams fib reads, by its own timer, and by the
// application's calls, serialized by a mutex; what each of them sends
// goes out in one batch once it releases the mutex.
type Session struct {
	mu      sync.Mutex
	cfg     Config
	k       *KCP
	fr      *framer
	fc      *fib.Connection
	remote  netip.AddrPort
	local   net.Addr
	epoch   time.Time
	idle    time.Duration
	heardAt time.Time

	timer   *time.Timer
	timerAt time.Time

	dgrams [][]byte
	arena  []byte

	closed   bool
	err      error
	die      chan struct{}
	readable chan struct{}
	writable chan struct{}
	after    []func()
	onClosed func(*Session) // the owner's cleanup

	writeQueue int
	pending    []byte // the rest of a message Read took part of
	rbuf       []byte

	deliverMu sync.Mutex
	inbox     [][]byte
	inArena   []byte

	rdl, wdl time.Time
}

func newSession(cfg Config, conv uint32, remote netip.AddrPort) (*Session, error) {
	fr, err := newFramer(cfg.Crypt, cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}
	s := &Session{
		cfg:      cfg,
		fr:       fr,
		remote:   remote,
		epoch:    time.Now(),
		die:      make(chan struct{}),
		readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1),
	}
	if s.k, err = cfg.newKCP(conv, fr.overhead(), s.output); err != nil {
		return nil, err
	}
	s.idle = cfg.IdleTimeout
	if s.idle == 0 {
		s.idle = DefaultIdleTimeout
	}
	s.writeQueue = cfg.WriteQueue
	if s.writeQueue <= 0 {
		s.writeQueue = 2 * s.k.SendWindow()
	}
	s.heardAt = s.epoch
	s.timer = time.AfterFunc(time.Hour, s.onTimer)
	s.timer.Stop()
	return s, nil
}

func (s *Session) now() (time.Time, uint32) {
	t := time.Now()
	return t, uint32(t.Sub(s.epoch) / time.Millisecond)
}

// output frames a KCP packet into datagrams, kept to send once the mutex
// is released.
func (s *Session) output(p []byte) {
	s.fr.encode(p, int64(time.Since(s.epoch)/time.Millisecond), func(d []byte) {
		start := len(s.arena)
		s.arena = append(s.arena, d...)
		s.dgrams = append(s.dgrams, s.arena[start:len(s.arena):len(s.arena)])
	})
}

// unlock flushes what is due, sends it, rearms the timer, releases the
// mutex and runs what had to wait for that.
func (s *Session) unlock() {
	if !s.closed {
		_, now := s.now()
		if at, ok := s.k.NextFlush(now); ok && timediff(at, now) <= 0 {
			s.k.Flush(now)
		}
		if s.k.Dead() {
			s.closeLocked(ErrDeadLink)
		}
	}
	s.send()
	s.rearm()
	after := s.after
	s.after = nil
	s.mu.Unlock()
	for _, fn := range after {
		fn()
	}
}

func (s *Session) send() {
	if len(s.dgrams) == 0 {
		return
	}
	if s.fc != nil {
		_, _ = s.fc.SendDatagrams(s.dgrams)
	}
	clear(s.dgrams)
	s.dgrams, s.arena = s.dgrams[:0], s.arena[:0]
}

func (s *Session) rearm() {
	if s.closed {
		s.timer.Stop()
		s.timerAt = time.Time{}
		return
	}
	t, now := s.now()
	var at time.Time
	if next, ok := s.k.NextFlush(now); ok {
		at = t.Add(time.Duration(timediff(next, now)) * time.Millisecond)
	}
	if s.idle > 0 {
		if idleAt := s.heardAt.Add(s.idle); at.IsZero() || idleAt.Before(at) {
			at = idleAt
		}
	}
	if at.IsZero() {
		s.timer.Stop()
		s.timerAt = time.Time{}
		return
	}
	if at.Equal(s.timerAt) {
		return
	}
	s.timerAt = at
	s.timer.Reset(max(time.Until(at), 0))
}

func (s *Session) onTimer() {
	s.mu.Lock()
	s.timerAt = time.Time{}
	if !s.closed && s.idle > 0 && time.Since(s.heardAt) >= s.idle {
		s.closeLocked(ErrIdle)
	}
	s.unlock()
}

// input reads opened datagrams that arrived on fc.
func (s *Session) input(fc *fib.Connection, dgrams ...[]byte) {
	s.deliverMu.Lock()
	defer s.deliverMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if fc != nil {
		s.fc = fc
	}
	t, now := s.now()
	s.heardAt = t
	waited := s.k.WaitSend()
	for _, d := range dgrams {
		s.fr.decode(d, func(p []byte) { _ = s.k.Input(p, now) })
	}
	if s.cfg.OnMessage != nil {
		s.takeMessages()
	} else if s.k.PeekSize() >= 0 {
		notify(s.readable)
	}
	if w := s.k.WaitSend(); w < waited && w < s.writeQueue {
		notify(s.writable)
	}
	inbox := s.inbox
	s.unlock()
	for _, m := range inbox {
		s.cfg.OnMessage(s, m)
	}
	clear(s.inbox)
	s.inbox, s.inArena = s.inbox[:0], s.inArena[:0]
}

// takeMessages moves the complete messages out of KCP into the inbox.
func (s *Session) takeMessages() {
	for {
		size := s.k.PeekSize()
		if size < 0 {
			return
		}
		start := len(s.inArena)
		s.inArena = append(s.inArena, make([]byte, size)...)
		n, _ := s.k.Recv(s.inArena[start:])
		s.inbox = append(s.inbox, s.inArena[start:start+n:start+n])
	}
}

func notify(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// Read reads the peer's bytes, as net.Conn. A session with
// Config.OnMessage has none to read.
func (s *Session) Read(b []byte) (int, error) {
	for {
		s.mu.Lock()
		if len(s.pending) > 0 {
			n := copy(b, s.pending)
			s.pending = s.pending[n:]
			s.mu.Unlock()
			return n, nil
		}
		if size := s.k.PeekSize(); size >= 0 {
			var n int
			if size <= len(b) {
				n, _ = s.k.Recv(b)
			} else {
				if cap(s.rbuf) < size {
					s.rbuf = make([]byte, size)
				}
				m, _ := s.k.Recv(s.rbuf[:size])
				n = copy(b, s.rbuf[:m])
				s.pending = s.rbuf[n:m]
			}
			if s.k.PeekSize() >= 0 {
				notify(s.readable)
			}
			s.unlock()
			return n, nil
		}
		if s.closed {
			err := s.err
			s.mu.Unlock()
			if err == nil || err == ErrIdle || err == ErrReplaced {
				return 0, io.EOF
			}
			return 0, err
		}
		dl := s.rdl
		s.mu.Unlock()
		if err := wait(s.readable, s.die, dl); err != nil {
			return 0, err
		}
	}
}

// wait waits for c, the session to close, or the deadline.
func wait(c, die chan struct{}, dl time.Time) error {
	var timeout <-chan time.Time
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return os.ErrDeadlineExceeded
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case <-c:
	case <-die:
	case <-timeout:
		return os.ErrDeadlineExceeded
	}
	return nil
}

// Write sends b, as net.Conn: in segments of their own, or packed into
// whole ones in stream mode. It blocks while Config.WriteQueue segments
// are queued or in flight.
func (s *Session) Write(b []byte) (int, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, s.closedErr()
		}
		if s.k.WaitSend() < s.writeQueue {
			n := len(b)
			if s.k.stream {
				_ = s.k.Send(b)
			} else {
				mss := s.k.MSS()
				for len(b) > 0 {
					m := min(len(b), mss)
					_ = s.k.Send(b[:m])
					b = b[m:]
				}
			}
			s.unlock()
			return n, nil
		}
		dl := s.wdl
		s.mu.Unlock()
		if err := wait(s.writable, s.die, dl); err != nil {
			return 0, err
		}
	}
}

// Send queues msg as one message, which the peer's OnMessage or Recv gets
// whole, without waiting: up to 255 segments of MSS bytes. Unlike Write
// it never blocks, so a session that sends faster than its peer
// acknowledges should watch WaitSend.
func (s *Session) Send(msg []byte) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return s.closedErr()
	}
	err := s.k.Send(msg)
	s.unlock()
	return err
}

// WaitSend returns how many segments are queued or in flight.
func (s *Session) WaitSend() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.k.WaitSend()
}

// MSS returns the largest payload of a segment.
func (s *Session) MSS() int { return s.k.MSS() }

// Conv returns the conversation ID.
func (s *Session) Conv() uint32 { return s.k.Conv() }

// Stats returns the counters of the session's KCP.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.k.Stats()
	st.FECRecovered = s.fr.recovered
	return st
}

// RTT returns the smoothed round-trip time.
func (s *Session) RTT() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Duration(s.k.SRTT()) * time.Millisecond
}

func (s *Session) closedErr() error {
	if s.err != nil && s.err != ErrIdle && s.err != ErrReplaced {
		return s.err
	}
	return net.ErrClosed
}

// Close sends what is due and closes the session. KCP has no close of its
// own: the peer finds out when its idle timeout or dead link does, or
// from the application.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	_, now := s.now()
	s.k.Flush(now)
	s.closeLocked(nil)
	s.unlock()
	return nil
}

func (s *Session) closeWith(err error) {
	s.mu.Lock()
	s.closeLocked(err)
	s.unlock()
}

func (s *Session) closeLocked(err error) {
	if s.closed {
		return
	}
	s.closed, s.err = true, err
	close(s.die)
	if fn := s.onClosed; fn != nil {
		s.after = append(s.after, func() { fn(s) })
	}
	if fn := s.cfg.OnClose; fn != nil {
		s.after = append(s.after, func() { fn(s, err) })
	}
}

// LocalAddr returns the address of the UDP socket.
func (s *Session) LocalAddr() net.Addr { return s.local }

// RemoteAddr returns the peer's address.
func (s *Session) RemoteAddr() net.Addr { return net.UDPAddrFromAddrPort(s.remote) }

// RemoteAddrPort returns the peer's address.
func (s *Session) RemoteAddrPort() netip.AddrPort { return s.remote }

// SetDeadline sets the read and write deadlines.
func (s *Session) SetDeadline(t time.Time) error {
	s.mu.Lock()
	s.rdl, s.wdl = t, t
	s.mu.Unlock()
	notify(s.readable)
	notify(s.writable)
	return nil
}

// SetReadDeadline sets the read deadline.
func (s *Session) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.rdl = t
	s.mu.Unlock()
	notify(s.readable)
	return nil
}

// SetWriteDeadline sets the write deadline.
func (s *Session) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.wdl = t
	s.mu.Unlock()
	notify(s.writable)
	return nil
}

var _ net.Conn = (*Session)(nil)
