package srt

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// Defaults of the sessions of a Server or a client.
const (
	// DefaultMaxSendBacklog is the payload a sending session may hold
	// unacknowledged before it drops packets (hub.NewDropper).
	DefaultMaxSendBacklog = 4 << 20
	// maxPending bounds the packets a sending session holds while its
	// handshake finishes.
	maxPending = 1024
)

// Session is one SRT connection on a fib engine, a Server's or a client's:
// a Conn driven by the datagrams of a fib UDP connection and by a timer of
// its own, serialized by one mutex. A session that publishes demuxes the
// MPEG-TS it receives into the hub; one that plays muxes a hub
// subscription into MPEG-TS and sends it, dropping (hub.NewDropper) while
// the peer is too far behind. A session handed to the application
// (ServerConfig.OnSession, or a client without a hub) carries whatever its
// Write and OnData carry.
type Session struct {
	mu      sync.Mutex
	conn    *Conn
	fc      *fib.Connection
	remote  netip.AddrPort
	id      StreamID
	timer   *time.Timer
	timerAt time.Time
	closed  bool
	ready   bool

	dgrams [][]byte
	arena  []byte
	after  []func()

	backlog    atomic.Int64
	maxBacklog int64

	onData   func([]byte)
	onClose  func(error)
	onClosed func(*Session) // the owner's cleanup
	closeReq error          // a close asked for from inside a Conn callback
	closeSet bool

	// Publishing into a hub.
	hub    *hub.Hub
	path   string
	info   hub.PublishInfo
	pub    *hub.Publisher
	demux  *mpegts.Demuxer
	demuxC mpegts.DemuxerConfig

	// Playing from a hub.
	sub      *hub.Subscription
	muxCfg   mpegts.MuxerConfig
	mux      *mpegts.Muxer
	tracks   []*av.Track
	ts       []byte
	pending  []*av.Packet
	waitKey  bool
	isPlayer bool
	// After new tracks (a republish, restart), the next publisher's clock
	// may start anywhere, and MPEG-TS players take a step back for a
	// discontinuity: offset moves its times on from lastDTS, the largest
	// sent (at lastAt), into shifted, as flv.Timeline does for RTMP.
	restart bool
	offset  time.Duration
	lastDTS time.Duration
	lastAt  time.Time
	shifted av.Packet
}

// maxClockJump is how far past the last packet the first one after new
// tracks may be and still be taken as the same clock carrying on. A new
// clock starts after the last packet by as long as the stream was quiet,
// at least minRestartGap and at most maxClockJump.
const (
	maxClockJump  = 10 * time.Second
	minRestartGap = 20 * time.Millisecond
)

func newSession(remote netip.AddrPort, maxBacklog int64) *Session {
	s := &Session{remote: remote, maxBacklog: maxBacklog}
	if s.maxBacklog <= 0 {
		s.maxBacklog = DefaultMaxSendBacklog
	}
	s.timer = time.AfterFunc(time.Hour, s.onTimer)
	s.timer.Stop()
	return s
}

// StreamID returns the session's parsed stream ID.
func (s *Session) StreamID() StreamID { return s.id }

// Remote returns the peer's address.
func (s *Session) Remote() netip.AddrPort { return s.remote }

// Stats returns the counters of the session's Conn.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Stats()
}

// Latency returns the negotiated TSBPD latency of what the session
// receives.
func (s *Session) Latency() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Latency()
}

// SetOnData sets the function that hears the payloads of a session the
// application took (ServerConfig.OnSession); it is called with the
// session's mutex held and must not call the session.
func (s *Session) SetOnData(fn func(payload []byte)) {
	s.mu.Lock()
	s.onData = fn
	s.mu.Unlock()
}

// Write sends one payload of at most MaxPayloadSize bytes, as a message of
// its own: MPEG-TS goes in TSPayloadSize chunks.
func (s *Session) Write(payload []byte) error {
	s.lock()
	defer s.unlock()
	return s.conn.Write(payload, time.Now())
}

// Close shuts the session down, telling the peer.
func (s *Session) Close() {
	s.lock()
	s.conn.Close(time.Now())
	s.unlock()
}

// Backlog returns the payload bytes the session holds that the peer has
// not acknowledged.
func (s *Session) Backlog() int64 { return s.backlog.Load() }

func (s *Session) lock() { s.mu.Lock() }

// unlock sends what the Conn emitted, rearms the timer, releases the mutex
// and runs what had to wait until it was released.
func (s *Session) unlock() {
	if s.closeSet && s.conn != nil {
		s.closeSet = false
		s.conn.Close(time.Now())
	}
	if len(s.dgrams) > 0 {
		if s.fc != nil {
			_, _ = s.fc.SendDatagrams(s.dgrams)
		}
		clear(s.dgrams)
		s.dgrams, s.arena = s.dgrams[:0], s.arena[:0]
	}
	if s.conn != nil {
		s.backlog.Store(int64(s.conn.SendBacklog()))
	}
	s.rearm()
	after := s.after
	s.after = nil
	s.mu.Unlock()
	for _, fn := range after {
		fn()
	}
}

// output collects a datagram of the Conn, to be sent when the mutex is
// released.
func (s *Session) output(b []byte) {
	start := len(s.arena)
	s.arena = append(s.arena, b...)
	s.dgrams = append(s.dgrams, s.arena[start:len(s.arena):len(s.arena)])
}

func (s *Session) rearm() {
	at, ok := time.Time{}, false
	if s.conn != nil && !s.closed {
		at, ok = s.conn.NextTimeout()
	}
	if !ok {
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
	s.lock()
	s.timerAt = time.Time{}
	if s.conn != nil {
		s.conn.HandleTimeout(time.Now())
	}
	s.unlock()
}

// abandon releases what a session that never got a Conn holds.
func (s *Session) abandon() {
	s.timer.Stop()
	if s.sub != nil {
		s.sub.Close()
		s.sub = nil
	}
	if s.pub != nil {
		s.pub.Close()
		s.pub = nil
	}
}

// handle reads a datagram that arrived on fc.
func (s *Session) handle(fc *fib.Connection, b []byte) {
	s.lock()
	s.fc = fc
	s.conn.Handle(b, time.Now())
	s.unlock()
}

// closeFromCallback closes the session once the Conn callback running
// returns.
func (s *Session) closeFromCallback() {
	s.closeSet = true
}

// connConfig fills in the callbacks of the session's Conn.
func (s *Session) connConfig(cfg Config) Config {
	cfg.Output = s.output
	cfg.OnData = s.data
	cfg.OnClose = s.closedConn
	return cfg
}

// data hears a payload from the Conn.
func (s *Session) data(b []byte) {
	switch {
	case s.demux != nil:
		s.demux.Write(b)
	case s.onData != nil:
		s.onData(b)
	}
}

// closedConn hears the Conn close: the hub roles end, and the owner and
// the application hear of it once the mutex is released.
func (s *Session) closedConn(err error) {
	if s.closed {
		return
	}
	if s.demux != nil && s.pub != nil {
		// The last frame of each stream waits in the demuxer for the
		// next PES to start, which will not come now.
		s.demux.Flush()
	}
	s.closed = true
	if s.pub != nil {
		s.pub.Close()
		s.pub = nil
	}
	if s.sub != nil {
		s.sub.Close()
		s.sub = nil
	}
	s.pending, s.ts = nil, nil
	s.timer.Stop()
	if fn := s.onClosed; fn != nil {
		s.after = append(s.after, func() { fn(s) })
	}
	if fn := s.onClose; fn != nil {
		s.after = append(s.after, func() { fn(err) })
	}
}

// startPublish makes the session demux what it receives into h at path,
// publishing once the tracks are known.
func (s *Session) startPublish(h *hub.Hub, path string, info hub.PublishInfo, cfg mpegts.DemuxerConfig) {
	s.hub, s.path, s.info = h, path, info
	cfg.OnTracks = s.pubTracks
	cfg.OnPacket = s.pubPacket
	s.demux = mpegts.NewDemuxer(cfg)
}

func (s *Session) pubTracks(tracks []*av.Track) {
	if s.pub != nil {
		s.pub.SetTracks(tracks)
		return
	}
	if s.closed {
		return
	}
	p, err := s.hub.Publish(s.path, tracks, s.info)
	if err != nil {
		s.closeFromCallback()
		return
	}
	s.pub = p
}

func (s *Session) pubPacket(pkt *av.Packet) {
	if s.pub != nil {
		s.pub.WritePacket(pkt)
	}
}

// playSink is the hub.Sink of a session that plays a stream.
type playSink struct{ s *Session }

// sink returns the session's sink, behind the slow-consumer policy.
func (s *Session) sink() hub.Sink {
	s.isPlayer = true
	return hub.NewDropper(playSink{s}, s.backlog.Load, s.maxBacklog)
}

func (p playSink) OnTracks(tracks []*av.Track) {
	s := p.s
	s.lock()
	defer s.unlock()
	s.tracks = tracks
	s.mux = nil
	s.pending = s.pending[:0]
	s.restart = !s.lastAt.IsZero()
	if s.ready {
		s.newMuxer()
	}
}

func (p playSink) OnPacket(pkt *av.Packet) {
	s := p.s
	s.lock()
	defer s.unlock()
	if s.closed {
		return
	}
	if !s.ready {
		if len(s.pending) >= maxPending {
			s.pending = s.pending[:0]
			s.waitKey = true
		}
		if s.waitKey && !(pkt.Keyframe && pkt.Track < len(s.tracks) && s.tracks[pkt.Track].Codec.IsVideo()) {
			return
		}
		s.waitKey = false
		s.pending = append(s.pending, pkt)
		return
	}
	s.sendPacket(pkt, time.Now())
}

func (p playSink) OnClose(error) {
	s := p.s
	s.lock()
	s.sub = nil
	if s.conn == nil {
		s.closeSet = true // closed as soon as it has a Conn
	} else {
		s.conn.Close(time.Now())
	}
	s.unlock()
}

// newMuxer starts a transport stream of the tracks; none of them carried
// leaves the muxer nil and the packets dropped.
func (s *Session) newMuxer() {
	s.mux, _ = mpegts.NewMuxer(s.tracks, s.muxCfg)
}

// startPlaying sends what the sink held while the handshake finished.
// Called with the mutex held, once the Conn is connected.
func (s *Session) startPlaying(now time.Time) {
	s.ready = true
	if s.tracks != nil {
		s.newMuxer()
	}
	pending := s.pending
	s.pending = nil
	for _, pkt := range pending {
		s.sendPacket(pkt, now)
	}
}

// sendPacket muxes pkt and sends the transport packets in TSPayloadSize
// payloads.
func (s *Session) sendPacket(pkt *av.Packet, now time.Time) {
	if s.mux == nil || !s.mux.Supported(pkt.Track) {
		return
	}
	if s.restart {
		s.restart = false
		if d := pkt.DTS + s.offset; d <= s.lastDTS || d > s.lastDTS+maxClockJump {
			at := s.lastDTS + max(min(now.Sub(s.lastAt), maxClockJump), minRestartGap)
			s.offset = at - pkt.DTS
		}
	}
	if s.offset != 0 {
		s.shifted = *pkt
		s.shifted.DTS += s.offset
		s.shifted.PTS += s.offset
		pkt = &s.shifted
	}
	if s.lastAt.IsZero() || pkt.DTS > s.lastDTS {
		s.lastDTS = pkt.DTS
	}
	s.lastAt = now
	var err error
	if s.ts, err = s.mux.WritePacket(s.ts[:0], pkt); err != nil {
		return
	}
	for b := s.ts; len(b) > 0; {
		n := min(len(b), TSPayloadSize)
		if s.conn.Write(b[:n], now) != nil {
			return
		}
		b = b[n:]
	}
}
