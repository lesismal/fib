package rtc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/rtp"
)

// sendEpoch is the media time the first packet a sender sends is
// stamped with: later than zero, so that a packet of another track a
// little before it in decode order still has a positive time.
const sendEpoch = time.Second

// A republished stream whose times jump by more than these, back or
// forward, is rebased onto the times already sent, so that what a viewer
// receives stays continuous.
const (
	maxJumpBack    = time.Second
	maxJumpForward = 5 * time.Second
)

// outTrack is one track a sender sends: an RTP packetizer feeding a
// TrackLocal on the PeerConnection.
type outTrack struct {
	codec av.CodecID
	local *webrtc.TrackLocal
	rtp   *webrtc.RTPSender
	pk    *rtpcodec.Packetizer
	// hub is the index of the stream track it sends, -1 for none.
	hub int
}

// sender sends a stream of av.Packets over a PeerConnection: the
// egress of a WHEP session and of a WHIP client. It holds what comes
// before the PeerConnection is connected, the latest GOP of it, so that a
// viewer starts with a picture, then packetizes each packet and writes it
// to its track. Its methods are those of a hub.Sink, and are called one at
// a time; they never block.
//
// Writing to a TrackLocal may run the PeerConnection's callbacks on the
// spot, so none of those this package sets takes the sender's lock: they
// store atomics, or go to a goroutine of their own.
type sender struct {
	mu      sync.Mutex
	tracks  []*outTrack
	byHub   []int
	streams []*av.Track
	live    bool
	closed  bool
	// The packets held until the PeerConnection is connected: from the
	// latest video keyframe on, at most heldLimit bytes of them.
	held      []*av.Packet
	heldBytes int
	heldLimit int
	waitKey   bool
	hasVideo  bool

	// The times sent are the stream's plus offset; lastOut is the
	// latest sent, at wall time lastWall, and rebase says the stream's
	// tracks changed, after which a jump in its times is taken out.
	started  bool
	offset   time.Duration
	lastOut  time.Duration
	lastWall time.Time
	rebase   bool
	pkt      av.Packet

	// lost is the fraction of the packets the peer reported lost, out of
	// 256, in its latest receiver report: the backlog hub.NewDropper is
	// given, as a UDP sender has no queue to measure.
	lost atomic.Uint32
	// onClose ends the session: the stream is gone or cannot be sent.
	onClose func(err error)
	// tracked is closed by the first OnTracks; closeErr is what OnClose
	// was given.
	tracked  chan struct{}
	closeErr error
}

// newSender returns a sender with no tracks yet, which holds what it is
// given until setTracks and start.
func newSender(heldLimit int) *sender {
	return &sender{heldLimit: heldLimit, tracked: make(chan struct{})}
}

// setTracks gives the sender the tracks it sends.
func (s *sender) setTracks(tracks []*outTrack) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracks = tracks
	s.mapTracks(s.streams)
}

// Streams returns the stream's tracks, as the hub last gave them.
func (s *sender) Streams() []*av.Track {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams
}

// mapTracks gives each outTrack the stream track it sends: the one of the
// same index if it has the same codec, or else the first unclaimed one of
// that codec. A track of a codec the stream no longer has sends nothing.
func (s *sender) mapTracks(streams []*av.Track) {
	s.streams = streams
	s.hasVideo = false
	for _, st := range streams {
		if st != nil && st.Codec.IsVideo() {
			s.hasVideo = true
		}
	}
	s.byHub = make([]int, len(streams))
	for i := range s.byHub {
		s.byHub[i] = -1
	}
	claim := func(i, j int) {
		s.byHub[j] = i
		s.tracks[i].hub = j
		_ = s.tracks[i].pk.SetTrack(streams[j]) // same codec: cannot fail
	}
	for i, t := range s.tracks {
		t.hub = -1
		if i < len(streams) && streams[i] != nil && streams[i].Codec == t.codec {
			claim(i, i)
		}
	}
	for i, t := range s.tracks {
		if t.hub >= 0 {
			continue
		}
		for j, st := range streams {
			if st != nil && st.Codec == t.codec && s.byHub[j] < 0 {
				claim(i, j)
				break
			}
		}
	}
}

func (s *sender) OnTracks(tracks []*av.Track) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.tracked:
	default:
		close(s.tracked)
	}
	s.mapTracks(tracks)
	s.rebase = true
	s.held, s.heldBytes = nil, 0
}

func (s *sender) OnPacket(pkt *av.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || pkt.Track < 0 || pkt.Track >= len(s.byHub) || s.streams[pkt.Track] == nil {
		return
	}
	if !s.live {
		s.hold(pkt)
		return
	}
	if s.byHub[pkt.Track] >= 0 {
		s.send(pkt)
	}
}

func (s *sender) OnClose(err error) {
	s.mu.Lock()
	s.closed = true
	s.held = nil
	s.closeErr = err
	fn := s.onClose
	s.mu.Unlock()
	if fn != nil {
		fn(err)
	}
}

// err returns what OnClose was given, nil before it.
func (s *sender) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// hold keeps pkt until the PeerConnection is connected: from the latest
// video keyframe on, within heldLimit.
func (s *sender) hold(pkt *av.Packet) {
	video := s.streams[pkt.Track].Codec.IsVideo()
	if video && pkt.Keyframe {
		s.held, s.heldBytes, s.waitKey = s.held[:0], 0, false
	} else if video && s.waitKey {
		return
	}
	if s.heldBytes+len(pkt.Data) > s.heldLimit {
		s.held, s.heldBytes = s.held[:0], 0
		s.waitKey = s.hasVideo
		if video {
			return
		}
	}
	s.held = append(s.held, pkt)
	s.heldBytes += len(pkt.Data)
}

// start sends what was held and everything after it, once the
// PeerConnection is connected.
func (s *sender) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live || s.closed {
		return
	}
	s.live = true
	for _, pkt := range s.held {
		if pkt.Track < len(s.byHub) && s.byHub[pkt.Track] >= 0 {
			s.send(pkt)
		}
	}
	s.held, s.heldBytes = nil, 0
}

// close stops sending.
func (s *sender) close() {
	s.mu.Lock()
	s.closed = true
	s.held = nil
	s.mu.Unlock()
}

// send packetizes pkt onto its track, with its times made continuous.
func (s *sender) send(pkt *av.Packet) {
	t := s.tracks[s.byHub[pkt.Track]]
	now := time.Now()
	if !s.started {
		s.started = true
		s.offset = sendEpoch - pkt.DTS
		for _, o := range s.tracks {
			// One clock for every track's sender reports, as RTSP's
			// player has: the first packet's time is now, and the times
			// go on with the wall clock from there.
			o.pk.SetOrigin(0)
			o.local.SetReportClock(now, o.pk.Timestamp(sendEpoch))
		}
	} else if s.rebase {
		// The first packet after a change of tracks, as a republish
		// makes: a new publisher's times may start anywhere.
		at := pkt.DTS + s.offset
		if at < s.lastOut-maxJumpBack || at > s.lastOut+maxJumpForward {
			gap := min(max(now.Sub(s.lastWall), time.Millisecond), maxJumpForward)
			s.offset = s.lastOut + gap - pkt.DTS
		}
	}
	s.rebase = false
	s.pkt = *pkt
	s.pkt.DTS += s.offset
	s.pkt.PTS += s.offset
	if s.pkt.DTS > s.lastOut {
		s.lastOut = s.pkt.DTS
	}
	s.lastWall = now
	_ = t.pk.Packetize(&s.pkt, func(p *rtp.Packet) error {
		return t.local.WritePacket(p)
	})
	s.pkt.Data = nil
}

// backlog is the probe hub.NewDropper reads: the loss the peer reports.
func (s *sender) backlog() int64 { return int64(s.lost.Load()) }

// onRTCP takes the loss the peer reports for the sender's tracks from its
// receiver and sender reports.
func (s *sender) onRTCP(ssrcs func(uint32) bool, pkt []byte) {
	var rr rtp.ReceiverReport
	var sr rtp.SenderReport
	for rest := pkt; len(rest) > 0; {
		h, p, r, err := rtp.NextRTCP(rest)
		if err != nil {
			return
		}
		rest = r
		var reports []rtp.ReceptionReport
		switch h.Type {
		case rtp.TypeRR:
			if rr.Unmarshal(p) != nil {
				continue
			}
			reports = rr.Reports
		case rtp.TypeSR:
			if sr.Unmarshal(p) != nil {
				continue
			}
			reports = sr.Reports
		}
		for _, rep := range reports {
			if ssrcs(rep.SSRC) {
				s.lost.Store(uint32(rep.FractionLost))
			}
		}
	}
}
