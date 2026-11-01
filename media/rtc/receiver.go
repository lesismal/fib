package rtc

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// recvEpoch is the media time a received stream starts at: later than
// zero, so that the decode times a depacketizer recovers ahead of the
// first frame's presentation time are not cut off.
const recvEpoch = time.Second

// inStream is one media section a receiver depacketizes.
type inStream struct {
	index int
	video bool
	tr    *webrtc.TrackRemote
	// codecs are the section's codecs the package can receive, and pt
	// the payload type dp reads.
	codecs    []webrtc.Codec
	pt        uint8
	dp        *rtpcodec.Depacketizer
	reorder   *rtpcodec.Reorder
	originSet bool
	pkt       rtp.Packet
	// lost is the depacketizer's loss count last seen; needKey says the
	// stream lost something or has not had a keyframe yet, and lastPLI is
	// when a keyframe was last asked for.
	lost    uint64
	needKey bool
	lastPLI time.Time
}

// recvConfig tunes a receiver.
type recvConfig struct {
	reorderSize      int
	reorderDelay     time.Duration
	keyFrameInterval time.Duration
	pliInterval      time.Duration
}

// receiver turns the RTP a PeerConnection receives into av.Packets: the
// ingest of a WHIP session and of a WHEP client. Each media section is a
// track, depacketized from the TrackRemote that arrives on it, through a
// reorder buffer, and timed from when the receiver started.
//
// Its callbacks, onTracks and onPacket, are called under its lock, one at
// a time. A PeerConnection's methods are never called with the lock held,
// since a call into one may deliver its RTP on the spot.
type receiver struct {
	cfg recvConfig

	mu      sync.Mutex
	start   time.Time
	tracks  []*av.Track
	streams []*inStream
	byMid   map[string]*inStream
	closed  bool
	// timer is armed for timerAt, under the lock, or to fire at once
	// by requestKeyFrame, without it.
	timer    *time.Timer
	timerAt  time.Time
	periodic time.Time
	onTracks func(tracks []*av.Track)
	onPacket func(pkt *av.Packet)
	// kick asks for a keyframe from outside, as a viewer's PLI does.
	kick atomic.Bool
}

// newReceiver returns a receiver of the media sections of desc, the
// description that says what the peer sends: its tracks are those of the
// sections' first codecs.
func newReceiver(cfg recvConfig, desc *sdp.SessionDescription, remoteSends bool) *receiver {
	r := &receiver{cfg: cfg, start: time.Now(), byMid: make(map[string]*inStream)}
	r.timer = time.AfterFunc(time.Hour, r.onTimer)
	r.timer.Stop()
	for _, md := range desc.Media {
		kind := md.Kind()
		if (kind != "audio" && kind != "video") || md.Rejected() {
			continue
		}
		dir := desc.MediaDirection(md)
		if remoteSends && dir != sdp.SendOnly && dir != sdp.SendRecv ||
			!remoteSends && dir != sdp.RecvOnly && dir != sdp.SendRecv {
			continue
		}
		mid, _ := md.Mid()
		var codecs []webrtc.Codec
		for _, sc := range md.Codecs() {
			if c := fromSDP(kind, sc); sc.Name != "" && receivable(c) {
				codecs = append(codecs, c)
			}
		}
		if len(codecs) == 0 {
			continue
		}
		f, _ := formatOf(codecs[0]) // receivable: cannot fail
		st := &inStream{index: len(r.streams), video: kind == "video", needKey: kind == "video", codecs: codecs}
		r.streams = append(r.streams, st)
		r.tracks = append(r.tracks, f.Track)
		r.byMid[mid] = st
	}
	return r
}

// Tracks returns the tracks received.
func (r *receiver) Tracks() []*av.Track {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.tracks)
}

// onTrack takes a stream the peer started sending: the first on each
// media section is depacketized, further ones (other simulcast layers)
// are not.
func (r *receiver) onTrack(tr *webrtc.TrackRemote) {
	r.mu.Lock()
	st := r.byMid[tr.Transceiver().Mid()]
	if r.closed || st == nil || st.tr != nil || !r.use(st, tr.Codec().PayloadType) {
		r.mu.Unlock()
		return
	}
	st.tr = tr
	st.reorder = rtpcodec.NewReorder(r.cfg.reorderSize, r.cfg.reorderDelay)
	r.mu.Unlock()
	tr.OnRTP(func(b []byte) { r.receive(st, b) })
	if st.video {
		r.requestKeyFrames()
	}
}

// use has st depacketize payload type pt, when it is one of the codecs of
// its section, and reports whether it is; under the lock. A codec other
// than the track's makes a new track.
func (r *receiver) use(st *inStream, pt uint8) bool {
	for _, c := range st.codecs {
		if c.PayloadType != pt {
			continue
		}
		f, _ := formatOf(c)                                                 // receivable: cannot fail
		dp, _ := rtpcodec.NewDepacketizer(f, rtpcodec.DepacketizerConfig{}) // nor can this
		st.dp, st.pt, st.originSet, st.lost, st.needKey = dp, pt, false, 0, st.video
		if r.tracks[st.index].Codec != f.Track.Codec {
			r.tracks[st.index] = f.Track
			r.emitTracks()
		}
		return true
	}
	return false
}

// emitTracks hands the tracks to onTracks; under the lock.
func (r *receiver) emitTracks() {
	if r.onTracks != nil {
		r.onTracks(slices.Clone(r.tracks))
	}
}

// receive takes an RTP packet of st.
func (r *receiver) receive(st *inStream, b []byte) {
	now := time.Now()
	r.mu.Lock()
	if r.closed || st.pkt.Unmarshal(b) != nil {
		r.mu.Unlock()
		return
	}
	if st.pkt.PayloadType != st.pt {
		// The sender switched to another payload type the answer has.
		r.use(st, st.pkt.PayloadType)
	}
	st.reorder.Push(now, &st.pkt, func(p *rtp.Packet) { r.depacketize(st, now, p) })
	pli := r.dueLocked(now)
	r.scheduleLocked(now)
	r.mu.Unlock()
	sendPLIs(pli)
}

func (r *receiver) depacketize(st *inStream, now time.Time, p *rtp.Packet) {
	if p.PayloadType != st.pt {
		return
	}
	if !st.originSet {
		st.originSet = true
		st.dp.SetOrigin(p.Timestamp, recvEpoch+now.Sub(r.start))
	}
	_ = st.dp.Depacketize(p, func(pkt *av.Packet) { r.emit(st, pkt) })
	if lost := st.dp.Lost(); lost != st.lost {
		st.lost = lost
		st.needKey = st.video
	}
}

func (r *receiver) emit(st *inStream, pkt *av.Packet) {
	if t := st.dp.Track(); t != r.tracks[st.index] {
		r.tracks[st.index] = t
		r.emitTracks()
	}
	if st.video && pkt.Keyframe {
		st.needKey = false
	}
	pkt.Track = st.index
	if r.onPacket != nil {
		r.onPacket(pkt)
	}
}

// requestKeyFrame asks the sender for a keyframe of every video stream,
// at the next turn of the timer, from any goroutine, without the lock: a
// viewer's keyframe request may come from inside a packet this receiver
// is publishing.
func (r *receiver) requestKeyFrame() {
	r.kick.Store(true)
	r.timer.Reset(0)
}

// requestKeyFrames asks for a keyframe of the video streams now, within
// the PLI interval.
func (r *receiver) requestKeyFrames() {
	r.kick.Store(true)
	now := time.Now()
	r.mu.Lock()
	pli := r.dueLocked(now)
	r.scheduleLocked(now)
	r.mu.Unlock()
	sendPLIs(pli)
}

// dueLocked returns the streams a keyframe is to be asked of now: those
// that need one, all of them when kicked or periodically, at most once a
// PLI interval each.
func (r *receiver) dueLocked(now time.Time) []*webrtc.TrackRemote {
	kick := r.kick.Swap(false)
	if r.cfg.keyFrameInterval > 0 && !now.Before(r.periodic) {
		kick = true
		r.periodic = now.Add(r.cfg.keyFrameInterval)
	}
	var out []*webrtc.TrackRemote
	for _, st := range r.streams {
		if !st.video || st.tr == nil || !(st.needKey || kick) {
			continue
		}
		if now.Sub(st.lastPLI) < r.cfg.pliInterval {
			if kick {
				// Not yet: the timer asks once the interval is up.
				st.needKey = true
			}
			continue
		}
		st.lastPLI = now
		out = append(out, st.tr)
	}
	return out
}

func sendPLIs(trs []*webrtc.TrackRemote) {
	for _, tr := range trs {
		_ = tr.RequestKeyFrame()
	}
}

// scheduleLocked arms the timer for the earliest of the reorder buffers'
// deadlines, the next PLI a stream waits for, and the periodic one.
func (r *receiver) scheduleLocked(now time.Time) {
	var at time.Time
	take := func(t time.Time) {
		if at.IsZero() || t.Before(at) {
			at = t
		}
	}
	for _, st := range r.streams {
		if st.reorder != nil {
			if t, ok := st.reorder.NextTimeout(); ok {
				take(t)
			}
		}
		if st.video && st.tr != nil && st.needKey {
			take(st.lastPLI.Add(r.cfg.pliInterval))
		}
	}
	if r.cfg.keyFrameInterval > 0 && !r.periodic.IsZero() {
		take(r.periodic)
	}
	if at.IsZero() || (!r.timerAt.IsZero() && !r.timerAt.After(at) && r.timerAt.After(now)) {
		return
	}
	r.timerAt = at
	r.timer.Reset(max(at.Sub(now), 0))
}

func (r *receiver) onTimer() {
	now := time.Now()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.timerAt = time.Time{}
	for _, st := range r.streams {
		if st.reorder != nil {
			st.reorder.Expire(now, func(p *rtp.Packet) { r.depacketize(st, now, p) })
		}
	}
	pli := r.dueLocked(now)
	r.scheduleLocked(now)
	r.mu.Unlock()
	sendPLIs(pli)
}

// startPeriodic starts the periodic keyframe requests, if configured.
func (r *receiver) startPeriodic() {
	if r.cfg.keyFrameInterval <= 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	r.periodic = now.Add(r.cfg.keyFrameInterval)
	if !r.closed {
		r.scheduleLocked(now)
	}
	r.mu.Unlock()
}

// close stops the receiver, flushing what the depacketizers hold.
func (r *receiver) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	for _, st := range r.streams {
		if st.dp != nil {
			st.dp.Flush(func(pkt *av.Packet) { r.emit(st, pkt) })
		}
	}
	r.closed = true
	r.timer.Stop()
}
