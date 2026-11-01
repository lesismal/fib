package hls

import (
	"slices"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/mpegts"
)

// MPEGTS is the MPEG-2 transport stream format (RFC 8216 section 3.2):
// segments that stand alone, each starting with a PAT and a PMT, in a
// version 3 playlist without EXT-X-MAP, which is what players before fMP4
// (and many set-top boxes and older smart TVs) take. It carries H.264,
// H.265, AAC (as ADTS) and Opus (with its registration descriptor) through
// package mpegts, in ffmpeg's layout; other tracks are left out. Parts of
// Low-Latency HLS work as with FMP4, each a run of whole transport
// packets, INDEPENDENT when its first video access unit is a keyframe.
//
// The transport stream runs on across segments: one mpegts.Muxer serves
// a configuration of the tracks, so continuity counters and timestamps
// continue from one segment to the next, as a player reading them in order
// expects, and a new configuration (OnTracks) starts a new one after the
// discontinuity the Muxer marks. Its timestamps carry on from where the
// last configuration's ended, as fMP4's decode times do, rather than
// following the new publisher's clock back: RFC 8216 lets them jump at a
// discontinuity, but ffmpeg's correction of the jump left a decode time
// that repeated one before it.
var MPEGTS = Format{
	Name:        "ts",
	Extension:   ".ts",
	ContentType: "video/mp2t",
	Version:     3,
	New: func(tracks []*av.Track) (SegmentWriter, error) {
		m, err := mpegts.NewMuxer(tracks, mpegts.MuxerConfig{})
		if err != nil {
			return nil, err
		}
		w := &tsWriter{m: m, video: -1, last: make([]tsTrackEnd, len(tracks))}
		for i, t := range tracks {
			if m.Supported(i) {
				w.tracks = append(w.tracks, i)
				if t.Codec.IsVideo() && w.video < 0 {
					w.video = i
				}
			}
		}
		return w, nil
	},
}

// Bounds on what a tsWriter carrying on another's timeline holds before
// it knows where the new clock starts: every track's first packet, or this
// many packets, or this much time past the first.
const (
	tsMaxHeld     = 256
	tsMaxHeldTime = time.Second
)

// tsMinStep is the least a track's timeline is taken to run past its last
// packet, when no packet after it gave its duration.
const tsMinStep = time.Millisecond

// tsWriter is the SegmentWriter of MPEGTS. It muxes each packet as it
// comes and keeps the transport packets until the Flush that hands them
// over: TS has no sample durations, so nothing waits for the next packet.
type tsWriter struct {
	m      *mpegts.Muxer
	tracks []int
	video  int // the first video track carried, -1 for none
	buf    []byte
	tables bool // the next packet starts a segment
	// seenVideo says a video access unit was written since the last
	// Flush, firstKey whether the first one was a keyframe.
	seenVideo, firstKey bool

	// The timeline (StartAt, End): shift is added to every packet's
	// times, into shifted. A writer told to start at startAt holds its
	// first packets in held until it knows the earliest of the tracks',
	// so that none lands before startAt; last is where each track ends.
	started bool
	holding bool
	startAt time.Duration
	shift   time.Duration
	held    []*av.Packet
	heldN   int
	shifted av.Packet
	last    []tsTrackEnd
}

// tsTrackEnd is the last decode time written of a track, and the step
// between its last two.
type tsTrackEnd struct {
	dts, step time.Duration
	seen      bool
}

func (w *tsWriter) Tracks() []int { return w.tracks }

// Init is nil: every segment starts with its own PAT and PMT.
func (w *tsWriter) Init() []byte { return nil }

func (w *tsWriter) BeginSegment() { w.tables = true }

// StartAt makes the times of the packets written start at t, the End of
// the writer before a discontinuity, rather than at the publisher's own.
// It has no effect once a packet has been written.
func (w *tsWriter) StartAt(t time.Duration) {
	if !w.started && t > 0 {
		w.startAt, w.holding = t, true
	}
}

// End returns where the packets written end: the latest of the tracks'
// last decode times, each run on by the step to it, so a writer that
// starts there repeats none of them.
func (w *tsWriter) End() time.Duration {
	var end time.Duration
	for _, l := range w.last {
		if l.seen {
			end = max(end, l.dts+max(l.step, tsMinStep))
		}
	}
	return end
}

func (w *tsWriter) WritePacket(pkt *av.Packet) {
	w.started = true
	if !w.holding {
		w.write(pkt)
		return
	}
	w.held = append(w.held, pkt)
	w.heldN += len(pkt.Data)
	all := true
	for _, i := range w.tracks {
		if !slices.ContainsFunc(w.held, func(p *av.Packet) bool { return p.Track == i }) {
			all = false
			break
		}
	}
	if all || len(w.held) >= tsMaxHeld || pkt.DTS-w.held[0].DTS >= tsMaxHeldTime {
		w.release()
	}
}

// release starts the timeline at startAt from the earliest packet held,
// and writes what was held.
func (w *tsWriter) release() {
	w.holding = false
	if len(w.held) == 0 {
		return
	}
	first := w.held[0].DTS
	for _, p := range w.held[1:] {
		first = min(first, p.DTS)
	}
	w.shift = w.startAt - first
	for i, p := range w.held {
		w.write(p)
		w.held[i] = nil
	}
	w.held, w.heldN = w.held[:0], 0
}

// write muxes a packet, at its place on the timeline.
func (w *tsWriter) write(pkt *av.Packet) {
	if w.shift != 0 {
		w.shifted = *pkt
		w.shifted.DTS += w.shift
		w.shifted.PTS += w.shift
		pkt = &w.shifted
	}
	if w.tables {
		// A segment starts with the program's tables, which the muxer
		// then does not repeat before the keyframe that follows.
		w.buf = w.m.WriteTables(w.buf)
		w.tables = false
	}
	if pkt.Track == w.video && !w.seenVideo {
		w.seenVideo, w.firstKey = true, pkt.Keyframe
	}
	// A packet the muxer refuses (video that is not AVCC) is left out;
	// what it wrote of it, nothing, stays as it was.
	var err error
	if w.buf, err = w.m.WritePacket(w.buf, pkt); err == nil && pkt.Track >= 0 && pkt.Track < len(w.last) {
		l := &w.last[pkt.Track]
		if l.seen && pkt.DTS > l.dts {
			l.step = pkt.DTS - l.dts
		}
		if !l.seen || pkt.DTS > l.dts {
			l.dts = pkt.DTS
		}
		l.seen = true
	}
}

// Flush hands over the transport packets written since the last Flush,
// and reports whether a player can start at them: always without video,
// else when the first video access unit among them is a keyframe. next
// only matters at the end of the stream (nil), where audio the muxer
// groups is flushed too. Packets held for the timeline are written first.
func (w *tsWriter) Flush(dst []byte, next *av.Packet) ([]byte, bool) {
	if w.holding {
		w.release()
	}
	if next == nil {
		w.buf = w.m.Flush(w.buf)
	}
	independent := w.video < 0 || w.seenVideo && w.firstKey
	dst = append(dst, w.buf...)
	w.buf = w.buf[:0]
	w.seenVideo, w.firstKey = false, false
	return dst, independent
}

func (w *tsWriter) Buffered() int { return len(w.buf) + w.heldN }
