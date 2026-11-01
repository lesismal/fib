package hls

import (
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
// discontinuity the Muxer marks.
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
		w := &tsWriter{m: m, video: -1}
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
}

func (w *tsWriter) Tracks() []int { return w.tracks }

// Init is nil: every segment starts with its own PAT and PMT.
func (w *tsWriter) Init() []byte { return nil }

func (w *tsWriter) BeginSegment() { w.tables = true }

func (w *tsWriter) WritePacket(pkt *av.Packet) {
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
	w.buf, _ = w.m.WritePacket(w.buf, pkt)
}

// Flush hands over the transport packets written since the last Flush,
// and reports whether a player can start at them: always without video,
// else when the first video access unit among them is a keyframe. next
// only matters at the end of the stream (nil), where audio the muxer
// groups is flushed too.
func (w *tsWriter) Flush(dst []byte, next *av.Packet) ([]byte, bool) {
	if next == nil {
		w.buf = w.m.Flush(w.buf)
	}
	independent := w.video < 0 || w.seenVideo && w.firstKey
	dst = append(dst, w.buf...)
	w.buf = w.buf[:0]
	w.seenVideo, w.firstKey = false, false
	return dst, independent
}

func (w *tsWriter) Buffered() int { return len(w.buf) }
