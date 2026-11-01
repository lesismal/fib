package fmp4

import (
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/opus"
)

// Writer turns the packets of a stream into a fragmented MP4: the init
// segment, once, then a fragment at every Flush. It owns no IO and no clock;
// the caller decides where fragments end, as HLS does at keyframes and part
// boundaries.
//
// A sample's duration is the distance to the next sample of its track, so
// the Writer holds the last packet of each track back until the next one
// arrives, or until a Flush that is told what comes next or that the stream
// ends. Durations are taken from the difference of the packets' DTS in the
// track's timescale, so they add up to the stream's clock without drifting.
// A step back, or one longer than ten seconds, is a jump in the publisher's
// clock rather than a sample that long: the sample before it gets the
// duration of the one before it instead (or one AAC frame, an Opus packet's
// own duration, a 30 fps frame), and the track's decode time carries on
// without the jump, so the timeline of a Writer never goes back.
//
// Decode times start at 0 from the earliest first packet of the tracks,
// so that they keep the stream's lining up of the tracks: a stream joined
// at a keyframe often has audio a little earlier than the keyframe come
// after it, and a track starting late at 0 would play that much late for
// good. Until the first Flush, a track whose first packet is earlier than
// the start so far moves it back, and the tracks already started forward
// by as much; after it, or when the step is more than ten seconds, such a
// track starts at 0, as no fragment can take back a decode time written.
//
// A Writer is not safe for concurrent use. It keeps the packets' data, which
// the hub shares and nobody modifies, until the Flush that writes them.
type Writer struct {
	tracks  []*wtrack // by stream track index; nil for tracks left out
	order   []int     // stream indexes of the tracks carried, by track_ID-1
	infos   []*trackInfo
	init    []byte
	seq     uint32
	epoch   time.Duration
	started bool
	// buffered counts the bytes of samples held or pending.
	buffered int
	frag     Fragment
}

type wtrack struct {
	id   uint32
	info *trackInfo
	// held is the last packet, whose duration is not known yet; dec is its
	// decode time, and the decode time of the sample after the pending
	// ones while there is none.
	held    *av.Packet
	dec     uint64
	started bool
	last    uint32
	pending []Sample
	start   uint64 // decode time of pending[0]
}

// NewWriter returns a Writer for a stream of tracks. Tracks fragmented MP4
// does not carry here (see Supported), nil ones and ones whose Config does
// not parse are left out: their packets are ignored, and Tracks says which
// are carried. It fails with ErrNoTracks when none is.
func NewWriter(tracks []*av.Track) (*Writer, error) {
	w := &Writer{tracks: make([]*wtrack, len(tracks))}
	for i, t := range tracks {
		ti, err := newTrackInfo(t)
		if err != nil {
			continue
		}
		w.order = append(w.order, i)
		w.infos = append(w.infos, ti)
		w.tracks[i] = &wtrack{id: uint32(len(w.order)), info: ti}
	}
	if len(w.order) == 0 {
		return nil, ErrNoTracks
	}
	w.init = appendInit(nil, w.infos)
	return w, nil
}

// Init returns the init segment, which the caller must not modify.
func (w *Writer) Init() []byte { return w.init }

// Tracks returns the indexes of the stream's tracks the Writer carries, in
// the order of their track_IDs, 1 on. The caller must not modify it.
func (w *Writer) Tracks() []int { return w.order }

// Timescale returns the timescale of the stream's track index, 0 for a
// track not carried.
func (w *Writer) Timescale(index int) uint32 {
	if index < 0 || index >= len(w.tracks) || w.tracks[index] == nil {
		return 0
	}
	return w.tracks[index].info.timescale
}

// Buffered returns the bytes of media written and not yet flushed, so a
// caller can bound them by flushing.
func (w *Writer) Buffered() int { return w.buffered }

// WritePacket adds pkt to the stream. Packets of each track must come in
// decode order; tracks interleave as they will. Packets of tracks not
// carried are ignored.
func (w *Writer) WritePacket(pkt *av.Packet) {
	t := w.track(pkt)
	if t == nil {
		return
	}
	if !w.started {
		w.started, w.epoch = true, pkt.DTS
	}
	if !t.started {
		d := pkt.DTS - w.epoch
		if d < 0 && w.seq == 0 && d >= -maxGap {
			w.rebase(-d)
			d = 0
		}
		t.started = true
		if d > 0 {
			t.dec = uint64(av.ToTicks(d, int(t.info.timescale)))
		}
	} else if t.held != nil {
		t.complete(w.ticks(t, pkt.DTS))
	}
	t.held = pkt
	w.buffered += len(pkt.Data)
}

// rebase moves the start of decode times by d earlier: the tracks started
// so far, none of them flushed, start d later.
func (w *Writer) rebase(d time.Duration) {
	w.epoch -= d
	for _, u := range w.tracks {
		if u == nil || !u.started {
			continue
		}
		n := uint64(av.ToTicks(d, int(u.info.timescale)))
		u.dec += n
		if len(u.pending) > 0 {
			u.start += n
		}
	}
}

func (w *Writer) track(pkt *av.Packet) *wtrack {
	if pkt == nil || pkt.Track < 0 || pkt.Track >= len(w.tracks) {
		return nil
	}
	return w.tracks[pkt.Track]
}

// ticks returns the duration of t's held sample were the next one to start
// at next, or 0 when that is no duration at all.
func (w *Writer) ticks(t *wtrack, next time.Duration) uint32 {
	d := next - t.held.DTS
	if d <= 0 || d > maxGap {
		return 0
	}
	ts := int(t.info.timescale)
	n := av.ToTicks(next, ts) - av.ToTicks(t.held.DTS, ts)
	if n <= 0 {
		return 0
	}
	return uint32(n)
}

// complete moves t's held sample to the pending ones with duration d, or the
// track's best guess when d is 0.
func (t *wtrack) complete(d uint32) {
	p := t.held
	t.held = nil
	if d == 0 {
		d = t.guess(p)
	}
	ts := int(t.info.timescale)
	cts := av.ToTicks(p.PTS, ts) - av.ToTicks(p.DTS, ts)
	if cts > 1<<31-1 || cts < -1<<31 {
		cts = 0
	}
	if len(t.pending) == 0 {
		t.start = t.dec
	}
	t.pending = append(t.pending, Sample{
		Duration:          d,
		CompositionOffset: int32(cts),
		Keyframe:          p.Keyframe || !t.info.video,
		Data:              p.Data,
	})
	t.dec += uint64(d)
	t.last = d
}

// guess is the duration of p when no next sample tells it.
func (t *wtrack) guess(p *av.Packet) uint32 {
	if t.info.track.Codec == av.CodecOpus {
		if d, err := opus.PacketDuration(p.Data); err == nil && d > 0 {
			return uint32(av.ToTicks(d, 48000))
		}
	}
	if t.last > 0 {
		return t.last
	}
	if t.info.frame > 0 {
		return t.info.frame
	}
	if t.info.video {
		return frameTicks
	}
	return t.info.timescale / 50 // 20 ms
}

// Flush appends to dst a fragment holding the samples whose durations are
// known, and reports whether its video starts with, or the fragment holds, a
// sync sample (always true for a stream without video): whether a player
// can start decoding with it. It appends nothing, and reports false, when
// there are none.
//
// next is the packet that will be written after the Flush, which completes
// the held sample of its track: a segmenter cutting before a keyframe passes
// the keyframe, so the video sample before it ends the fragment and the
// keyframe begins the next. The held samples of other tracks wait for the
// next fragment. A nil next ends the stream: every held sample is written,
// the last of each track with its best guess at a duration.
func (w *Writer) Flush(dst []byte, next *av.Packet) ([]byte, bool) {
	if next == nil {
		for _, t := range w.tracks {
			if t != nil && t.held != nil {
				t.complete(0)
			}
		}
	} else if t := w.track(next); t != nil && t.held != nil {
		t.complete(w.ticks(t, next.DTS))
	}
	f := &w.frag
	f.Tracks = f.Tracks[:0]
	sync, video := false, false
	for _, i := range w.order {
		t := w.tracks[i]
		if t.info.video {
			video = true
		}
		if len(t.pending) == 0 {
			continue
		}
		if t.info.video {
			for _, s := range t.pending {
				if s.Keyframe {
					sync = true
					break
				}
			}
		}
		f.Tracks = append(f.Tracks, TrackFragment{TrackID: t.id, BaseDecodeTime: t.start, Video: t.info.video, Samples: t.pending})
	}
	if len(f.Tracks) == 0 {
		return dst, false
	}
	w.seq++
	f.Sequence = w.seq
	out, err := f.Append(dst)
	written := 0
	for _, i := range w.order {
		t := w.tracks[i]
		for _, s := range t.pending {
			written += len(s.Data)
		}
		clear(t.pending)
		t.pending = t.pending[:0]
	}
	w.buffered -= written
	for i := range f.Tracks {
		f.Tracks[i] = TrackFragment{}
	}
	if err != nil {
		// Larger than 4 GiB: only a caller that never flushes gets here,
		// and its samples are gone either way.
		return dst, false
	}
	return out, sync || !video
}
