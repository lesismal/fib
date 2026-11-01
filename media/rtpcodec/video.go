package rtpcodec

import (
	"bytes"
	"slices"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
)

// Parameter set kinds, the index of each in videoAssembler.sets.
const (
	psNone = -1
	psVPS  = 0
	psSPS  = 1
	psPPS  = 2
)

// nalCodec is what the access unit assembly needs to know of H.264 or
// H.265 NAL units.
type nalCodec interface {
	// kind tells a NAL unit apart: a parameter set (psVPS, psSPS, psPPS),
	// or psNone, with whether it is a keyframe slice, and whether it is
	// left out of the access unit (an access unit delimiter).
	kind(nal []byte) (ps int, key, skip bool)
	// config makes a track of parameter sets, or reports false.
	config(t *av.Track, sets *[3][][]byte) bool
	// reorder returns the reordering depth an SPS signals, -1 when it
	// signals none, and the frame duration it signals, 0 when none.
	reorder(sps []byte) (int, time.Duration)
}

// videoAssembler gathers the NAL units of one access unit, keeps the
// parameter sets out of it and up to date, and emits it with its decode
// time once it is complete.
type videoAssembler struct {
	d     *Depacketizer
	codec nalCodec

	au     []byte
	auTime time.Duration
	auTS   uint32
	open   bool
	key    bool
	broken bool
	// seen are the parameter sets the access unit carried, sets the ones
	// the track was made of.
	seen    [3][][]byte
	sets    [3][][]byte
	waitKey bool
	dts     dtsExtractor
}

func newVideoAssembler(d *Depacketizer, codec nalCodec) *videoAssembler {
	v := &videoAssembler{d: d, codec: codec, waitKey: true}
	v.dts.reorder, v.dts.frame = 4, 40*time.Millisecond
	// The track's config, from the SDP, gives the parameter sets until the
	// stream sends others.
	switch d.track.Codec {
	case av.CodecH264:
		var c h264.AVCConfig
		if c.Unmarshal(d.track.Config) == nil {
			v.sets[psSPS], v.sets[psPPS] = c.SPS, c.PPS
		}
	case av.CodecH265:
		var c h265.HEVCConfig
		if c.Unmarshal(d.track.Config) == nil {
			v.sets = [3][][]byte{c.NALUs(h265.NALVPS), c.NALUs(h265.NALSPS), c.NALUs(h265.NALPPS)}
		}
	}
	if len(v.sets[psSPS]) > 0 {
		v.setReorder(v.sets[psSPS][0])
	}
	return v
}

func (v *videoAssembler) setReorder(sps []byte) {
	n, frame := v.codec.reorder(sps)
	if n >= 0 {
		v.dts.reorder = n
	}
	if frame > 0 {
		v.dts.frame = frame
	}
}

// begin starts or continues the access unit of RTP timestamp ts, ending
// the one before when its marker was lost.
func (v *videoAssembler) begin(ts uint32, t time.Duration) {
	if v.open && ts != v.auTS {
		v.flush()
	}
	if !v.open {
		v.open, v.auTS, v.auTime = true, ts, t
	}
}

// add appends one NAL unit to the access unit.
func (v *videoAssembler) add(nal []byte) {
	if len(nal) == 0 || v.broken {
		return
	}
	ps, key, skip := v.codec.kind(nal)
	switch {
	case skip:
		return
	case ps != psNone:
		if len(v.seen[ps]) < 16 {
			v.seen[ps] = append(v.seen[ps], bytes.Clone(nal))
		}
		return
	}
	if len(v.au)+4+len(nal) > v.d.maxAU {
		v.broken = true
		return
	}
	v.key = v.key || key
	v.au = h264.AppendAVCC(v.au, nal)
}

// lose drops the access unit being assembled, and has video wait for the
// next keyframe.
func (v *videoAssembler) lose() {
	if v.open {
		v.broken = true
	}
	v.waitKey = true
}

// flush ends the access unit and emits it when it is whole and usable.
func (v *videoAssembler) flush() {
	if !v.open {
		return
	}
	au, key, broken, t := v.au, v.key, v.broken, v.auTime
	v.au, v.key, v.broken, v.open = nil, false, false, false
	seen := v.seen
	v.seen = [3][][]byte{}
	if broken {
		v.waitKey = true
		return
	}
	v.updateSets(&seen)
	if len(au) == 0 || v.d.track.Config == nil {
		return
	}
	if v.waitKey {
		if !key {
			return
		}
		v.waitKey = false
	}
	dts := v.dts.next(t)
	v.d.emit(&av.Packet{DTS: dts, PTS: t, Keyframe: key, Data: au})
}

// updateSets takes the parameter sets an access unit carried: when they
// differ from the track's, and make a config, the track is replaced.
func (v *videoAssembler) updateSets(seen *[3][][]byte) {
	changed := false
	for i := range seen {
		if len(seen[i]) > 0 && !slices.EqualFunc(seen[i], v.sets[i], bytes.Equal) {
			changed = true
		}
	}
	if !changed {
		return
	}
	sets := v.sets
	for i := range seen {
		if len(seen[i]) > 0 {
			sets[i] = seen[i]
		}
	}
	t := *v.d.track
	if !v.codec.config(&t, &sets) {
		return
	}
	v.sets = sets
	v.d.track = &t
	v.setReorder(sets[psSPS][0])
}

// dtsExtractor recovers decode times from presentation times. With a
// reordering depth of n, a picture is decoded no later than it is shown,
// and no picture shown after it is decoded more than n pictures before it,
// so the decode time of the i-th picture in decode order is the (n+1)-th
// largest presentation time among the first i+1 pictures. That needs only
// the n+1 largest presentation times seen. The first n pictures, before
// there are n+1, are given decode times a frame apart before the first's
// presentation time.
type dtsExtractor struct {
	reorder int
	frame   time.Duration
	top     []time.Duration
	count   int
	first   time.Duration
	last    time.Duration
}

func (e *dtsExtractor) next(pts time.Duration) time.Duration {
	n := min(e.reorder, 16)
	if e.count == 0 {
		e.first = pts
	}
	// Keep the n+1 largest, ascending.
	i, _ := slices.BinarySearch(e.top, pts)
	e.top = slices.Insert(e.top, i, pts)
	if len(e.top) > n+1 {
		e.top = slices.Delete(e.top, 0, len(e.top)-(n+1))
	}
	var dts time.Duration
	if e.count >= n {
		dts = e.top[0]
	} else {
		dts = e.first - time.Duration(n-e.count)*e.frame
	}
	e.count++
	dts = min(max(dts, e.last, 0), pts)
	e.last = max(dts, e.last)
	return dts
}

// h264Codec is nalCodec for H.264.
type h264Codec struct{}

func (h264Codec) kind(nal []byte) (int, bool, bool) {
	switch h264.Type(nal) {
	case h264.NALSPS:
		return psSPS, false, false
	case h264.NALPPS:
		return psPPS, false, false
	case h264.NALIDR:
		return psNone, true, false
	case h264.NALAUD, h264.NALFiller:
		return psNone, false, true
	}
	return psNone, false, false
}

func (h264Codec) config(t *av.Track, sets *[3][][]byte) bool {
	return setH264Config(t, sets[psSPS], sets[psPPS])
}

func (h264Codec) reorder(sps []byte) (int, time.Duration) {
	var s h264.SPS
	if s.Unmarshal(sps) != nil {
		return -1, 0
	}
	var frame time.Duration
	if fps := s.FrameRate(); fps > 0 && fps < 1000 {
		frame = time.Duration(float64(time.Second) / fps)
	}
	n := s.MaxNumReorderFrames
	if n < 0 && (s.ProfileIDC == 66 || s.ProfileIDC == 0) {
		// Baseline has no B-frames.
		n = 0
	}
	return n, frame
}

// h265Codec is nalCodec for H.265.
type h265Codec struct{}

func (h265Codec) kind(nal []byte) (int, bool, bool) {
	t := h265.Type(nal)
	switch t {
	case h265.NALVPS:
		return psVPS, false, false
	case h265.NALSPS:
		return psSPS, false, false
	case h265.NALPPS:
		return psPPS, false, false
	case h265.NALAUD, h265.NALFD:
		return psNone, false, true
	}
	return psNone, t.IsIRAP(), false
}

func (h265Codec) config(t *av.Track, sets *[3][][]byte) bool {
	return setH265Config(t, sets[psVPS], sets[psSPS], sets[psPPS])
}

func (h265Codec) reorder(sps []byte) (int, time.Duration) {
	var s h265.SPS
	if s.Unmarshal(sps) != nil {
		return -1, 0
	}
	return s.MaxNumReorderPics, 0
}
