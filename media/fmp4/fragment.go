package fmp4

import (
	"encoding/binary"
	"errors"
)

// ErrFragmentSize is the error for a fragment whose media would not fit the
// 32-bit sizes and offsets of a moof and its mdat.
var ErrFragmentSize = errors.New("fmp4: fragment too large")

// Sample flags (ISO/IEC 14496-12 section 8.8.3.1) of the trun: a sync
// sample depends on no other; a non-sync one depends on others and is not a
// random access point.
const (
	SyncSampleFlags    uint32 = 0x02000000
	NonSyncSampleFlags uint32 = 0x01010000
)

// Sample is one sample of a TrackFragment.
type Sample struct {
	// Duration is in the track's timescale.
	Duration uint32
	// CompositionOffset is PTS - DTS in the track's timescale, signed as a
	// version 1 trun has it.
	CompositionOffset int32
	// Keyframe marks a sync sample.
	Keyframe bool
	Data     []byte
}

// TrackFragment is the run of samples of one track in a Fragment: a traf
// with one trun.
type TrackFragment struct {
	TrackID uint32
	// BaseDecodeTime is the decode time of the first sample, in the
	// track's timescale (tfdt).
	BaseDecodeTime uint64
	// Video says the samples carry flags and composition offsets of their
	// own; audio samples are all sync samples, which tfhd says once.
	Video   bool
	Samples []Sample
}

// Fragment is a movie fragment: a moof followed by the mdat holding its
// samples, track after track.
type Fragment struct {
	// Sequence is the mfhd sequence_number, which increases by one from
	// fragment to fragment, starting at 1.
	Sequence uint32
	Tracks   []TrackFragment
}

// tfhd and trun flags.
const (
	tfhdDefaultFlags  = 0x000020
	tfhdBaseIsMoof    = 0x020000
	trunDataOffset    = 0x000001
	trunDuration      = 0x000100
	trunSize          = 0x000200
	trunFlags         = 0x000400
	trunCompositionTO = 0x000800
)

// Append appends the fragment, moof and mdat, to dst. Track fragments
// without samples are left out. Every traf has default-base-is-moof set, so
// the data offsets are from the start of the moof and the fragment can be
// moved anywhere, as HLS segments and parts are.
func (f *Fragment) Append(dst []byte) ([]byte, error) {
	start := len(dst)
	media := 0
	for i := range f.Tracks {
		for j := range f.Tracks[i].Samples {
			media += len(f.Tracks[i].Samples[j].Data)
		}
	}
	b, moof := begin(dst, "moof")
	b, at := beginFull(b, "mfhd", 0, 0)
	b = u32(b, f.Sequence)
	b = end(b, at)
	var offsets []int
	for i := range f.Tracks {
		tf := &f.Tracks[i]
		if len(tf.Samples) == 0 {
			continue
		}
		var traf int
		b, traf = begin(b, "traf")
		flags := uint32(tfhdBaseIsMoof)
		if !tf.Video {
			flags |= tfhdDefaultFlags
		}
		b, at = beginFull(b, "tfhd", 0, flags)
		b = u32(b, tf.TrackID)
		if !tf.Video {
			b = u32(b, SyncSampleFlags)
		}
		b = end(b, at)
		b, at = beginFull(b, "tfdt", 1, 0)
		b = u64(b, tf.BaseDecodeTime)
		b = end(b, at)
		flags = trunDataOffset | trunDuration | trunSize
		if tf.Video {
			flags |= trunFlags | trunCompositionTO
		}
		b, at = beginFull(b, "trun", 1, flags)
		b = u32(b, uint32(len(tf.Samples)))
		offsets = append(offsets, len(b))
		b = u32(b, 0) // data_offset, filled in below
		for j := range tf.Samples {
			s := &tf.Samples[j]
			b = u32(b, s.Duration)
			b = u32(b, uint32(len(s.Data)))
			if tf.Video {
				if s.Keyframe {
					b = u32(b, SyncSampleFlags)
				} else {
					b = u32(b, NonSyncSampleFlags)
				}
				b = u32(b, uint32(s.CompositionOffset))
			}
		}
		b = end(b, at)
		b = end(b, traf)
	}
	b = end(b, moof)
	if uint64(len(b)-start)+8+uint64(media) > 0xffffffff {
		return dst[:start], ErrFragmentSize
	}
	// The samples of each traf start where those of the one before end.
	pos := len(b) - start + 8
	k := 0
	for i := range f.Tracks {
		tf := &f.Tracks[i]
		if len(tf.Samples) == 0 {
			continue
		}
		binary.BigEndian.PutUint32(b[offsets[k]:], uint32(pos))
		k++
		for j := range tf.Samples {
			pos += len(tf.Samples[j].Data)
		}
	}
	b = u32(b, uint32(8+media))
	b = append(b, "mdat"...)
	for i := range f.Tracks {
		for j := range f.Tracks[i].Samples {
			b = append(b, f.Tracks[i].Samples[j].Data...)
		}
	}
	return b, nil
}

// AppendStyp appends a segment type box (section 8.16.2) naming the msdh
// and msix brands, which a media segment of DASH or CMAF may start with.
// HLS needs none.
func AppendStyp(dst []byte) []byte {
	b, at := begin(dst, "styp")
	b = append(b, "msdh"...)
	b = u32(b, 0)
	b = append(b, "msdh"...)
	b = append(b, "msix"...)
	return end(b, at)
}

// SidxReference is one subsegment of a segment index.
type SidxReference struct {
	// Size is the subsegment's size in bytes: its moof and mdat.
	Size uint32
	// Duration is the subsegment's duration in the index's timescale.
	Duration uint32
	// StartsWithSAP says the subsegment starts with a sync sample.
	StartsWithSAP bool
}

// Sidx is a segment index box (section 8.16.3), which DASH on-demand
// profiles want before the fragments it indexes.
type Sidx struct {
	ReferenceID uint32
	Timescale   uint32
	// EarliestPresentationTime is of the first subsegment, in Timescale.
	EarliestPresentationTime uint64
	// FirstOffset is the distance from the end of the sidx to the first
	// subsegment.
	FirstOffset uint64
	References  []SidxReference
}

// Append appends the sidx, version 1, to dst.
func (s *Sidx) Append(dst []byte) []byte {
	b, at := beginFull(dst, "sidx", 1, 0)
	b = u32(b, s.ReferenceID)
	b = u32(b, s.Timescale)
	b = u64(b, s.EarliestPresentationTime)
	b = u64(b, s.FirstOffset)
	b = u16(b, 0)
	b = u16(b, uint16(len(s.References)))
	for _, r := range s.References {
		b = u32(b, r.Size&0x7fffffff) // reference_type 0: media
		b = u32(b, r.Duration)
		if r.StartsWithSAP {
			b = u32(b, 0x90000000) // SAP type 1
		} else {
			b = u32(b, 0)
		}
	}
	return end(b, at)
}
