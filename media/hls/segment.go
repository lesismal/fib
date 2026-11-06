package hls

import (
	"strconv"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/fmp4"
)

// SegmentWriter encodes a stream's packets in one segment format. The Muxer
// decides where segments and parts end and what they are called; a
// SegmentWriter only turns packets into bytes, so a format plugs in by
// implementing it and nothing else. One SegmentWriter serves one
// configuration of the stream's tracks, from one OnTracks to the next, and
// is used by one goroutine at a time.
//
// The calls come in this order: BeginSegment, then WritePacket for every
// packet of the segment, with a Flush wherever a part ends, then a last
// Flush whose next packet starts the next segment, and BeginSegment again.
// Flush(dst, nil) ends the stream.
//
// fMP4 (FMP4) and MPEG-TS (MPEGTS) are the implementations. MPEGTS shows
// what a format without an initialization section does: Tracks are the
// tracks it carries (H.264, H.265, AAC as ADTS, Opus with
// its registration descriptor), Init returns nil since every TS segment
// stands alone, BeginSegment makes the next Flush start with PAT and PMT,
// WritePacket packs a PES (parameter sets in band before keyframes) and
// keeps the TS packets, and Flush appends what it keeps and reports
// whether a player can start at it, ignoring next since TS has no
// durations to complete. Format.Version is then 3, Format.Extension ".ts" and
// Format.ContentType "video/mp2t".
type SegmentWriter interface {
	// Tracks returns the indexes of the stream's tracks the writer carries;
	// packets of the others are left out of the segments and of the
	// playlist's CODECS. The caller does not modify it.
	Tracks() []int
	// Init returns the initialization section every segment needs before
	// it, which the playlist names in EXT-X-MAP, or nil for a format whose
	// segments stand alone. The caller does not modify it.
	Init() []byte
	// BeginSegment says the packets written next begin a segment.
	BeginSegment()
	// WritePacket adds pkt, of a track the writer carries, to the segment.
	// Packets come in the order the publisher wrote them; a segment of a
	// stream with video starts with a keyframe, unless it outgrew its
	// bound first. The writer may keep pkt and its data, which nobody
	// modifies, until the Flush that writes it.
	WritePacket(pkt *av.Packet)
	// Flush appends to dst the media of the packets written since the last
	// Flush that the writer has finished, and reports whether it holds a
	// keyframe (always true without video): whether a player can start
	// decoding at it, which is a part's INDEPENDENT attribute. next is the
	// packet to be written after the Flush, not yet written, for a format
	// that has to see the next packet of a track to finish the one before
	// (fMP4, whose sample durations it is); nil ends the stream, and the
	// writer appends everything it holds. Appending nothing is fine.
	Flush(dst []byte, next *av.Packet) ([]byte, bool)
	// Buffered returns the bytes written and not yet flushed, which the
	// Muxer counts against its bounds.
	Buffered() int
}

// Format is a segment format: how to make its SegmentWriter and how the
// playlist and the HTTP handler present its segments.
type Format struct {
	// Name names the format, as "fmp4" or "ts": the value of the format
	// query parameter that picks it among HandlerConfig.Formats.
	Name string
	// Extension is the file extension of media segments and parts, as
	// ".m4s"; InitExtension that of the initialization section.
	Extension     string
	InitExtension string
	// ContentType is the media type of segments, parts and the
	// initialization section.
	ContentType string
	// Version is the EXT-X-VERSION of the media playlist the format needs:
	// 7 for fMP4 (EXT-X-MAP in a playlist without I-frames needs 6, and
	// 7 is what players expect of it), 3 for MPEG-TS.
	Version int
	// New returns the SegmentWriter of a stream with tracks, or an error
	// when it carries none of them.
	New func(tracks []*av.Track) (SegmentWriter, error)
}

// FMP4 is the fragmented MP4 format (RFC 8216 section 3.3, CMAF-style
// segments): one init section per track configuration, and each segment
// and part a moof and mdat of package fmp4.
var FMP4 = Format{
	Name:          "fmp4",
	Extension:     ".m4s",
	InitExtension: ".mp4",
	ContentType:   "video/mp4",
	Version:       7,
	New: func(tracks []*av.Track) (SegmentWriter, error) {
		w, err := fmp4.NewWriter(tracks)
		if err != nil {
			return nil, err
		}
		return fmp4Writer{w}, nil
	},
}

// fmp4Writer adapts fmp4.Writer, whose timeline runs across segments, to
// SegmentWriter.
type fmp4Writer struct{ *fmp4.Writer }

func (fmp4Writer) BeginSegment() {}

func (fmp4Writer) holdsLast() {}

// holder is a SegmentWriter that holds each track's last packet back until
// the next one of its track is written, as fMP4's does to know its
// duration: at a segment's cut, the first packet of each other track after
// the keyframe is written before the cut, so that the one before it, which
// decodes before the keyframe, ends the segment rather than begins the next.
type holder interface{ holdsLast() }

// CodecString returns the RFC 6381 codecs parameter of t for a playlist's
// CODECS attribute, or "" when it cannot be told.
func CodecString(t *av.Track) string {
	if t == nil {
		return ""
	}
	switch t.Codec {
	case av.CodecH264:
		var c h264.AVCConfig
		if c.Unmarshal(t.Config) == nil {
			return c.CodecString()
		}
	case av.CodecH265:
		var c h265.HEVCConfig
		if c.Unmarshal(t.Config) == nil {
			return c.CodecString()
		}
	case av.CodecAV1:
		var c av1.Config
		if c.Unmarshal(t.Config) == nil {
			return c.CodecString()
		}
	case av.CodecAAC:
		var c aac.Config
		if c.Unmarshal(t.Config) != nil {
			return "mp4a.40.2"
		}
		aot := c.ObjectType
		if c.ExtObjectType != 0 {
			aot = c.ExtObjectType
		}
		return "mp4a.40." + strconv.Itoa(int(aot))
	case av.CodecOpus:
		return "opus"
	case av.CodecMP3:
		return "mp4a.40.34"
	}
	return ""
}

// videoSize returns the picture size of a video track, from the track or
// its parameter sets.
func videoSize(t *av.Track) (int, int) {
	if t.Width > 0 && t.Height > 0 {
		return t.Width, t.Height
	}
	switch t.Codec {
	case av.CodecH264:
		var c h264.AVCConfig
		var sps h264.SPS
		if c.Unmarshal(t.Config) == nil && len(c.SPS) > 0 && sps.Unmarshal(c.SPS[0]) == nil {
			return sps.Width, sps.Height
		}
	case av.CodecH265:
		var c h265.HEVCConfig
		var sps h265.SPS
		if c.Unmarshal(t.Config) == nil {
			if n := c.NALUs(h265.NALSPS); len(n) > 0 && sps.Unmarshal(n[0]) == nil {
				return sps.Width, sps.Height
			}
		}
	case av.CodecAV1:
		var c av1.Config
		var s av1.SequenceHeader
		if c.Unmarshal(t.Config) == nil && s.Unmarshal(c.SequenceHeader()) == nil {
			return s.Width, s.Height
		}
	}
	return 0, 0
}
