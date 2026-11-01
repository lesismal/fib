package av

import (
	"strconv"
	"time"
)

// CodecID names the codec of a Track.
type CodecID uint8

// The codecs the media stack carries. None is transcoded: a track an egress
// cannot carry is left out of what it sends.
const (
	CodecUnknown CodecID = iota
	CodecH264
	CodecH265
	// CodecAV1 is carried where the container allows it; nothing in the
	// media stack parses it.
	CodecAV1
	CodecAAC
	CodecOpus
	CodecPCMU
	CodecPCMA
	// CodecMP3 is passed through where the container allows it.
	CodecMP3
)

var codecNames = [...]string{
	CodecUnknown: "unknown",
	CodecH264:    "H264",
	CodecH265:    "H265",
	CodecAV1:     "AV1",
	CodecAAC:     "AAC",
	CodecOpus:    "Opus",
	CodecPCMU:    "PCMU",
	CodecPCMA:    "PCMA",
	CodecMP3:     "MP3",
}

// String returns the codec's name, as "H264" or "Opus".
func (c CodecID) String() string {
	if int(c) < len(codecNames) {
		return codecNames[c]
	}
	return "CodecID(" + strconv.Itoa(int(c)) + ")"
}

// IsVideo reports whether c is a video codec.
func (c CodecID) IsVideo() bool {
	return c == CodecH264 || c == CodecH265 || c == CodecAV1
}

// IsAudio reports whether c is an audio codec.
func (c CodecID) IsAudio() bool {
	return c >= CodecAAC && c <= CodecMP3
}

// Track describes one elementary stream of a published stream.
type Track struct {
	Codec CodecID
	// Config is the codec's out-of-band configuration in its ISO BMFF form:
	// AVCDecoderConfigurationRecord (H.264), HEVCDecoderConfigurationRecord
	// (H.265), AV1CodecConfigurationRecord (AV1), AudioSpecificConfig
	// (AAC), OpusHead (Opus, optional), nil for G.711. Immutable once
	// published.
	Config []byte
	// Width and Height are the video's display size in pixels, from the
	// SPS when known.
	Width, Height int
	// SampleRate and Channels describe audio.
	SampleRate int
	Channels   int
}

// Packet is one access unit or frame of a Track, in the canonical form the
// package documentation describes.
type Packet struct {
	// Track is the index of the packet's track in the stream's []*Track.
	Track int
	// DTS is the decode time from the stream's start (or any fixed epoch).
	DTS time.Duration
	// PTS is the presentation time; it equals DTS when frames are not
	// reordered.
	PTS time.Duration
	// Keyframe marks a video IDR or IRAP access unit, one a decoder can
	// start from. Audio packets are always keyframes.
	Keyframe bool
	// Data is the access unit: AVCC for H.264 and H.265, a raw access unit
	// for AAC, an Opus packet, G.711 samples. It is immutable once handed
	// to hub.Publisher.WritePacket, since subscribers share it.
	Data []byte
}

// CTS returns the composition time offset, PTS - DTS.
func (p *Packet) CTS() time.Duration { return p.PTS - p.DTS }

// ToTicks converts d to ticks of a clock of clockRate Hz, rounded to the
// nearest tick, half away from zero. It returns 0 for a clockRate that is
// not positive.
func ToTicks(d time.Duration, clockRate int) int64 {
	if clockRate <= 0 {
		return 0
	}
	// Split d into seconds and the rest so the products stay in range for
	// any Duration and any clock up to 2^32 Hz.
	r := int64(clockRate)
	sec, ns := int64(d)/int64(time.Second), int64(d)%int64(time.Second)
	return sec*r + roundDiv(ns*r, int64(time.Second))
}

// FromTicks converts ticks of a clock of clockRate Hz to a Duration,
// rounded to the nearest nanosecond, half away from zero. It returns 0 for
// a clockRate that is not positive.
func FromTicks(ticks int64, clockRate int) time.Duration {
	if clockRate <= 0 {
		return 0
	}
	r := int64(clockRate)
	sec, rest := ticks/r, ticks%r
	return time.Duration(sec*int64(time.Second) + roundDiv(rest*int64(time.Second), r))
}

// roundDiv returns a/b rounded half away from zero, for b > 0.
func roundDiv(a, b int64) int64 {
	if a >= 0 {
		return (a + b/2) / b
	}
	return -((-a + b/2) / b)
}
