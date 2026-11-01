package opus

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrShortHead   = errors.New("opus: OpusHead too short")
	ErrMagic       = errors.New("opus: no OpusHead magic signature")
	ErrVersion     = errors.New("opus: OpusHead version with an unknown major version")
	ErrChannels    = errors.New("opus: invalid channel count or mapping")
	ErrEmptyPacket = errors.New("opus: empty packet")
	ErrFrameCount  = errors.New("opus: invalid frame count")
)

// Head is the identification header of RFC 7845 section 5.1, OpusHead: the
// configuration of an Opus track in Ogg, and in MP4 as the dOps box (whose
// fields are the same, big-endian and without the magic). It is
// av.Track.Config for Opus, which a track may leave nil.
type Head struct {
	// Version is 1 for this specification; Append writes 1 for 0.
	Version uint8
	// Channels is the output channel count, 1 to 255.
	Channels uint8
	// PreSkip is the number of 48 kHz samples to discard from the decoder
	// output at the start of playback.
	PreSkip uint16
	// InputSampleRate is the sample rate of the original input,
	// informational only: Opus always decodes at 48 kHz.
	InputSampleRate uint32
	// OutputGain is the gain to apply to the output, in Q7.8 dB.
	OutputGain int16
	// MappingFamily is 0 for mono or stereo in one stream, 1 for the
	// Vorbis channel orders of up to 8 channels, 255 for an unspecified
	// order; families other than 0 carry the fields below.
	MappingFamily  uint8
	StreamCount    uint8
	CoupledCount   uint8
	ChannelMapping []byte
}

var magic = []byte("OpusHead")

// Unmarshal parses the OpusHead packet b. ChannelMapping refers to b.
func (h *Head) Unmarshal(b []byte) error {
	if len(b) < 19 {
		return ErrShortHead
	}
	if string(b[:8]) != string(magic) {
		return ErrMagic
	}
	// Section 5.1: a version whose major part (the high nibble) is 0 is
	// compatible.
	if b[8]>>4 != 0 {
		return ErrVersion
	}
	*h = Head{
		Version:         b[8],
		Channels:        b[9],
		PreSkip:         binary.LittleEndian.Uint16(b[10:]),
		InputSampleRate: binary.LittleEndian.Uint32(b[12:]),
		OutputGain:      int16(binary.LittleEndian.Uint16(b[16:])),
		MappingFamily:   b[18],
	}
	if h.Channels == 0 {
		return ErrChannels
	}
	if h.MappingFamily == 0 {
		if h.Channels > 2 {
			return ErrChannels
		}
		return nil
	}
	if len(b) < 21+int(h.Channels) {
		return ErrShortHead
	}
	h.StreamCount, h.CoupledCount = b[19], b[20]
	h.ChannelMapping = b[21 : 21+int(h.Channels) : 21+int(h.Channels)]
	return h.checkMapping()
}

// checkMapping checks the channel mapping table of section 5.1.1.
func (h *Head) checkMapping() error {
	if h.StreamCount == 0 || h.CoupledCount > h.StreamCount ||
		int(h.StreamCount)+int(h.CoupledCount) > 255 || len(h.ChannelMapping) != int(h.Channels) {
		return ErrChannels
	}
	for _, m := range h.ChannelMapping {
		if m != 255 && int(m) >= int(h.StreamCount)+int(h.CoupledCount) {
			return ErrChannels
		}
	}
	return nil
}

// MarshalSize returns the number of bytes Append adds.
func (h *Head) MarshalSize() int {
	if h.MappingFamily == 0 {
		return 19
	}
	return 21 + len(h.ChannelMapping)
}

// Append appends the OpusHead packet to dst.
func (h *Head) Append(dst []byte) ([]byte, error) {
	if h.Version>>4 != 0 {
		return dst, ErrVersion
	}
	if h.Channels == 0 || h.MappingFamily == 0 && h.Channels > 2 {
		return dst, ErrChannels
	}
	if h.MappingFamily != 0 {
		if err := h.checkMapping(); err != nil {
			return dst, err
		}
	}
	v := h.Version
	if v == 0 {
		v = 1
	}
	dst = append(dst, magic...)
	dst = append(dst, v, h.Channels)
	dst = binary.LittleEndian.AppendUint16(dst, h.PreSkip)
	dst = binary.LittleEndian.AppendUint32(dst, h.InputSampleRate)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(h.OutputGain))
	dst = append(dst, h.MappingFamily)
	if h.MappingFamily != 0 {
		dst = append(dst, h.StreamCount, h.CoupledCount)
		dst = append(dst, h.ChannelMapping...)
	}
	return dst, nil
}

// frameSizes are the frame durations of the TOC's configuration numbers
// (RFC 6716 section 3.1, Table 2), in units of 2.5 ms.
var frameSizes = [32]uint8{
	4, 8, 16, 24, 4, 8, 16, 24, 4, 8, 16, 24, // SILK NB, MB, WB: 10, 20, 40, 60 ms
	4, 8, 4, 8, // Hybrid SWB, FB: 10, 20 ms
	1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8, // CELT: 2.5, 5, 10, 20 ms
}

// FrameDuration returns the duration of each frame of a packet whose TOC
// byte is toc.
func FrameDuration(toc byte) time.Duration {
	return time.Duration(frameSizes[toc>>3]) * 2500 * time.Microsecond
}

// FrameCount returns the number of frames in the Opus packet pkt, from its
// TOC byte and, for code 3 packets, its frame count byte (RFC 6716 section
// 3.2).
func FrameCount(pkt []byte) (int, error) {
	if len(pkt) == 0 {
		return 0, ErrEmptyPacket
	}
	switch pkt[0] & 3 {
	case 0:
		return 1, nil
	case 1, 2:
		return 2, nil
	}
	if len(pkt) < 2 {
		return 0, ErrFrameCount
	}
	n := int(pkt[1] & 0x3f)
	if n == 0 {
		return 0, ErrFrameCount
	}
	return n, nil
}

// PacketDuration returns the duration of the Opus packet pkt. A packet of
// more than 120 ms is an error (RFC 6716 section 3.2.5).
func PacketDuration(pkt []byte) (time.Duration, error) {
	n, err := FrameCount(pkt)
	if err != nil {
		return 0, err
	}
	d := time.Duration(n) * FrameDuration(pkt[0])
	if d > 120*time.Millisecond {
		return 0, ErrFrameCount
	}
	return d, nil
}

// PacketSamples returns the number of 48 kHz samples the Opus packet pkt
// decodes to, its duration in the RTP and Ogg clock.
func PacketSamples(pkt []byte) (int, error) {
	d, err := PacketDuration(pkt)
	return int(d / (time.Second / 48000)), err
}
