package mpegts

import (
	"errors"

	"github.com/lesismal/fib/media/av"
)

// PacketSize is the size of a transport packet.
const PacketSize = 188

// syncByte starts every transport packet.
const syncByte = 0x47

// The PIDs with a fixed meaning.
const (
	pidPAT  = 0x0000
	pidNull = 0x1fff
)

// Stream types of the PMT (ISO/IEC 13818-1 Table 2-34, and the private
// stream type Opus is carried under).
const (
	StreamTypeAAC     = 0x0f
	StreamTypeH264    = 0x1b
	StreamTypeH265    = 0x24
	StreamTypePrivate = 0x06
)

// Descriptor tags the PMT carries for Opus.
const (
	descRegistration = 0x05
	descExtension    = 0x7f
	// extOpus is the extension descriptor tag of Opus's channel
	// configuration, after descExtension.
	extOpus = 0x80
)

// The PES stream_id of each kind of stream.
const (
	streamIDVideo    = 0xe0
	streamIDAudio    = 0xc0
	streamIDPrivate1 = 0xbd
)

// clock90k is the rate of PTS and DTS; the PCR runs 300 times faster.
const clock90k = 90000

// tsMask keeps the 33 bits of a PTS, DTS or PCR base.
const tsMask = 1<<33 - 1

var (
	// ErrNoTracks is what NewMuxer reports when none of the tracks has a
	// codec MPEG-TS carries.
	ErrNoTracks = errors.New("mpegts: no track MPEG-TS can carry")
	// ErrTooManyTracks is what NewMuxer reports when the PMT of the tracks
	// would not fit one transport packet.
	ErrTooManyTracks = errors.New("mpegts: too many tracks for one PMT packet")
	// ErrTrack is what WritePacket reports for a packet of a track the
	// muxer does not carry.
	ErrTrack = errors.New("mpegts: packet of a track the muxer leaves out")
	// ErrBadData is what WritePacket reports for a video access unit that
	// is not valid AVCC.
	ErrBadData = errors.New("mpegts: access unit is not 4-byte AVCC")
)

// streamTypeOf returns the PMT stream type of codec c, or 0 when MPEG-TS,
// as this package muxes it, cannot carry c.
func streamTypeOf(c av.CodecID) uint8 {
	switch c {
	case av.CodecH264:
		return StreamTypeH264
	case av.CodecH265:
		return StreamTypeH265
	case av.CodecAAC:
		return StreamTypeAAC
	case av.CodecOpus:
		return StreamTypePrivate
	}
	return 0
}

// crcTable is the table of the MPEG-2 CRC-32: polynomial 0x04C11DB7, not
// reflected, which hash/crc32 does not offer.
var crcTable = func() (t [256]uint32) {
	for i := range t {
		c := uint32(i) << 24
		for range 8 {
			if c&0x80000000 != 0 {
				c = c<<1 ^ 0x04c11db7
			} else {
				c <<= 1
			}
		}
		t[i] = c
	}
	return t
}()

// crc32MPEG returns the CRC_32 of a PSI section (Annex A), which the section
// ends with; computed over a whole section, CRC included, it is 0.
func crc32MPEG(b []byte) uint32 {
	c := uint32(0xffffffff)
	for _, v := range b {
		c = c<<8 ^ crcTable[byte(c>>24)^v]
	}
	return c
}

// Opus channel layouts of channel_config_code 1 to 8 (Table 6-2 of the
// Opus in MPEG-TS draft): the stream and coupled stream counts and the
// Vorbis channel order of mapping family 1.
var (
	opusStreams = [9]uint8{1, 1, 1, 2, 2, 3, 4, 4, 5}
	opusCoupled = [9]uint8{1, 0, 1, 1, 2, 2, 2, 3, 3}
	opusMapping = [9][]byte{
		nil,
		{0},
		{0, 1},
		{0, 2, 1},
		{0, 1, 2, 3},
		{0, 4, 1, 2, 3},
		{0, 4, 1, 2, 3, 5},
		{0, 4, 1, 2, 3, 5, 6},
		{0, 6, 1, 2, 3, 4, 5, 7},
	}
)
