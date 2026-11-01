package aac

import "errors"

var (
	ErrShortADTS      = errors.New("aac: ADTS header too short")
	ErrSync           = errors.New("aac: no ADTS syncword")
	ErrADTSLength     = errors.New("aac: ADTS frame length shorter than its header or past the data")
	ErrRawBlocks      = errors.New("aac: ADTS frame with more than one raw data block")
	ErrADTSObjectType = errors.New("aac: object type not carried in ADTS")
)

// ADTSHeader is the header of an ADTS frame (ISO/IEC 14496-3 section 1.A.2,
// adts_fixed_header and adts_variable_header), as MPEG-TS carries AAC.
type ADTSHeader struct {
	// MPEG2 is the ID bit: the frame claims MPEG-2 AAC rather than MPEG-4.
	MPEG2 bool
	// CRC says a CRC follows the header (protection_absent is 0), making it
	// 9 bytes long instead of 7.
	CRC bool
	// ObjectType is the 2-bit profile plus one: 1 to 4.
	ObjectType      uint8
	SampleRateIndex uint8
	ChannelConfig   uint8
	// FrameLength is the length of the frame, header included.
	FrameLength int
	// BufferFullness is adts_buffer_fullness; 0x7ff signals a variable
	// bit rate.
	BufferFullness uint16
}

// HeaderSize returns the header's size, 7 bytes or 9 with a CRC.
func (h *ADTSHeader) HeaderSize() int {
	if h.CRC {
		return 9
	}
	return 7
}

// Unmarshal parses the ADTS header at the start of b. A frame of more than
// one raw data block is an error: splitting it takes the AAC decoder's
// syntax, and no encoder a server meets writes one.
func (h *ADTSHeader) Unmarshal(b []byte) error {
	if len(b) < 7 {
		return ErrShortADTS
	}
	if b[0] != 0xff || b[1]&0xf6 != 0xf0 { // syncword, layer 0
		return ErrSync
	}
	*h = ADTSHeader{
		MPEG2:           b[1]&8 != 0,
		CRC:             b[1]&1 == 0,
		ObjectType:      b[2]>>6 + 1,
		SampleRateIndex: b[2] >> 2 & 0xf,
		ChannelConfig:   b[2]&1<<2 | b[3]>>6,
		FrameLength:     int(b[3]&3)<<11 | int(b[4])<<3 | int(b[5]>>5),
		BufferFullness:  uint16(b[5]&0x1f)<<6 | uint16(b[6]>>2),
	}
	if int(h.SampleRateIndex) >= len(SampleRates) {
		return ErrSampleRate
	}
	if h.FrameLength < h.HeaderSize() {
		return ErrADTSLength
	}
	if b[6]&3 != 0 {
		return ErrRawBlocks
	}
	return nil
}

// Append appends the header to dst, with a CRC field of zero when CRC is
// set: computing the CRC takes the frame's payload, which a muxer has no
// reason to protect, so a muxer leaves CRC unset.
func (h *ADTSHeader) Append(dst []byte) ([]byte, error) {
	if h.ObjectType < 1 || h.ObjectType > 4 {
		return dst, ErrADTSObjectType
	}
	if int(h.SampleRateIndex) >= len(SampleRates) {
		return dst, ErrSampleRate
	}
	if h.ChannelConfig > 7 {
		return dst, ErrChannels
	}
	if h.FrameLength < h.HeaderSize() || h.FrameLength >= 1<<13 {
		return dst, ErrADTSLength
	}
	b1 := byte(0xf1)
	if h.MPEG2 {
		b1 |= 8
	}
	if h.CRC {
		b1 &^= 1
	}
	n, bf := h.FrameLength, h.BufferFullness&0x7ff
	dst = append(dst, 0xff, b1,
		(h.ObjectType-1)<<6|h.SampleRateIndex<<2|h.ChannelConfig>>2,
		h.ChannelConfig&3<<6|byte(n>>11),
		byte(n>>3),
		byte(n<<5)|byte(bf>>6),
		byte(bf<<2))
	if h.CRC {
		dst = append(dst, 0, 0)
	}
	return dst, nil
}

// SampleRate returns the sampling frequency of SampleRateIndex.
func (h *ADTSHeader) SampleRate() int {
	if int(h.SampleRateIndex) >= len(SampleRates) {
		return 0
	}
	return SampleRates[h.SampleRateIndex]
}

// Config returns the AudioSpecificConfig the header stands for.
func (h *ADTSHeader) Config() Config {
	return Config{ObjectType: h.ObjectType, SampleRate: h.SampleRate(), ChannelConfig: h.ChannelConfig}
}

// NextADTS reads the ADTS frame at the start of b and returns its header,
// its raw access unit and the bytes after the frame. The access unit
// refers to b.
func NextADTS(b []byte) (h ADTSHeader, au, rest []byte, err error) {
	if err = h.Unmarshal(b); err != nil {
		return h, nil, nil, err
	}
	if h.FrameLength > len(b) {
		return h, nil, nil, ErrADTSLength
	}
	return h, b[h.HeaderSize():h.FrameLength:h.FrameLength], b[h.FrameLength:], nil
}

// AppendADTS appends to dst the ADTS header of a frame carrying an access
// unit of auLen bytes of the config's stream, without a CRC and with a
// variable bit rate's buffer fullness. HE-AAC is signalled implicitly, as
// ADTS has no way to do otherwise: the header carries the core object type
// and sampling frequency.
func (c *Config) AppendADTS(dst []byte, auLen int) ([]byte, error) {
	i, ok := SampleRateIndex(c.SampleRate)
	if !ok {
		return dst, ErrSampleRate
	}
	h := ADTSHeader{
		ObjectType:      c.ObjectType,
		SampleRateIndex: uint8(i),
		ChannelConfig:   c.ChannelConfig,
		FrameLength:     7 + auLen,
		BufferFullness:  0x7ff,
	}
	return h.Append(dst)
}
