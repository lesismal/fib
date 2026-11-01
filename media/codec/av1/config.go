package av1

import (
	"errors"
	"strconv"
	"strings"
)

var (
	ErrShortConfig   = errors.New("av1: AV1CodecConfigurationRecord too short")
	ErrConfigVersion = errors.New("av1: AV1CodecConfigurationRecord marker or version is not 1")
	ErrConfigOBUs    = errors.New("av1: malformed configOBUs")
)

// Config is an AV1CodecConfigurationRecord (the AV1 ISO BMFF binding,
// section 2.3), the configuration of an AV1 track in MP4, Matroska and
// Enhanced RTMP, and av.Track.Config for AV1.
type Config struct {
	Profile              uint8
	Level                uint8
	Tier                 uint8
	HighBitdepth         bool
	TwelveBit            bool
	Monochrome           bool
	ChromaSubsamplingX   bool
	ChromaSubsamplingY   bool
	ChromaSamplePosition uint8
	// InitialPresentationDelay is initial_presentation_delay_minus_one + 1,
	// 0 when absent.
	InitialPresentationDelay uint8
	// OBUs are the configOBUs: the sequence header and any metadata OBUs,
	// each with its size field. They refer to the bytes the record was
	// read from.
	OBUs []byte
}

// NewConfig builds the record of a sequence header OBU, taking the profile,
// level and color fields from it. Its OBUs are the sequence header with its
// size field, a copy.
func NewConfig(seqHeader []byte) (*Config, error) {
	o, _, err := NextOBU(seqHeader)
	if err != nil {
		return nil, ErrSequenceHeader
	}
	if o.Type() != OBUSequenceHeader {
		return nil, ErrNotSequenceHeader
	}
	var s SequenceHeader
	if err := s.parse(o.Payload); err != nil {
		return nil, err
	}
	return &Config{
		Profile:                  s.Profile,
		Level:                    s.Level,
		Tier:                     s.Tier,
		HighBitdepth:             s.BitDepth > 8,
		TwelveBit:                s.BitDepth == 12,
		Monochrome:               s.Monochrome,
		ChromaSubsamplingX:       s.SubsamplingX,
		ChromaSubsamplingY:       s.SubsamplingY,
		ChromaSamplePosition:     s.ChromaSamplePosition,
		InitialPresentationDelay: s.InitialDisplayDelay,
		OBUs:                     AppendOBU(nil, o, true),
	}, nil
}

// Unmarshal parses a record. The configOBUs must be whole OBUs.
func (c *Config) Unmarshal(b []byte) error {
	if len(b) < 4 {
		return ErrShortConfig
	}
	if b[0] != 0x81 {
		return ErrConfigVersion
	}
	*c = Config{
		Profile:              b[1] >> 5,
		Level:                b[1] & 0x1f,
		Tier:                 b[2] >> 7,
		HighBitdepth:         b[2]&0x40 != 0,
		TwelveBit:            b[2]&0x20 != 0,
		Monochrome:           b[2]&0x10 != 0,
		ChromaSubsamplingX:   b[2]&0x08 != 0,
		ChromaSubsamplingY:   b[2]&0x04 != 0,
		ChromaSamplePosition: b[2] & 0x03,
	}
	if b[3]&0x10 != 0 {
		c.InitialPresentationDelay = b[3]&0x0f + 1
	}
	if !ValidOBUs(b[4:]) {
		return ErrConfigOBUs
	}
	c.OBUs = b[4:len(b):len(b)]
	return nil
}

// Append appends the record to dst.
func (c *Config) Append(dst []byte) []byte {
	b2 := c.Tier<<7 | c.ChromaSamplePosition&0x03
	for i, f := range []bool{c.HighBitdepth, c.TwelveBit, c.Monochrome, c.ChromaSubsamplingX, c.ChromaSubsamplingY} {
		if f {
			b2 |= 0x40 >> i
		}
	}
	var b3 byte
	if c.InitialPresentationDelay > 0 {
		b3 = 0x10 | (c.InitialPresentationDelay-1)&0x0f
	}
	dst = append(dst, 0x81, c.Profile<<5|c.Level&0x1f, b2, b3)
	return append(dst, c.OBUs...)
}

// SequenceHeader returns the first sequence header OBU of the configOBUs,
// with its size field, or nil when they have none.
func (c *Config) SequenceHeader() []byte {
	b := c.OBUs
	for len(b) > 0 {
		o, rest, err := NextOBU(b)
		if err != nil {
			return nil
		}
		if o.Type() == OBUSequenceHeader {
			return b[: len(b)-len(rest) : len(b)-len(rest)]
		}
		b = rest
	}
	return nil
}

// CodecString returns the codecs parameter of the record, "av01.P.LLT.DD"
// (the AV1 ISO BMFF binding, section "Codecs Parameter String"), as HLS
// and DASH name the codec.
func (c *Config) CodecString() string {
	tier := "M"
	if c.Tier != 0 {
		tier = "H"
	}
	depth := "08"
	if c.HighBitdepth {
		depth = "10"
		if c.Profile == 2 && c.TwelveBit {
			depth = "12"
		}
	}
	level := strconv.Itoa(int(c.Level))
	if len(level) < 2 {
		level = "0" + level
	}
	return strings.Join([]string{"av01", strconv.Itoa(int(c.Profile)), level + tier, depth}, ".")
}
