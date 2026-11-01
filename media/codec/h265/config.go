package h265

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

var (
	ErrShortConfig   = errors.New("h265: HEVCDecoderConfigurationRecord too short")
	ErrConfigVersion = errors.New("h265: HEVCDecoderConfigurationRecord version is not 1")
	ErrLengthSize    = errors.New("h265: NAL unit length size is not 1, 2 or 4")
	ErrNoSPS         = errors.New("h265: no SPS")
	ErrTooMany       = errors.New("h265: too many NAL units")
	ErrSetSize       = errors.New("h265: NAL unit larger than 65535 bytes")
)

// NALArray is one array of HEVCDecoderConfigurationRecord: the NAL units of
// one type.
type NALArray struct {
	// Complete is array_completeness: every NAL unit of the type is in the
	// array, none in the samples.
	Complete bool
	Type     NALUType
	NALUs    [][]byte
}

// HEVCConfig is an HEVCDecoderConfigurationRecord (ISO/IEC 14496-15
// section 8.3.3.1), the configuration of an H.265 track in MP4, enhanced
// RTMP and Matroska, and av.Track.Config for H.265.
type HEVCConfig struct {
	// The general profile, tier and level of the stream.
	ProfileTierLevel
	MinSpatialSegmentation uint16
	ParallelismType        uint8
	ChromaFormat           uint8
	BitDepthLuma           uint8
	BitDepthChroma         uint8
	AvgFrameRate           uint16
	ConstantFrameRate      uint8
	NumTemporalLayers      uint8
	TemporalIDNested       bool
	// LengthSize is the size of the NAL unit lengths of the track's
	// samples: 1, 2 or 4.
	LengthSize int
	Arrays     []NALArray
}

// NewHEVCConfig builds the record of the parameter sets vps, sps and pps,
// one complete array of each, taking the general profile, tier and level,
// chroma format, bit depths and temporal layers from the first SPS and
// the VPSs, as ffmpeg does. The record refers to the parameter sets.
func NewHEVCConfig(vps, sps, pps [][]byte) (*HEVCConfig, error) {
	if len(sps) == 0 {
		return nil, ErrNoSPS
	}
	var s SPS
	if err := s.Unmarshal(sps[0]); err != nil {
		return nil, err
	}
	c := &HEVCConfig{
		ProfileTierLevel:  s.ProfileTierLevel,
		ChromaFormat:      s.ChromaFormatIDC,
		BitDepthLuma:      s.BitDepthLuma,
		BitDepthChroma:    s.BitDepthChroma,
		NumTemporalLayers: s.MaxSubLayers,
		TemporalIDNested:  s.TemporalIDNesting,
		LengthSize:        4,
	}
	// ffmpeg's hvcC merges the profile_tier_level of every parameter set:
	// the highest tier, profile and level, the flags all of them set.
	for _, b := range vps {
		var v VPS
		if err := v.Unmarshal(b); err != nil {
			return nil, err
		}
		c.Tier = max(c.Tier, v.Tier)
		c.ProfileIDC = max(c.ProfileIDC, v.ProfileIDC)
		c.LevelIDC = max(c.LevelIDC, v.LevelIDC)
		c.CompatibilityFlags &= v.CompatibilityFlags
		c.ConstraintFlags &= v.ConstraintFlags
		c.NumTemporalLayers = max(c.NumTemporalLayers, v.MaxSubLayers)
	}
	for _, a := range []NALArray{{true, NALVPS, vps}, {true, NALSPS, sps}, {true, NALPPS, pps}} {
		if len(a.NALUs) > 0 {
			c.Arrays = append(c.Arrays, a)
		}
	}
	return c, nil
}

// NALUs returns the NAL units of type t in the record's arrays.
func (c *HEVCConfig) NALUs(t NALUType) [][]byte {
	var out [][]byte
	for _, a := range c.Arrays {
		if a.Type == t {
			out = append(out, a.NALUs...)
		}
	}
	return out
}

// Unmarshal parses the record b. The NAL units refer to b.
func (c *HEVCConfig) Unmarshal(b []byte) error {
	if len(b) < 23 {
		return ErrShortConfig
	}
	if b[0] != 1 {
		return ErrConfigVersion
	}
	*c = HEVCConfig{
		ProfileTierLevel: ProfileTierLevel{
			ProfileSpace:       b[1] >> 6,
			Tier:               b[1] >> 5 & 1,
			ProfileIDC:         b[1] & 0x1f,
			CompatibilityFlags: binary.BigEndian.Uint32(b[2:]),
			ConstraintFlags:    binary.BigEndian.Uint64(b[4:]) & (1<<48 - 1),
			LevelIDC:           b[12],
		},
		MinSpatialSegmentation: binary.BigEndian.Uint16(b[13:]) & 0xfff,
		ParallelismType:        b[15] & 3,
		ChromaFormat:           b[16] & 3,
		BitDepthLuma:           b[17]&7 + 8,
		BitDepthChroma:         b[18]&7 + 8,
		AvgFrameRate:           binary.BigEndian.Uint16(b[19:]),
		ConstantFrameRate:      b[21] >> 6,
		NumTemporalLayers:      b[21] >> 3 & 7,
		TemporalIDNested:       b[21]&4 != 0,
		LengthSize:             int(b[21]&3) + 1,
		Arrays:                 c.Arrays[:0],
	}
	if c.LengthSize == 3 {
		return ErrLengthSize
	}
	n := int(b[22])
	b = b[23:]
	for range n {
		if len(b) < 3 {
			return ErrShortConfig
		}
		a := NALArray{Complete: b[0]&0x80 != 0, Type: NALUType(b[0] & 0x3f)}
		count := int(binary.BigEndian.Uint16(b[1:]))
		b = b[3:]
		for range count {
			if len(b) < 2 {
				return ErrShortConfig
			}
			size := int(binary.BigEndian.Uint16(b))
			if len(b) < 2+size {
				return ErrShortConfig
			}
			a.NALUs = append(a.NALUs, b[2:2+size:2+size])
			b = b[2+size:]
		}
		c.Arrays = append(c.Arrays, a)
	}
	return nil
}

// MarshalSize returns the number of bytes Append adds.
func (c *HEVCConfig) MarshalSize() int {
	n := 23
	for _, a := range c.Arrays {
		n += 3
		for _, u := range a.NALUs {
			n += 2 + len(u)
		}
	}
	return n
}

// Append appends the record to dst.
func (c *HEVCConfig) Append(dst []byte) ([]byte, error) {
	ls := c.LengthSize
	if ls == 0 {
		ls = 4
	}
	if ls != 1 && ls != 2 && ls != 4 {
		return dst, ErrLengthSize
	}
	if len(c.Arrays) > 255 {
		return dst, ErrTooMany
	}
	for _, a := range c.Arrays {
		if len(a.NALUs) > 0xffff {
			return dst, ErrTooMany
		}
		for _, u := range a.NALUs {
			if len(u) > 0xffff {
				return dst, ErrSetSize
			}
		}
	}
	dst = append(dst, 1, c.ProfileSpace<<6|c.Tier&1<<5|c.ProfileIDC&0x1f)
	dst = binary.BigEndian.AppendUint32(dst, c.CompatibilityFlags)
	dst = binary.BigEndian.AppendUint16(dst, uint16(c.ConstraintFlags>>32))
	dst = binary.BigEndian.AppendUint32(dst, uint32(c.ConstraintFlags))
	dst = append(dst, c.LevelIDC)
	dst = binary.BigEndian.AppendUint16(dst, 0xf000|c.MinSpatialSegmentation&0xfff)
	dst = append(dst, 0xfc|c.ParallelismType&3, 0xfc|c.ChromaFormat&3,
		0xf8|(c.BitDepthLuma-8)&7, 0xf8|(c.BitDepthChroma-8)&7)
	dst = binary.BigEndian.AppendUint16(dst, c.AvgFrameRate)
	b21 := c.ConstantFrameRate<<6 | c.NumTemporalLayers&7<<3 | byte(ls-1)
	if c.TemporalIDNested {
		b21 |= 4
	}
	dst = append(dst, b21, byte(len(c.Arrays)))
	for _, a := range c.Arrays {
		t := byte(a.Type) & 0x3f
		if a.Complete {
			t |= 0x80
		}
		dst = append(dst, t)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(a.NALUs)))
		for _, u := range a.NALUs {
			dst = binary.BigEndian.AppendUint16(dst, uint16(len(u)))
			dst = append(dst, u...)
		}
	}
	return dst, nil
}

// CodecString returns the RFC 6381 codecs parameter of the track as ISO/IEC
// 14496-15 Annex E has it, as "hvc1.1.6.L93.B0", for HLS playlists and MIME
// types. A track whose samples carry parameter sets in band is "hev1"
// instead: replace the first four letters.
func (c *HEVCConfig) CodecString() string {
	var sb strings.Builder
	sb.WriteString("hvc1.")
	if c.ProfileSpace > 0 {
		sb.WriteByte('A' + c.ProfileSpace - 1)
	}
	sb.WriteString(strconv.Itoa(int(c.ProfileIDC)))
	sb.WriteByte('.')
	// The compatibility flags in reverse bit order.
	var rev uint32
	for i := range 32 {
		rev |= (c.CompatibilityFlags >> i & 1) << (31 - i)
	}
	sb.WriteString(strconv.FormatUint(uint64(rev), 16))
	if c.Tier == 1 {
		sb.WriteString(".H")
	} else {
		sb.WriteString(".L")
	}
	sb.WriteString(strconv.Itoa(int(c.LevelIDC)))
	// The constraint bytes, trailing zero bytes left out.
	n := 6
	for n > 0 && c.ConstraintFlags>>(8*(6-n))&0xff == 0 {
		n--
	}
	for i := range n {
		sb.WriteByte('.')
		sb.WriteString(strings.ToUpper(strconv.FormatUint(c.ConstraintFlags>>(8*(5-i))&0xff, 16)))
	}
	return sb.String()
}
