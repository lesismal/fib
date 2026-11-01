package h264

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrShortConfig   = errors.New("h264: AVCDecoderConfigurationRecord too short")
	ErrConfigVersion = errors.New("h264: AVCDecoderConfigurationRecord version is not 1")
	ErrNoSPS         = errors.New("h264: no SPS")
	ErrTooMany       = errors.New("h264: too many parameter sets")
	ErrSetSize       = errors.New("h264: parameter set larger than 65535 bytes")
)

// AVCConfig is an AVCDecoderConfigurationRecord (ISO/IEC 14496-15 section
// 5.3.3.1), the configuration of an H.264 track in MP4, FLV and Matroska,
// and av.Track.Config for H.264.
type AVCConfig struct {
	ProfileIndication    uint8
	ProfileCompatibility uint8
	LevelIndication      uint8
	// LengthSize is the size of the NAL unit lengths of the track's
	// samples: 1, 2 or 4.
	LengthSize int
	SPS        [][]byte
	PPS        [][]byte

	// Ext says the record carries the fields that follow the PPS for
	// profiles other than Baseline, Main and Extended: the chroma format,
	// the bit depths and the SPS extensions.
	Ext            bool
	ChromaFormat   uint8
	BitDepthLuma   uint8
	BitDepthChroma uint8
	SPSExt         [][]byte
}

// NewAVCConfig builds the record of the parameter sets sps and pps, taking
// the profile, level and chroma fields from the first SPS. The extension
// fields are written for every profile but Baseline, Main and Extended, as
// ffmpeg writes them, though older writers leave them out for High too.
// The record refers to sps and pps.
func NewAVCConfig(sps, pps [][]byte) (*AVCConfig, error) {
	if len(sps) == 0 {
		return nil, ErrNoSPS
	}
	var s SPS
	if err := s.Unmarshal(sps[0]); err != nil {
		return nil, err
	}
	c := &AVCConfig{
		ProfileIndication:    s.ProfileIDC,
		ProfileCompatibility: s.ConstraintFlags,
		LevelIndication:      s.LevelIDC,
		LengthSize:           4,
		SPS:                  sps,
		PPS:                  pps,
	}
	if hasConfigExt(s.ProfileIDC) {
		c.Ext = true
		c.ChromaFormat = s.ChromaFormatIDC
		c.BitDepthLuma = s.BitDepthLuma
		c.BitDepthChroma = s.BitDepthChroma
	}
	return c, nil
}

func hasConfigExt(profile uint8) bool {
	return profile != 66 && profile != 77 && profile != 88
}

// Unmarshal parses the record b. The parameter sets refer to b. Extension
// fields that are absent, cut short or malformed are taken as absent, as
// players take them.
func (c *AVCConfig) Unmarshal(b []byte) error {
	if len(b) < 7 {
		return ErrShortConfig
	}
	if b[0] != 1 {
		return ErrConfigVersion
	}
	*c = AVCConfig{
		ProfileIndication:    b[1],
		ProfileCompatibility: b[2],
		LevelIndication:      b[3],
		LengthSize:           int(b[4]&3) + 1,
		SPS:                  c.SPS[:0],
		PPS:                  c.PPS[:0],
		SPSExt:               c.SPSExt[:0],
	}
	if c.LengthSize == 3 {
		return ErrLengthSize
	}
	var err error
	b = b[5:]
	if c.SPS, b, err = readSets(c.SPS, b, int(b[0]&0x1f), 1); err != nil {
		return err
	}
	if len(b) < 1 {
		return ErrShortConfig
	}
	if c.PPS, b, err = readSets(c.PPS, b, int(b[0]), 1); err != nil {
		return err
	}
	if len(b) >= 4 && hasConfigExt(c.ProfileIndication) {
		ext, _, err := readSets(c.SPSExt, b[3:], int(b[3]), 1)
		if err == nil {
			c.Ext = true
			c.ChromaFormat = b[0] & 3
			c.BitDepthLuma = b[1]&7 + 8
			c.BitDepthChroma = b[2]&7 + 8
			c.SPSExt = ext
		}
	}
	if !c.Ext {
		c.SPSExt = nil
	}
	return nil
}

// readSets appends to sets the n parameter sets of b after its first skip
// bytes, each prefixed with a 2-byte length, and returns the bytes after
// them.
func readSets(sets [][]byte, b []byte, n, skip int) ([][]byte, []byte, error) {
	b = b[skip:]
	for range n {
		if len(b) < 2 {
			return sets, nil, ErrShortConfig
		}
		size := int(binary.BigEndian.Uint16(b))
		if len(b) < 2+size {
			return sets, nil, ErrShortConfig
		}
		sets = append(sets, b[2:2+size:2+size])
		b = b[2+size:]
	}
	return sets, b, nil
}

// MarshalSize returns the number of bytes Append adds.
func (c *AVCConfig) MarshalSize() int {
	n := 7
	for _, s := range c.SPS {
		n += 2 + len(s)
	}
	for _, s := range c.PPS {
		n += 2 + len(s)
	}
	if c.Ext {
		n += 4
		for _, s := range c.SPSExt {
			n += 2 + len(s)
		}
	}
	return n
}

// Append appends the record to dst.
func (c *AVCConfig) Append(dst []byte) ([]byte, error) {
	if len(c.SPS) > 31 || len(c.PPS) > 255 || len(c.SPSExt) > 255 {
		return dst, ErrTooMany
	}
	ls := c.LengthSize
	if ls == 0 {
		ls = 4
	}
	if ls != 1 && ls != 2 && ls != 4 {
		return dst, ErrLengthSize
	}
	for _, sets := range [][][]byte{c.SPS, c.PPS, c.SPSExt} {
		for _, s := range sets {
			if len(s) > 0xffff {
				return dst, ErrSetSize
			}
		}
	}
	dst = append(dst, 1, c.ProfileIndication, c.ProfileCompatibility, c.LevelIndication,
		0xfc|byte(ls-1), 0xe0|byte(len(c.SPS)))
	dst = appendSets(dst, c.SPS)
	dst = append(dst, byte(len(c.PPS)))
	dst = appendSets(dst, c.PPS)
	if c.Ext {
		dst = append(dst, 0xfc|c.ChromaFormat&3, 0xf8|(c.BitDepthLuma-8)&7,
			0xf8|(c.BitDepthChroma-8)&7, byte(len(c.SPSExt)))
		dst = appendSets(dst, c.SPSExt)
	}
	return dst, nil
}

func appendSets(dst []byte, sets [][]byte) []byte {
	for _, s := range sets {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
		dst = append(dst, s...)
	}
	return dst
}

// CodecString returns the RFC 6381 codecs parameter of the track, as
// "avc1.64001f", for HLS playlists and MIME types.
func (c *AVCConfig) CodecString() string {
	return fmt.Sprintf("avc1.%02x%02x%02x", c.ProfileIndication, c.ProfileCompatibility, c.LevelIndication)
}
