package h264

import (
	"encoding/binary"
	"errors"
	"iter"
	"strconv"
)

var (
	ErrLengthSize = errors.New("h264: NAL unit length size is not 1, 2 or 4")
	ErrShortNALU  = errors.New("h264: NAL unit length runs past the data")
	ErrNALUSize   = errors.New("h264: NAL unit too large for its length field")
)

// NALUType is nal_unit_type, the low 5 bits of a NAL unit's first byte
// (ITU-T H.264 Table 7-1).
type NALUType uint8

// The NAL unit types of Table 7-1. Types 24 to 31 are unspecified by H.264;
// the RTP payload format (RFC 6184) uses 24 to 29 for its aggregation and
// fragmentation units.
const (
	NALSlice         NALUType = 1  // coded slice of a non-IDR picture
	NALSliceA        NALUType = 2  // coded slice data partition A
	NALSliceB        NALUType = 3  // coded slice data partition B
	NALSliceC        NALUType = 4  // coded slice data partition C
	NALIDR           NALUType = 5  // coded slice of an IDR picture
	NALSEI           NALUType = 6  // supplemental enhancement information
	NALSPS           NALUType = 7  // sequence parameter set
	NALPPS           NALUType = 8  // picture parameter set
	NALAUD           NALUType = 9  // access unit delimiter
	NALEndOfSequence NALUType = 10 // end of sequence
	NALEndOfStream   NALUType = 11 // end of stream
	NALFiller        NALUType = 12 // filler data
	NALSPSExt        NALUType = 13 // sequence parameter set extension
	NALPrefix        NALUType = 14 // prefix NAL unit (SVC, MVC)
	NALSubsetSPS     NALUType = 15 // subset sequence parameter set
	NALDPS           NALUType = 16 // depth parameter set
	NALAuxSlice      NALUType = 19 // coded slice of an auxiliary picture
	NALSliceExt      NALUType = 20 // coded slice extension (SVC, MVC)
	NALSliceExtDepth NALUType = 21 // coded slice extension for a depth view
)

var naluNames = [...]string{
	NALSlice: "Slice", NALSliceA: "SliceA", NALSliceB: "SliceB", NALSliceC: "SliceC",
	NALIDR: "IDR", NALSEI: "SEI", NALSPS: "SPS", NALPPS: "PPS", NALAUD: "AUD",
	NALEndOfSequence: "EndOfSequence", NALEndOfStream: "EndOfStream", NALFiller: "Filler",
	NALSPSExt: "SPSExt", NALPrefix: "Prefix", NALSubsetSPS: "SubsetSPS", NALDPS: "DPS",
	NALAuxSlice: "AuxSlice", NALSliceExt: "SliceExt", NALSliceExtDepth: "SliceExtDepth",
}

// String returns the type's name, as "IDR", or its number.
func (t NALUType) String() string {
	if int(t) < len(naluNames) && naluNames[t] != "" {
		return naluNames[t]
	}
	return "NALUType(" + strconv.Itoa(int(t)) + ")"
}

// Type returns the type of the NAL unit nalu, 0 for an empty one.
func Type(nalu []byte) NALUType {
	if len(nalu) == 0 {
		return 0
	}
	return NALUType(nalu[0] & 0x1f)
}

// IsAnnexB reports whether b starts with an Annex B start code, 00 00 01
// or 00 00 00 01.
func IsAnnexB(b []byte) bool {
	return len(b) >= 3 && b[0] == 0 && b[1] == 0 && (b[2] == 1 || len(b) >= 4 && b[2] == 0 && b[3] == 1)
}

// SplitAnnexB yields the NAL units of an Annex B byte stream (ITU-T H.264
// Annex B, which H.265 shares), without their start codes, whether those
// are 3 or 4 bytes long, and without the zero bytes that trail a NAL unit.
// Bytes before the first start code are skipped; empty NAL units are not
// yielded. The NAL units refer to b.
func SplitAnnexB(b []byte) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		start := nextStartCode(b, 0)
		if start < 0 {
			return
		}
		for start < len(b) {
			from := start
			next := nextStartCode(b, from)
			end := len(b)
			if next >= 0 {
				end = next - 3 // the start code's 00 00 01
			} else {
				next = len(b)
			}
			for end > from && b[end-1] == 0 {
				end--
			}
			if end > from && !yield(b[from:end:end]) {
				return
			}
			start = next
		}
	}
}

// nextStartCode returns the index just past the first 00 00 01 in b at or
// after i, or -1.
func nextStartCode(b []byte, i int) int {
	for ; i+2 < len(b); i++ {
		if b[i+2] > 1 {
			i += 2 // no start code can end at or before i+2
			continue
		}
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			return i + 3
		}
	}
	return -1
}

// NextAVCC reads one NAL unit from the start of b, length-prefixed with a
// lengthSize-byte big-endian length (1, 2 or 4) as AVCC and ISO BMFF
// samples carry them, and returns it and the bytes after it. The NAL unit
// refers to b.
func NextAVCC(b []byte, lengthSize int) (nalu, rest []byte, err error) {
	var n int
	switch lengthSize {
	case 1:
		if len(b) < 1 {
			return nil, nil, ErrShortNALU
		}
		n = int(b[0])
	case 2:
		if len(b) < 2 {
			return nil, nil, ErrShortNALU
		}
		n = int(binary.BigEndian.Uint16(b))
	case 4:
		if len(b) < 4 {
			return nil, nil, ErrShortNALU
		}
		n = int(binary.BigEndian.Uint32(b))
	default:
		return nil, nil, ErrLengthSize
	}
	b = b[lengthSize:]
	if n > len(b) || n < 0 {
		return nil, nil, ErrShortNALU
	}
	return b[:n:n], b[n:], nil
}

// SplitAVCC yields the NAL units of b, each prefixed with a lengthSize-byte
// length. It stops at the first length that runs past the data; ValidAVCC
// or NextAVCC tell such data apart. The NAL units refer to b.
func SplitAVCC(b []byte, lengthSize int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for len(b) > 0 {
			nalu, rest, err := NextAVCC(b, lengthSize)
			if err != nil || !yield(nalu) {
				return
			}
			b = rest
		}
	}
}

// ValidAVCC reports whether b is a whole number of NAL units prefixed with
// lengthSize-byte lengths.
func ValidAVCC(b []byte, lengthSize int) bool {
	for len(b) > 0 {
		_, rest, err := NextAVCC(b, lengthSize)
		if err != nil {
			return false
		}
		b = rest
	}
	return lengthSize == 1 || lengthSize == 2 || lengthSize == 4
}

// AppendAVCC appends nalu to dst prefixed with its 4-byte length, the
// canonical form of av.Packet.
func AppendAVCC(dst, nalu []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(nalu)))
	return append(dst, nalu...)
}

// AnnexBToAVCC appends the NAL units of the Annex B stream src to dst,
// each prefixed with its 4-byte length. When keep is not nil, only the NAL
// units it returns true for are appended, which is how an ingest leaves
// out access unit delimiters and in-band parameter sets.
func AnnexBToAVCC(dst, src []byte, keep func(nalu []byte) bool) []byte {
	for nalu := range SplitAnnexB(src) {
		if keep == nil || keep(nalu) {
			dst = AppendAVCC(dst, nalu)
		}
	}
	return dst
}

// AVCCToAnnexB appends the NAL units of src, prefixed with lengthSize-byte
// lengths, to dst, each after a 4-byte start code. On error dst holds the
// NAL units before the malformed one.
func AVCCToAnnexB(dst, src []byte, lengthSize int) ([]byte, error) {
	for len(src) > 0 {
		nalu, rest, err := NextAVCC(src, lengthSize)
		if err != nil {
			return dst, err
		}
		dst = append(dst, 0, 0, 0, 1)
		dst = append(dst, nalu...)
		src = rest
	}
	return dst, nil
}

// NormalizeAVCC appends the NAL units of src, prefixed with lengthSize-byte
// lengths, to dst with 4-byte lengths, the canonical form of av.Packet.
func NormalizeAVCC(dst, src []byte, lengthSize int) ([]byte, error) {
	for len(src) > 0 {
		nalu, rest, err := NextAVCC(src, lengthSize)
		if err != nil {
			return dst, err
		}
		dst = AppendAVCC(dst, nalu)
		src = rest
	}
	return dst, nil
}

// AppendAVCCSize appends nalu to dst prefixed with a lengthSize-byte
// length, for muxers writing a configuration record's length size other
// than 4.
func AppendAVCCSize(dst, nalu []byte, lengthSize int) ([]byte, error) {
	switch lengthSize {
	case 1:
		if len(nalu) > 0xff {
			return dst, ErrNALUSize
		}
		dst = append(dst, byte(len(nalu)))
	case 2:
		if len(nalu) > 0xffff {
			return dst, ErrNALUSize
		}
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(nalu)))
	case 4:
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(nalu)))
	default:
		return dst, ErrLengthSize
	}
	return append(dst, nalu...), nil
}

// IsKeyframe reports whether the access unit au, in 4-byte AVCC, holds an
// IDR slice.
func IsKeyframe(au []byte) bool {
	for nalu := range SplitAVCC(au, 4) {
		if Type(nalu) == NALIDR {
			return true
		}
	}
	return false
}

// IsKeyframeAnnexB reports whether the Annex B access unit au holds an IDR
// slice.
func IsKeyframeAnnexB(au []byte) bool {
	for nalu := range SplitAnnexB(au) {
		if Type(nalu) == NALIDR {
			return true
		}
	}
	return false
}

// PrependParameterSets appends to dst the parameter sets sets, then the
// access unit au, all in 4-byte AVCC: what a muxer that needs parameter
// sets in band (MPEG-TS, RTP without sprop-parameter-sets) sends for a
// keyframe. When au already holds an SPS, it is appended alone.
func PrependParameterSets(dst, au []byte, sets ...[]byte) []byte {
	for nalu := range SplitAVCC(au, 4) {
		if Type(nalu) == NALSPS {
			return append(dst, au...)
		}
	}
	for _, ps := range sets {
		dst = AppendAVCC(dst, ps)
	}
	return append(dst, au...)
}

// RemoveEmulationPrevention appends src to dst without its emulation
// prevention bytes, the 03 of every 00 00 03 (ITU-T H.264 section 7.4.1):
// the RBSP a NAL unit's syntax is read from.
func RemoveEmulationPrevention(dst, src []byte) []byte {
	zeros := 0
	last := 0
	for i := 0; i < len(src); i++ {
		c := src[i]
		if zeros >= 2 && c == 3 {
			dst = append(dst, src[last:i]...)
			last = i + 1
			zeros = 0
			continue
		}
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return append(dst, src[last:]...)
}

// AddEmulationPrevention appends the RBSP src to dst with an emulation
// prevention byte inserted wherever two zero bytes are followed by a byte
// of 3 or less, so that no start code appears inside the NAL unit.
func AddEmulationPrevention(dst, src []byte) []byte {
	zeros := 0
	for _, c := range src {
		if zeros >= 2 && c <= 3 {
			dst = append(dst, 3)
			zeros = 0
		}
		dst = append(dst, c)
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return dst
}
