package h265

import (
	"errors"
	"strconv"

	"github.com/lesismal/fib/media/codec/h264"
)

var (
	ErrShortHeader = errors.New("h265: NAL unit shorter than its 2-byte header")
	ErrHeader      = errors.New("h265: NAL unit header with nuh_temporal_id_plus1 0")
)

// NALUType is nal_unit_type, bits 1 to 6 of a NAL unit's 2-byte header
// (ITU-T H.265 Table 7-1).
type NALUType uint8

// The NAL unit types of Table 7-1. The RTP payload format (RFC 7798) uses
// 48 for aggregation packets and 49 for fragmentation units.
const (
	NALTrailN    NALUType = 0
	NALTrailR    NALUType = 1
	NALTSAN      NALUType = 2
	NALTSAR      NALUType = 3
	NALSTSAN     NALUType = 4
	NALSTSAR     NALUType = 5
	NALRADLN     NALUType = 6
	NALRADLR     NALUType = 7
	NALRASLN     NALUType = 8
	NALRASLR     NALUType = 9
	NALBLAWLP    NALUType = 16
	NALBLAWRADL  NALUType = 17
	NALBLANLP    NALUType = 18
	NALIDRWRADL  NALUType = 19
	NALIDRNLP    NALUType = 20
	NALCRA       NALUType = 21
	NALVPS       NALUType = 32
	NALSPS       NALUType = 33
	NALPPS       NALUType = 34
	NALAUD       NALUType = 35
	NALEOS       NALUType = 36
	NALEOB       NALUType = 37
	NALFD        NALUType = 38
	NALSEIPrefix NALUType = 39
	NALSEISuffix NALUType = 40
)

var naluNames = [...]string{
	NALTrailN: "TRAIL_N", NALTrailR: "TRAIL_R", NALTSAN: "TSA_N", NALTSAR: "TSA_R",
	NALSTSAN: "STSA_N", NALSTSAR: "STSA_R", NALRADLN: "RADL_N", NALRADLR: "RADL_R",
	NALRASLN: "RASL_N", NALRASLR: "RASL_R", NALBLAWLP: "BLA_W_LP", NALBLAWRADL: "BLA_W_RADL",
	NALBLANLP: "BLA_N_LP", NALIDRWRADL: "IDR_W_RADL", NALIDRNLP: "IDR_N_LP", NALCRA: "CRA_NUT",
	NALVPS: "VPS", NALSPS: "SPS", NALPPS: "PPS", NALAUD: "AUD", NALEOS: "EOS", NALEOB: "EOB",
	NALFD: "FD", NALSEIPrefix: "SEI_PREFIX", NALSEISuffix: "SEI_SUFFIX",
}

// String returns the type's name as Table 7-1 spells it, as "IDR_W_RADL",
// or its number.
func (t NALUType) String() string {
	if int(t) < len(naluNames) && naluNames[t] != "" {
		return naluNames[t]
	}
	return "NALUType(" + strconv.Itoa(int(t)) + ")"
}

// IsIRAP reports whether t is an intra random access point picture, BLA,
// IDR or CRA (types 16 to 23, the last two reserved): an access unit a
// decoder can start from.
func (t NALUType) IsIRAP() bool { return t >= NALBLAWLP && t <= 23 }

// IsVCL reports whether t is a coded slice segment.
func (t NALUType) IsVCL() bool { return t < 32 }

// Header is the 2-byte NAL unit header of section 7.3.1.2.
type Header struct {
	Type    NALUType
	LayerID uint8
	// TemporalID is nuh_temporal_id_plus1 - 1.
	TemporalID uint8
}

// ParseHeader parses the header of the NAL unit nalu.
func ParseHeader(nalu []byte) (Header, error) {
	if len(nalu) < 2 {
		return Header{}, ErrShortHeader
	}
	if nalu[1]&7 == 0 {
		return Header{}, ErrHeader
	}
	return Header{
		Type:       NALUType(nalu[0] >> 1 & 0x3f),
		LayerID:    nalu[0]&1<<5 | nalu[1]>>3,
		TemporalID: (nalu[1] & 7) - 1,
	}, nil
}

// Type returns the type of the NAL unit nalu, 0 for an empty one.
func Type(nalu []byte) NALUType {
	if len(nalu) == 0 {
		return 0
	}
	return NALUType(nalu[0] >> 1 & 0x3f)
}

// IsKeyframe reports whether the access unit au, in 4-byte AVCC, holds an
// IRAP slice.
func IsKeyframe(au []byte) bool {
	for nalu := range h264.SplitAVCC(au, 4) {
		if Type(nalu).IsIRAP() {
			return true
		}
	}
	return false
}

// IsKeyframeAnnexB reports whether the Annex B access unit au holds an
// IRAP slice.
func IsKeyframeAnnexB(au []byte) bool {
	for nalu := range h264.SplitAnnexB(au) {
		if Type(nalu).IsIRAP() {
			return true
		}
	}
	return false
}

// PrependParameterSets appends to dst the parameter sets sets, then the
// access unit au, all in 4-byte AVCC: what a muxer that needs parameter
// sets in band sends for a keyframe. When au already holds a VPS or an
// SPS, it is appended alone.
func PrependParameterSets(dst, au []byte, sets ...[]byte) []byte {
	for nalu := range h264.SplitAVCC(au, 4) {
		if t := Type(nalu); t == NALVPS || t == NALSPS {
			return append(dst, au...)
		}
	}
	for _, ps := range sets {
		dst = h264.AppendAVCC(dst, ps)
	}
	return append(dst, au...)
}
