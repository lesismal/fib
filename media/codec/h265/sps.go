package h265

import (
	"errors"

	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/internal/bitio"
)

var (
	ErrNotSPS = errors.New("h265: NAL unit is not an SPS")
	ErrNotVPS = errors.New("h265: NAL unit is not a VPS")
	ErrSPS    = errors.New("h265: malformed SPS")
	ErrVPS    = errors.New("h265: malformed VPS")
)

// ProfileTierLevel holds the general fields of profile_tier_level()
// (section 7.3.3), as the HEVCDecoderConfigurationRecord carries them.
type ProfileTierLevel struct {
	ProfileSpace uint8
	// Tier is 0 for the Main tier, 1 for the High tier.
	Tier       uint8
	ProfileIDC uint8
	// CompatibilityFlags is general_profile_compatibility_flag[0..31],
	// flag 0 in the most significant bit.
	CompatibilityFlags uint32
	// ConstraintFlags is the 48 bits from general_progressive_source_flag
	// on, in the low 48 bits.
	ConstraintFlags uint64
	LevelIDC        uint8
}

// parsePTL reads profile_tier_level(1, maxSubLayersMinus1).
func (p *ProfileTierLevel) parse(r *bitio.Reader, maxSubLayersMinus1 int) {
	p.ProfileSpace = uint8(r.Bits(2))
	p.Tier = uint8(r.Bits(1))
	p.ProfileIDC = uint8(r.Bits(5))
	p.CompatibilityFlags = r.Bits(32)
	p.ConstraintFlags = r.Bits64(48)
	p.LevelIDC = uint8(r.Bits(8))
	var profile, level [8]bool
	for i := range maxSubLayersMinus1 {
		profile[i] = r.Flag()
		level[i] = r.Flag()
	}
	if maxSubLayersMinus1 > 0 {
		r.Skip(2 * (8 - maxSubLayersMinus1)) // reserved_zero_2bits
	}
	for i := range maxSubLayersMinus1 {
		if profile[i] {
			r.Skip(88)
		}
		if level[i] {
			r.Skip(8)
		}
	}
}

// SPS holds the fields of a sequence parameter set (section 7.3.2.2) up to
// the bit depths: profile, tier and level, chroma format and the picture
// size after the conformance window.
type SPS struct {
	VPSID             uint8
	MaxSubLayers      uint8
	TemporalIDNesting bool
	ProfileTierLevel
	ID uint32
	// ChromaFormatIDC is 0 for monochrome, 1 for 4:2:0, 2 for 4:2:2 and 3
	// for 4:4:4.
	ChromaFormatIDC     uint8
	SeparateColourPlane bool
	// PicWidth and PicHeight are the coded size in luma samples.
	PicWidth, PicHeight uint32
	// The conformance window offsets, in chroma sample units.
	ConfLeft, ConfRight, ConfTop, ConfBottom uint32
	// Width and Height are the picture size after the conformance window,
	// what a player displays.
	Width, Height  int
	BitDepthLuma   uint8
	BitDepthChroma uint8
	// MaxNumReorderPics is sps_max_num_reorder_pics of the highest
	// sub-layer: how many pictures may precede one in decode order and
	// follow it in output order, which a depacketizer needs to recover
	// decode times from presentation times. It is -1 when the SPS is cut
	// short before it.
	MaxNumReorderPics int
}

// Unmarshal parses the SPS NAL unit nalu, header included. It keeps
// nothing of nalu.
func (s *SPS) Unmarshal(nalu []byte) error {
	if len(nalu) < 2 || Type(nalu) != NALSPS {
		return ErrNotSPS
	}
	var buf [256]byte
	rbsp := h264.RemoveEmulationPrevention(buf[:0], nalu[2:])
	r := bitio.NewReader(rbsp)
	*s = SPS{}
	s.VPSID = uint8(r.Bits(4))
	sub := int(r.Bits(3))
	if sub > 6 {
		return ErrSPS
	}
	s.MaxSubLayers = uint8(sub + 1)
	s.TemporalIDNesting = r.Flag()
	s.ProfileTierLevel.parse(&r, sub)
	s.ID = r.UE()
	cf := r.UE()
	if s.ID > 15 || cf > 3 {
		return ErrSPS
	}
	s.ChromaFormatIDC = uint8(cf)
	if cf == 3 {
		s.SeparateColourPlane = r.Flag()
	}
	s.PicWidth, s.PicHeight = r.UE(), r.UE()
	if r.Flag() { // conformance_window_flag
		s.ConfLeft, s.ConfRight, s.ConfTop, s.ConfBottom = r.UE(), r.UE(), r.UE(), r.UE()
	}
	bl, bc := r.UE(), r.UE()
	if r.Err() != nil || bl > 8 || bc > 8 {
		return ErrSPS
	}
	s.BitDepthLuma, s.BitDepthChroma = uint8(bl+8), uint8(bc+8)
	// Table 6-1: the chroma sample units of the window offsets.
	subW, subH := uint64(1), uint64(1)
	if !s.SeparateColourPlane && cf != 0 {
		if cf != 3 {
			subW = 2
		}
		if cf == 1 {
			subH = 2
		}
	}
	w, h := uint64(s.PicWidth), uint64(s.PicHeight)
	cw := subW * (uint64(s.ConfLeft) + uint64(s.ConfRight))
	ch := subH * (uint64(s.ConfTop) + uint64(s.ConfBottom))
	// 2^20 luma samples either way is far beyond any level.
	if w == 0 || h == 0 || w > 1<<20 || h > 1<<20 || cw >= w || ch >= h {
		return ErrSPS
	}
	s.Width, s.Height = int(w-cw), int(h-ch)
	s.MaxNumReorderPics = -1
	r.UE() // log2_max_pic_order_cnt_lsb_minus4
	first := sub
	if r.Flag() { // sps_sub_layer_ordering_info_present_flag
		first = 0
	}
	reorder := uint32(0)
	for i := first; i <= sub; i++ {
		r.UE() // sps_max_dec_pic_buffering_minus1
		reorder = r.UE()
		r.UE() // sps_max_latency_increase_plus1
	}
	if r.Err() == nil && reorder <= 16 {
		s.MaxNumReorderPics = int(reorder)
	}
	return nil
}

// VPS holds the fields of a video parameter set (section 7.3.2.1) the
// HEVCDecoderConfigurationRecord takes.
type VPS struct {
	ID                uint8
	MaxSubLayers      uint8
	TemporalIDNesting bool
	ProfileTierLevel
}

// Unmarshal parses the VPS NAL unit nalu, header included.
func (v *VPS) Unmarshal(nalu []byte) error {
	if len(nalu) < 2 || Type(nalu) != NALVPS {
		return ErrNotVPS
	}
	var buf [128]byte
	rbsp := h264.RemoveEmulationPrevention(buf[:0], nalu[2:])
	r := bitio.NewReader(rbsp)
	*v = VPS{}
	v.ID = uint8(r.Bits(4))
	r.Skip(8) // vps_base_layer_internal_flag, available_flag, max_layers_minus1
	sub := int(r.Bits(3))
	v.MaxSubLayers = uint8(sub + 1)
	v.TemporalIDNesting = r.Flag()
	r.Skip(16) // vps_reserved_0xffff_16bits
	v.ProfileTierLevel.parse(&r, min(sub, 6))
	if r.Err() != nil || sub > 6 {
		return ErrVPS
	}
	return nil
}
