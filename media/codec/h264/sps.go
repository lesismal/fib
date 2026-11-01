package h264

import (
	"errors"

	"github.com/lesismal/fib/media/internal/bitio"
)

var (
	ErrNotSPS = errors.New("h264: NAL unit is not an SPS")
	ErrSPS    = errors.New("h264: malformed SPS")
)

// SPS holds the fields of a sequence parameter set (ITU-T H.264 section
// 7.3.2.1.1) a muxer or player needs: profile and level, chroma format and
// bit depth, the picture size after frame cropping, and from the VUI the
// sample aspect ratio, colour description, timing and reordering.
type SPS struct {
	ProfileIDC uint8
	// ConstraintFlags is the byte of constraint_set0_flag to
	// constraint_set5_flag and the two reserved bits, as the
	// AVCDecoderConfigurationRecord's profile_compatibility carries it.
	ConstraintFlags uint8
	LevelIDC        uint8
	ID              uint32

	// ChromaFormatIDC is 0 for monochrome, 1 for 4:2:0 (the default when
	// the profile does not signal it), 2 for 4:2:2 and 3 for 4:4:4.
	ChromaFormatIDC     uint8
	SeparateColourPlane bool
	BitDepthLuma        uint8
	BitDepthChroma      uint8

	Log2MaxFrameNum       uint8
	PicOrderCntType       uint8
	Log2MaxPicOrderCntLsb uint8
	MaxNumRefFrames       uint32
	FrameMbsOnly          bool

	// PicWidthInMbs and PicHeightInMapUnits are the coded size in
	// macroblocks; a map unit is a macroblock pair when FrameMbsOnly is
	// false.
	PicWidthInMbs       uint32
	PicHeightInMapUnits uint32
	// The frame cropping offsets, in the units of section 7.4.2.1.1.
	CropLeft, CropRight, CropTop, CropBottom uint32

	// Width and Height are the decoded picture size in luma samples after
	// cropping, what a player displays (before the sample aspect ratio).
	Width, Height int

	// VUI says the SPS carries VUI parameters (Annex E). A VUI cut short is
	// tolerated, as decoders tolerate it, and leaves the fields after the
	// cut at zero.
	VUI bool
	// SarWidth and SarHeight are the sample aspect ratio, 0 when unknown.
	SarWidth, SarHeight uint16
	// VideoFullRange and the colour description of section E.2.1; the
	// colour fields are 2 (unspecified) when absent.
	VideoFullRange          bool
	ColourPrimaries         uint8
	TransferCharacteristics uint8
	MatrixCoefficients      uint8
	// NumUnitsInTick and TimeScale are the timing information, 0 when
	// absent; FixedFrameRate is fixed_frame_rate_flag.
	NumUnitsInTick uint32
	TimeScale      uint32
	FixedFrameRate bool
	// MaxNumReorderFrames is from the bitstream restriction: 0 means the
	// stream has no B-frame reordering. It is -1 when not signalled.
	MaxNumReorderFrames int
}

// Table E-1, the sample aspect ratios of aspect_ratio_idc 1 to 16.
var sarTable = [17][2]uint16{
	{0, 0}, {1, 1}, {12, 11}, {10, 11}, {16, 11}, {40, 33}, {24, 11}, {20, 11}, {32, 11},
	{80, 33}, {18, 11}, {15, 11}, {64, 33}, {160, 99}, {4, 3}, {3, 2}, {2, 1},
}

// hasChromaInfo reports whether an SPS of profile p carries
// chroma_format_idc and the fields after it.
func hasChromaInfo(p uint8) bool {
	switch p {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		return true
	}
	return false
}

// Unmarshal parses the SPS NAL unit nalu, header byte included. It keeps
// nothing of nalu.
func (s *SPS) Unmarshal(nalu []byte) error {
	if Type(nalu) != NALSPS {
		return ErrNotSPS
	}
	var buf [256]byte
	rbsp := RemoveEmulationPrevention(buf[:0], nalu[1:])
	r := bitio.NewReader(rbsp)
	*s = SPS{ChromaFormatIDC: 1, BitDepthLuma: 8, BitDepthChroma: 8,
		ColourPrimaries: 2, TransferCharacteristics: 2, MatrixCoefficients: 2,
		MaxNumReorderFrames: -1}

	s.ProfileIDC = uint8(r.Bits(8))
	s.ConstraintFlags = uint8(r.Bits(8))
	s.LevelIDC = uint8(r.Bits(8))
	s.ID = r.UE()
	if s.ID > 31 {
		return ErrSPS
	}
	if hasChromaInfo(s.ProfileIDC) {
		cf := r.UE()
		if cf > 3 {
			return ErrSPS
		}
		s.ChromaFormatIDC = uint8(cf)
		if cf == 3 {
			s.SeparateColourPlane = r.Flag()
		}
		bl, bc := r.UE(), r.UE()
		if bl > 6 || bc > 6 {
			return ErrSPS
		}
		s.BitDepthLuma, s.BitDepthChroma = uint8(bl+8), uint8(bc+8)
		// qpprime_y_zero_transform_bypass_flag, then
		// seq_scaling_matrix_present_flag.
		r.Skip(1)
		if r.Flag() {
			n := 8
			if cf == 3 {
				n = 12
			}
			for i := range n {
				if r.Flag() {
					size := 16
					if i >= 6 {
						size = 64
					}
					skipScalingList(&r, size)
				}
			}
		}
	}
	lmfn := r.UE()
	if lmfn > 12 {
		return ErrSPS
	}
	s.Log2MaxFrameNum = uint8(lmfn + 4)
	poc := r.UE()
	switch poc {
	case 0:
		l := r.UE()
		if l > 12 {
			return ErrSPS
		}
		s.Log2MaxPicOrderCntLsb = uint8(l + 4)
	case 1:
		r.Skip(1) // delta_pic_order_always_zero_flag
		r.SE()    // offset_for_non_ref_pic
		r.SE()    // offset_for_top_to_bottom_field
		n := r.UE()
		if n > 255 {
			return ErrSPS
		}
		for range n {
			r.SE()
		}
	case 2:
	default:
		return ErrSPS
	}
	s.PicOrderCntType = uint8(poc)
	s.MaxNumRefFrames = r.UE()
	r.Skip(1) // gaps_in_frame_num_value_allowed_flag
	w, h := r.UE(), r.UE()
	// 2^16 macroblocks either way is far beyond any level.
	if w >= 1<<16 || h >= 1<<16 {
		return ErrSPS
	}
	s.PicWidthInMbs, s.PicHeightInMapUnits = w+1, h+1
	s.FrameMbsOnly = r.Flag()
	if !s.FrameMbsOnly {
		r.Skip(1) // mb_adaptive_frame_field_flag
	}
	r.Skip(1) // direct_8x8_inference_flag
	if r.Flag() {
		s.CropLeft, s.CropRight, s.CropTop, s.CropBottom = r.UE(), r.UE(), r.UE(), r.UE()
	}
	s.VUI = r.Flag()
	if r.Err() != nil {
		return ErrSPS
	}

	// Section 7.4.2.1.1: the cropping units follow ChromaArrayType.
	fieldMul := uint64(1)
	if !s.FrameMbsOnly {
		fieldMul = 2
	}
	cropX, cropY := uint64(1), fieldMul
	if !s.SeparateColourPlane && s.ChromaFormatIDC != 0 {
		if s.ChromaFormatIDC != 3 {
			cropX = 2
		}
		if s.ChromaFormatIDC == 1 {
			cropY *= 2
		}
	}
	width := uint64(s.PicWidthInMbs) * 16
	height := uint64(s.PicHeightInMapUnits) * 16 * fieldMul
	cw := cropX * (uint64(s.CropLeft) + uint64(s.CropRight))
	ch := cropY * (uint64(s.CropTop) + uint64(s.CropBottom))
	if cw >= width || ch >= height {
		return ErrSPS
	}
	s.Width, s.Height = int(width-cw), int(height-ch)

	if s.VUI {
		s.parseVUI(&r)
	}
	return nil
}

// skipScalingList reads past scaling_list() of section 7.3.2.1.1.1.
func skipScalingList(r *bitio.Reader, size int) {
	last, next := int32(8), int32(8)
	for range size {
		if next != 0 {
			delta := r.SE()
			if r.Err() != nil {
				return
			}
			next = (last + delta + 256) % 256
		}
		if next != 0 {
			last = next
		}
	}
}

// parseVUI reads vui_parameters() of section E.1.1 as far as the data
// goes.
func (s *SPS) parseVUI(r *bitio.Reader) {
	if r.Flag() { // aspect_ratio_info_present_flag
		idc := r.Bits(8)
		if idc == 255 {
			s.SarWidth, s.SarHeight = uint16(r.Bits(16)), uint16(r.Bits(16))
		} else if idc < uint32(len(sarTable)) {
			s.SarWidth, s.SarHeight = sarTable[idc][0], sarTable[idc][1]
		}
	}
	if r.Flag() { // overscan_info_present_flag
		r.Skip(1)
	}
	if r.Flag() { // video_signal_type_present_flag
		r.Skip(3) // video_format
		s.VideoFullRange = r.Flag()
		if r.Flag() { // colour_description_present_flag
			s.ColourPrimaries = uint8(r.Bits(8))
			s.TransferCharacteristics = uint8(r.Bits(8))
			s.MatrixCoefficients = uint8(r.Bits(8))
		}
	}
	if r.Flag() { // chroma_loc_info_present_flag
		r.UE()
		r.UE()
	}
	if r.Flag() { // timing_info_present_flag
		s.NumUnitsInTick = r.Bits(32)
		s.TimeScale = r.Bits(32)
		s.FixedFrameRate = r.Flag()
	}
	nal, vcl := r.Flag(), false
	if nal {
		skipHRD(r)
	}
	vcl = r.Flag()
	if vcl {
		skipHRD(r)
	}
	if nal || vcl {
		r.Skip(1) // low_delay_hrd_flag
	}
	// pic_struct_present_flag, then bitstream_restriction_flag with
	// motion_vectors_over_pic_boundaries_flag and the four codes from
	// max_bytes_per_pic_denom to log2_max_mv_length_vertical.
	r.Skip(1)
	if r.Flag() {
		r.Skip(1)
		for range 4 {
			r.UE()
		}
		reorder := r.UE()
		r.UE() // max_dec_frame_buffering
		if r.Err() == nil && reorder <= 16 {
			s.MaxNumReorderFrames = int(reorder)
		}
	}
	if r.Err() != nil {
		// Cut short: keep what came before the cut in the fields above,
		// but nothing read from past the end.
		if s.NumUnitsInTick == 0 || s.TimeScale == 0 {
			s.NumUnitsInTick, s.TimeScale = 0, 0
		}
	}
}

// skipHRD reads past hrd_parameters() of section E.1.2.
func skipHRD(r *bitio.Reader) {
	n := r.UE() // cpb_cnt_minus1
	if n > 31 {
		r.Skip(r.Left() + 1) // malformed: stop reading
		return
	}
	r.Skip(8) // bit_rate_scale, cpb_size_scale
	for range n + 1 {
		r.UE()
		r.UE()
		r.Skip(1)
	}
	r.Skip(20) // four 5-bit lengths
}

// FrameRate returns the frame rate the VUI timing signals, time_scale /
// (2 * num_units_in_tick), or 0 when the SPS carries no timing.
func (s *SPS) FrameRate() float64 {
	if s.NumUnitsInTick == 0 || s.TimeScale == 0 {
		return 0
	}
	return float64(s.TimeScale) / float64(2*uint64(s.NumUnitsInTick))
}
