package av1

import (
	"errors"

	"github.com/lesismal/fib/media/internal/bitio"
)

var (
	ErrNotSequenceHeader = errors.New("av1: OBU is not a sequence header")
	ErrSequenceHeader    = errors.New("av1: malformed sequence header")
)

// SequenceHeader holds the fields of a sequence header OBU (section 5.5)
// a muxer or player needs: profile, the level and tier of the first
// operating point, the maximum picture size, timing and the color
// configuration.
type SequenceHeader struct {
	// Profile is seq_profile: 0 (Main), 1 (High) or 2 (Professional).
	Profile                   uint8
	StillPicture              bool
	ReducedStillPictureHeader bool
	// Level and Tier are seq_level_idx[0] and seq_tier[0], of the first
	// operating point, which a decoder that does not choose one decodes.
	Level uint8
	Tier  uint8
	// InitialDisplayDelay is initial_display_delay_minus_1[0] + 1, 0 when
	// absent.
	InitialDisplayDelay uint8

	// NumUnitsInDisplayTick and TimeScale are the timing information, 0
	// when absent.
	NumUnitsInDisplayTick uint32
	TimeScale             uint32

	// Width and Height are max_frame_width_minus_1 + 1 and
	// max_frame_height_minus_1 + 1: the size of every frame of the
	// sequence, short of frame size overrides and superres.
	Width, Height int

	// The color configuration of section 5.5.2. BitDepth is 8, 10 or 12;
	// the color description fields are 2 (unspecified) when absent.
	BitDepth                uint8
	Monochrome              bool
	ColorPrimaries          uint8
	TransferCharacteristics uint8
	MatrixCoefficients      uint8
	ColorRange              bool
	SubsamplingX            bool
	SubsamplingY            bool
	ChromaSamplePosition    uint8
}

// Unmarshal reads a sequence header OBU, with or without its size field.
func (s *SequenceHeader) Unmarshal(obu []byte) error {
	o, _, err := NextOBU(obu)
	if err != nil {
		return ErrSequenceHeader
	}
	if o.Type() != OBUSequenceHeader {
		return ErrNotSequenceHeader
	}
	return s.parse(o.Payload)
}

// parse reads the payload of a sequence header OBU.
func (s *SequenceHeader) parse(b []byte) error {
	*s = SequenceHeader{}
	r := bitio.NewReader(b)
	s.Profile = uint8(r.Bits(3))
	s.StillPicture = r.Flag()
	s.ReducedStillPictureHeader = r.Flag()
	if s.ReducedStillPictureHeader {
		s.Level = uint8(r.Bits(5))
	} else {
		s.readOperatingPoints(&r)
	}
	wbits, hbits := int(r.Bits(4))+1, int(r.Bits(4))+1
	s.Width, s.Height = int(r.Bits(wbits))+1, int(r.Bits(hbits))+1
	if !s.ReducedStillPictureHeader && r.Flag() { // frame_id_numbers_present_flag
		r.Skip(4 + 3)
	}
	r.Skip(3) // use_128x128_superblock, enable_filter_intra, enable_intra_edge_filter
	if !s.ReducedStillPictureHeader {
		r.Skip(4) // enable_interintra_compound, masked_compound, warped_motion, dual_filter
		orderHint := r.Flag()
		if orderHint {
			r.Skip(2) // enable_jnt_comp, enable_ref_frame_mvs
		}
		forceScreenContent := uint32(2) // SELECT_SCREEN_CONTENT_TOOLS
		if !r.Flag() {                  // seq_choose_screen_content_tools
			forceScreenContent = r.Bit()
		}
		if forceScreenContent > 0 && !r.Flag() { // seq_choose_integer_mv
			r.Skip(1) // seq_force_integer_mv
		}
		if orderHint {
			r.Skip(3) // order_hint_bits_minus_1
		}
	}
	r.Skip(3) // enable_superres, enable_cdef, enable_restoration
	s.readColorConfig(&r)
	r.Skip(1) // film_grain_params_present
	if r.Err() != nil {
		return ErrSequenceHeader
	}
	return nil
}

// readOperatingPoints reads timing_info, decoder_model_info and the
// operating points, keeping the level and tier of the first.
func (s *SequenceHeader) readOperatingPoints(r *bitio.Reader) {
	decoderModel := false
	bufferDelayLength := 0
	if r.Flag() { // timing_info_present_flag
		s.NumUnitsInDisplayTick, s.TimeScale = r.Bits(32), r.Bits(32)
		if r.Flag() { // equal_picture_interval
			readUVLC(r)
		}
		decoderModel = r.Flag()
		if decoderModel {
			bufferDelayLength = int(r.Bits(5)) + 1
			r.Skip(32 + 5 + 5) // num_units_in_decoding_tick, two lengths
		}
	}
	displayDelay := r.Flag()
	count := int(r.Bits(5)) + 1
	for i := range count {
		r.Skip(12) // operating_point_idc
		level := uint8(r.Bits(5))
		var tier uint8
		if level > 7 {
			tier = uint8(r.Bit())
		}
		if decoderModel && r.Flag() {
			r.Skip(2*bufferDelayLength + 1)
		}
		var delay uint8
		if displayDelay && r.Flag() {
			delay = uint8(r.Bits(4)) + 1
		}
		if i == 0 {
			s.Level, s.Tier, s.InitialDisplayDelay = level, tier, delay
		}
	}
}

// readColorConfig reads color_config (section 5.5.2).
func (s *SequenceHeader) readColorConfig(r *bitio.Reader) {
	s.BitDepth = 8
	if r.Flag() { // high_bitdepth
		s.BitDepth = 10
		if s.Profile == 2 && r.Flag() { // twelve_bit
			s.BitDepth = 12
		}
	}
	if s.Profile != 1 {
		s.Monochrome = r.Flag()
	}
	s.ColorPrimaries, s.TransferCharacteristics, s.MatrixCoefficients = 2, 2, 2
	if r.Flag() { // color_description_present_flag
		s.ColorPrimaries, s.TransferCharacteristics, s.MatrixCoefficients =
			uint8(r.Bits(8)), uint8(r.Bits(8)), uint8(r.Bits(8))
	}
	switch {
	case s.Monochrome:
		s.ColorRange = r.Flag()
		s.SubsamplingX, s.SubsamplingY = true, true
		return
	case s.ColorPrimaries == 1 && s.TransferCharacteristics == 13 && s.MatrixCoefficients == 0:
		// sRGB: 4:4:4, full range.
		s.ColorRange = true
	default:
		s.ColorRange = r.Flag()
		switch s.Profile {
		case 0:
			s.SubsamplingX, s.SubsamplingY = true, true
		case 1:
		default:
			s.SubsamplingX = true
			if s.BitDepth == 12 {
				s.SubsamplingX = r.Flag()
				if s.SubsamplingX {
					s.SubsamplingY = r.Flag()
				}
			}
		}
		if s.SubsamplingX && s.SubsamplingY {
			s.ChromaSamplePosition = uint8(r.Bits(2))
		}
	}
	r.Skip(1) // separate_uv_delta_q
}

// readUVLC reads a uvlc() (section 4.10.3).
func readUVLC(r *bitio.Reader) uint32 {
	zeros := 0
	for r.Err() == nil && !r.Flag() {
		zeros++
	}
	if zeros >= 32 {
		return 1<<32 - 1
	}
	return r.Bits(zeros) + (1 << zeros) - 1
}
