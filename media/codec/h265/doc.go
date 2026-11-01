// Package h265 handles the parts of H.265 (HEVC, ITU-T H.265, ISO/IEC
// 23008-2) a streaming server needs without decoding video: NAL unit
// headers and types, keyframe detection, the sequence and video parameter
// sets, and the configuration record that carries the parameter sets out
// of band.
//
// NAL units are framed as in H.264, by Annex B start codes or by lengths
// (AVCC), so this package leaves the framing to package h264: SplitAnnexB,
// SplitAVCC, AnnexBToAVCC and the rest work on H.265 as they are. What
// differs is the 2-byte NAL unit header (ParseHeader, Type) and the types,
// among them VPS, SPS, PPS, AUD and SEI. An access unit is a keyframe when
// it holds an IRAP picture: BLA, IDR or CRA (IsKeyframe).
//
// SPS.Unmarshal reads a sequence parameter set (section 7.3.2.2) through
// profile_tier_level with its sub-layers, the chroma format, the picture
// size and conformance window, the bit depths, and
// sps_max_num_reorder_pics: what the HEVCDecoderConfigurationRecord, a
// player's picture size and an RTP depacketizer recovering decode times
// need. VPS
// reads the profile, tier and level and sub-layers of a video parameter
// set. HEVCConfig parses and builds the HEVCDecoderConfigurationRecord of
// ISO/IEC 14496-15 section 8.3.3.1, av.Track.Config for H.265, and
// NewHEVCConfig builds it from parameter sets as ffmpeg's MP4 muxer does.
//
// Left out: the SPS beyond the bit depths and the reordering count
// (short-term reference picture sets, scaling lists and the VUI, so no
// frame rate or sample aspect ratio), the picture parameter set and slice headers, and the
// multi-layer extensions (SHVC, MV-HEVC), whose layered NAL units pass
// through as any other.
package h265
