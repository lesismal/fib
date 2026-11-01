// Package h264 handles the parts of H.264 (ITU-T H.264, ISO/IEC 14496-10)
// a streaming server needs without decoding video: the framing of NAL
// units, the sequence parameter set, and the configuration record that
// carries the parameter sets out of band.
//
// NAL units come in two framings. Annex B byte streams (MPEG-TS, raw .h264
// files, RTSP sources after depacketizing) separate them with 3- or 4-byte
// start codes: SplitAnnexB walks them. ISO BMFF, FLV and Matroska prefix
// each with its length (AVCC): NextAVCC and SplitAVCC walk lengths of 1, 2
// or 4 bytes. AnnexBToAVCC, AVCCToAnnexB and NormalizeAVCC convert between
// the two, appending into a buffer the caller gives, so a muxer reusing its
// buffer converts without allocating. The canonical form of
// av.Packet.Data is AVCC with 4-byte lengths. The framing is the same for
// H.265, whose package uses these functions.
//
// SPS.Unmarshal reads a sequence parameter set (section 7.3.2.1.1) for the
// profile, level, chroma format, bit depth and the picture size after frame
// cropping, through the scaling matrices of the High profiles, and from
// its VUI (Annex E) the sample aspect ratio, colour description, timing
// and the number of reordered frames. AVCConfig parses and builds the
// AVCDecoderConfigurationRecord of ISO/IEC 14496-15 section 5.3.3.1,
// av.Track.Config for H.264, including the chroma and bit depth fields that
// follow it for the High profiles. PrependParameterSets puts parameter sets
// in band before a keyframe, for muxers whose format has nowhere else for
// them.
//
// Left out: the picture parameter set and slice headers (nothing here
// needs them), SVC and MVC beyond their NAL unit types, and the subset
// SPS. Emulation prevention bytes are removed before an SPS is read;
// RemoveEmulationPrevention and AddEmulationPrevention do it for callers
// that parse or build other NAL units.
package h264
