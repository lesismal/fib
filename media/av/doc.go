// Package av holds the types every package of fib's media stack shares: the
// codecs it knows (CodecID), the description of an elementary stream
// (Track) and one timed unit of it (Packet), with the conversions between
// time.Duration and the tick counts of a codec clock.
//
// Every protocol converts what it receives into one canonical form and
// muxes from that form, so a stream published over RTMP plays over RTSP,
// HLS or WebRTC with no codec-specific glue between the two protocols. The
// canonical form is the one ISO BMFF (MP4) uses:
//
// Track.Config is the codec's out-of-band configuration: the
// AVCDecoderConfigurationRecord for H.264 (ISO/IEC 14496-15 section
// 5.3.3.1), the HEVCDecoderConfigurationRecord for H.265 (section 8.3.3.1),
// the AV1CodecConfigurationRecord for AV1, the AudioSpecificConfig for AAC
// (ISO/IEC 14496-3 section 1.6.2.1), the OpusHead for Opus (RFC 7845
// section 5.1, optional), and nil for G.711. It is immutable once
// published: a change of configuration is a new []*Track.
//
// Packet.Data is one access unit or frame:
//
//   - H.264 and H.265: AVCC, the NAL units of one access unit each prefixed
//     by a 4-byte big-endian length, without access unit delimiters.
//     Parameter sets live in Track.Config, and muxers that need them in
//     band (MPEG-TS, RTP without sprop) insert them before keyframes, but
//     they may also appear in the data.
//   - AV1: a low-overhead OBU stream (one temporal unit) without the
//     temporal delimiter OBU.
//   - AAC: one raw access unit, without an ADTS header.
//   - Opus: one Opus packet (RFC 6716 section 3).
//   - G.711 (PCMU, PCMA): the 8-bit samples.
//
// Packet.Data is immutable once handed to hub.Publisher.WritePacket: every
// subscriber shares the same bytes.
//
// Times are time.Duration from the start of the stream (or any fixed epoch)
// rather than ticks of a codec clock, so tracks with different clocks
// compare directly. ToTicks and FromTicks round to the nearest tick and
// nanosecond, so a tick count converted to a Duration and back is the
// count it was for any clock rate up to 1 GHz.
package av
