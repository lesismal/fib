// Package flv reads and writes FLV (Adobe "Video File Format Specification"
// version 10.1, annex E) and the Enhanced RTMP extensions to its tags
// (Veovera "Enhanced RTMP" v1 and v2), which RTMP carries as messages and
// HTTP-FLV as a file that never ends.
//
// The package works at three levels:
//
//   - Tag bodies. ParseVideoTag and ParseAudioTag read the header of a video
//     or audio tag body, the legacy one (codec id 7 for H.264, and 12 for
//     H.265 as ffmpeg's Chinese forks, SRS and OBS builds in China write it;
//     sound formats 10 AAC, 7 and 8 G.711, 2 MP3) and the enhanced one
//     (FourCC avc1, hvc1, av01 for video, Opus, mp4a and .mp3 for audio,
//     packet types SequenceStart, CodedFrames, CodedFramesX, SequenceEnd and
//     Metadata, and the ModEx timestamp offset of v2). AppendVideoTag and
//     AppendAudioTag write them. ParseScript reads script data, such as
//     onMetaData.
//   - Streams. Encoder turns an av stream (tracks and packets) into tag
//     bodies: onMetaData, a sequence header per track, then a body per
//     packet. Decoder does the reverse, holding packets back until it knows
//     the stream's tracks, so that what it reports is a complete []*av.Track
//     followed by packets that index it. RTMP uses both on its message
//     bodies.
//   - Files. Muxer frames what Encoder makes into an FLV file: the header,
//     then tags with their previous-tag-size trailers, appended to a buffer
//     the caller gives. Demuxer splits a byte stream fed to it in pieces of
//     any size back into tags, holding at most one tag.
//
// Timestamps are 32-bit milliseconds (24 bits and an extension byte in the
// tag header); Decoder unwraps them into av's time.Duration and adds the
// composition time offset of H.264 and H.265 for the presentation time.
// Encoder writes H.265 with the enhanced header (hvc1) unless
// EncoderConfig.LegacyHEVC asks for codec id 12, which players that predate
// Enhanced RTMP (ffmpeg before 6.1 with the Chinese patches, flv.js forks)
// expect, and always writes Opus and AV1 enhanced, having no legacy form.
//
// Left out: the multitrack packet types of Enhanced RTMP v2 (a tag that
// uses them is reported as ErrMultitrack and skipped by Decoder), VP8, VP9
// and the codecs of the original specification other than those listed
// (Sorenson H.263, VP6, Screen Video, Nellymoser, Speex, ADPCM: their tags
// parse, with Codec left unknown, and Decoder drops them), and FLV's
// encryption headers.
package flv
