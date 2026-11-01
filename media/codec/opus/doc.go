// Package opus handles what a streaming server needs of Opus without
// decoding audio: the OpusHead identification header that describes a
// track, and the duration of a packet.
//
// Head parses and builds OpusHead (RFC 7845 section 5.1), with the channel
// mapping table of families other than 0: what Ogg carries as its first
// packet, what MP4's dOps box and MPEG-TS's Opus descriptor are made from,
// and av.Track.Config for Opus. FrameCount, FrameDuration and
// PacketDuration read a packet's TOC byte and frame count byte (RFC 6716
// section 3.1 and 3.2) for the timing of muxers that need it, such as
// MPEG-TS and fMP4, since Opus packets carry no timestamps of their own.
//
// Left out: the OpusTags comment header, the self-delimiting framing of
// multistream packets after the first stream, and the padding and frame
// lengths of code 3 packets, which nothing here needs.
package opus
