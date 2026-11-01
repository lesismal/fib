// Package av1 handles the parts of AV1 (the AV1 Bitstream & Decoding
// Process Specification) a streaming server needs without decoding video:
// the framing of OBUs, the sequence header, keyframe detection, and the
// configuration record that carries the sequence header out of band.
//
// An AV1 stream is a sequence of OBUs (open bitstream units), each a header
// of one or two bytes, an optional LEB128 size and a payload. The
// low-overhead bitstream format of section 5.2, which ISO BMFF, Matroska,
// FLV and av.Packet.Data use, gives every OBU its size; the RTP payload
// format leaves the size out, the packet framing giving it instead.
// NextOBU and SplitOBUs walk either, and AppendOBU writes an OBU with or
// without its size field. ReadLEB128 and AppendLEB128 read and write the
// sizes.
//
// SequenceHeader.Unmarshal reads a sequence header OBU (section 5.5) for
// the profile, the level and tier of the first operating point, the
// maximum picture size, and the color configuration. Config parses and
// builds the AV1CodecConfigurationRecord of the AV1 ISO BMFF binding
// (section 2.3), av.Track.Config for AV1, and NewConfig makes one of a
// sequence header. IsKeyframe tells a temporal unit that starts with a key
// frame, one a decoder can start from given the sequence header.
//
// Left out: the frame header beyond its frame type, tile data, metadata
// OBUs and the scalability structures of operating points other than the
// first.
package av1
