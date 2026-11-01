// Package fmp4 writes and reads fragmented MP4 (ISO/IEC 14496-12, the ISO
// base media file format), the container of HLS fMP4 segments, DASH and
// CMAF: an init segment describing the tracks, then movie fragments holding
// their samples.
//
// AppendInit writes the init segment: an ftyp with the iso5, iso6 and mp41
// brands and a moov whose tracks have empty sample tables and whose mvex
// says the samples come in fragments. The sample entries are avc1 with avcC
// (H.264, ISO/IEC 14496-15), hvc1 with hvcC (H.265), av01 with av1C (AV1,
// the AV1 ISOBMFF binding), mp4a with an esds holding the AudioSpecificConfig
// (AAC, ISO/IEC 14496-14) and Opus with dOps (Encapsulation of Opus in ISO
// BMFF): av.Track.Config is in each case the configuration box's payload,
// except Opus, whose OpusHead becomes dOps, and which may leave it nil.
// Fragment.Append writes a moof (mfhd, and for each track a traf with tfhd,
// tfdt and a version 1 trun of durations, sizes, sync flags and signed
// composition offsets) and its mdat; AppendStyp and Sidx.Append write the
// boxes DASH segments may start with.
//
// Writer is the sans-IO muxer on top: packets in, the init segment once and
// a fragment at each Flush out. Since a sample's duration is the distance to
// the next sample of its track, it holds the last packet of each track back
// until the next one, or a Flush told what comes next, arrives; see Writer
// for how jumps in the clock are taken. StartAt and End let a new Writer
// carry on where another ended, as HLS does after a discontinuity.
//
// ParseInit and Init.ParseFragment read init segments and fragments back
// into av.Tracks and av.Packets, for tests and for ingests that take fMP4;
// NextBox and Boxes walk boxes. The reader is lenient where players are and
// never trusts a size or count beyond the bytes it is given.
//
// Left out: edit lists (a stream with B-frames starts presenting a little
// after 0, which players take), encryption (cenc), sample groups and
// subsample information, the fragment random access box (mfra), and reading
// base data offsets, which are from the start of a file a fragment read on
// its own has no knowledge of: the reader takes offsets from the moof, as
// default-base-is-moof says and every fragmented MP4 writer in use does.
// G.711 and MP3 tracks are not carried.
package fmp4
