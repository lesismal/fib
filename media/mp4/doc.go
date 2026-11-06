// Package mp4 reads MP4 files that are not fragmented (ISO/IEC 14496-12,
// the ISO base media file format, and the files ffmpeg, encoders and
// cameras write as .mp4, .m4v, .m4a and .mov): their tracks, codecs and
// sample tables, for serving a file's samples as they are stored, as HLS
// segments are cut from them, without reading the file whole.
//
// Open reads through an io.ReaderAt. It walks the top-level box headers to
// the moov, which may come before the media data (ffmpeg's -movflags
// +faststart) or after it (ffmpeg's default), and reads the moov's boxes
// but for the sample tables, of which it takes only where they are and how
// many entries they have. Each Track has its handler, language, timescale,
// edit list and its codec as an av.Track, read from the first sample entry
// by media/fmp4's ParseSampleEntry: avc1 and avc3 with avcC (H.264), hvc1
// and hev1 with hvcC (H.265), av01 with av1C (AV1), mp4a with esds (AAC)
// and Opus with dOps, Config being the configuration box's payload as
// package av has it. Other formats give av.CodecUnknown and keep their
// sample tables.
//
// Track.ReadTable reads the track's stts, ctts, stss, stsc, stsz or stz2,
// and stco or co64 in one pass, through a buffer of 16 KiB, into a Table:
// the tables in a compact form rather than an entry a sample, whose size,
// Track.TableBytes, Open already knows from the entry counts, so that a
// caller can bound the memory of the tables it keeps before it reads them.
// A Table finds a sample by index (Sample), walks samples in decode order
// (Samples), finds them by time (IndexAt, SeekKeyframe) and by sync sample
// (Keyframes, KeyframeBefore, KeyframeAfter); its samples carry their
// offset, size, decode time, composition offset, duration and sync flag, in
// ticks of the media's timescale. Table.Time applies the edit list as
// players do: the empty edits before the first that plays delay the
// media, and that edit's media time is presented at its start, so that
// ffmpeg's files with B-frames or AAC start presenting at 0. ReadSample
// reads a sample's data.
//
// Everything is synchronous over the io.ReaderAt, so a server reads files
// on a pool of its own, never on an event loop. Nothing in a file is
// trusted: a size, count or offset past the file or past the limits
// (MaxTracks, MaxBoxes, MaxSampleEntries, MaxEdits, MaxSamples) is an
// error, never a panic or an allocation the file's bytes do not account
// for. Every chunk's samples are checked to lie within the file.
//
// Samples are as the file stores them: H.264 and H.265 access units are
// NAL units each after a length of the size the configuration record says
// (4 bytes in ffmpeg's files, as package av expects), AV1 samples are OBUs
// without temporal delimiters, AAC and Opus samples are raw frames.
//
// Left out: fragmented files (ErrFragmented; media/fmp4 reads them),
// compressed moov (cmov), edit lists past the first edit that plays (Edits
// has them all), sample descriptions after the first, sample groups and
// encryption. Writing MP4 files is media/fmp4's.
package mp4
