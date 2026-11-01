// Package hls serves the streams of a media hub over HTTP Live Streaming
// (RFC 8216), with the Low-Latency extensions of its second edition
// (draft-pantos-hls-rfc8216bis: partial segments, preload hints, blocking
// playlist reload).
//
// Muxer is a hub.Sink that cuts a stream into segments held in memory: a
// stream with video at the first keyframe TargetDuration after the
// segment's start, an audio-only stream on time. It keeps a sliding window
// of PlaylistSize segments in the playlist and KeepSegments more for clients
// that read the playlist just before a segment left it, all under MaxBytes:
// old segments go first, and a segment that outgrows a quarter of the bound
// (a stream whose keyframes stop coming) is cut without one. A republish or
// a change of codec configuration (OnTracks again), or a jump in the
// publisher's clock, ends the segment being written and continues after an
// EXT-X-DISCONTINUITY, with a new EXT-X-MAP when the tracks changed;
// EXT-X-DISCONTINUITY-SEQUENCE counts those that left the playlist. With
// Config.PartDuration set, segments are also written in parts, and the
// media playlist carries EXT-X-SERVER-CONTROL (CAN-BLOCK-RELOAD,
// PART-HOLD-BACK of three parts), EXT-X-PART-INF, EXT-X-PART for the
// segments of the last three target durations and EXT-X-PRELOAD-HINT for
// the next part.
//
// Segments are written by a SegmentWriter of a Format. FMP4 is the
// implementation here, fragmented MP4 segments of package fmp4 with an
// EXT-X-MAP initialization section and a version 7 playlist, for H.264,
// H.265, AV1, AAC and Opus; other tracks are left out. MPEG-TS segments
// plug in as another Format whose SegmentWriter wraps package mpegts; see
// SegmentWriter for what it has to do.
//
// When a stream has both audio and video, its index.m3u8 is a multivariant
// playlist of one variant whose CODECS come from the tracks (CodecString),
// with the peak segment bit rate as BANDWIDTH and the picture size as
// RESOLUTION, naming media.m3u8; otherwise index.m3u8 is the media playlist.
//
// Handler is the fib/http glue: it starts a Muxer for a stream on the first
// playlist request (404 when the hub has no such stream), stops it after
// IdleTimeout without requests, answers requests that must wait (the first
// segment, a blocking reload, a hinted part) from the goroutine that
// completes what they wait for, with the request retained rather than a
// worker held, and sets Content-Type, Cache-Control and optionally CORS.
//
// Left out: delta playlist updates (_HLS_skip, EXT-X-SKIP) and rendition
// reports, byte-range parts, EXT-X-PROGRAM-DATE-TIME, multiple variants
// (no transcoding), I-frame playlists, encryption, and TS segments until
// package mpegts provides the writer. EXT-X-TARGETDURATION is not known
// before the segments are: it starts at TargetDuration rounded up and is
// raised if a segment comes out longer (a keyframe interval longer than
// the target), which strictly the specification does not allow; set
// TargetDuration to the keyframe interval to avoid it.
package hls
