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
// Segments are written by a SegmentWriter of a Format, of which there are
// two. FMP4 (the default) writes fragmented MP4 segments of package fmp4
// with an EXT-X-MAP initialization section and a version 7 playlist, for
// H.264, H.265, AV1, AAC and Opus. MPEGTS writes MPEG-2 transport stream
// segments of package mpegts, each starting with its PAT and PMT and
// standing alone, in a version 3 playlist, for H.264, H.265, AAC and
// Opus: the format of players and devices older than fMP4 in HLS. Either
// leaves out the tracks it cannot carry, and either may be written in
// Low-Latency parts. Another format plugs in by implementing
// SegmentWriter.
//
// When a stream has both audio and video, its index.m3u8 is a multivariant
// playlist of one variant whose CODECS come from the tracks (CodecString),
// with the peak segment bit rate as BANDWIDTH and the picture size as
// RESOLUTION, naming media.m3u8; otherwise index.m3u8 is the media playlist.
//
// Handler is the fib/http glue: it starts a Muxer for a stream on the first
// playlist request (404 when the hub has no publisher for it, unless
// HandlerConfig.PlayWait lets the request wait for one), stops it after
// IdleTimeout without requests, answers requests that must wait (the first
// segment, a blocking reload, a hinted part) from the goroutine that
// completes what they wait for, with the request retained rather than a
// worker held, and sets Content-Type, Cache-Control and optionally CORS.
// HandlerConfig.Muxer.Format is the format a stream is served in;
// HandlerConfig.Formats lists others a player may pick with a format query
// parameter on the playlist URL, as index.m3u8?format=ts, each format
// played running a Muxer of its own:
//
//	h := hls.NewHandler(hls.HandlerConfig{
//		Hub:     hb,
//		Formats: []hls.Format{hls.MPEGTS},
//	})
//	// /live/s/index.m3u8 is fMP4, /live/s/index.m3u8?format=ts MPEG-TS
//
// With Config.Encryption (or HandlerConfig.Encrypt, per stream), a Muxer
// encrypts its segments with AES-128 (METHOD=AES-128: CBC, PKCS#7 padding,
// the IV the segment's media sequence number) under keys the caller
// supplies, a new one every Encryption.Rotation segments; each Low-Latency
// part is encrypted on its own with its segment's key and IV.
// Initialization sections stay clear. The encryption, and the asking for
// keys, run neither under the Muxer's lock nor on the publisher's
// goroutine, but on one of their own (or Encryption.Run's pool): a segment
// or part is listed and served once encrypted, a little later than it is
// cut, and no request waits on a cipher. The media
// playlist names each key by the caller's URI in an EXT-X-KEY where the key
// changes, before a segment's first part, with METHOD=NONE before an
// EXT-X-MAP that follows a key. The Muxer never serves, prints or keeps a
// key but as its cipher: the caller answers the key URIs, with its own
// access control, checking a request against Handler.SessionMuxer and
// Muxer.KeyEpoch:
//
//	h := hls.NewHandler(hls.HandlerConfig{
//		Hub: hb,
//		Encrypt: func(path string) (*hls.Encryption, error) {
//			return &hls.Encryption{Rotation: 10, Key: func(session string, epoch uint64) ([16]byte, string, error) {
//				return deriveKey(path, session, epoch), "key-" + session + "-" + strconv.FormatUint(epoch, 10) + ".key", nil
//			}}, nil
//		},
//	})
//
// Left out: delta playlist updates (_HLS_skip, EXT-X-SKIP) and rendition
// reports, byte-range parts, EXT-X-PROGRAM-DATE-TIME, multiple variants
// (no transcoding), I-frame playlists, and encryption other than AES-128
// (SAMPLE-AES, key formats other than identity, an IV other than the media
// sequence number).
// EXT-X-TARGETDURATION is not known before the segments are: it starts at
// TargetDuration rounded up and is raised if a segment comes out longer (a
// keyframe interval longer than the target), which strictly the
// specification does not allow; set TargetDuration to the keyframe
// interval to avoid it.
package hls
