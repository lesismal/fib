package flv

import (
	"bytes"
	"time"

	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/codec/opus"
)

// Defaults for the DecoderConfig fields left at zero.
const (
	DefaultMaxPending = 512
	DefaultMaxHold    = time.Second
)

// DecoderConfig configures a Decoder.
type DecoderConfig struct {
	// OnTracks hears the stream's tracks: once they are known, and again
	// when a codec configuration changes or a track appears late. Packets
	// after it index the tracks it gave.
	OnTracks func(tracks []*av.Track)
	// OnPacket hears each packet. The packet and its data belong to the
	// callee.
	OnPacket func(pkt *av.Packet)
	// MaxPending and MaxHold bound what the Decoder holds back while it
	// waits to know every track: once it holds MaxPending packets, or
	// packets MaxHold apart, it reports the tracks it knows. Zero means
	// DefaultMaxPending and DefaultMaxHold.
	MaxPending int
	MaxHold    time.Duration
}

// Decoder turns FLV tag bodies (from a file, or RTMP messages) into an av
// stream. It learns the tracks from the sequence headers, and from
// onMetaData which tracks to wait for; until it has them all it holds the
// packets back, within DecoderConfig's bounds, so that the stream starts
// with every track and with the packets it would otherwise have lost.
type Decoder struct {
	config  DecoderConfig
	tracks  []*av.Track
	ready   bool
	pending []*av.Packet
	// kinds of the pending packets' tracks, before indices exist: video
	// and audio tracks found so far.
	video, audio       *av.Track
	videoIdx, audioIdx int
	lengthSize         int
	expectVideo        bool
	expectAudio        bool
	sawMeta            bool
	meta               amf.ECMAArray
	// Timestamps are unwrapped from 32 bits by their signed distance from
	// the last one.
	started bool
	last    uint32
	ext     int64
}

// NewDecoder returns a Decoder.
func NewDecoder(config DecoderConfig) *Decoder {
	if config.MaxPending <= 0 {
		config.MaxPending = DefaultMaxPending
	}
	if config.MaxHold <= 0 {
		config.MaxHold = DefaultMaxHold
	}
	if config.OnTracks == nil {
		config.OnTracks = func([]*av.Track) {}
	}
	if config.OnPacket == nil {
		config.OnPacket = func(*av.Packet) {}
	}
	return &Decoder{config: config, videoIdx: -1, audioIdx: -1}
}

// MetaData returns the properties of the last onMetaData the stream sent.
func (d *Decoder) MetaData() amf.ECMAArray { return d.meta }

// Tracks returns the tracks reported so far.
func (d *Decoder) Tracks() []*av.Track { return d.tracks }

func (d *Decoder) time(ts uint32) time.Duration {
	if !d.started {
		d.started, d.last, d.ext = true, ts, int64(ts)
	}
	d.ext += int64(int32(ts - d.last))
	d.last = ts
	return time.Duration(d.ext) * time.Millisecond
}

// WriteTag decodes the body of one tag. body may be reused once WriteTag
// returns. Tags of codecs FLV has but av does not, multitrack tags, and
// frames of a video track whose sequence header has not come are dropped;
// a body too short for its header fails with ErrShort, as does a sequence
// header that does not parse.
func (d *Decoder) WriteTag(typ uint8, timestamp uint32, body []byte) error {
	switch typ {
	case TagScript:
		name, args, err := ParseScript(body)
		if err != nil {
			return err
		}
		if name == "onMetaData" {
			d.metaData(MetaData(args))
		}
		return nil
	case TagVideo:
		v, err := ParseVideoTag(body)
		if err == ErrMultitrack {
			return nil
		}
		if err != nil {
			return err
		}
		return d.videoTag(timestamp, &v)
	case TagAudio:
		a, err := ParseAudioTag(body)
		if err == ErrMultitrack {
			return nil
		}
		if err != nil {
			return err
		}
		return d.audioTag(timestamp, &a)
	}
	return ErrTagType
}

func (d *Decoder) metaData(meta amf.ECMAArray) {
	d.meta = meta
	if d.sawMeta || d.ready {
		return
	}
	d.sawMeta = true
	if v, ok := meta.Number("videocodecid"); ok {
		switch uint32(v) {
		case VideoCodecAVC, VideoCodecHEVC, FourCCAVC, FourCCHEVC, FourCCAV1:
			d.expectVideo = true
		}
	}
	if a, ok := meta.Number("audiocodecid"); ok {
		switch uint32(a) {
		case SoundAAC, SoundPCMA, SoundPCMU, SoundMP3, FourCCOpus, FourCCAAC, FourCCMP3:
			d.expectAudio = true
		}
	}
}

func (d *Decoder) videoTag(ts uint32, v *VideoTag) error {
	if v.Codec == av.CodecUnknown || v.FrameType == FrameCommand {
		return nil
	}
	switch v.PacketType {
	case PacketSequenceStart:
		if len(v.Data) == 0 {
			// ffmpeg writes an empty AV1 sequence header before the
			// real one.
			return nil
		}
		t, lengthSize, err := videoTrack(v.Codec, v.Data)
		if err != nil {
			return err
		}
		if d.video != nil && d.video.Codec == t.Codec && bytes.Equal(d.video.Config, t.Config) {
			return nil
		}
		d.video, d.lengthSize = t, lengthSize
		d.trackChanged(true)
		return nil
	case PacketCodedFrames:
	default:
		return nil
	}
	if d.video == nil || d.video.Codec != v.Codec {
		return nil
	}
	data := v.Data
	var err error
	if v.Codec == av.CodecAV1 {
		data = append([]byte(nil), data...)
	} else {
		if data, err = h264.NormalizeAVCC(make([]byte, 0, len(data)+8), data, d.lengthSize); err != nil {
			return nil
		}
		data = data[audLen(v.Codec, data):]
	}
	if len(data) == 0 {
		return nil
	}
	dts := d.time(ts) + time.Duration(v.Nanos)
	d.packet(true, &av.Packet{
		DTS:      dts,
		PTS:      dts + time.Duration(v.CTS)*time.Millisecond,
		Keyframe: v.FrameType == FrameKey,
		Data:     data,
	})
	return nil
}

func (d *Decoder) audioTag(ts uint32, a *AudioTag) error {
	if a.Codec == av.CodecUnknown {
		return nil
	}
	switch a.PacketType {
	case PacketSequenceStart:
		if a.Codec != av.CodecAAC && a.Codec != av.CodecOpus {
			return nil
		}
		t, err := audioTrack(a, a.Data)
		if err != nil {
			return err
		}
		if d.audio != nil && d.audio.Codec == t.Codec && bytes.Equal(d.audio.Config, t.Config) {
			return nil
		}
		d.audio = t
		d.trackChanged(false)
		return nil
	case PacketCodedFrames:
	default:
		return nil
	}
	if d.audio == nil || d.audio.Codec != a.Codec {
		if a.Codec == av.CodecAAC {
			return nil
		}
		// The codecs without a sequence header start their track with
		// their first frame.
		t, _ := audioTrack(a, nil)
		d.audio = t
		d.trackChanged(false)
	}
	if len(a.Data) == 0 {
		return nil
	}
	dts := d.time(ts) + time.Duration(a.Nanos)
	d.packet(false, &av.Packet{DTS: dts, PTS: dts, Keyframe: true, Data: append([]byte(nil), a.Data...)})
	return nil
}

// trackChanged reports a new or changed track once the stream is under
// way, and checks whether the tracks are now complete before.
func (d *Decoder) trackChanged(video bool) {
	if !d.ready {
		d.checkReady(false)
		return
	}
	tracks := append([]*av.Track(nil), d.tracks...)
	idx, t := &d.audioIdx, d.audio
	if video {
		idx, t = &d.videoIdx, d.video
	}
	if *idx < 0 {
		*idx = len(tracks)
		tracks = append(tracks, t)
	} else {
		tracks[*idx] = t
	}
	d.tracks = tracks
	d.config.OnTracks(tracks)
}

func (d *Decoder) packet(video bool, pkt *av.Packet) {
	if d.ready {
		pkt.Track = d.audioIdx
		if video {
			pkt.Track = d.videoIdx
		}
		d.config.OnPacket(pkt)
		return
	}
	// Before the tracks are known a packet's Track is its kind: 0 for
	// video, 1 for audio.
	if !video {
		pkt.Track = 1
	}
	d.pending = append(d.pending, pkt)
	d.checkReady(false)
}

// checkReady reports the tracks, and the packets held back, once every
// track the stream announced has come, or the hold's bounds are reached,
// or force asks for it.
func (d *Decoder) checkReady(force bool) {
	if d.video == nil && d.audio == nil {
		if len(d.pending) >= d.config.MaxPending {
			d.pending = d.pending[:0]
		}
		return
	}
	done := d.video != nil && d.audio != nil
	if d.sawMeta {
		done = (!d.expectVideo || d.video != nil) && (!d.expectAudio || d.audio != nil)
	}
	if n := len(d.pending); n > 0 && (n >= d.config.MaxPending || d.pending[n-1].DTS-d.pending[0].DTS >= d.config.MaxHold) {
		done = true
	}
	if !done && !force {
		return
	}
	d.ready = true
	if d.video != nil {
		d.videoIdx = len(d.tracks)
		d.tracks = append(d.tracks, d.video)
	}
	if d.audio != nil {
		d.audioIdx = len(d.tracks)
		d.tracks = append(d.tracks, d.audio)
	}
	d.config.OnTracks(d.tracks)
	pending := d.pending
	d.pending = nil
	for _, pkt := range pending {
		idx := d.videoIdx
		if pkt.Track == 1 {
			idx = d.audioIdx
		}
		if idx < 0 {
			continue
		}
		pkt.Track = idx
		d.config.OnPacket(pkt)
	}
}

// Flush reports the tracks known and the packets held back, without
// waiting for the rest, as when the stream ends before they came.
func (d *Decoder) Flush() {
	if !d.ready {
		d.checkReady(true)
	}
}

// videoTrack builds the track a video sequence header describes, and the
// NAL unit length size of its frames. The picture size comes from the
// H.264 or H.265 SPS, or the AV1 sequence header.
func videoTrack(codec av.CodecID, config []byte) (*av.Track, int, error) {
	t := &av.Track{Codec: codec, Config: append([]byte(nil), config...)}
	switch codec {
	case av.CodecH264:
		var c h264.AVCConfig
		if err := c.Unmarshal(config); err != nil || len(c.SPS) == 0 {
			return nil, 0, ErrConfig
		}
		var sps h264.SPS
		if sps.Unmarshal(c.SPS[0]) == nil {
			t.Width, t.Height = sps.Width, sps.Height
		}
		return t, c.LengthSize, nil
	case av.CodecH265:
		var c h265.HEVCConfig
		if err := c.Unmarshal(config); err != nil {
			return nil, 0, ErrConfig
		}
		if s := c.NALUs(h265.NALSPS); len(s) > 0 {
			var sps h265.SPS
			if sps.Unmarshal(s[0]) == nil {
				t.Width, t.Height = sps.Width, sps.Height
			}
		}
		return t, c.LengthSize, nil
	case av.CodecAV1:
		var c av1.Config
		var s av1.SequenceHeader
		if c.Unmarshal(config) == nil && s.Unmarshal(c.SequenceHeader()) == nil {
			t.Width, t.Height = s.Width, s.Height
		}
	}
	return t, 0, nil
}

var mp3Rates = [4]int{5512, 11025, 22050, 44100}

// audioTrack builds the track an audio sequence header, or the first
// frame of a codec without one, describes.
func audioTrack(a *AudioTag, config []byte) (*av.Track, error) {
	t := &av.Track{Codec: a.Codec, Channels: int(a.SoundType) + 1}
	switch a.Codec {
	case av.CodecAAC:
		var c aac.Config
		if err := c.Unmarshal(config); err != nil {
			return nil, ErrConfig
		}
		t.Config = append([]byte(nil), config...)
		t.SampleRate, t.Channels = c.OutputSampleRate(), c.Channels()
	case av.CodecOpus:
		t.SampleRate, t.Channels = 48000, 2
		if len(config) > 0 {
			var h opus.Head
			if err := h.Unmarshal(config); err != nil {
				return nil, ErrConfig
			}
			t.Config = append([]byte(nil), config...)
			t.Channels = int(h.Channels)
		}
	case av.CodecPCMA, av.CodecPCMU:
		t.SampleRate = 8000
	default:
		t.SampleRate = mp3Rates[a.SoundRate&3]
		if a.SoundFormat == SoundExHeader {
			t.SampleRate, t.Channels = 0, 0
		}
	}
	return t, nil
}

// audLen returns the size of the access unit delimiter an AVCC access unit
// starts with, or 0: the canonical form leaves it out.
func audLen(codec av.CodecID, au []byte) int {
	nalu, rest, err := h264.NextAVCC(au, 4)
	if err != nil || len(nalu) == 0 {
		return 0
	}
	if codec == av.CodecH264 && h264.Type(nalu) == h264.NALAUD ||
		codec == av.CodecH265 && h265.Type(nalu) == h265.NALAUD {
		return len(au) - len(rest)
	}
	return 0
}
