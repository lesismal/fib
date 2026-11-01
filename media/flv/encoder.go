package flv

import (
	"time"

	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
)

// EncoderConfig configures an Encoder.
type EncoderConfig struct {
	// LegacyHEVC writes H.265 with legacy codec id 12 instead of the
	// enhanced hvc1 header, for players that predate Enhanced RTMP.
	LegacyHEVC bool
}

// Encoder turns an av stream into FLV tag bodies: SetTracks picks the
// tracks FLV can carry, Headers gives the onMetaData and sequence header
// bodies a player needs first, and AppendPacket the body of each packet.
// It keeps no state between packets beyond the tracks, so one Encoder
// serves one stream for any number of consumers that each start with
// Headers.
type Encoder struct {
	config       EncoderConfig
	tracks       []*av.Track
	video, audio int
}

// NewEncoder returns an Encoder.
func NewEncoder(config EncoderConfig) *Encoder {
	return &Encoder{config: config, video: -1, audio: -1}
}

// Carries reports whether FLV carries codec c.
func Carries(c av.CodecID) bool {
	switch c {
	case av.CodecH264, av.CodecH265, av.CodecAV1,
		av.CodecAAC, av.CodecOpus, av.CodecPCMA, av.CodecPCMU, av.CodecMP3:
		return true
	}
	return false
}

// SetTracks sets the stream's tracks. FLV has one video and one audio
// track: the first of each kind FLV carries is used, and SetTracks returns
// the indices of the tracks left out, for the caller to log. A video track
// whose configuration is missing is left out too, since no player could
// decode it.
func (e *Encoder) SetTracks(tracks []*av.Track) (dropped []int) {
	e.tracks, e.video, e.audio = tracks, -1, -1
	for i, t := range tracks {
		switch {
		case t == nil || !Carries(t.Codec):
		case t.Codec.IsVideo() && e.video < 0 && len(t.Config) > 0:
			e.video = i
			continue
		case t.Codec.IsAudio() && e.audio < 0 && (t.Codec != av.CodecAAC || len(t.Config) > 0):
			e.audio = i
			continue
		}
		dropped = append(dropped, i)
	}
	return dropped
}

// Tracks returns the video and audio tracks SetTracks picked; either may be
// nil.
func (e *Encoder) Tracks() (video, audio *av.Track) {
	if e.video >= 0 {
		video = e.tracks[e.video]
	}
	if e.audio >= 0 {
		audio = e.tracks[e.audio]
	}
	return video, audio
}

// codecID is the value onMetaData's videocodecid and audiocodecid take:
// the legacy id, or the FourCC for an enhanced codec, as ffmpeg writes it.
func (e *Encoder) codecID(t *av.Track) float64 {
	switch t.Codec {
	case av.CodecH264:
		return VideoCodecAVC
	case av.CodecH265:
		if e.config.LegacyHEVC {
			return VideoCodecHEVC
		}
		return FourCCHEVC
	case av.CodecAV1:
		return FourCCAV1
	case av.CodecAAC:
		return SoundAAC
	case av.CodecPCMA:
		return SoundPCMA
	case av.CodecPCMU:
		return SoundPCMU
	case av.CodecMP3:
		return SoundMP3
	}
	return FourCCOpus
}

// AppendMetaData appends the body of an onMetaData script tag describing
// the tracks.
func (e *Encoder) AppendMetaData(dst []byte) []byte {
	meta := amf.ECMAArray{{Key: "duration", Value: 0.0}}
	video, audio := e.Tracks()
	if video != nil {
		if video.Width > 0 && video.Height > 0 {
			meta = append(meta, amf.Property{Key: "width", Value: float64(video.Width)},
				amf.Property{Key: "height", Value: float64(video.Height)})
		}
		meta = append(meta, amf.Property{Key: "videocodecid", Value: e.codecID(video)})
	}
	if audio != nil {
		meta = append(meta, amf.Property{Key: "audiocodecid", Value: e.codecID(audio)})
		if audio.SampleRate > 0 {
			meta = append(meta, amf.Property{Key: "audiosamplerate", Value: float64(audio.SampleRate)})
		}
		if audio.Channels > 0 {
			meta = append(meta, amf.Property{Key: "stereo", Value: audio.Channels > 1})
		}
	}
	meta = append(meta, amf.Property{Key: "encoder", Value: "fib"})
	dst = amf.AppendString(dst, "onMetaData")
	dst, _ = amf.Append(dst, meta)
	return dst
}

// Headers calls fn with the body of each tag a consumer needs before the
// packets: onMetaData, then the video and audio sequence headers. The body
// is valid only during the call.
func (e *Encoder) Headers(fn func(typ uint8, body []byte)) {
	buf := e.AppendMetaData(make([]byte, 0, 256))
	fn(TagScript, buf)
	video, audio := e.Tracks()
	if video != nil {
		buf, _ = e.appendVideo(buf[:0], video, &av.Packet{Keyframe: true, Data: video.Config}, PacketSequenceStart)
		fn(TagVideo, buf)
	}
	if audio != nil && (audio.Codec == av.CodecAAC || audio.Codec == av.CodecOpus && len(audio.Config) > 0) {
		buf, _ = e.appendAudio(buf[:0], audio, audio.Config, PacketSequenceStart)
		fn(TagAudio, buf)
	}
}

// AppendPacket appends the body of pkt's tag and returns its type, or 0
// with dst as it was for a packet of a track FLV leaves out.
func (e *Encoder) AppendPacket(dst []byte, pkt *av.Packet) ([]byte, uint8, error) {
	switch pkt.Track {
	case e.video:
		if e.video < 0 {
			break
		}
		b, err := e.appendVideo(dst, e.tracks[e.video], pkt, PacketCodedFrames)
		return b, TagVideo, err
	case e.audio:
		if e.audio < 0 {
			break
		}
		b, err := e.appendAudio(dst, e.tracks[e.audio], pkt.Data, PacketCodedFrames)
		return b, TagAudio, err
	}
	return dst, 0, nil
}

func (e *Encoder) appendVideo(dst []byte, t *av.Track, pkt *av.Packet, packetType uint8) ([]byte, error) {
	v := VideoTag{
		FrameType:  FrameInter,
		Enhanced:   t.Codec == av.CodecAV1 || t.Codec == av.CodecH265 && !e.config.LegacyHEVC,
		Codec:      t.Codec,
		PacketType: packetType,
		Data:       pkt.Data,
	}
	if pkt.Keyframe {
		v.FrameType = FrameKey
	}
	if packetType == PacketCodedFrames {
		cts := pkt.PTS - pkt.DTS
		v.CTS = int32(cts / time.Millisecond)
		if t.Codec == av.CodecAV1 {
			v.CTS = 0
		}
	}
	return AppendVideoTag(dst, &v)
}

func (e *Encoder) appendAudio(dst []byte, t *av.Track, data []byte, packetType uint8) ([]byte, error) {
	a := AudioTag{Codec: t.Codec, PacketType: packetType, Data: data,
		SoundRate: soundRate(t.SampleRate), SoundSize: 1}
	if t.Channels > 1 {
		a.SoundType = 1
	}
	if t.Codec == av.CodecPCMA || t.Codec == av.CodecPCMU {
		// G.711's formats are 8 kHz by definition, and the rate field is
		// ignored.
		a.SoundRate = 0
	}
	return AppendAudioTag(dst, &a)
}
