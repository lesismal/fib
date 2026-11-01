package fmp4

import (
	"errors"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/codec/opus"
)

var (
	// ErrCodec is the error for a track whose codec fragmented MP4 does not
	// carry here: anything but H.264, H.265, AV1, AAC and Opus.
	ErrCodec = errors.New("fmp4: codec not carried")
	// ErrConfig is the error for a track whose Config is missing or does
	// not parse.
	ErrConfig = errors.New("fmp4: track configuration missing or malformed")
	// ErrNoTracks is the error for an init segment without a track.
	ErrNoTracks = errors.New("fmp4: no tracks")
)

// VideoTimescale is the timescale of every video track: the 90 kHz clock of
// MPEG-TS and RTP, which every frame rate in use divides finely enough.
const VideoTimescale = 90000

// Supported reports whether a track of codec c can be carried: H.264,
// H.265, AV1, AAC and Opus.
func Supported(c av.CodecID) bool {
	switch c {
	case av.CodecH264, av.CodecH265, av.CodecAV1, av.CodecAAC, av.CodecOpus:
		return true
	}
	return false
}

// trackInfo is what writing a track takes, worked out once from its
// av.Track.
type trackInfo struct {
	track     *av.Track
	video     bool
	timescale uint32
	// width and height are the display size, from the track or its SPS.
	width, height int
	channels      int
	sampleRate    int
	// entry is the sample entry box for stsd.
	entry []byte
	// frame is the duration of one frame in timescale ticks for audio,
	// the duration a last sample takes when nothing follows it; 0 when it
	// depends on each packet (Opus) or is unknown (video).
	frame uint32
}

// newTrackInfo checks t and builds its sample entry.
func newTrackInfo(t *av.Track) (*trackInfo, error) {
	if t == nil || !Supported(t.Codec) {
		return nil, ErrCodec
	}
	ti := &trackInfo{track: t, video: t.Codec.IsVideo(), width: t.Width, height: t.Height}
	var err error
	switch t.Codec {
	case av.CodecH264:
		err = ti.avc()
	case av.CodecH265:
		err = ti.hevc()
	case av.CodecAV1:
		if len(t.Config) < 4 || t.Config[0]&0x80 == 0 {
			return nil, ErrConfig
		}
		ti.entry = ti.visualEntry("av01", "av1C", t.Config)
	case av.CodecAAC:
		err = ti.mp4a()
	case av.CodecOpus:
		err = ti.opus()
	}
	if err != nil {
		return nil, err
	}
	if ti.video {
		ti.timescale = VideoTimescale
	}
	return ti, nil
}

func (ti *trackInfo) avc() error {
	var c h264.AVCConfig
	if err := c.Unmarshal(ti.track.Config); err != nil || len(c.SPS) == 0 {
		return ErrConfig
	}
	if ti.width == 0 || ti.height == 0 {
		var sps h264.SPS
		if sps.Unmarshal(c.SPS[0]) == nil {
			ti.width, ti.height = sps.Width, sps.Height
		}
	}
	// Packets carry 4-byte lengths, so the record must say so whatever the
	// publisher's said.
	config := ti.track.Config
	if c.LengthSize != 4 {
		c.LengthSize = 4
		var err error
		if config, err = c.Append(nil); err != nil {
			return ErrConfig
		}
	}
	ti.entry = ti.visualEntry("avc1", "avcC", config)
	return nil
}

func (ti *trackInfo) hevc() error {
	var c h265.HEVCConfig
	if err := c.Unmarshal(ti.track.Config); err != nil {
		return ErrConfig
	}
	if ti.width == 0 || ti.height == 0 {
		for _, nalu := range c.NALUs(h265.NALSPS) {
			var sps h265.SPS
			if sps.Unmarshal(nalu) == nil {
				ti.width, ti.height = sps.Width, sps.Height
				break
			}
		}
	}
	config := ti.track.Config
	if c.LengthSize != 4 {
		c.LengthSize = 4
		var err error
		if config, err = c.Append(nil); err != nil {
			return ErrConfig
		}
	}
	ti.entry = ti.visualEntry("hvc1", "hvcC", config)
	return nil
}

func (ti *trackInfo) mp4a() error {
	var c aac.Config
	if err := c.Unmarshal(ti.track.Config); err != nil {
		return ErrConfig
	}
	ti.sampleRate = c.SampleRate
	if ti.track.SampleRate > 0 {
		ti.sampleRate = ti.track.SampleRate
	}
	ti.channels = c.Channels()
	if ti.track.Channels > 0 {
		ti.channels = ti.track.Channels
	}
	if ti.channels == 0 {
		ti.channels = 2
	}
	ti.timescale = uint32(ti.sampleRate)
	ti.frame = uint32(av.ToTicks(c.FrameDuration(), ti.sampleRate))

	b, esds := beginFull(nil, "esds", 0, 0)
	// ES_Descriptor (ISO/IEC 14496-1 section 7.2.6.5) holding the
	// DecoderConfigDescriptor, whose DecoderSpecificInfo is the
	// AudioSpecificConfig, and an SLConfigDescriptor of the MP4 kind.
	asc := ti.track.Config
	dsi := descriptor(nil, 0x05, asc)
	dcd := []byte{0x40, 0x15, 0, 0, 0}
	dcd = u32(dcd, 0)
	dcd = u32(dcd, 0)
	dcd = append(dcd, dsi...)
	es := []byte{0, 0, 0}
	es = descriptor(es, 0x04, dcd)
	es = descriptor(es, 0x06, []byte{0x02})
	b = descriptor(b, 0x03, es)
	b = end(b, esds)
	ti.entry = ti.audioEntry(b, "mp4a", ti.sampleRate)
	return nil
}

func (ti *trackInfo) opus() error {
	var h opus.Head
	if len(ti.track.Config) > 0 {
		if err := h.Unmarshal(ti.track.Config); err != nil {
			return ErrConfig
		}
	} else {
		ch := ti.track.Channels
		if ch <= 0 || ch > 2 {
			ch = 2
		}
		h = opus.Head{Channels: uint8(ch), PreSkip: 312, InputSampleRate: 48000}
	}
	ti.channels = int(h.Channels)
	ti.sampleRate = 48000
	ti.timescale = 48000
	// dOps (Encapsulation of Opus in ISO BMFF, section 4.3.2): OpusHead's
	// fields after its magic, big-endian, with version 0.
	b, at := begin(nil, "dOps")
	b = append(b, 0, h.Channels)
	b = u16(b, h.PreSkip)
	b = u32(b, h.InputSampleRate)
	b = u16(b, uint16(h.OutputGain))
	b = append(b, h.MappingFamily)
	if h.MappingFamily != 0 {
		b = append(b, h.StreamCount, h.CoupledCount)
		b = append(b, h.ChannelMapping...)
	}
	b = end(b, at)
	ti.entry = ti.audioEntry(b, "Opus", 48000)
	return nil
}

// descriptor appends an MPEG-4 descriptor of tag with payload, its size in
// the 4-byte expandable form ffmpeg writes.
func descriptor(b []byte, tag byte, payload []byte) []byte {
	n := len(payload)
	b = append(b, tag, byte(n>>21)|0x80, byte(n>>14)|0x80, byte(n>>7)|0x80, byte(n)&0x7f)
	return append(b, payload...)
}

// visualEntry builds a VisualSampleEntry (ISO/IEC 14496-12 section 12.1.3)
// of format with the configuration box configType holding config.
func (ti *trackInfo) visualEntry(format, configType string, config []byte) []byte {
	b, at := begin(nil, format)
	b = zeros(b, 6)
	b = u16(b, 1) // data_reference_index
	b = zeros(b, 16)
	b = u16(b, uint16(ti.width))
	b = u16(b, uint16(ti.height))
	b = u32(b, 0x00480000) // 72 dpi
	b = u32(b, 0x00480000)
	b = u32(b, 0)
	b = u16(b, 1) // frame_count
	b = zeros(b, 32)
	b = u16(b, 0x0018)
	b = u16(b, 0xffff)
	b, c := begin(b, configType)
	b = append(b, config...)
	b = end(b, c)
	return end(b, at)
}

// audioEntry builds an AudioSampleEntry (section 12.2.3) of format, followed
// by the boxes in children.
func (ti *trackInfo) audioEntry(children []byte, format string, rate int) []byte {
	b, at := begin(nil, format)
	b = zeros(b, 6)
	b = u16(b, 1)
	b = zeros(b, 8)
	b = u16(b, uint16(ti.channels))
	b = u16(b, 16)
	b = zeros(b, 4)
	if rate > 0xffff {
		// The field is 16.16; rates past it are left to the codec config.
		rate = 0
	}
	b = u32(b, uint32(rate)<<16)
	b = append(b, children...)
	return end(b, at)
}

// unity is the identity transformation matrix of mvhd and tkhd.
var unity = [9]uint32{0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000}

func appendMatrix(b []byte) []byte {
	for _, v := range unity {
		b = u32(b, v)
	}
	return b
}

// AppendInit appends to dst the initialization segment of a fragmented MP4
// carrying tracks: ftyp and a moov whose tracks have empty sample tables and
// an mvex announcing that the samples come in movie fragments. Track i has
// track_ID i+1. Video tracks have a timescale of VideoTimescale, AAC its
// sampling frequency, Opus 48000. Every track must be Supported, with a
// Config that parses (Opus may leave it nil); a stream with other tracks is
// what Writer is for, which leaves them out.
func AppendInit(dst []byte, tracks []*av.Track) ([]byte, error) {
	if len(tracks) == 0 {
		return dst, ErrNoTracks
	}
	infos := make([]*trackInfo, len(tracks))
	for i, t := range tracks {
		ti, err := newTrackInfo(t)
		if err != nil {
			return dst, err
		}
		infos[i] = ti
	}
	return appendInit(dst, infos), nil
}

// The brands of the ftyp: iso5 for the default-base-is-moof of tfhd, iso6
// for the version 1 trun with signed composition offsets, mp41 for players
// that look for it.
var brands = []string{"iso5", "iso6", "mp41"}

func appendInit(b []byte, infos []*trackInfo) []byte {
	b, at := begin(b, "ftyp")
	b = append(b, "iso5"...)
	b = u32(b, 512)
	for _, brand := range brands {
		b = append(b, brand...)
	}
	b = end(b, at)

	b, moov := begin(b, "moov")
	b, at = beginFull(b, "mvhd", 0, 0)
	b = u32(b, 0)    // creation_time
	b = u32(b, 0)    // modification_time
	b = u32(b, 1000) // timescale
	b = u32(b, 0)    // duration: unknown, fragmented
	b = u32(b, 0x00010000)
	b = u16(b, 0x0100)
	b = zeros(b, 10)
	b = appendMatrix(b)
	b = zeros(b, 24)
	b = u32(b, uint32(len(infos)+1)) // next_track_ID
	b = end(b, at)
	for i, ti := range infos {
		b = appendTrak(b, uint32(i+1), ti)
	}
	b, mvex := begin(b, "mvex")
	for i := range infos {
		b, at = beginFull(b, "trex", 0, 0)
		b = u32(b, uint32(i+1))
		b = u32(b, 1) // default_sample_description_index
		b = u32(b, 0)
		b = u32(b, 0)
		b = u32(b, 0)
		b = end(b, at)
	}
	b = end(b, mvex)
	return end(b, moov)
}

func appendTrak(b []byte, id uint32, ti *trackInfo) []byte {
	b, trak := begin(b, "trak")
	b, at := beginFull(b, "tkhd", 0, 3) // enabled, in movie
	b = u32(b, 0)
	b = u32(b, 0)
	b = u32(b, id)
	b = u32(b, 0)
	b = u32(b, 0) // duration
	b = zeros(b, 8)
	b = u16(b, 0) // layer
	b = u16(b, 0) // alternate_group
	if ti.video {
		b = u16(b, 0)
	} else {
		b = u16(b, 0x0100)
	}
	b = u16(b, 0)
	b = appendMatrix(b)
	if ti.video {
		b = u32(b, uint32(ti.width)<<16)
		b = u32(b, uint32(ti.height)<<16)
	} else {
		b = u32(b, 0)
		b = u32(b, 0)
	}
	b = end(b, at)

	b, mdia := begin(b, "mdia")
	b, at = beginFull(b, "mdhd", 0, 0)
	b = u32(b, 0)
	b = u32(b, 0)
	b = u32(b, ti.timescale)
	b = u32(b, 0)
	b = u16(b, 0x55c4) // "und"
	b = u16(b, 0)
	b = end(b, at)
	b, at = beginFull(b, "hdlr", 0, 0)
	b = u32(b, 0)
	if ti.video {
		b = append(b, "vide"...)
	} else {
		b = append(b, "soun"...)
	}
	b = zeros(b, 12)
	if ti.video {
		b = append(b, "VideoHandler\x00"...)
	} else {
		b = append(b, "SoundHandler\x00"...)
	}
	b = end(b, at)

	b, minf := begin(b, "minf")
	if ti.video {
		b, at = beginFull(b, "vmhd", 0, 1)
		b = zeros(b, 8)
	} else {
		b, at = beginFull(b, "smhd", 0, 0)
		b = zeros(b, 4)
	}
	b = end(b, at)
	b, dinf := begin(b, "dinf")
	b, dref := beginFull(b, "dref", 0, 0)
	b = u32(b, 1)
	b, at = beginFull(b, "url ", 0, 1) // media in the same file
	b = end(b, at)
	b = end(b, dref)
	b = end(b, dinf)

	b, stbl := begin(b, "stbl")
	b, at = beginFull(b, "stsd", 0, 0)
	b = u32(b, 1)
	b = append(b, ti.entry...)
	b = end(b, at)
	for _, typ := range []string{"stts", "stsc", "stco"} {
		b, at = beginFull(b, typ, 0, 0)
		b = u32(b, 0)
		b = end(b, at)
	}
	b, at = beginFull(b, "stsz", 0, 0)
	b = u32(b, 0)
	b = u32(b, 0)
	b = end(b, at)
	b = end(b, stbl)
	b = end(b, minf)
	b = end(b, mdia)
	return end(b, trak)
}

// frameTicks is the duration a video sample takes when nothing tells it
// otherwise: one frame at 30 fps.
const frameTicks = VideoTimescale / 30

// maxGap is the largest step between two samples of a track taken as is;
// a larger one, or a step back, is a jump in the publisher's clock, which
// the sample before it does not last through.
const maxGap = 10 * time.Second
