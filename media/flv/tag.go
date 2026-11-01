package flv

import (
	"encoding/binary"

	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
)

// Video frame types.
const (
	FrameKey        = 1
	FrameInter      = 2
	FrameDisposable = 3
	FrameGenerated  = 4
	// FrameCommand is a video info/command frame, carrying no picture.
	FrameCommand = 5
)

// Legacy video codec ids.
const (
	VideoCodecAVC = 7
	// VideoCodecHEVC is the codec id ffmpeg's Chinese forks, SRS and OBS
	// builds in China use for H.265, with the AVC packet layout.
	VideoCodecHEVC = 12
)

// Packet types. The enhanced header's packet types are used for legacy tags
// too: an AVC packet type of 0, 1 or 2 is SequenceStart, CodedFrames or
// SequenceEnd, and AAC's 0 and 1 are SequenceStart and CodedFrames.
const (
	PacketSequenceStart         = 0
	PacketCodedFrames           = 1
	PacketSequenceEnd           = 2
	PacketCodedFramesX          = 3
	PacketMetadata              = 4
	PacketMPEG2TSSequenceStart  = 5
	PacketMultitrack            = 6
	PacketModEx                 = 7
	audioPacketMultichannelConf = 4
	audioPacketMultitrack       = 5
)

// Sound formats.
const (
	SoundMP3      = 2
	SoundPCMA     = 7
	SoundPCMU     = 8
	SoundExHeader = 9
	SoundAAC      = 10
)

// FourCC codes of Enhanced RTMP.
const (
	FourCCAVC  = 'a'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCHEVC = 'h'<<24 | 'v'<<16 | 'c'<<8 | '1'
	FourCCAV1  = 'a'<<24 | 'v'<<16 | '0'<<8 | '1'
	FourCCVP9  = 'v'<<24 | 'p'<<16 | '0'<<8 | '9'
	FourCCOpus = 'O'<<24 | 'p'<<16 | 'u'<<8 | 's'
	FourCCAAC  = 'm'<<24 | 'p'<<16 | '4'<<8 | 'a'
	FourCCMP3  = '.'<<24 | 'm'<<16 | 'p'<<8 | '3'
)

// VideoTag is the header of a video tag body, and the data after it.
type VideoTag struct {
	FrameType uint8
	// Enhanced says the tag uses Enhanced RTMP's header, and FourCC is
	// then its codec; otherwise CodecID is the legacy codec id.
	Enhanced bool
	FourCC   uint32
	CodecID  uint8
	// Codec is the codec, CodecUnknown for one this package does not map.
	Codec av.CodecID
	// PacketType is one of the Packet constants; a legacy AVC packet type
	// maps to its enhanced equivalent.
	PacketType uint8
	// CTS is the composition time offset in milliseconds, of CodedFrames
	// for H.264 and H.265, and Nanos the ModEx sub-millisecond offset of
	// the timestamp, 0 to 999999.
	CTS   int32
	Nanos uint32
	// Command is the command of a FrameCommand tag.
	Command uint8
	Data    []byte
}

func fourCCVideo(f uint32) av.CodecID {
	switch f {
	case FourCCAVC:
		return av.CodecH264
	case FourCCHEVC:
		return av.CodecH265
	case FourCCAV1:
		return av.CodecAV1
	}
	return av.CodecUnknown
}

func fourCCAudio(f uint32) av.CodecID {
	switch f {
	case FourCCOpus:
		return av.CodecOpus
	case FourCCAAC:
		return av.CodecAAC
	case FourCCMP3:
		return av.CodecMP3
	}
	return av.CodecUnknown
}

func si24(b []byte) int32 { return int32(uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8) >> 8 }

// modEx reads Enhanced RTMP v2's ModEx prefixes, which come before the
// packet type they modify, returning the real packet type, the timestamp's
// nanosecond offset and the rest.
func modEx(b []byte, packetType uint8) (uint8, uint32, []byte, error) {
	var nanos uint32
	for packetType == PacketModEx {
		if len(b) < 1 {
			return 0, 0, nil, ErrShort
		}
		size := int(b[0]) + 1
		b = b[1:]
		if size == 256 {
			if len(b) < 2 {
				return 0, 0, nil, ErrShort
			}
			size = int(binary.BigEndian.Uint16(b)) + 1
			b = b[2:]
		}
		if len(b) < size+1 {
			return 0, 0, nil, ErrShort
		}
		data := b[:size]
		modType := b[size] >> 4
		packetType = b[size] & 0x0f
		b = b[size+1:]
		if modType == 0 && size >= 3 { // TimestampOffsetNano
			nanos = uint32(data[0])<<16 | uint32(data[1])<<8 | uint32(data[2])
		}
	}
	return packetType, nanos, b, nil
}

// ParseVideoTag reads the header of a video tag body. Data refers to b.
func ParseVideoTag(b []byte) (VideoTag, error) {
	var v VideoTag
	if len(b) < 1 {
		return v, ErrShort
	}
	if b[0]&0x80 == 0 {
		v.FrameType, v.CodecID = b[0]>>4, b[0]&0x0f
		b = b[1:]
		switch v.CodecID {
		case VideoCodecAVC, VideoCodecHEVC:
			v.Codec = av.CodecH264
			if v.CodecID == VideoCodecHEVC {
				v.Codec = av.CodecH265
			}
			if v.FrameType == FrameCommand {
				if len(b) > 0 {
					v.Command = b[0]
				}
				v.PacketType = PacketMetadata
				return v, nil
			}
			if len(b) < 4 {
				return v, ErrShort
			}
			v.PacketType = b[0]
			if v.PacketType > PacketSequenceEnd {
				return v, ErrShort
			}
			v.CTS = si24(b[1:])
			v.Data = b[4:]
		default:
			v.PacketType = PacketCodedFrames
			v.Data = b
		}
		return v, nil
	}
	v.Enhanced = true
	v.FrameType = b[0] >> 4 & 7
	packetType, nanos, b, err := modEx(b[1:], b[0]&0x0f)
	if err != nil {
		return v, err
	}
	v.PacketType, v.Nanos = packetType, nanos
	if v.FrameType == FrameCommand && v.PacketType != PacketMetadata {
		if len(b) < 1 {
			return v, ErrShort
		}
		v.Command = b[0]
		return v, nil
	}
	if v.PacketType == PacketMultitrack {
		return v, ErrMultitrack
	}
	if len(b) < 4 {
		return v, ErrShort
	}
	v.FourCC = binary.BigEndian.Uint32(b)
	v.Codec = fourCCVideo(v.FourCC)
	b = b[4:]
	if v.PacketType == PacketCodedFrames && (v.FourCC == FourCCAVC || v.FourCC == FourCCHEVC) {
		if len(b) < 3 {
			return v, ErrShort
		}
		v.CTS = si24(b)
		b = b[3:]
	}
	if v.PacketType == PacketCodedFramesX {
		v.PacketType = PacketCodedFrames
	}
	v.Data = b
	return v, nil
}

// AppendVideoTag appends a video tag body with v's header and data. A tag
// whose Enhanced is false is written with the legacy header, which only
// H.264 and H.265 (codec id 12) have; the others are written enhanced.
// CodedFrames without a composition offset are written as CodedFramesX.
func AppendVideoTag(dst []byte, v *VideoTag) ([]byte, error) {
	if !v.Enhanced && (v.Codec == av.CodecH264 || v.Codec == av.CodecH265) {
		id := byte(VideoCodecAVC)
		if v.Codec == av.CodecH265 {
			id = VideoCodecHEVC
		}
		pt := v.PacketType
		if pt > PacketSequenceEnd {
			pt = PacketCodedFrames
		}
		dst = append(dst, v.FrameType<<4|id, pt, byte(v.CTS>>16), byte(v.CTS>>8), byte(v.CTS))
		return append(dst, v.Data...), nil
	}
	var fourCC uint32
	switch v.Codec {
	case av.CodecH264:
		fourCC = FourCCAVC
	case av.CodecH265:
		fourCC = FourCCHEVC
	case av.CodecAV1:
		fourCC = FourCCAV1
	default:
		return dst, ErrCodec
	}
	pt := v.PacketType
	withCTS := pt == PacketCodedFrames && fourCC != FourCCAV1
	if withCTS && v.CTS == 0 {
		pt, withCTS = PacketCodedFramesX, false
	}
	if v.Nanos != 0 {
		dst = append(dst, 0x80|v.FrameType<<4|PacketModEx, 2,
			byte(v.Nanos>>16), byte(v.Nanos>>8), byte(v.Nanos), pt)
	} else {
		dst = append(dst, 0x80|v.FrameType<<4|pt)
	}
	dst = binary.BigEndian.AppendUint32(dst, fourCC)
	if withCTS {
		dst = append(dst, byte(v.CTS>>16), byte(v.CTS>>8), byte(v.CTS))
	}
	return append(dst, v.Data...), nil
}

// AudioTag is the header of an audio tag body, and the data after it.
type AudioTag struct {
	// SoundFormat is the legacy sound format, SoundExHeader for an
	// enhanced tag, whose codec FourCC is then.
	SoundFormat uint8
	// SoundRate (0 to 3: 5.5, 11, 22 and 44 kHz), SoundSize (0 8-bit, 1
	// 16-bit) and SoundType (0 mono, 1 stereo) are the legacy header's
	// fields.
	SoundRate, SoundSize, SoundType uint8
	FourCC                          uint32
	Codec                           av.CodecID
	// PacketType is PacketSequenceStart or PacketCodedFrames for AAC and
	// enhanced tags (or SequenceEnd, and 4 for an enhanced multichannel
	// configuration), and PacketCodedFrames for the other legacy formats.
	PacketType uint8
	Nanos      uint32
	Data       []byte
}

// ParseAudioTag reads the header of an audio tag body. Data refers to b.
func ParseAudioTag(b []byte) (AudioTag, error) {
	var a AudioTag
	if len(b) < 1 {
		return a, ErrShort
	}
	a.SoundFormat = b[0] >> 4
	if a.SoundFormat == SoundExHeader {
		packetType, nanos, rest, err := modEx(b[1:], b[0]&0x0f)
		if err != nil {
			return a, err
		}
		a.PacketType, a.Nanos = packetType, nanos
		if a.PacketType == audioPacketMultitrack {
			return a, ErrMultitrack
		}
		if len(rest) < 4 {
			return a, ErrShort
		}
		a.FourCC = binary.BigEndian.Uint32(rest)
		a.Codec = fourCCAudio(a.FourCC)
		a.Data = rest[4:]
		return a, nil
	}
	a.SoundRate, a.SoundSize, a.SoundType = b[0]>>2&3, b[0]>>1&1, b[0]&1
	a.PacketType = PacketCodedFrames
	b = b[1:]
	switch a.SoundFormat {
	case SoundAAC:
		a.Codec = av.CodecAAC
		if len(b) < 1 {
			return a, ErrShort
		}
		a.PacketType = b[0]
		if a.PacketType > PacketCodedFrames {
			return a, ErrShort
		}
		b = b[1:]
	case SoundPCMA:
		a.Codec = av.CodecPCMA
	case SoundPCMU:
		a.Codec = av.CodecPCMU
	case SoundMP3:
		a.Codec = av.CodecMP3
	}
	a.Data = b
	return a, nil
}

// soundRate maps a sample rate to the legacy header's rate field.
func soundRate(rate int) uint8 {
	switch {
	case rate <= 5512:
		return 0
	case rate <= 11025:
		return 1
	case rate <= 22050:
		return 2
	}
	return 3
}

// AppendAudioTag appends an audio tag body with a's header and data.
// A's codec decides the header: AAC, G.711 and MP3 are written with the
// legacy one, from SoundRate, SoundSize and SoundType (AAC always as 44
// kHz, 16-bit stereo, which the specification requires), and Opus with the
// enhanced one.
func AppendAudioTag(dst []byte, a *AudioTag) ([]byte, error) {
	switch a.Codec {
	case av.CodecAAC:
		return append(append(dst, SoundAAC<<4|3<<2|1<<1|1, a.PacketType), a.Data...), nil
	case av.CodecPCMA, av.CodecPCMU, av.CodecMP3:
		format := byte(SoundMP3)
		if a.Codec == av.CodecPCMA {
			format = SoundPCMA
		} else if a.Codec == av.CodecPCMU {
			format = SoundPCMU
		}
		return append(append(dst, format<<4|a.SoundRate&3<<2|a.SoundSize&1<<1|a.SoundType&1), a.Data...), nil
	case av.CodecOpus:
		dst = append(dst, SoundExHeader<<4|a.PacketType&0x0f)
		dst = binary.BigEndian.AppendUint32(dst, FourCCOpus)
		return append(dst, a.Data...), nil
	}
	return dst, ErrCodec
}

// ParseScript reads a script data body: the name of the call, such as
// "onMetaData", and its arguments. A body that starts with
// "@setDataFrame", as RTMP publishers send metadata, is read as the call
// it wraps.
func ParseScript(b []byte) (name string, args []any, err error) {
	values, err := amf.Decode(b)
	if err != nil && len(values) == 0 {
		return "", nil, err
	}
	if len(values) == 0 {
		return "", nil, ErrShort
	}
	name, _ = values[0].(string)
	args = values[1:]
	if name == "@setDataFrame" && len(args) > 0 {
		name, _ = args[0].(string)
		args = args[1:]
	}
	return name, args, nil
}

// MetaData returns the properties of an onMetaData call's first argument,
// an ECMA array or an object.
func MetaData(args []any) amf.ECMAArray {
	if len(args) == 0 {
		return nil
	}
	switch v := args[0].(type) {
	case amf.ECMAArray:
		return v
	case amf.Object:
		return amf.ECMAArray(v)
	}
	return nil
}
