package aac

import (
	"errors"
	"time"

	"github.com/lesismal/fib/media/internal/bitio"
)

var (
	ErrShortConfig = errors.New("aac: AudioSpecificConfig too short")
	ErrObjectType  = errors.New("aac: invalid audio object type")
	ErrSampleRate  = errors.New("aac: invalid sampling frequency")
	ErrChannels    = errors.New("aac: invalid channel configuration")
)

// The audio object types of ISO/IEC 14496-3 Table 1.17 a streaming server
// meets. LC is what nearly every encoder sends; SBR and PS are HE-AAC v1
// and v2.
const (
	ObjectTypeMain = 1
	ObjectTypeLC   = 2
	ObjectTypeSSR  = 3
	ObjectTypeLTP  = 4
	ObjectTypeSBR  = 5
	ObjectTypePS   = 29
)

// SampleRates are the sampling frequencies of samplingFrequencyIndex 0 to
// 12 (Table 1.18).
var SampleRates = [13]int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// SampleRateIndex returns the samplingFrequencyIndex of rate, and false
// when the table has no such rate.
func SampleRateIndex(rate int) (int, bool) {
	for i, r := range SampleRates {
		if r == rate {
			return i, true
		}
	}
	return 0, false
}

// channelCounts maps channelConfiguration to a number of channels (Table
// 1.19 and ISO/IEC 23001-8); 0 is defined by a program config element.
var channelCounts = [16]int{0, 1, 2, 3, 4, 5, 6, 8, 0, 0, 0, 7, 8, 24, 8, 0}

// Config is an AudioSpecificConfig (ISO/IEC 14496-3 section 1.6.2.1), the
// configuration of an AAC track in MP4, FLV and RTP (config=), and
// av.Track.Config for AAC.
type Config struct {
	// ObjectType is the audio object type of the core codec, 2 for AAC LC
	// even when ExtObjectType signals SBR or PS on top of it.
	ObjectType uint8
	// SampleRate is the core codec's sampling frequency.
	SampleRate int
	// ChannelConfig is channelConfiguration; 0 means a program config
	// element, which is not read, describes the channels.
	ChannelConfig uint8
	// ExtObjectType is 5 (SBR) or 29 (PS) when the config signals HE-AAC
	// explicitly and hierarchically, as Table 1.15 allows, with
	// ExtSampleRate the output sampling frequency; it is 0 otherwise.
	// Backward-compatible signalling after the GASpecificConfig is
	// tolerated but not read.
	ExtObjectType uint8
	ExtSampleRate int
	// FrameLength960 is frameLengthFlag: frames of 960 samples, not 1024.
	FrameLength960 bool
}

// hasGASpecific reports whether object type t carries a GASpecificConfig.
func hasGASpecific(t uint8) bool {
	switch t {
	case 1, 2, 3, 4, 6, 7, 17, 19, 20, 21, 22, 23:
		return true
	}
	return false
}

func readObjectType(r *bitio.Reader) uint8 {
	t := r.Bits(5)
	if t == 31 {
		t = 32 + r.Bits(6)
	}
	return uint8(t)
}

func readSampleRate(r *bitio.Reader) int {
	i := r.Bits(4)
	if i == 15 {
		return int(r.Bits(24))
	}
	if i >= uint32(len(SampleRates)) {
		return 0
	}
	return SampleRates[i]
}

// Unmarshal parses the config b.
func (c *Config) Unmarshal(b []byte) error {
	r := bitio.NewReader(b)
	*c = Config{}
	c.ObjectType = readObjectType(&r)
	c.SampleRate = readSampleRate(&r)
	c.ChannelConfig = uint8(r.Bits(4))
	if c.ObjectType == ObjectTypeSBR || c.ObjectType == ObjectTypePS {
		c.ExtObjectType = c.ObjectType
		c.ExtSampleRate = readSampleRate(&r)
		c.ObjectType = readObjectType(&r)
		if c.ExtSampleRate == 0 && r.Err() == nil {
			return ErrSampleRate
		}
	}
	if hasGASpecific(c.ObjectType) {
		c.FrameLength960 = r.Flag()
	}
	if r.Err() != nil {
		return ErrShortConfig
	}
	if c.ObjectType == 0 || c.ObjectType == ObjectTypeSBR || c.ObjectType == ObjectTypePS {
		return ErrObjectType
	}
	if c.SampleRate == 0 {
		return ErrSampleRate
	}
	return nil
}

func writeObjectType(w *bitio.Writer, t uint8) {
	if t >= 31 {
		w.Bits(31, 5)
		w.Bits(uint64(t-32), 6)
	} else {
		w.Bits(uint64(t), 5)
	}
}

func writeSampleRate(w *bitio.Writer, rate int) {
	if i, ok := SampleRateIndex(rate); ok {
		w.Bits(uint64(i), 4)
	} else {
		w.Bits(15, 4)
		w.Bits(uint64(rate), 24)
	}
}

// Append appends the config to dst: the object type, sampling frequency
// and channel configuration, the explicit SBR or PS signalling when
// ExtObjectType is set, and for the AAC object types a GASpecificConfig
// with frameLengthFlag and no core coder or extension.
func (c *Config) Append(dst []byte) ([]byte, error) {
	if c.ObjectType == 0 || c.ObjectType > 95 || c.ObjectType == ObjectTypeSBR || c.ObjectType == ObjectTypePS {
		return dst, ErrObjectType
	}
	if c.ExtObjectType != 0 && c.ExtObjectType != ObjectTypeSBR && c.ExtObjectType != ObjectTypePS {
		return dst, ErrObjectType
	}
	if c.SampleRate <= 0 || c.SampleRate >= 1<<24 || c.ExtObjectType != 0 && (c.ExtSampleRate <= 0 || c.ExtSampleRate >= 1<<24) {
		return dst, ErrSampleRate
	}
	if c.ChannelConfig > 15 {
		return dst, ErrChannels
	}
	w := bitio.NewWriter(dst)
	if c.ExtObjectType != 0 {
		writeObjectType(&w, c.ExtObjectType)
	} else {
		writeObjectType(&w, c.ObjectType)
	}
	writeSampleRate(&w, c.SampleRate)
	w.Bits(uint64(c.ChannelConfig), 4)
	if c.ExtObjectType != 0 {
		writeSampleRate(&w, c.ExtSampleRate)
		writeObjectType(&w, c.ObjectType)
	}
	if hasGASpecific(c.ObjectType) {
		w.Flag(c.FrameLength960)
		w.Bits(0, 2) // dependsOnCoreCoder, extensionFlag
	}
	return w.Bytes(), nil
}

// Channels returns the number of channels ChannelConfig stands for, 0 when
// a program config element describes them.
func (c *Config) Channels() int { return channelCounts[c.ChannelConfig&15] }

// OutputSampleRate returns the sampling frequency a decoder outputs: the
// SBR rate for explicitly signalled HE-AAC, else SampleRate.
func (c *Config) OutputSampleRate() int {
	if c.ExtObjectType != 0 {
		return c.ExtSampleRate
	}
	return c.SampleRate
}

// FrameSamples returns the number of samples an access unit holds at
// SampleRate: 1024, or 960 with FrameLength960.
func (c *Config) FrameSamples() int {
	if c.FrameLength960 {
		return 960
	}
	return 1024
}

// FrameDuration returns the duration of one access unit, rounded to the
// nanosecond, or 0 without a sampling frequency.
func (c *Config) FrameDuration() time.Duration {
	if c.SampleRate <= 0 {
		return 0
	}
	return (time.Duration(c.FrameSamples())*time.Second + time.Duration(c.SampleRate/2)) / time.Duration(c.SampleRate)
}
