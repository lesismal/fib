package aac

import (
	"bytes"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/lesismal/fib/media/internal/bitio"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// configs are AudioSpecificConfigs ffmpeg's MP4 muxer wrote for its AAC
// encoder, with what ffprobe reports, and hand-made ones for the syntax
// ffmpeg does not write.
var configs = []struct {
	hex      string
	want     Config
	channels int
	exact    bool // Append writes the same bytes back
}{
	{"1190", Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}, 2, true},
	{"1208", Config{ObjectType: 2, SampleRate: 44100, ChannelConfig: 1}, 1, true},
	{"11b0", Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 6}, 6, true},
	{"1608", Config{ObjectType: 2, SampleRate: 7350, ChannelConfig: 1}, 1, true},
	// 960-sample frames.
	{"1194", Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2, FrameLength960: true}, 2, true},
	// HE-AAC v1 signalled explicitly: SBR at 44100 Hz over LC at 22050.
	{"2b920800", Config{ObjectType: 2, SampleRate: 22050, ChannelConfig: 2, ExtObjectType: 5, ExtSampleRate: 44100}, 2, true},
	// HE-AAC v2 (PS), mono core: 24000 Hz to 48000 Hz.
	{"eb098800", Config{ObjectType: 2, SampleRate: 24000, ChannelConfig: 1, ExtObjectType: 29, ExtSampleRate: 48000}, 1, true},
	// LC with backward-compatible SBR signalling after it: the sync
	// extension is tolerated.
	{"139056e5a0", Config{ObjectType: 2, SampleRate: 22050, ChannelConfig: 2}, 2, false},
	// Escape-coded object type 42 (USAC), which has no GASpecificConfig,
	// with an explicit 24-bit sampling frequency of 12345 Hz.
	{"f95e00607220", Config{ObjectType: 42, SampleRate: 12345, ChannelConfig: 1}, 1, true},
	// Program config element.
	{"1180", Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 0}, 0, false},
}

func TestConfig(t *testing.T) {
	for _, tc := range configs {
		var c Config
		if err := c.Unmarshal(unhex(t, tc.hex)); err != nil {
			t.Errorf("%s: %v", tc.hex, err)
			continue
		}
		if c != tc.want || c.Channels() != tc.channels {
			t.Errorf("%s: %+v channels %d", tc.hex, c, c.Channels())
		}
		b, err := c.Append(nil)
		if err != nil {
			t.Fatalf("%s: %v", tc.hex, err)
		}
		if tc.exact && hex.EncodeToString(b) != tc.hex {
			t.Errorf("%s: wrote %x", tc.hex, b)
		}
		var d Config
		if err := d.Unmarshal(b); err != nil || d != c {
			t.Errorf("%s: round trip %v %+v", tc.hex, err, d)
		}
	}
}

func TestConfigBuildEscape(t *testing.T) {
	c := Config{ObjectType: 42, SampleRate: 12345, ChannelConfig: 1}
	b, err := c.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	var d Config
	if err := d.Unmarshal(b); err != nil || d != c {
		t.Fatalf("%x %v %+v", b, err, d)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, tc := range []struct {
		fields []int // value, bit count, ...
		want   error
	}{
		{nil, ErrShortConfig},
		{[]int{2, 5, 3, 3}, ErrShortConfig},
		{[]int{0, 5, 3, 4, 2, 4, 0, 3}, ErrObjectType},
		{[]int{2, 5, 13, 4, 2, 4, 0, 3}, ErrSampleRate},
		{[]int{2, 5, 15, 4, 0, 24, 2, 4, 0, 3}, ErrSampleRate},
		{[]int{2, 5, 15, 4, 0, 20}, ErrShortConfig},
		{[]int{5, 5, 7, 4, 2, 4, 14, 4, 2, 5, 0, 3}, ErrSampleRate},
		{[]int{5, 5, 7, 4, 2, 4, 4, 4, 5, 5, 0, 3}, ErrObjectType},
		{[]int{29, 5, 7, 4, 2, 4, 4, 4, 29, 5, 0, 3}, ErrObjectType},
		{[]int{5, 5, 7, 4, 2, 4, 4, 3}, ErrShortConfig},
	} {
		w := bitio.NewWriter(nil)
		for i := 0; i < len(tc.fields); i += 2 {
			w.Bits(uint64(tc.fields[i]), tc.fields[i+1])
		}
		var c Config
		if err := c.Unmarshal(w.Bytes()); err != tc.want {
			t.Errorf("%v: %v, want %v", tc.fields, err, tc.want)
		}
	}
	for _, tc := range []struct {
		c    Config
		want error
	}{
		{Config{SampleRate: 48000}, ErrObjectType},
		{Config{ObjectType: 5, SampleRate: 48000}, ErrObjectType},
		{Config{ObjectType: 96, SampleRate: 48000}, ErrObjectType},
		{Config{ObjectType: 2, SampleRate: 48000, ExtObjectType: 3, ExtSampleRate: 48000}, ErrObjectType},
		{Config{ObjectType: 2}, ErrSampleRate},
		{Config{ObjectType: 2, SampleRate: 1 << 24}, ErrSampleRate},
		{Config{ObjectType: 2, SampleRate: 24000, ExtObjectType: 5}, ErrSampleRate},
		{Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 16}, ErrChannels},
	} {
		if _, err := tc.c.Append(nil); err != tc.want {
			t.Errorf("%+v: %v, want %v", tc.c, err, tc.want)
		}
	}
}

func TestDurations(t *testing.T) {
	c := Config{ObjectType: 2, SampleRate: 48000}
	if c.FrameSamples() != 1024 || c.FrameDuration() != 21333333*time.Nanosecond || c.OutputSampleRate() != 48000 {
		t.Fatal(c.FrameDuration())
	}
	c = Config{ObjectType: 2, SampleRate: 44100, FrameLength960: true, ExtObjectType: 5, ExtSampleRate: 88200}
	if c.FrameSamples() != 960 || c.FrameDuration() != 21768707*time.Nanosecond || c.OutputSampleRate() != 88200 {
		t.Fatal(c.FrameDuration())
	}
	if (&Config{}).FrameDuration() != 0 {
		t.Fatal("no rate")
	}
	if i, ok := SampleRateIndex(7350); !ok || i != 12 {
		t.Fatal(i)
	}
	if _, ok := SampleRateIndex(1); ok {
		t.Fatal("rate 1")
	}
}

func TestADTSStream(t *testing.T) {
	for _, tc := range []struct {
		file     string
		rate     int
		channels uint8
		asc      string
	}{
		{"mono44100.aac", 44100, 1, "1208"},
		{"51.aac", 48000, 6, "11b0"},
	} {
		b, err := os.ReadFile("testdata/" + tc.file)
		if err != nil {
			t.Fatal(err)
		}
		frames := 0
		for len(b) > 0 {
			h, au, rest, err := NextADTS(b)
			if err == ErrADTSLength && tc.file == "51.aac" {
				break // the fixture is cut mid-frame
			}
			if err != nil {
				t.Fatalf("%s frame %d: %v", tc.file, frames, err)
			}
			if h.SampleRate() != tc.rate || h.ChannelConfig != tc.channels || h.ObjectType != 2 || h.CRC || len(au) != h.FrameLength-7 {
				t.Fatalf("%s: %+v", tc.file, h)
			}
			cfg := h.Config()
			asc, _ := cfg.Append(nil)
			if hex.EncodeToString(asc) != tc.asc {
				t.Fatalf("%s: config %x", tc.file, asc)
			}
			// The header a muxer writes back is ffmpeg's but for the
			// buffer fullness.
			hdr, err := cfg.AppendADTS(nil, len(au))
			if err != nil {
				t.Fatal(err)
			}
			var back ADTSHeader
			if err := back.Unmarshal(hdr); err != nil || back.FrameLength != h.FrameLength || back.Config() != cfg {
				t.Fatalf("%s: %v %+v", tc.file, err, back)
			}
			if h.BufferFullness == 0x7ff && !bytes.Equal(hdr, b[:7]) {
				t.Fatalf("%s: wrote %x want %x", tc.file, hdr, b[:7])
			}
			b = rest
			frames++
		}
		if frames < 3 {
			t.Fatalf("%s: %d frames", tc.file, frames)
		}
	}
}

func TestADTSCRC(t *testing.T) {
	h := ADTSHeader{MPEG2: true, CRC: true, ObjectType: 1, SampleRateIndex: 4, ChannelConfig: 7, FrameLength: 9 + 3, BufferFullness: 0x123}
	b, err := h.Append(nil)
	if err != nil || len(b) != 9 || h.HeaderSize() != 9 {
		t.Fatal(err, len(b))
	}
	b = append(b, 1, 2, 3, 0xff)
	got, au, rest, err := NextADTS(b)
	if err != nil || got != h || !bytes.Equal(au, []byte{1, 2, 3}) || !bytes.Equal(rest, []byte{0xff}) {
		t.Fatalf("%v %+v %x %x", err, got, au, rest)
	}
	if _, _, _, err := NextADTS(rest); err != ErrShortADTS {
		t.Fatal(err)
	}
}

func TestADTSErrors(t *testing.T) {
	good, _ := (&Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}).AppendADTS(nil, 10)
	mod := func(i int, v byte) []byte {
		b := bytes.Clone(good)
		b[i] = v
		return b
	}
	for _, tc := range []struct {
		b    []byte
		want error
	}{
		{good[:6], ErrShortADTS},
		{mod(0, 0xfe), ErrSync},
		{mod(1, 0xf3), ErrSync}, // layer 1
		{mod(2, good[2]|0x34), ErrSampleRate},
		{mod(4, 0), ErrADTSLength},
		{mod(6, good[6]|1), ErrRawBlocks},
	} {
		var h ADTSHeader
		if err := h.Unmarshal(tc.b); err != tc.want {
			t.Errorf("%x: %v, want %v", tc.b, err, tc.want)
		}
	}
	if _, _, _, err := NextADTS(good); err != ErrADTSLength {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		h    ADTSHeader
		want error
	}{
		{ADTSHeader{ObjectType: 5, FrameLength: 7}, ErrADTSObjectType},
		{ADTSHeader{ObjectType: 0, FrameLength: 7}, ErrADTSObjectType},
		{ADTSHeader{ObjectType: 2, SampleRateIndex: 13, FrameLength: 7}, ErrSampleRate},
		{ADTSHeader{ObjectType: 2, ChannelConfig: 8, FrameLength: 7}, ErrChannels},
		{ADTSHeader{ObjectType: 2, CRC: true, FrameLength: 8}, ErrADTSLength},
		{ADTSHeader{ObjectType: 2, FrameLength: 8192}, ErrADTSLength},
	} {
		if _, err := tc.h.Append(nil); err != tc.want {
			t.Errorf("%+v: %v, want %v", tc.h, err, tc.want)
		}
	}
	if (&ADTSHeader{SampleRateIndex: 15}).SampleRate() != 0 {
		t.Fatal("rate")
	}
	if _, err := (&Config{ObjectType: 2, SampleRate: 12345}).AppendADTS(nil, 1); err != ErrSampleRate {
		t.Fatal(err)
	}
}
