package opus

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// OpusHeads libopus wrote through ffmpeg 5.1 into Ogg: mono, and 5.1 with
// mapping family 1.
const (
	monoHead = "4f707573486561640101380180bb0000000000"
	head51   = "4f707573486561640106380180bb00000000010402000401020305"
)

func TestHead(t *testing.T) {
	for _, tc := range []struct {
		hex  string
		want Head
	}{
		{monoHead, Head{Version: 1, Channels: 1, PreSkip: 312, InputSampleRate: 48000}},
		{head51, Head{Version: 1, Channels: 6, PreSkip: 312, InputSampleRate: 48000, MappingFamily: 1,
			StreamCount: 4, CoupledCount: 2, ChannelMapping: []byte{0, 4, 1, 2, 3, 5}}},
	} {
		b := unhex(t, tc.hex)
		var h Head
		if err := h.Unmarshal(b); err != nil {
			t.Fatal(err)
		}
		if h.Version != tc.want.Version || h.Channels != tc.want.Channels || h.PreSkip != tc.want.PreSkip ||
			h.InputSampleRate != tc.want.InputSampleRate || h.MappingFamily != tc.want.MappingFamily ||
			h.StreamCount != tc.want.StreamCount || h.CoupledCount != tc.want.CoupledCount ||
			!bytes.Equal(h.ChannelMapping, tc.want.ChannelMapping) {
			t.Fatalf("%+v", h)
		}
		out, err := h.Append(nil)
		if err != nil || !bytes.Equal(out, b) || h.MarshalSize() != len(b) {
			t.Fatalf("%v %x", err, out)
		}
	}
	h := Head{Channels: 2, OutputGain: -256}
	b, err := h.Append(nil)
	if err != nil || b[8] != 1 {
		t.Fatal(err)
	}
	var g Head
	if err := g.Unmarshal(b); err != nil || g.OutputGain != -256 {
		t.Fatal(err, g)
	}
}

func TestHeadErrors(t *testing.T) {
	mono, h51 := unhex(t, monoHead), unhex(t, head51)
	mod := func(b []byte, i int, v byte) []byte {
		b = bytes.Clone(b)
		b[i] = v
		return b
	}
	for _, tc := range []struct {
		b    []byte
		want error
	}{
		{mono[:18], ErrShortHead},
		{mod(mono, 0, 'o'), ErrMagic},
		{mod(mono, 8, 0x10), ErrVersion},
		{mod(mono, 9, 0), ErrChannels},
		{mod(mono, 9, 3), ErrChannels},
		{h51[:26], ErrShortHead},
		{mod(h51, 19, 0), ErrChannels},
		{mod(h51, 20, 5), ErrChannels},
		{mod(h51, 21, 6), ErrChannels},
	} {
		var h Head
		if err := h.Unmarshal(tc.b); err != tc.want {
			t.Errorf("%x: %v, want %v", tc.b, err, tc.want)
		}
	}
	// 255 is a silent channel.
	var h Head
	if err := h.Unmarshal(mod(h51, 21, 255)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		h    Head
		want error
	}{
		{Head{Version: 0x10, Channels: 1}, ErrVersion},
		{Head{}, ErrChannels},
		{Head{Channels: 3}, ErrChannels},
		{Head{Channels: 3, MappingFamily: 1, StreamCount: 2, CoupledCount: 1, ChannelMapping: []byte{0, 1}}, ErrChannels},
		{Head{Channels: 1, MappingFamily: 255, StreamCount: 200, CoupledCount: 100, ChannelMapping: []byte{0}}, ErrChannels},
	} {
		if _, err := tc.h.Append(nil); err != tc.want {
			t.Errorf("%+v: %v, want %v", tc.h, err, tc.want)
		}
	}
}

func TestDuration(t *testing.T) {
	for _, tc := range []struct {
		pkt     []byte
		frames  int
		d       time.Duration
		samples int
	}{
		{[]byte{0xf8, 1}, 1, 20 * time.Millisecond, 960}, // CELT FB 20 ms, libopus's default
		{[]byte{0xfc}, 1, 20 * time.Millisecond, 960},
		{[]byte{0x01}, 2, 20 * time.Millisecond, 960},   // SILK NB 10 ms x 2
		{[]byte{0x1a}, 2, 120 * time.Millisecond, 5760}, // SILK NB 60 ms x 2
		{[]byte{0x83, 0x05}, 5, 12500 * time.Microsecond, 600},
		{[]byte{0x6b, 0x03}, 3, 60 * time.Millisecond, 2880}, // hybrid FB 20 ms x 3
	} {
		n, err := FrameCount(tc.pkt)
		if err != nil || n != tc.frames {
			t.Errorf("%x: %d frames %v", tc.pkt, n, err)
		}
		d, err := PacketDuration(tc.pkt)
		if err != nil || d != tc.d {
			t.Errorf("%x: %v %v", tc.pkt, d, err)
		}
		if s, err := PacketSamples(tc.pkt); err != nil || s != tc.samples {
			t.Errorf("%x: %d samples %v", tc.pkt, s, err)
		}
	}
	for _, pkt := range [][]byte{nil, {0x03}, {0x03, 0x00}, {0x1b, 0x03}} {
		if _, err := PacketDuration(pkt); err == nil {
			t.Errorf("%x: no error", pkt)
		}
	}
	if _, err := PacketSamples(nil); err != ErrEmptyPacket {
		t.Fatal(err)
	}
}

// FuzzHead parses arbitrary bytes as OpusHead: it either fails or yields a
// header that builds back the bytes it was read from.
func FuzzHead(f *testing.F) {
	f.Add(unhex(f, monoHead))
	f.Add(unhex(f, head51))
	f.Fuzz(func(t *testing.T, b []byte) {
		var h Head
		if err := h.Unmarshal(b); err != nil {
			_, _ = PacketDuration(b)
			return
		}
		out, err := h.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out, b[:h.MarshalSize()]) && h.Version != 0 {
			t.Fatalf("got %x want %x", out, b)
		}
	})
}
