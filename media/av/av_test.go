package av

import (
	"math"
	"testing"
	"time"
)

func TestCodecID(t *testing.T) {
	for _, tc := range []struct {
		c            CodecID
		name         string
		video, audio bool
	}{
		{CodecUnknown, "unknown", false, false},
		{CodecH264, "H264", true, false},
		{CodecH265, "H265", true, false},
		{CodecAV1, "AV1", true, false},
		{CodecAAC, "AAC", false, true},
		{CodecOpus, "Opus", false, true},
		{CodecPCMU, "PCMU", false, true},
		{CodecPCMA, "PCMA", false, true},
		{CodecMP3, "MP3", false, true},
		{CodecID(200), "CodecID(200)", false, false},
	} {
		if got := tc.c.String(); got != tc.name {
			t.Errorf("%d: String %q, want %q", tc.c, got, tc.name)
		}
		if tc.c.IsVideo() != tc.video || tc.c.IsAudio() != tc.audio {
			t.Errorf("%v: video %v audio %v", tc.c, tc.c.IsVideo(), tc.c.IsAudio())
		}
	}
}

func TestTicks(t *testing.T) {
	for _, tc := range []struct {
		d     time.Duration
		rate  int
		ticks int64
	}{
		{time.Second, 90000, 90000},
		{-time.Second, 90000, -90000},
		{time.Second / 3, 90000, 30000},
		{5555, 90000, 0},   // 0.49995 ticks
		{5556, 90000, 1},   // 0.50004 ticks
		{-5556, 90000, -1}, // rounds away from zero
		{20 * time.Millisecond, 48000, 960},
		{time.Duration(math.MaxInt64), 1000, math.MaxInt64/1000000 + 1},
		{time.Duration(math.MinInt64), 1000, -math.MaxInt64/1000000 - 1},
		{time.Second, 0, 0},
	} {
		if got := ToTicks(tc.d, tc.rate); got != tc.ticks {
			t.Errorf("ToTicks(%v, %d) = %d, want %d", tc.d, tc.rate, got, tc.ticks)
		}
	}
	if got := FromTicks(1, 3); got != 333333333 {
		t.Errorf("FromTicks(1, 3) = %d", got)
	}
	if got := FromTicks(2, 3); got != 666666667 {
		t.Errorf("FromTicks(2, 3) = %d", got)
	}
	if got := FromTicks(-2, 3); got != -666666667 {
		t.Errorf("FromTicks(-2, 3) = %d", got)
	}
	if got := FromTicks(90000*3600, 90000); got != time.Hour {
		t.Errorf("FromTicks hour = %v", got)
	}
	if got := FromTicks(5, -1); got != 0 {
		t.Errorf("FromTicks rate -1 = %v", got)
	}
}

func TestPacketCTS(t *testing.T) {
	p := Packet{DTS: time.Second, PTS: time.Second + 40*time.Millisecond}
	if p.CTS() != 40*time.Millisecond {
		t.Fatal(p.CTS())
	}
}

// FuzzTicks checks that ticks -> Duration -> ticks is exact for every
// clock rate up to 1 GHz, which is what lets every protocol keep times as
// Durations without drift.
func FuzzTicks(f *testing.F) {
	f.Add(int64(0), 90000)
	f.Add(int64(1), 3)
	f.Add(int64(-12345678901), 48000)
	f.Add(int64(1)<<40, 1000000000)
	f.Fuzz(func(t *testing.T, ticks int64, rate int) {
		if rate <= 0 || rate > 1e9 {
			return
		}
		// Keep the Duration in range: |ticks/rate| seconds must fit.
		if ticks/int64(rate) > math.MaxInt64/int64(time.Second)-1 || ticks/int64(rate) < math.MinInt64/int64(time.Second)+1 {
			return
		}
		d := FromTicks(ticks, rate)
		if got := ToTicks(d, rate); got != ticks {
			t.Fatalf("ticks %d rate %d: via %d ns back to %d", ticks, rate, d, got)
		}
	})
}
