package aac

import (
	"encoding/hex"
	"os"
	"testing"
)

// FuzzConfig parses arbitrary bytes as an AudioSpecificConfig: it either
// fails or yields a config that builds and parses back the same.
func FuzzConfig(f *testing.F) {
	for _, c := range configs {
		b, _ := hex.DecodeString(c.hex)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var c Config
		if err := c.Unmarshal(b); err != nil {
			return
		}
		out, err := c.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		var d Config
		if err := d.Unmarshal(out); err != nil || d != c {
			t.Fatalf("%x -> %+v -> %x -> %v %+v", b, c, out, err, d)
		}
		_ = c.FrameDuration()
		_ = c.Channels()
		_, _ = c.AppendADTS(nil, 100)
	})
}

// FuzzADTS splits arbitrary bytes as an ADTS stream: every frame it yields
// has a header that builds back the same.
func FuzzADTS(f *testing.F) {
	for _, name := range []string{"mono44100.aac", "51.aac"} {
		b, err := os.ReadFile("testdata/" + name)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b[:400])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		for len(b) > 0 {
			h, au, rest, err := NextADTS(b)
			if err != nil {
				return
			}
			if len(au)+h.HeaderSize()+len(rest) != len(b) {
				t.Fatal("lengths")
			}
			out, err := h.Append(nil)
			if err != nil {
				t.Fatal(err)
			}
			var g ADTSHeader
			if err := g.Unmarshal(out); err != nil || g != h {
				t.Fatalf("%+v -> %x -> %v %+v", h, out, err, g)
			}
			b = rest
		}
	})
}
