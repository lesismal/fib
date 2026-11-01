package fmp4

import (
	"path/filepath"
	"testing"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/opus"
)

// seeds returns init segments and fragments this package writes, and the
// ffmpeg captures in testdata, to start the fuzzers from.
func seeds(f *testing.F) (inits, frags [][]byte) {
	head := &opus.Head{Channels: 3, MappingFamily: 1, StreamCount: 2, CoupledCount: 1, ChannelMapping: []byte{0, 2, 1}}
	tracks := []*av.Track{h264Track(f), h265Track(f), {Codec: av.CodecAV1, Config: av1Config}, aacTrack(f, 44100), opusTrack(f, head)}
	w, err := NewWriter(tracks)
	if err != nil {
		f.Fatal(err)
	}
	inits = append(inits, w.Init())
	for i := range 6 {
		for track := range tracks {
			w.WritePacket(pkt(track, i*40, 40-i*10, i%3 == 0, "sample data"))
		}
		if i%2 == 1 {
			frag, _ := w.Flush(nil, nil)
			frags = append(frags, frag)
		}
	}
	names, _ := filepath.Glob("testdata/*.mp4")
	for _, name := range names {
		b := readFile(f, filepath.Base(name))
		inits = append(inits, b)
		frags = append(frags, b)
	}
	return inits, frags
}

// FuzzParseInit parses arbitrary bytes as an init segment: it fails or
// yields tracks, without panicking.
func FuzzParseInit(f *testing.F) {
	inits, _ := seeds(f)
	for _, b := range inits {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		init, err := ParseInit(b)
		if err != nil {
			return
		}
		for _, tr := range init.Tracks {
			if tr.Track == nil {
				t.Fatal("nil track")
			}
		}
	})
}

// FuzzParseFragment parses arbitrary bytes as a fragment of the seed init
// segment: it fails or yields packets whose data lies within the input,
// without panicking.
func FuzzParseFragment(f *testing.F) {
	inits, frags := seeds(f)
	init, err := ParseInit(inits[0])
	if err != nil {
		f.Fatal(err)
	}
	for _, b := range frags {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, pkts, rest, err := init.ParseFragment(b)
		if err != nil {
			return
		}
		if len(rest) > len(b) {
			t.Fatal("rest longer than input")
		}
		// Runs may share bytes, but every sample lies within the input.
		for _, p := range pkts {
			if p.Track < 0 || p.Track >= len(init.Tracks) {
				t.Fatal(p.Track)
			}
			if len(p.Data) > len(b) {
				t.Fatal("sample larger than input")
			}
		}
	})
}

// FuzzBoxes walks arbitrary bytes as boxes.
func FuzzBoxes(f *testing.F) {
	inits, frags := seeds(f)
	for _, b := range append(inits, frags...) {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		n := 0
		for box, err := range Boxes(b) {
			if err != nil {
				return
			}
			n += box.Size()
		}
		if n != len(b) {
			t.Fatal(n, len(b))
		}
	})
}
