package mp4

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// FuzzOpen reads arbitrary bytes as a file: Open and ReadTable fail or
// give tracks whose tables take no more than TableBytes, at most four times
// the bytes of the file, and whose samples lie within the file, in order of
// decode time; no lookup panics. The seeds are ffmpeg's files of testdata
// and files built in each way TestTables writes them.
func FuzzOpen(f *testing.F) {
	names, _ := filepath.Glob("testdata/*.mp4")
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	for i, v := range variants() {
		if i%5 == 0 {
			f.Add(buildFile(v.tracks(f), v.moovFirst, v.large))
		}
	}
	// Small files, whose tables mutations reach more often: a few samples
	// with each kind of size table, composition offsets, sync samples and
	// edit list, and no sample entry worth reading.
	for i, sizes := range []int{32, 16, 8, 4, 0} {
		samples := audioSamples(6)
		if sizes == 0 {
			for j := range samples {
				samples[j].size = 3
			}
		}
		tr := synth{timescale: 1000, handler: "soun", stsd: box("stsd", full(0, 0), be32(1), box("mp4a", make([]byte, 28))),
			samples: samples, perChunk: []int{1, 2}, co64: i%2 == 0, sizes: sizes, ctts: i % 3,
			stss: []string{"list", "none", "empty", "reversed", "list"}[i], editV1: i%2 == 1,
			edits: []Edit{{Duration: 10, MediaTime: -1, Rate: 1 << 16}, {Duration: 100, MediaTime: 5, Rate: 1 << 16}}}
		f.Add(buildFile([]synth{tr}, i%2 == 0, i == 3))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r := bytes.NewReader(b)
		file, err := Open(r, int64(len(b)))
		if err != nil {
			return
		}
		for _, tr := range file.Tracks {
			if tr.Track == nil {
				t.Fatal("no codec")
			}
			bound := tr.TableBytes()
			if bound > tableBaseBytes+4*int64(len(b)) {
				t.Fatalf("TableBytes %d of a file of %d", bound, len(b))
			}
			tb, err := tr.ReadTable(r)
			if err != nil {
				continue
			}
			if tb.Bytes() > bound {
				t.Fatalf("%d bytes, bound %d", tb.Bytes(), bound)
			}
			checkFuzzed(t, tb, int64(len(b)))
		}
	})
}

func checkFuzzed(t *testing.T, tb *Table, size int64) {
	var prev int64
	n := 0
	for i, s := range tb.Samples(0) {
		if s.Offset < 0 || s.Offset+int64(s.Size) > size || s.DTS < prev {
			t.Fatalf("sample %d: %+v in a file of %d after %d", i, s, size, prev)
		}
		if i%7 == 0 && s != tb.Sample(i) {
			t.Fatalf("sample %d: walked %+v, looked up %+v", i, s, tb.Sample(i))
		}
		prev = s.DTS
		if n++; n == 1000 {
			break
		}
	}
	for _, i := range []int{-1, 0, 1, tb.Len() / 2, tb.Len() - 1, tb.Len()} {
		tb.KeyframeBefore(i)
		tb.KeyframeAfter(i)
		for range tb.Samples(i) {
			break
		}
	}
	if k := tb.Keyframes(); k > 0 {
		tb.Keyframe(k - 1)
	}
	for _, d := range []time.Duration{-time.Hour, 0, time.Second, tb.Time(tb.End()), 1 << 62} {
		if i := tb.IndexAt(d); i < -1 || i >= tb.Len() {
			t.Fatalf("IndexAt(%v) = %d of %d", d, i, tb.Len())
		}
		if i := tb.SeekKeyframe(d); i < -1 || i >= tb.Len() {
			t.Fatalf("SeekKeyframe(%v) = %d of %d", d, i, tb.Len())
		}
	}
}
