package hls

import (
	"testing"
	"time"
)

// FuzzParseName parses arbitrary request file names: what it accepts names
// a file the Muxer would call the same, and nothing panics, File and
// fileWait included.
func FuzzParseName(f *testing.F) {
	m := NewMuxer(Config{PartDuration: 500 * time.Millisecond})
	s := m.session
	for _, seed := range []string{
		"init-" + s + "-1.mp4", "seg-" + s + "-12.m4s", "part-" + s + "-3-4.m4s",
		"seg-" + s + "-18446744073709551615.m4s", "part-" + s + "-1-99999999999.m4s", "index.m3u8", "",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		kind, a, b := m.parseName(name)
		_, _ = m.File(name)
		_, _, _ = m.fileWait(name)
		var want []byte
		switch kind {
		case fileNone:
			return
		case fileInit:
			want = m.initName(nil, int(a))
		case fileSegment:
			want = m.segName(nil, a)
		case filePart:
			want = m.partName(nil, a, int(b))
		}
		// Numbers may come with leading zeros or a sign; the name the Muxer
		// gives them parses the same.
		if k, a2, b2 := m.parseName(string(want)); k != kind || a2 != a || b2 != b {
			t.Fatalf("%q -> %q", name, want)
		}
	})
}
