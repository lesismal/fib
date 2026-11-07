package metainfo

import (
	"reflect"
	"testing"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// FuzzParse checks that parsing never panics, that the layout of what
// parses is consistent, and that it re-encodes to the same torrent.
func FuzzParse(f *testing.F) {
	for _, v := range []Version{V1, V2, Hybrid} {
		for _, root := range []string{"d", "single.bin"} {
			mi, err := (&Builder{Version: v, PieceLength: 16 << 10}).Build(testFS(), root)
			if err != nil {
				f.Fatal(err)
			}
			b, _ := bencode.Marshal(mi)
			f.Add(b)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		mi, err := Parse(data)
		if err != nil {
			return
		}
		info := mi.Info
		n := info.PieceCount()
		check := func(i int) {
			size := info.PieceSize(i)
			if size <= 0 || size > info.PieceLength {
				t.Fatalf("piece %d of %d has size %d", i, n, size)
			}
			var sum int64
			for _, s := range info.PieceSegments(i) {
				if s.Length <= 0 || s.Offset < 0 || s.Offset+s.Length > info.FileList()[s.File].Length {
					t.Fatalf("piece %d: bad segment %+v", i, s)
				}
				sum += s.Length
			}
			if sum != size && info.HasV1() {
				t.Fatalf("piece %d: segments cover %d of %d bytes", i, sum, size)
			}
			_ = info.VerifyPiece(i, make([]byte, min(size, 1<<16)), mi.PieceLayers)
		}
		for i := 0; i < min(n, 64); i++ {
			check(i)
		}
		if n > 0 {
			check(n - 1)
		}
		for i := range info.FileList() {
			b, e := info.FilePieces(i)
			if b < 0 || e < b || e > n {
				t.Fatalf("file %d: pieces %d-%d of %d", i, b, e, n)
			}
		}
		_ = mi.Magnet().String()
		enc, err := bencode.Marshal(mi)
		if err != nil {
			t.Fatal(err)
		}
		mi2, err := Parse(enc)
		if err != nil {
			t.Fatalf("re-encoding does not parse: %v", err)
		}
		if mi2.Info.InfoHashV1() != info.InfoHashV1() {
			t.Fatal("re-encoding changed the info hash")
		}
	})
}

// FuzzParseMagnet checks that whatever parses formats back to a link that
// parses to the same thing.
func FuzzParseMagnet(f *testing.F) {
	f.Add("magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&dn=x&tr=http%3A%2F%2Fa&so=0,2-3&x.pe=1.2.3.4:5&xl=9&kt=a")
	f.Add("magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e&ws=http://w")
	f.Fuzz(func(t *testing.T, s string) {
		m, err := ParseMagnet(s)
		if err != nil {
			return
		}
		m2, err := ParseMagnet(m.String())
		if err != nil {
			t.Fatalf("%q formatted as %q, which does not parse: %v", s, m.String(), err)
		}
		if !reflect.DeepEqual(m, m2) {
			t.Fatalf("%q: %+v formatted as %q parses to %+v", s, m, m.String(), m2)
		}
	})
}
