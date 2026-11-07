package interop

import (
	"bytes"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"

	abencode "github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/merkle"
	ametainfo "github.com/anacrolix/torrent/metainfo"
	infohash_v2 "github.com/anacrolix/torrent/types/infohash-v2"
)

// sampleFiles are the files the torrents are made of: empty, smaller than
// a block, exactly a piece, a piece and a byte, several pieces, nested.
var sampleFiles = map[string]int{
	"a.bin":        100,
	"b/c.bin":      64 << 10,
	"b/d.bin":      64<<10 + 1,
	"b/empty":      0,
	"e f/big.bin":  5*64<<10 + 12345,
	"z.bin":        1,
	"z/deep/x.bin": 3*16<<10 + 5,
}

func writeSample(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "sample")
	seed := uint64(1)
	for name, n := range sampleFiles {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewPCG(seed, seed))
		seed++
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(r.Uint32())
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// fileData reads file f of info below dir: zeros for padding.
func fileData(t *testing.T, dir string, info *metainfo.Info, f metainfo.File) []byte {
	t.Helper()
	if f.IsPad() {
		return make([]byte, f.Length)
	}
	p := dir
	if f.Path != nil {
		p = filepath.Join(dir, filepath.Join(f.Path...))
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// verifyAllPieces checks every piece of a torrent of the files below dir.
func verifyAllPieces(t *testing.T, dir string, mi *metainfo.MetaInfo) {
	t.Helper()
	info := mi.Info
	cache := map[int][]byte{}
	for i := 0; i < info.PieceCount(); i++ {
		var piece []byte
		for _, s := range info.PieceSegments(i) {
			data, ok := cache[s.File]
			if !ok {
				data = fileData(t, dir, info, info.FileList()[s.File])
				cache[s.File] = data
			}
			piece = append(piece, data[s.Offset:s.Offset+s.Length]...)
		}
		if err := info.VerifyPiece(i, piece, mi.PieceLayers); err != nil {
			t.Fatalf("piece %d: %v", i, err)
		}
	}
}

// compareWithAnacrolix parses enc, a .torrent file, with anacrolix and
// checks that it agrees with fib's parse of it on hashes, version, files
// and pieces.
func compareWithAnacrolix(t *testing.T, enc []byte, mi *metainfo.MetaInfo) *ametainfo.Info {
	t.Helper()
	ami, err := ametainfo.Load(bytes.NewReader(enc))
	if err != nil {
		t.Fatalf("anacrolix Load: %v", err)
	}
	ainfo, err := ami.UnmarshalInfo()
	if err != nil {
		t.Fatalf("anacrolix UnmarshalInfo: %v", err)
	}
	info := mi.Info
	if ainfo.HasV1() != info.HasV1() || ainfo.HasV2() != info.HasV2() {
		t.Fatalf("versions: anacrolix v1 %v v2 %v, fib v1 %v v2 %v", ainfo.HasV1(), ainfo.HasV2(), info.HasV1(), info.HasV2())
	}
	if ami.HashInfoBytes() != ametainfo.Hash(info.InfoHashV1()) {
		t.Fatalf("v1 info hash: anacrolix %s, fib %s", ami.HashInfoBytes(), info.InfoHashV1())
	}
	if infohash_v2.HashBytes(ami.InfoBytes) != infohash_v2.T(info.InfoHashV2()) {
		t.Fatal("v2 info hash differs")
	}
	if ainfo.NumPieces() != info.PieceCount() {
		t.Fatalf("pieces: anacrolix %d, fib %d", ainfo.NumPieces(), info.PieceCount())
	}
	if ainfo.Name != info.Name || ainfo.PieceLength != info.PieceLength {
		t.Fatalf("name %q/%q piece length %d/%d", ainfo.Name, info.Name, ainfo.PieceLength, info.PieceLength)
	}
	// anacrolix lists a v2 torrent's files from its tree and a v1's from
	// the files list, padding included; fib lists a hybrid's from the
	// files list, so leave padding out to compare.
	var afiles, ffiles []string
	for _, f := range ainfo.UpvertedFiles() {
		if strings.Contains(f.Attr, "p") {
			continue
		}
		afiles = append(afiles, strings.Join(f.Path, "/")+":"+itoa(f.Length)+"@"+itoa(f.TorrentOffset))
	}
	for _, f := range info.FileList() {
		if f.IsPad() {
			continue
		}
		p := f.Path
		if p == nil && info.HasV2() {
			p = []string{info.Name} // anacrolix gives a v2 single file its name as path
		}
		ffiles = append(ffiles, strings.Join(p, "/")+":"+itoa(f.Length)+"@"+itoa(f.Offset))
	}
	if !reflect.DeepEqual(afiles, ffiles) {
		t.Fatalf("files:\nanacrolix %q\nfib       %q", afiles, ffiles)
	}
	if info.HasV2() {
		layers := map[string]string{}
		for root, layer := range mi.PieceLayers {
			layers[string(root[:])] = string(layer)
		}
		if err := ametainfo.ValidatePieceLayers(layers, &ainfo.FileTree, ainfo.PieceLength); err != nil {
			t.Fatalf("anacrolix ValidatePieceLayers: %v", err)
		}
	}
	return &ainfo
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// TestFibTorrentsInAnacrolix builds v1, v2 and hybrid torrents with fib
// and checks that anacrolix/torrent reads them the same, including its own
// merkle roots of each file and its validation of the piece layers.
func TestFibTorrentsInAnacrolix(t *testing.T) {
	dir := writeSample(t)
	for _, v := range []metainfo.Version{metainfo.V1, metainfo.V2, metainfo.Hybrid} {
		for _, root := range []string{dir, filepath.Join(dir, "e f", "big.bin")} {
			b := metainfo.Builder{
				Version:     v,
				PieceLength: 64 << 10,
				Trackers:    [][]string{{"http://t1/announce"}, {"udp://t2:6969", "udp://t3:6969"}},
				WebSeeds:    []string{"http://ws/"},
				Private:     true,
				CreatedBy:   "fib",
			}
			mi, err := b.BuildPath(root)
			if err != nil {
				t.Fatal(err)
			}
			enc, err := bencode.Marshal(mi)
			if err != nil {
				t.Fatal(err)
			}
			ainfo := compareWithAnacrolix(t, enc, mi)
			if ainfo.Private == nil || !*ainfo.Private {
				t.Fatal("private flag lost")
			}
			ami, _ := ametainfo.Load(bytes.NewReader(enc))
			if !reflect.DeepEqual([][]string(ami.UpvertedAnnounceList()), mi.Trackers()) || ami.UrlList[0] != "http://ws/" {
				t.Fatalf("trackers %q, web seeds %q", ami.UpvertedAnnounceList(), ami.UrlList)
			}
			// anacrolix's own merkle hash of each file is fib's pieces root.
			if v != metainfo.V1 {
				for _, f := range mi.Info.FileList() {
					if f.IsPad() || f.Length == 0 {
						continue
					}
					h := merkle.NewHash()
					h.Write(fileData(t, root, mi.Info, f))
					if !bytes.Equal(h.Sum(nil), f.PiecesRoot[:]) {
						t.Fatalf("%q: anacrolix merkle root differs from fib's pieces root", f.Path)
					}
				}
			}
		}
	}
}

// TestAnacrolixTorrentsInFib builds torrents with anacrolix and checks
// that fib parses them to the same hash and files, verifies every piece of
// them, and, for files whose order both agree on, builds the same info
// dictionary byte for byte. (anacrolix sorts v1 files by their joined
// path, fib by path component as a v2 file tree is ordered, so "z.bin"
// comes before "z/x" in anacrolix and after it in fib.)
func TestAnacrolixTorrentsInFib(t *testing.T) {
	dir := writeSample(t)
	for _, root := range []string{dir, filepath.Join(dir, "b"), filepath.Join(dir, "a.bin")} {
		ainfo := ametainfo.Info{PieceLength: 64 << 10}
		if err := ainfo.BuildFromFilePath(root); err != nil {
			t.Fatal(err)
		}
		infoBytes, err := abencode.Marshal(ainfo)
		if err != nil {
			t.Fatal(err)
		}
		ami := ametainfo.MetaInfo{
			InfoBytes:    infoBytes,
			Announce:     "http://t1/announce",
			AnnounceList: [][]string{{"http://t1/announce"}, {"udp://t2:6969"}},
			UrlList:      []string{"http://ws/"},
			Comment:      "anacrolix",
		}
		var buf bytes.Buffer
		if err := ami.Write(&buf); err != nil {
			t.Fatal(err)
		}
		mi, err := metainfo.Parse(buf.Bytes())
		if err != nil {
			t.Fatalf("fib Parse: %v", err)
		}
		compareWithAnacrolix(t, buf.Bytes(), mi)
		if mi.Comment != "anacrolix" || len(mi.Trackers()) != 2 || mi.URLList[0] != "http://ws/" {
			t.Fatalf("metadata %+v", mi)
		}
		verifyAllPieces(t, root, mi) // a single file's nil Path reads root itself

		ours, err := (&metainfo.Builder{Version: metainfo.V1, PieceLength: 64 << 10}).BuildPath(root)
		if err != nil {
			t.Fatal(err)
		}
		if root == dir {
			continue // the files whose order differs
		}
		if !bytes.Equal(ours.InfoBytes, infoBytes) {
			t.Fatalf("info dictionaries differ:\nanacrolix %.200q\nfib       %.200q", infoBytes, ours.InfoBytes)
		}
	}
}

// TestRealWorldTorrents parses the .torrent files of anacrolix/torrent's
// test data, among them libtorrent's v2 and hybrid test torrents and
// torrents from the wild, with both and compares them.
func TestRealWorldTorrents(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/anacrolix/torrent").Output()
	if err != nil {
		t.Skipf("cannot find anacrolix/torrent's module: %v", err)
	}
	modDir := strings.TrimSpace(string(out))
	var paths []string
	for _, pattern := range []string{"testdata/*.torrent", "metainfo/testdata/*.torrent"} {
		m, _ := filepath.Glob(filepath.Join(modDir, pattern))
		paths = append(paths, m...)
	}
	if len(paths) < 10 {
		t.Fatalf("found only %d torrents in %s", len(paths), modDir)
	}
	var v2, hybrid int
	for _, p := range paths {
		name := filepath.Base(p)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		mi, ferr := metainfo.Parse(data)
		ami, aerr := ametainfo.Load(bytes.NewReader(data))
		if aerr == nil {
			_, aerr = ami.UnmarshalInfo()
		}
		switch {
		case ferr != nil && aerr != nil:
			t.Logf("%s: rejected by both (fib: %v)", name, ferr)
			continue
		case ferr != nil:
			t.Errorf("%s: anacrolix parses it, fib does not: %v", name, ferr)
			continue
		case aerr != nil:
			// anacrolix rejects trailing data after the dictionary, which
			// fib allows when it is whitespace.
			t.Logf("%s: fib parses it, anacrolix does not: %v", name, aerr)
			continue
		}
		t.Run(name, func(t *testing.T) {
			compareWithAnacrolix(t, bytes.TrimRight(data, " \t\r\n"), mi)
			if mi.Info.HasV2() {
				v2++
				if mi.Info.HasV1() {
					hybrid++
				}
				if missing, err := mi.Info.ValidatePieceLayers(mi.PieceLayers); missing != 0 || err != nil {
					t.Fatalf("piece layers: %d missing, %v", missing, err)
				}
			}
			// The magnet link of each reads in anacrolix.
			am, err := ametainfo.ParseMagnetV2Uri(mi.Magnet().String())
			if err != nil {
				t.Fatal(err)
			}
			if mi.Info.HasV1() && am.InfoHash.Value != ametainfo.Hash(mi.Info.InfoHashV1()) {
				t.Fatal("magnet v1 hash")
			}
			if mi.Info.HasV2() && am.V2InfoHash.Value != infohash_v2.T(mi.Info.InfoHashV2()) {
				t.Fatal("magnet v2 hash")
			}
		})
	}
	if v2 < 2 || hybrid < 1 {
		t.Fatalf("expected the v2 and hybrid test torrents: %d v2, %d hybrid", v2, hybrid)
	}
}

// TestMagnetInterop formats magnet links on each side and parses them on
// the other.
func TestMagnetInterop(t *testing.T) {
	v1, _ := metainfo.ParseInfoHash("dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c")
	v2, _ := metainfo.ParseInfoHashV2("caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e")
	for _, m := range []*metainfo.Magnet{
		{InfoHash: v1, Name: "Big Buck Bunny", Trackers: []string{"udp://explodie.org:6969", "wss://tracker.btorrent.xyz"},
			WebSeeds: []string{"https://webtorrent.io/torrents/"}, Peers: []string{"10.0.0.1:6881"}},
		{InfoHashV2: v2, Name: "v2 only", Trackers: []string{"http://t/announce?a=b&c=d"}},
		{InfoHash: v1, InfoHashV2: v2, Name: "hybrid ü", Select: []metainfo.IndexRange{{First: 0, Last: 0}, {First: 2, Last: 5}}, Length: 42},
	} {
		s := m.String()
		am, err := ametainfo.ParseMagnetV2Uri(s)
		if err != nil {
			t.Fatalf("anacrolix parses %q: %v", s, err)
		}
		if am.InfoHash.Ok != !m.InfoHash.IsZero() || am.InfoHash.Ok && am.InfoHash.Value != ametainfo.Hash(m.InfoHash) ||
			am.V2InfoHash.Ok != !m.InfoHashV2.IsZero() || am.V2InfoHash.Ok && am.V2InfoHash.Value != infohash_v2.T(m.InfoHashV2) {
			t.Fatalf("hashes of %q: %+v", s, am)
		}
		if am.DisplayName != m.Name || !reflect.DeepEqual(am.Trackers, m.Trackers) ||
			!reflect.DeepEqual(am.Params["ws"], m.WebSeeds) || !reflect.DeepEqual(am.Params["x.pe"], m.Peers) {
			t.Fatalf("%q: anacrolix %+v", s, am)
		}

		// And back: anacrolix's formatting parses in fib.
		back, err := metainfo.ParseMagnet(am.String())
		if err != nil {
			t.Fatalf("fib parses %q: %v", am.String(), err)
		}
		if !reflect.DeepEqual(back, m) {
			t.Fatalf("round trip through anacrolix:\n%+v\n%+v", back, m)
		}
	}
	// A v1 link in base32, as old clients write it, in anacrolix's v1 parser.
	am, err := ametainfo.ParseMagnetUri("magnet:?xt=urn:btih:3WBFL3G4PSSV7MF36ATSHWDQMLNR63I4")
	if err == nil {
		m, err := metainfo.ParseMagnet("magnet:?xt=urn:btih:3WBFL3G4PSSV7MF36ATSHWDQMLNR63I4")
		if err != nil || ametainfo.Hash(m.InfoHash) != am.InfoHash {
			t.Fatalf("base32: %v", err)
		}
	}
}
