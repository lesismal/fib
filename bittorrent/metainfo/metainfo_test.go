package metainfo

import (
	"bytes"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// content returns n deterministic pseudo-random bytes.
func content(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// testFS has files of the sizes that matter: empty, smaller than a block,
// exactly a piece, a piece and a bit, several pieces, nested directories.
func testFS() fstest.MapFS {
	return fstest.MapFS{
		"d/a.bin":       {Data: content(1, 100)},
		"d/b/c.bin":     {Data: content(2, 64<<10)},
		"d/b/d.bin":     {Data: content(3, 64<<10+1)},
		"d/b/empty":     {Data: nil},
		"d/e f/big.bin": {Data: content(4, 5*64<<10+12345)},
		"d/z.bin":       {Data: content(5, 1)},
		"single.bin":    {Data: content(6, 3*64<<10+7)},
	}
}

// readRange reads [off, off+n) of the torrent's data from fsys, zeros for
// padding files.
func readRange(t *testing.T, info *Info, fsys fstest.MapFS, dir string, off, n int64) []byte {
	t.Helper()
	var out []byte
	for _, seg := range info.Segments(off, n) {
		f := info.FileList()[seg.File]
		if f.IsPad() {
			out = append(out, make([]byte, seg.Length)...)
			continue
		}
		name := dir
		if f.Path != nil {
			name = dir + "/" + strings.Join(f.Path, "/")
		}
		data := fsys[name].Data
		out = append(out, data[seg.Offset:seg.Offset+seg.Length]...)
	}
	return out
}

func TestBuildAndParse(t *testing.T) {
	fsys := testFS()
	for _, v := range []Version{V1, V2, Hybrid} {
		for _, root := range []string{"d", "single.bin"} {
			b := Builder{
				Version:      v,
				PieceLength:  64 << 10,
				Trackers:     [][]string{{"http://a/announce", "http://b/announce"}, {"udp://c:6969"}},
				WebSeeds:     []string{"http://seed/"},
				Nodes:        []Node{{"router.example", 6881}},
				Private:      true,
				Source:       "src",
				Comment:      "comment",
				CreatedBy:    "fib",
				CreationDate: time.Unix(1700000000, 0),
			}
			mi, err := b.Build(fsys, root)
			if err != nil {
				t.Fatalf("v%d %s: %v", v, root, err)
			}
			enc, err := bencode.Marshal(mi)
			if err != nil {
				t.Fatal(err)
			}
			mi2, err := Parse(enc)
			if err != nil {
				t.Fatalf("v%d %s: Parse: %v", v, root, err)
			}
			info := mi2.Info
			if info.InfoHashV1() != mi.Info.InfoHashV1() || info.InfoHashV2() != mi.Info.InfoHashV2() {
				t.Fatal("hashes changed through encoding")
			}
			if info.HasV1() != (v != V2) || info.HasV2() != (v != V1) || !info.Private || info.Source != "src" {
				t.Fatalf("v%d: HasV1 %v HasV2 %v", v, info.HasV1(), info.HasV2())
			}
			if mi2.Announce != "http://a/announce" || len(mi2.Trackers()) != 2 || mi2.URLList[0] != "http://seed/" ||
				mi2.Nodes[0] != (Node{"router.example", 6881}) || mi2.CreationDate != 1700000000 {
				t.Fatalf("metadata: %+v", mi2)
			}
			if missing, err := info.ValidatePieceLayers(mi2.PieceLayers); missing != 0 || err != nil {
				t.Fatal(missing, err)
			}
			checkLayout(t, info, fsys, root, v)
			// Every piece verifies; a corrupted one does not.
			for i := 0; i < info.PieceCount(); i++ {
				data := readRange(t, info, fsys, root, info.PieceOffset(i), info.PieceSize(i))
				if int64(len(data)) != info.PieceSize(i) {
					t.Fatalf("piece %d: %d bytes, want %d", i, len(data), info.PieceSize(i))
				}
				if err := info.VerifyPiece(i, data, mi2.PieceLayers); err != nil {
					t.Fatalf("v%d %s piece %d: %v", v, root, i, err)
				}
				data[len(data)/2] ^= 1
				if err := info.VerifyPiece(i, data, mi2.PieceLayers); err != ErrPieceHash {
					t.Fatalf("corrupt piece %d: %v", i, err)
				}
			}
		}
	}
}

func checkLayout(t *testing.T, info *Info, fsys fstest.MapFS, root string, v Version) {
	t.Helper()
	files := info.FileList()
	var real []string
	for _, f := range files {
		if f.IsPad() {
			if v != Hybrid {
				t.Fatal("padding outside a hybrid torrent")
			}
			continue
		}
		real = append(real, strings.Join(f.Path, "/"))
		if v != V1 && f.Length > 0 && f.Offset%info.PieceLength != 0 {
			t.Fatalf("%q at %d is not piece aligned", f.Path, f.Offset)
		}
		if v != V1 && f.Length > 0 && f.PiecesRoot == ([32]byte{}) {
			t.Fatalf("%q has no pieces root", f.Path)
		}
	}
	if root == "single.bin" {
		if len(files) != 1 || files[0].Path != nil || files[0].Length != int64(len(fsys[root].Data)) {
			t.Fatalf("single file: %+v", files)
		}
		return
	}
	want := []string{"a.bin", "b/c.bin", "b/d.bin", "b/empty", "e f/big.bin", "z.bin"}
	if !reflect.DeepEqual(real, want) {
		t.Fatalf("files %q, want %q", real, want)
	}
	// Each file reads back whole through Segments.
	for i, f := range files {
		if f.IsPad() {
			continue
		}
		got := readRange(t, info, fsys, root, f.Offset, f.Length)
		if !bytes.Equal(got, fsys[root+"/"+strings.Join(f.Path, "/")].Data) {
			t.Fatalf("file %d %q reads back wrong", i, f.Path)
		}
		begin, end := info.FilePieces(i)
		if f.Length > 0 && (int64(begin)*info.PieceLength > f.Offset || int64(end)*info.PieceLength < f.Offset+f.Length) {
			t.Fatalf("FilePieces(%d) = %d, %d", i, begin, end)
		}
	}
}

func TestPiecesRootIsFullTreeRoot(t *testing.T) {
	// A file's pieces root from its piece layer equals the root of the
	// tree over all its blocks.
	for _, n := range []int{1, BlockSize, BlockSize + 1, 4 * BlockSize, 9*BlockSize + 5, 33 * BlockSize} {
		data := content(uint64(n), n)
		leaves := HashBlocks(data)
		full := MerkleRoot(leaves, len(leaves), [32]byte{})
		for _, pl := range []int64{BlockSize, 2 * BlockSize, 4 * BlockSize} {
			root, layer := fileRoot(leaves, int64(n), pl)
			if root != full {
				t.Fatalf("n=%d pl=%d: root from layer differs", n, pl)
			}
			if layer != nil {
				if r, err := LayerRoot(layer, pl); err != nil || r != full {
					t.Fatalf("LayerRoot: %v", err)
				}
			}
		}
	}
	if PadHash(1) != sha256.Sum256(make([]byte, 64)) {
		t.Fatal("PadHash(1)")
	}
	if MerkleRoot(nil, 4, [32]byte{}) != PadHash(2) {
		t.Fatal("empty MerkleRoot")
	}
}

func TestVerifyProof(t *testing.T) {
	leaves := HashBlocks(content(9, 13*BlockSize))
	root := MerkleRoot(leaves, 16, [32]byte{})
	// Tree layers, padded to 16 leaves.
	layers := [][][32]byte{append(append([][32]byte(nil), leaves...), make([][32]byte, 3)...)}
	for len(layers[len(layers)-1]) > 1 {
		prev := layers[len(layers)-1]
		next := make([][32]byte, len(prev)/2)
		for i := range next {
			next[i] = hashPair(prev[2*i], prev[2*i+1])
		}
		layers = append(layers, next)
	}
	// Leaves 4..7 with uncles from layer 2 up.
	nodes := layers[0][4:8]
	uncles := [][32]byte{layers[2][0], layers[3][1]}
	if !VerifyProof(root, 4, nodes, uncles) {
		t.Fatal("valid proof rejected")
	}
	if VerifyProof(root, 8, nodes, uncles) || VerifyProof(root, 4, nodes, uncles[:1]) || VerifyProof(root, 2, nodes, uncles) {
		t.Fatal("invalid proof accepted")
	}
	bad := append([][32]byte(nil), nodes...)
	bad[1][0] ^= 1
	if VerifyProof(root, 4, bad, uncles) {
		t.Fatal("tampered proof accepted")
	}
	// Proof up to a node of a verified layer rather than the root.
	if !VerifyProof(layers[3][0], 4, nodes, uncles[:1]) {
		t.Fatal("proof to an inner node rejected")
	}
}

func TestMagnet(t *testing.T) {
	const v1 = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c"
	const v2 = "caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e"
	s := "magnet:?xt=urn:btih:" + v1 + "&xt=urn:btmh:1220" + v2 +
		"&dn=Big+Buck+Bunny&tr=udp%3A%2F%2Fexplodie.org%3A6969&tr=wss%3A%2F%2Ftracker.btorrent.xyz" +
		"&ws=https%3A%2F%2Fwebtorrent.io%2Ftorrents%2F&x.pe=10.0.0.1:6881&so=0,2,4-6&xl=1234&kt=a+b&xs=http%3A%2F%2Fx"
	m, err := ParseMagnet(s)
	if err != nil {
		t.Fatal(err)
	}
	if m.InfoHash.String() != v1 || m.InfoHashV2.String() != v2 || m.Name != "Big Buck Bunny" ||
		len(m.Trackers) != 2 || m.WebSeeds[0] != "https://webtorrent.io/torrents/" ||
		m.Peers[0] != "10.0.0.1:6881" || m.Length != 1234 || m.Params.Get("kt") != "a b" {
		t.Fatalf("%+v", m)
	}
	if !reflect.DeepEqual(m.Select, []IndexRange{{0, 0}, {2, 2}, {4, 6}}) || !m.Selected(5) || m.Selected(3) {
		t.Fatalf("so: %+v", m.Select)
	}
	m2, err := ParseMagnet(m.String())
	if err != nil || !reflect.DeepEqual(m, m2) {
		t.Fatalf("round trip: %v\n%+v\n%+v", err, m, m2)
	}
	// Base32 and upper-case hex.
	b32, err := ParseMagnet("magnet:?xt=urn:btih:" + base32.StdEncoding.EncodeToString(m.InfoHash[:]))
	if err != nil || b32.InfoHash.String() != v1 {
		t.Fatal(b32, err)
	}
	up, err := ParseMagnet("magnet:?xt=urn:btih:" + strings.ToUpper(v1))
	if err != nil || up.InfoHash.String() != v1 {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"http://x", "magnet:?dn=x", "magnet:?xt=urn:btih:abc", "magnet:?xt=urn:btmh:1114" + v2,
		"magnet:?xt=urn:btih:" + v1 + "&so=3-1", "magnet:?xt=urn:btih:" + v1 + "&xl=-1",
	} {
		if _, err := ParseMagnet(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestTorrentMagnet(t *testing.T) {
	mi, err := (&Builder{Trackers: [][]string{{"http://a"}, {"http://b", "http://a"}}}).Build(testFS(), "d")
	if err != nil {
		t.Fatal(err)
	}
	m := mi.Magnet()
	if m.InfoHash != mi.Info.InfoHashV1() || m.InfoHashV2 != mi.Info.InfoHashV2() || m.Name != "d" ||
		!reflect.DeepEqual(m.Trackers, []string{"http://a", "http://b"}) {
		t.Fatalf("%+v", m)
	}
}

func info(t *testing.T, s string) error {
	t.Helper()
	_, err := ParseInfo([]byte(s))
	return err
}

func TestParseInfoRejects(t *testing.T) {
	p20 := "20:" + strings.Repeat("x", 20)
	ok := "d6:lengthi10e4:name1:a12:piece lengthi16e6:pieces" + p20 + "e"
	if err := info(t, ok); err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]string{
		"dotdot name":    "d6:lengthi10e4:name2:..12:piece lengthi16e6:pieces" + p20 + "e",
		"slash name":     "d6:lengthi10e4:name3:a/b12:piece lengthi16e6:pieces" + p20 + "e",
		"no pieces":      "d6:lengthi10e4:name1:a12:piece lengthi16e6:pieces0:e",
		"extra pieces":   "d6:lengthi10e4:name1:a12:piece lengthi16e6:pieces40:" + strings.Repeat("x", 40) + "e",
		"zero piece len": "d6:lengthi10e4:name1:a12:piece lengthi0e6:pieces" + p20 + "e",
		"negative len":   "d6:lengthi-1e4:name1:a12:piece lengthi16e6:pieces0:e",
		"no length":      "d4:name1:a12:piece lengthi16e6:pieces0:e",
		"traversal":      "d5:filesld6:lengthi10e4:pathl2:..1:xeee4:name1:a12:piece lengthi16e6:pieces" + p20 + "e",
		"empty path":     "d5:filesld6:lengthi10e4:pathleee4:name1:a12:piece lengthi16e6:pieces" + p20 + "e",
		"clash":          "d5:filesld6:lengthi5e4:pathl1:xeed6:lengthi5e4:pathl1:xeee4:name1:a12:piece lengthi16e6:pieces" + p20 + "e",
		"file is dir":    "d5:filesld6:lengthi5e4:pathl1:xeed6:lengthi5e4:pathl1:x1:yeee4:name1:a12:piece lengthi16e6:pieces" + p20 + "e",
		"both":           "d5:filesld6:lengthi10e4:pathl1:xeee6:lengthi10e4:name1:a12:piece lengthi16e6:pieces" + p20 + "e",
		"v3":             "d6:lengthi10e12:meta versioni3e4:name1:a12:piece lengthi16e6:pieces" + p20 + "e",
		"v2 no tree":     "d12:meta versioni2e4:name1:a12:piece lengthi16384ee",
		"v2 bad pl":      "d9:file treed1:ad0:d6:lengthi1e11:pieces root32:" + strings.Repeat("r", 32) + "eee12:meta versioni2e4:name1:a12:piece lengthi1000ee",
		"v2 no root":     "d9:file treed1:ad0:d6:lengthi1eeee12:meta versioni2e4:name1:a12:piece lengthi16384ee",
		"v2 short root":  "d9:file treed1:ad0:d6:lengthi1e11:pieces root3:abceee12:meta versioni2e4:name1:a12:piece lengthi16384ee",
		"v2 file+dir":    "d9:file treed1:ad0:d6:lengthi1e11:pieces root32:" + strings.Repeat("r", 32) + "e1:bdeee12:meta versioni2e4:name1:a12:piece lengthi16384ee",
		"not a dict":     "le",
		"trailing":       ok + "x",
	} {
		if err := info(t, s); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestHybridMismatch(t *testing.T) {
	mi, err := (&Builder{PieceLength: 64 << 10}).Build(testFS(), "d")
	if err != nil {
		t.Fatal(err)
	}
	info := *mi.Info
	info.raw = nil
	info.Files = append([]FileInfo(nil), info.Files...)
	info.Files[0].Path = []string{"renamed"}
	if err := (&MetaInfo{}).SetInfo(&info); err == nil {
		t.Fatal("hybrid with different v1 and v2 files parsed")
	}
	// A piece layer that does not match its file fails Parse.
	for root, layer := range mi.PieceLayers {
		bad := bytes.Clone(layer)
		bad[0] ^= 1
		mi.PieceLayers[root] = bad
		break
	}
	enc, _ := bencode.Marshal(mi)
	if _, err := Parse(enc); err == nil {
		t.Fatal("bad piece layer parsed")
	}
	// Missing layers are allowed, and a hybrid torrent then checks v1.
	mi.PieceLayers = nil
	enc, _ = bencode.Marshal(mi)
	mi2, err := Parse(enc)
	if err != nil {
		t.Fatal(err)
	}
	if missing, _ := mi2.Info.ValidatePieceLayers(nil); missing == 0 {
		t.Fatal("no layers reported missing")
	}
	data := readRange(t, mi2.Info, testFS(), "d", 0, mi2.Info.PieceSize(0))
	if err := mi2.Info.VerifyPiece(0, data, nil); err != nil {
		t.Fatal(err)
	}
}

func TestV2OnlyPieceSizes(t *testing.T) {
	mi, err := (&Builder{Version: V2, PieceLength: 64 << 10}).Build(testFS(), "d")
	if err != nil {
		t.Fatal(err)
	}
	info := mi.Info
	// a.bin(100) c.bin(64K) d.bin(64K+1) big.bin(5*64K+12345) z.bin(1):
	// pieces 1 + 1 + 2 + 6 + 1, each file's last short.
	want := []int64{100, 64 << 10, 64 << 10, 1, 64 << 10, 64 << 10, 64 << 10, 64 << 10, 64 << 10, 12345, 1}
	if info.PieceCount() != len(want) {
		t.Fatalf("%d pieces", info.PieceCount())
	}
	for i, w := range want {
		if info.PieceSize(i) != w {
			t.Fatalf("piece %d: %d, want %d", i, info.PieceSize(i), w)
		}
	}
	if err := info.VerifyPieceV2(3, make([]byte, 1), nil); err != ErrNoPieceLayer {
		t.Fatalf("v2 piece without its layer: %v", err)
	}
}

func TestBuildPathAndLoad(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "share")
	for name, f := range testFS() {
		if !strings.HasPrefix(name, "d/") {
			continue
		}
		p := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(name, "d/")))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, f.Data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b := Builder{PieceLength: 64 << 10}
	mi, err := b.BuildPath(root)
	if err != nil {
		t.Fatal(err)
	}
	want, err := (&Builder{PieceLength: 64 << 10, Name: "share"}).Build(testFS(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if mi.Info.InfoHashV2() != want.Info.InfoHashV2() {
		t.Fatal("BuildPath and Build of the same files differ")
	}
	enc, _ := bencode.Marshal(mi)
	tf := filepath.Join(dir, "x.torrent")
	if err := os.WriteFile(tf, append(enc, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	mi2, err := Load(tf)
	if err != nil || mi2.Info.InfoHashV1() != mi.Info.InfoHashV1() {
		t.Fatal(err)
	}
	single, err := b.BuildPath(filepath.Join(root, "z.bin"))
	if err != nil || single.Info.Name != "z.bin" || single.Info.Length != 1 {
		t.Fatal(err)
	}
}

// TestGolden pins the info hashes of deterministic torrents, which the
// interop tests check against anacrolix/torrent, so that a change to the
// encoding shows.
func TestGolden(t *testing.T) {
	for _, c := range []struct {
		v      Version
		v1, v2 string
	}{
		{V1, "4bd048276d367122ea998668dfec6b892467406c", ""},
		{V2, "", "eb6e9a6e987a701590eb96168b2532056ebcea0d9ce4c722d88c4be432b86f8f"},
		{Hybrid, "a0b914d23026ec3b45ac91010c0896ac77032732", "373b35be4459d47a14975241ec194ca3556bc46e954a832f5ea38c1902bef129"},
	} {
		mi, err := (&Builder{Version: c.v, PieceLength: 64 << 10, CreatedBy: "fib"}).Build(testFS(), "d")
		if err != nil {
			t.Fatal(err)
		}
		if c.v1 != "" && mi.Info.InfoHashV1().String() != c.v1 || c.v2 != "" && mi.Info.InfoHashV2().String() != c.v2 {
			t.Errorf("version %d: %s %s", c.v, mi.Info.InfoHashV1(), mi.Info.InfoHashV2())
		}
	}
}

func TestURLListAndNodesTolerant(t *testing.T) {
	inf := "d6:lengthi10e4:name1:a12:piece lengthi16e6:pieces20:" + strings.Repeat("x", 20) + "e"
	mi, err := Parse([]byte("d13:creation date3:bad4:info" + inf + "5:nodesll1:hi1eei5eli1e1:xee8:url-list7:http://e"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string(mi.URLList), []string{"http://"}) || mi.Nodes[0] != (Node{"h", 1}) || mi.CreationDate != 0 {
		t.Fatalf("%+v", mi)
	}
	if _, err := Parse([]byte("d8:announce1:xe")); err == nil {
		t.Fatal("torrent without info parsed")
	}
	var se *bencode.SyntaxError
	if _, err := Parse([]byte("d4:info")); !errors.As(err, &se) {
		t.Fatal(err)
	}
}
