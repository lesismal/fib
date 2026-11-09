package metainfo

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/fstest"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// builderCases is a matrix of files and piece lengths whose whole .torrent
// bytes TestBuilderByteIdentical pins: files smaller than a piece, of
// exactly a piece, not a multiple of it, of leaf counts that are not a
// power of two, empty files and hybrid padding.
func builderCases() (fsys fstest.MapFS, roots []string, pls []int64) {
	fsys = testFS()
	fsys["e/three-leaves"] = &fstest.MapFile{Data: content(7, 3*BlockSize)}
	fsys["e/five-leaves-and-a-byte"] = &fstest.MapFile{Data: content(8, 5*BlockSize+1)}
	fsys["e/empty"] = &fstest.MapFile{}
	fsys["e/one-block"] = &fstest.MapFile{Data: content(9, BlockSize)}
	fsys["e/sub/odd"] = &fstest.MapFile{Data: content(10, 9*BlockSize+777)}
	fsys["exact.bin"] = &fstest.MapFile{Data: content(11, 8*BlockSize)}
	return fsys, []string{"d", "single.bin", "e", "exact.bin"}, []int64{0, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10}
}

func buildDigest(t *testing.T, fsys fstest.MapFS, root string, v Version, pl int64) string {
	t.Helper()
	mi, err := (&Builder{Version: v, PieceLength: pl, CreatedBy: "fib"}).Build(fsys, root)
	if err != nil {
		t.Fatal(root, v, pl, err)
	}
	b, err := bencode.Marshal(mi)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

// builderGolden holds the digests of the .torrent bytes the Builder made
// before it folded leaves per piece (key: root/version/piece length); the
// output must not change.
var builderGolden = map[string]string{
	"d/1/0":               "f8c4ffd0ddc1f4d8",
	"d/1/16384":           "f8c4ffd0ddc1f4d8",
	"d/1/32768":           "60c36800e96fe37f",
	"d/1/65536":           "f0b4bd9bb1a8ab7a",
	"d/1/131072":          "918695a07f25525b",
	"d/1/262144":          "2664860e7872d122",
	"d/2/0":               "79f61015d91be351",
	"d/2/16384":           "79f61015d91be351",
	"d/2/32768":           "46375e73acb49537",
	"d/2/65536":           "41822a10a6ffa426",
	"d/2/131072":          "19c46cebdca1b73d",
	"d/2/262144":          "ba0face443c1eebc",
	"d/0/0":               "dd99a21b5ceed9e0",
	"d/0/16384":           "dd99a21b5ceed9e0",
	"d/0/32768":           "9dc9a75e08b67103",
	"d/0/65536":           "37fd1aa14280f7ee",
	"d/0/131072":          "2af49b2fdf69345a",
	"d/0/262144":          "fee0cb28a489762b",
	"single.bin/1/0":      "f4d5553533a49408",
	"single.bin/1/16384":  "f4d5553533a49408",
	"single.bin/1/32768":  "ad0fb2a5c35fc88f",
	"single.bin/1/65536":  "dafecc50aefb06c3",
	"single.bin/1/131072": "0a73552a2d7389ea",
	"single.bin/1/262144": "d730d3f9cfbb125e",
	"single.bin/2/0":      "c2e1246f8e7962d8",
	"single.bin/2/16384":  "c2e1246f8e7962d8",
	"single.bin/2/32768":  "9aa42064c51b5b7a",
	"single.bin/2/65536":  "ff82395bb1ffb73e",
	"single.bin/2/131072": "9ad6c8b82c9afa5e",
	"single.bin/2/262144": "13ab477c01228baa",
	"single.bin/0/0":      "bdfddc0caf049d6c",
	"single.bin/0/16384":  "bdfddc0caf049d6c",
	"single.bin/0/32768":  "b1f87fd50eb59961",
	"single.bin/0/65536":  "8a0dad47119987ca",
	"single.bin/0/131072": "b51f351f7b244d79",
	"single.bin/0/262144": "5f425fa343013208",
	"e/1/0":               "bb6a2400fc7dbe62",
	"e/1/16384":           "bb6a2400fc7dbe62",
	"e/1/32768":           "92423de3876d45e8",
	"e/1/65536":           "fd4416abfdf9332a",
	"e/1/131072":          "656b1dd88b4006ea",
	"e/1/262144":          "c2fb2fbd9a4f9c3c",
	"e/2/0":               "08a39fa24388c3e4",
	"e/2/16384":           "08a39fa24388c3e4",
	"e/2/32768":           "1b96906976c7673a",
	"e/2/65536":           "01e802ebf78ce72e",
	"e/2/131072":          "bdb7ba90a58211c1",
	"e/2/262144":          "abc89f192688ae00",
	"e/0/0":               "4c2bbcbe8a84fab3",
	"e/0/16384":           "4c2bbcbe8a84fab3",
	"e/0/32768":           "147d7c61d5c33c40",
	"e/0/65536":           "66a3ddd34d1be562",
	"e/0/131072":          "2e31af780fe67fcc",
	"e/0/262144":          "5b1e905222f948e3",
	"exact.bin/1/0":       "2757e720110f1684",
	"exact.bin/1/16384":   "2757e720110f1684",
	"exact.bin/1/32768":   "02f798e05dfb04e3",
	"exact.bin/1/65536":   "3fad426e25c855bf",
	"exact.bin/1/131072":  "3d9be80f20f18579",
	"exact.bin/1/262144":  "27dfbc9833d9818e",
	"exact.bin/2/0":       "8944176566caf7c0",
	"exact.bin/2/16384":   "8944176566caf7c0",
	"exact.bin/2/32768":   "371842098d8e22b6",
	"exact.bin/2/65536":   "76da244fe3aed25d",
	"exact.bin/2/131072":  "d0d90f33218fd41f",
	"exact.bin/2/262144":  "e710dc6066e86925",
	"exact.bin/0/0":       "712a2038e46a5fc1",
	"exact.bin/0/16384":   "712a2038e46a5fc1",
	"exact.bin/0/32768":   "ac948286d6ba8492",
	"exact.bin/0/65536":   "1b79123cff1ee956",
	"exact.bin/0/131072":  "736f161bf9541d99",
	"exact.bin/0/262144":  "850f0c2295eebafa",
}

func TestBuilderByteIdentical(t *testing.T) {
	fsys, roots, pls := builderCases()
	for _, root := range roots {
		for _, v := range []Version{V1, V2, Hybrid} {
			for _, pl := range pls {
				key := fmt.Sprintf("%s/%d/%d", root, v, pl)
				got := buildDigest(t, fsys, root, v, pl)
				if os.Getenv("FIB_PRINT_GOLDEN") != "" {
					fmt.Printf("\t%q: %q,\n", key, got)
					continue
				}
				if want := builderGolden[key]; got != want {
					t.Errorf("%s: digest %s, want %s", key, got, want)
				}
			}
		}
	}
}

// refFileRoot is how the Builder made a file's pieces root and piece layer
// from all its leaves before fileHasher.
func refFileRoot(leaves [][32]byte, length, pieceLength int64) (root [32]byte, layer []byte) {
	perPiece := int(pieceLength / BlockSize)
	if length <= pieceLength {
		return MerkleRoot(leaves, len(leaves), [32]byte{}), nil
	}
	var nodes [][32]byte
	for i := 0; i < len(leaves); i += perPiece {
		nodes = append(nodes, MerkleRoot(leaves[i:min(i+perPiece, len(leaves))], perPiece, [32]byte{}))
	}
	for i := range nodes {
		layer = append(layer, nodes[i][:]...)
	}
	return MerkleRoot(nodes, len(nodes), PadHash(log2(int64(perPiece)))), layer
}

func leaf(i int) (l [32]byte) {
	binary.LittleEndian.PutUint64(l[:], uint64(i)+1)
	return l
}

func TestFileHasherMatchesAllLeaves(t *testing.T) {
	for _, pl := range []int64{16 << 10, 32 << 10, 64 << 10, 256 << 10} {
		h := newFileHasher(pl)
		perPiece := int(pl / BlockSize)
		// Leaf counts around piece boundaries, one hasher reused for all.
		for _, n := range []int{1, 2, 3, perPiece - 1, perPiece, perPiece + 1, 2 * perPiece, 3*perPiece + 1, 7*perPiece - 1, 33} {
			if n < 1 {
				continue
			}
			leaves := make([][32]byte, n)
			for i := range leaves {
				leaves[i] = leaf(i * 7)
				h.add(leaves[i])
			}
			// The last leaf may be short: the length only decides whether
			// the file spans more than a piece.
			length := int64(n-1)*BlockSize + 1
			wantRoot, wantLayer := refFileRoot(leaves, length, pl)
			root, layer := h.finish()
			if root != wantRoot || string(layer) != string(wantLayer) {
				t.Errorf("piece length %d, %d leaves: root or layer differs", pl, n)
			}
		}
	}
}

// TestFileHasherMemory hashes the leaves of a 4 GiB file, 8 MiB of leaf
// hashes, with 16 MiB pieces: the hasher keeps one piece's leaves and 256
// piece roots.
func TestFileHasherMemory(t *testing.T) {
	const pl, n = 16 << 20, 1 << 18
	h := newFileHasher(pl)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		h.add(leaf(i))
	}
	root, layer := h.finish()
	runtime.ReadMemStats(&after)
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 1<<20 {
		t.Errorf("hashing %d leaves allocated %d bytes", n, alloc)
	}
	if len(layer) != n/(pl/BlockSize)*32 {
		t.Fatalf("layer of %d bytes", len(layer))
	}
	leaves := make([][32]byte, n)
	for i := range leaves {
		leaves[i] = leaf(i)
	}
	wantRoot, wantLayer := refFileRoot(leaves, int64(n)*BlockSize, pl)
	if root != wantRoot || string(layer) != string(wantLayer) {
		t.Error("root or layer differs from hashing all leaves at once")
	}
}

// zeroFS is an fs.FS of one file, name, of size zero bytes, made as it is
// read; onRead sees the offset reached after each read.
type zeroFS struct {
	name   string
	size   int64
	onRead func(off int64)
}

func (z *zeroFS) Open(name string) (fs.File, error) {
	if name != z.name {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &zeroFile{fs: z}, nil
}

type zeroFile struct {
	fs  *zeroFS
	off int64
}

func (f *zeroFile) Stat() (fs.FileInfo, error) { return zeroInfo{f.fs}, nil }
func (f *zeroFile) Close() error               { return nil }
func (f *zeroFile) Read(p []byte) (int, error) {
	if f.off >= f.fs.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), f.fs.size-f.off))
	clear(p[:n])
	f.off += int64(n)
	if f.fs.onRead != nil {
		f.fs.onRead(f.off)
	}
	return n, nil
}

type zeroInfo struct{ fs *zeroFS }

func (i zeroInfo) Name() string       { return i.fs.name }
func (i zeroInfo) Size() int64        { return i.fs.size }
func (i zeroInfo) Mode() fs.FileMode  { return 0o644 }
func (i zeroInfo) ModTime() time.Time { return time.Time{} }
func (i zeroInfo) IsDir() bool        { return false }
func (i zeroInfo) Sys() any           { return nil }

// TestBuildLargeFileMemory builds a v2 torrent of a 256 MiB file made as
// it is read, checking its root and that the build allocates little more
// than its read buffers.
func TestBuildLargeFileMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("hashes 256 MiB")
	}
	const size, pl = 256 << 20, 4 << 20
	fsys := &zeroFS{name: "big.bin", size: size}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	mi, err := (&Builder{Version: V2, PieceLength: pl}).Build(fsys, "big.bin")
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4<<20 {
		t.Errorf("building a torrent of %d bytes allocated %d bytes", size, alloc)
	}
	leaves := make([][32]byte, size/BlockSize)
	for i := range leaves {
		leaves[i] = sha256.Sum256(zeros[:])
	}
	wantRoot, wantLayer := refFileRoot(leaves, size, pl)
	f := mi.Info.FileList()[0]
	if f.PiecesRoot != wantRoot || string(mi.PieceLayers[wantRoot]) != string(wantLayer) {
		t.Error("root or piece layer differs from hashing all leaves at once")
	}
}

func TestBuildContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&Builder{}).BuildContext(ctx, testFS(), "d"); !errors.Is(err, context.Canceled) {
		t.Errorf("Build with a cancelled context: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (&Builder{}).BuildPathContext(ctx, dir); !errors.Is(err, context.Canceled) {
		t.Errorf("BuildPath with a cancelled context: %v", err)
	}
	// A deadline that has passed gives its own error.
	dctx, dcancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer dcancel()
	if _, err := (&Builder{}).BuildContext(dctx, testFS(), "single.bin"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Build past its deadline: %v", err)
	}
}

// TestBuildContextStopsMidFile cancels while a 1 GiB file is being hashed:
// the build returns ctx.Err() having read little more past that point.
func TestBuildContextStopsMidFile(t *testing.T) {
	for _, v := range []Version{V1, V2, Hybrid} {
		ctx, cancel := context.WithCancel(context.Background())
		const at = 8 << 20
		var last int64
		fsys := &zeroFS{name: "big.bin", size: 1 << 30, onRead: func(off int64) {
			last = off
			if off >= at {
				cancel()
			}
		}}
		_, err := (&Builder{Version: v}).BuildContext(ctx, fsys, "big.bin")
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("version %d: %v", v, err)
		}
		// The read buffer runs a megabyte ahead and ctx is looked at every
		// megabyte hashed.
		if last > at+4<<20 {
			t.Errorf("version %d: read to %d after cancelling at %d", v, last, at)
		}
	}
}
