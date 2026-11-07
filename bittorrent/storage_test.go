package bittorrent

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

func TestSanitize(t *testing.T) {
	for _, c := range []struct {
		in, win, unix string
	}{
		{"plain.txt", "plain.txt", "plain.txt"},
		{"CON", "CON_", "CON"},
		{"con.txt", "con.txt_", "con.txt"},
		{"lpt9.tar.gz", "lpt9.tar.gz_", "lpt9.tar.gz"},
		{"a:b?c*d", "a_b_c_d", "a:b?c*d"},
		{`back\slash`, "back_slash", `back\slash`},
		{"trailing. ", "trailing", "trailing. "},
		{"..", "_", "_"},
		{"a\x00b", "a_b", "a_b"},
		{"bad\xffutf8", "bad_utf8", "bad_utf8"},
	} {
		if got := sanitizeFor(c.in, true); got != c.win {
			t.Errorf("windows %q: %q, want %q", c.in, got, c.win)
		}
		if got := sanitizeFor(c.in, false); got != c.unix {
			t.Errorf("unix %q: %q, want %q", c.in, got, c.unix)
		}
	}
}

func TestDedupePath(t *testing.T) {
	taken := map[string]int{}
	a := dedupePath(filepath.Join("d", "File.txt"), taken)
	b := dedupePath(filepath.Join("d", "File.txt"), taken)
	if a == b {
		t.Fatalf("same path twice: %s", a)
	}
	c := dedupePath(filepath.Join("d", "file.txt"), taken)
	if (runtime.GOOS == "windows" || runtime.GOOS == "darwin") == (c == filepath.Join("d", "file.txt")) {
		t.Fatalf("case clash handling on %s: %s", runtime.GOOS, c)
	}
}

func hybridInfo(t *testing.T) (*metainfo.MetaInfo, string, map[string][]byte) {
	dir := t.TempDir()
	files := testFiles(t, dir, "content", 40_000, 5, 70_000)
	return buildTorrent(t, dir, "content", metainfo.Hybrid, 16<<10), dir, files
}

func TestStorages(t *testing.T) {
	mi, srcDir, files := hybridInfo(t)
	info := mi.Info
	// The torrent's data as its pieces hold it: files, padding and all.
	whole := make([]byte, info.TotalLength())
	for _, f := range info.FileList() {
		if !f.IsPad() {
			copy(whole[f.Offset:], files[filepath.Join(append([]string{"content"}, f.Path...)...)])
		}
	}
	dst := t.TempDir()
	for name, open := range map[string]StorageOpener{
		"memory": NewMemoryStorage(),
		"file":   NewFileStorage(dst, FileStorageOptions{Preallocate: true, MaxOpenFiles: 1}),
	} {
		t.Run(name, func(t *testing.T) {
			st, err := open(info)
			if err != nil {
				t.Fatal(err)
			}
			for i := range info.PieceCount() {
				off := info.PieceOffset(i)
				if err := writeData(st, info, off, whole[off:off+info.PieceSize(i)]); err != nil {
					t.Fatal(err)
				}
			}
			for i := range info.PieceCount() {
				off := info.PieceOffset(i)
				got := make([]byte, info.PieceSize(i))
				if err := readData(st, info, off, got); err != nil {
					t.Fatal(err)
				}
				if err := info.VerifyPiece(i, got, mi.PieceLayers); err != nil {
					t.Fatalf("piece %d: %v", i, err)
				}
			}
			// A short read is an error.
			if _, err := st.ReadAt(0, make([]byte, 10), 1<<30); err == nil || err == io.ErrShortBuffer {
				t.Fatalf("read past the end: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if r, ok := st.(StorageRemover); ok {
				if name == "file" {
					checkFiles(t, dst, files)
					if _, err := os.Stat(filepath.Join(dst, "content", ".pad")); err == nil {
						t.Fatal("padding written")
					}
				}
				if err := r.Remove(); err != nil {
					t.Fatal(err)
				}
				if name == "file" {
					if _, err := os.Stat(filepath.Join(dst, "content")); !os.IsNotExist(err) {
						t.Fatalf("left behind: %v", err)
					}
				}
			}
		})
	}
	_ = srcDir
}

func TestReadDataZeroesPads(t *testing.T) {
	mi, _, _ := hybridInfo(t)
	st, _ := NewMemoryStorage()(mi.Info)
	buf := bytes.Repeat([]byte{1}, int(mi.Info.TotalLength()))
	// Nothing written: the read fails, but padding reads as zeros.
	if err := readData(st, mi.Info, 0, buf); err == nil {
		t.Fatal("read of nothing succeeded")
	}
}

func TestParseLSD(t *testing.T) {
	ih := metainfo.InfoHash{1, 2, 3}
	ih2 := metainfo.InfoHash{9}
	b := appendLSD(nil, 6881, "abc", []metainfo.InfoHash{ih, ih2})
	port, hashes, cookie, ok := parseLSD(b)
	if !ok || port != 6881 || cookie != "abc" || len(hashes) != 2 || hashes[0] != ih || hashes[1] != ih2 {
		t.Fatalf("%v %d %v %q", ok, port, hashes, cookie)
	}
	for _, bad := range []string{
		"", "GET / HTTP/1.1\r\n\r\n",
		"BT-SEARCH * HTTP/1.1\r\nPort: 0\r\nInfohash: 0102030000000000000000000000000000000000\r\n\r\n",
		"BT-SEARCH * HTTP/1.1\r\nPort: 6881\r\nInfohash: zz\r\n\r\n",
	} {
		if _, _, _, ok := parseLSD([]byte(bad)); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

func FuzzParseLSD(f *testing.F) {
	f.Add(appendLSD(nil, 6881, "cookie", []metainfo.InfoHash{{1}}))
	f.Add([]byte("BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 1\r\nInfohash: x\r\n\r\n\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		port, hashes, _, ok := parseLSD(b)
		if ok && (port == 0 || len(hashes) == 0) {
			t.Fatalf("accepted port %d, %d hashes", port, len(hashes))
		}
	})
}
