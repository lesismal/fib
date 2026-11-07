package bittorrent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

const transferTimeout = 60 * time.Second

// testFiles writes files of the given sizes, of random bytes, below
// dir/name and returns their contents by path below name.
func testFiles(t *testing.T, dir, name string, sizes ...int) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	r := rand.New(rand.NewPCG(uint64(len(sizes)), uint64(sizes[0])))
	for i, n := range sizes {
		b := make([]byte, n)
		for j := range b {
			b[j] = byte(r.Uint32())
		}
		rel := filepath.Join(name, fmt.Sprintf("dir%d", i%2), fmt.Sprintf("file%d.bin", i))
		if len(sizes) == 1 {
			rel = name
		}
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		out[rel] = b
	}
	return out
}

// buildTorrent makes a torrent of the files of testFiles.
func buildTorrent(t *testing.T, dir, name string, v metainfo.Version, pieceLength int64, trackers ...string) *metainfo.MetaInfo {
	t.Helper()
	b := metainfo.Builder{Version: v, PieceLength: pieceLength}
	if len(trackers) > 0 {
		b.Trackers = [][]string{trackers}
	}
	mi, err := b.BuildPath(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return mi
}

type clientOpt func(*Config)

func newTestClient(t *testing.T, dir string, opts ...clientOpt) *Client {
	t.Helper()
	cfg := Config{
		DataDir:         dir,
		ListenAddr:      "127.0.0.1:0",
		DisableDHT:      true,
		DisableLSD:      true,
		DisableTrackers: true,
	}
	for _, o := range opts {
		o(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func addrOf(c *Client) string {
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(c.Port())).String()
}

// waitDone waits for tor's download, and returns the peers it had last
// before the end, which seeds that are done drop.
func waitDone(t *testing.T, tor *Torrent) []PeerInfo {
	t.Helper()
	var last []PeerInfo
	deadline := time.After(transferTimeout)
	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-tor.Done():
			return last
		case <-tk.C:
			if p := tor.Peers(); len(p) > 0 {
				last = p
			}
		case <-deadline:
			t.Fatalf("download not done: %+v", tor.Stats())
		}
	}
}

func checkFiles(t *testing.T, dir string, want map[string][]byte) {
	t.Helper()
	for rel, b := range want {
		got, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("%s differs: %d bytes, want %d", rel, len(got), len(b))
		}
	}
}

func TestTransfer(t *testing.T) {
	type tc struct {
		version         metainfo.Version
		seeder, leecher []clientOpt
		enc             Encryption
		utp             bool
	}
	tcpOnly := func(c *Config) { c.DisableUTP = true }
	utpOnly := func(c *Config) { c.DisableTCP = true }
	require := func(c *Config) { c.Encryption = EncryptionRequire }
	cases := map[string]tc{}
	for _, v := range []metainfo.Version{metainfo.V1, metainfo.V2, metainfo.Hybrid} {
		vn := map[metainfo.Version]string{metainfo.V1: "v1", metainfo.V2: "v2", metainfo.Hybrid: "hybrid"}[v]
		cases[vn+"/tcp"] = tc{version: v, seeder: []clientOpt{tcpOnly}, leecher: []clientOpt{tcpOnly}}
		cases[vn+"/utp"] = tc{version: v, seeder: []clientOpt{utpOnly}, leecher: []clientOpt{utpOnly}, utp: true}
		cases[vn+"/mixed-mse"] = tc{version: v, seeder: []clientOpt{require}, leecher: []clientOpt{tcpOnly, require}, enc: EncryptionRequire}
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 100_000, 300_000, 0, 65_536, 1234)
			mi := buildTorrent(t, sdir, "content", c.version, 32<<10)
			seeder := newTestClient(t, sdir, c.seeder...)
			st, err := seeder.AddTorrent(mi, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, st)
			leecher := newTestClient(t, ldir, c.leecher...)
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			peers := waitDone(t, lt)
			checkFiles(t, ldir, files)
			if len(peers) == 0 {
				t.Fatal("no peer left connected")
			}
			for _, p := range peers {
				if c.enc == EncryptionRequire && !p.Encrypted {
					t.Errorf("peer %v not encrypted", p.Addr)
				}
				if p.UTP != c.utp {
					t.Errorf("peer %v: uTP %v, want %v", p.Addr, p.UTP, c.utp)
				}
			}
		})
	}
}

func TestMagnet(t *testing.T) {
	for _, v := range []metainfo.Version{metainfo.V1, metainfo.V2, metainfo.Hybrid} {
		t.Run(strconv.Itoa(int(v)), func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 200_000, 70_000)
			mi := buildTorrent(t, sdir, "content", v, 16<<10)
			seeder := newTestClient(t, sdir)
			st, err := seeder.AddTorrent(mi, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, st)
			leecher := newTestClient(t, ldir)
			m := mi.Magnet()
			m.Peers = []string{addrOf(seeder)}
			lt, err := leecher.AddMagnet(m.String(), nil)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-lt.GotInfo():
			case <-time.After(transferTimeout):
				t.Fatal("no metadata")
			}
			waitDone(t, lt)
			checkFiles(t, ldir, files)
		})
	}
}

func TestStreamingReader(t *testing.T) {
	sdir := t.TempDir()
	files := testFiles(t, sdir, "movie.bin", 1_000_000)
	mi := buildTorrent(t, sdir, "movie.bin", metainfo.V1, 16<<10)
	seeder := newTestClient(t, sdir, func(c *Config) { c.UploadRate = 400 << 10 })
	st, _ := seeder.AddTorrent(mi, nil)
	waitDone(t, st)
	leecher := newTestClient(t, "", func(c *Config) { c.Storage = NewMemoryStorage() })
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	r := lt.Files()[0].NewReader()
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), transferTimeout)
	defer cancel()
	r.SetContext(ctx)
	r.SetReadahead(64 << 10)
	// Read from the middle first: those pieces come before the rest.
	if _, err := r.Seek(600_000, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 50_000)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, files["movie.bin"][600_000:650_000]) {
		t.Fatal("middle read differs")
	}
	if lt.HaveAll() {
		t.Log("whole torrent already there; the rate limit did not hold it back")
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(all, files["movie.bin"]) {
		t.Fatal("full read differs")
	}
}
