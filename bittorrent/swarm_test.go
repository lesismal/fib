package bittorrent

import (
	"bytes"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/dht"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/tracker"
	fibhttp "github.com/lesismal/fib/http"
)

// seed starts a client seeding mi from dir.
func seed(t *testing.T, dir string, mi *metainfo.MetaInfo, opts ...clientOpt) (*Client, *Torrent) {
	t.Helper()
	c := newTestClient(t, dir, opts...)
	tor, err := c.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, tor)
	return c, tor
}

func TestFileSelection(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 300_000, 300_000, 300_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, _ := seed(t, sdir, mi)
	leecher := newTestClient(t, ldir)
	m := mi.Magnet()
	m.Peers = []string{addrOf(seeder)}
	idx := func(rel string) int {
		for i, f := range mi.Info.FileList() {
			if filepath.Join(append([]string{"content"}, f.Path...)...) == rel {
				return i
			}
		}
		t.Fatalf("no file %s", rel)
		return -1
	}
	sel, other, third := filepath.Join("content", "dir1", "file1.bin"), filepath.Join("content", "dir0", "file0.bin"), filepath.Join("content", "dir0", "file2.bin")
	m.Select = []metainfo.IndexRange{{First: idx(sel), Last: idx(sel)}}
	lt, err := leecher.AddMagnet(m.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, map[string][]byte{sel: files[sel]})
	st := lt.Stats()
	if st.PiecesHave == st.Pieces {
		t.Fatal("every piece downloaded, so= ignored")
	}
	file := func(rel string) *File {
		for _, f := range lt.Files() {
			if f.Index() == idx(rel) {
				return f
			}
		}
		return nil
	}
	if file(other).Priority() != PriorityNone || file(sel).Priority() != PriorityNormal {
		t.Fatalf("priorities %d %d", file(other).Priority(), file(sel).Priority())
	}
	// Selecting another file later downloads it too.
	file(third).SetPriority(PriorityHigh)
	waitDone(t, lt)
	checkFiles(t, ldir, map[string][]byte{third: files[third]})
	if n := file(other).BytesCompleted(); n >= file(other).Length() {
		t.Fatalf("unselected file complete: %d", n)
	}
}

func TestCorruptPieceBan(t *testing.T) {
	sdir := t.TempDir()
	files := testFiles(t, sdir, "content", 400_000, 200_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 32<<10)
	good, _ := seed(t, sdir, mi, func(c *Config) { c.UploadRate = 200 << 10 })

	// The bad seed holds the right data in memory until it has checked
	// it, then serves garbage.
	mem := NewMemoryStorage()
	st, _ := mem(mi.Info)
	for i, f := range mi.Info.FileList() {
		rel := filepath.Join(append([]string{"content"}, f.Path...)...)
		if !f.IsPad() {
			_, _ = st.WriteAt(i, files[rel], 0)
		}
	}
	bad := newTestClient(t, "", func(c *Config) { c.Storage = mem })
	bt, err := bad.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, bt)
	junk := bytes.Repeat([]byte{0xAA}, 400_000)
	for i := range mi.Info.FileList() {
		st.(*MemoryStorage).Corrupt(i, junk, 0)
	}

	var mu sync.Mutex
	var banned []netip.AddrPort
	ldir := t.TempDir()
	leecher := newTestClient(t, ldir, func(c *Config) {
		c.OnEvent = func(e Event) {
			if e.Type == EventPeerBanned {
				mu.Lock()
				banned = append(banned, e.Peer)
				mu.Unlock()
			}
		}
	})
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(bad), addrOf(good)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)
	if lt.Stats().HashFails == 0 {
		t.Fatal("no hash failures from the bad seed")
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	want := netip.MustParseAddrPort(addrOf(bad))
	found := false
	for _, b := range banned {
		if b == want {
			found = true
		}
		if b == netip.MustParseAddrPort(addrOf(good)) {
			t.Fatalf("good seed banned")
		}
	}
	if !found {
		t.Fatalf("bad seed %v not banned: %v", want, banned)
	}
}

func TestResume(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 500_000, 300_000)
	mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 16<<10)
	seeder, _ := seed(t, sdir, mi)
	// The leecher has the first file and half of the second.
	for rel, b := range files {
		p := filepath.Join(ldir, rel)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if len(b) == 300_000 {
			b = b[:150_000]
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	leecher := newTestClient(t, ldir)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)
	if d := lt.Stats().Downloaded; d > 200_000 {
		t.Fatalf("downloaded %d bytes: the data already there was not kept", d)
	}
	// No padding file was written.
	if _, err := os.Stat(filepath.Join(ldir, "content", ".pad")); err == nil {
		t.Fatal("padding files written to disk")
	}
}

func runTracker(t *testing.T) (httpURL, udpURL string) {
	t.Helper()
	srv := tracker.NewServer(tracker.ServerConfig{Interval: time.Second})
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	engine, err := fib.New(cfg,
		fib.Binding{Network: "tcp4", Addr: "127.0.0.1:0", Handler: fibhttp.NewHandler(srv)},
		fib.Binding{Network: "udp4", Addr: "127.0.0.1:0", Handler: srv.UDP()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	ta, _ := engine.BindingAddrs(0)
	ua, _ := engine.BindingAddrs(1)
	return "http://" + ta[0].String() + "/announce", "udp://" + ua[0].String() + "/announce"
}

func TestTrackerDiscovery(t *testing.T) {
	httpURL, udpURL := runTracker(t)
	for name, url := range map[string]string{"http": httpURL, "udp": udpURL} {
		t.Run(name, func(t *testing.T) {
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 150_000, 10_000)
			mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 16<<10, url)
			trackers := func(c *Config) { c.DisableTrackers = false }
			seed(t, sdir, mi, trackers)
			leecher := newTestClient(t, ldir, trackers)
			lt, err := leecher.AddMagnet(mi.Magnet().String(), nil)
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, lt)
			checkFiles(t, ldir, files)
		})
	}
}

func TestDHTDiscovery(t *testing.T) {
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	engine, err := fib.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	router, err := dht.Listen(engine, "127.0.0.1:0", dht.Config{Routers: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	withDHT := func(c *Config) {
		c.DisableDHT = false
		c.DHT.Routers = []string{router.LocalAddr().String()}
		c.DHTRetryInterval = time.Second
	}
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 100_000, 20_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seed(t, sdir, mi, withDHT)
	leecher := newTestClient(t, ldir, withDHT)
	lt, err := leecher.AddMagnet(mi.Magnet().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)
}

func TestPEX(t *testing.T) {
	sdir := t.TempDir()
	testFiles(t, sdir, "content", 400_000, 20_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, _ := seed(t, sdir, mi, func(c *Config) { c.UploadRate = 100 << 10 })
	a := newTestClient(t, t.TempDir())
	if _, err := a.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}}); err != nil {
		t.Fatal(err)
	}
	b := newTestClient(t, t.TempDir())
	bt, err := b.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(a)}})
	if err != nil {
		t.Fatal(err)
	}
	want := netip.MustParseAddrPort(addrOf(seeder))
	deadline := time.Now().Add(transferTimeout)
	for time.Now().Before(deadline) {
		bt.mu.Lock()
		cand := bt.pool[want]
		pex := cand != nil && cand.sources&SourcePEX != 0
		bt.mu.Unlock()
		if pex {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("b never learned of the seed through PEX")
}

func TestLSD(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 50_000, 20_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	lsd := func(c *Config) { c.DisableLSD = false }
	seeder, _ := seed(t, sdir, mi, lsd)
	if seeder.lsd == nil {
		t.Skip("no multicast here")
	}
	leecher := newTestClient(t, ldir, lsd)
	lt, err := leecher.AddMagnet(mi.Magnet().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-lt.Done():
	case <-time.After(10 * time.Second):
		// Multicast loopback is not routed everywhere (some CI hosts).
		t.Skip("no LSD announce arrived")
	}
	checkFiles(t, ldir, files)
}

func TestPauseResumeRemove(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 400_000, 20_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, _ := seed(t, sdir, mi, func(c *Config) { c.UploadRate = 200 << 10 })
	leecher := newTestClient(t, ldir)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	lt.Pause()
	if lt.State() != StatePaused {
		t.Fatalf("state %v", lt.State())
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(lt.Peers()); n != 0 {
		t.Fatalf("%d peers while paused", n)
	}
	lt.Stop()
	lt.Start()
	lt.AddPeers(addrOf(seeder))
	waitDone(t, lt)
	checkFiles(t, ldir, files)
	if err := lt.Remove(true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(ldir, "content")); !os.IsNotExist(err) {
		t.Fatalf("data left after Remove(true): %v", err)
	}
	if _, ok := leecher.Torrent(mi.Info.InfoHashV1()); ok {
		t.Fatal("torrent still in the client")
	}
}

func TestSeedRatio(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	testFiles(t, sdir, "content", 100_000, 20_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, st := seed(t, sdir, mi, func(c *Config) { c.SeedRatio = 0.5 })
	leecher := newTestClient(t, ldir)
	lt, _ := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	waitDone(t, lt)
	deadline := time.Now().Add(5 * time.Second)
	for st.State() != StatePaused {
		if time.Now().After(deadline) {
			t.Fatalf("seeder %v after uploading %d", st.State(), st.Stats().Uploaded)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestOwnAddrSkipped(t *testing.T) {
	c := newTestClient(t, t.TempDir())
	if !c.isOwnAddr(netip.MustParseAddrPort(addrOf(c))) {
		t.Fatal("own address not recognized")
	}
	if _, _, err := net.SplitHostPort(addrOf(c)); err != nil {
		t.Fatal(err)
	}
}
