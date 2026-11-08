package bittorrent

import (
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// The bounds below are loose on purpose: a limited transfer may burst a
// second's worth and run a block or two ahead each quarter second, and
// CI machines are slow; what is checked is that a limit holds a transfer
// well under what loopback does unlimited, and that lifting it lets the
// transfer finish.

// bytesOver is what count gains over d, after settle for a change to
// take hold.
func bytesOver(count func() int64, settle, d time.Duration) int64 {
	time.Sleep(settle)
	a := count()
	time.Sleep(d)
	return count() - a
}

func limitSwarm(t *testing.T, size int) (string, *metainfo.MetaInfo) {
	t.Helper()
	sdir := t.TempDir()
	testFiles(t, sdir, "content", size)
	return sdir, buildTorrent(t, sdir, "content", metainfo.V1, 32<<10)
}

func memStorage(c *Config) { c.Storage = NewMemoryStorage() }

func TestSetLimitsRates(t *testing.T) {
	const limit = 128 << 10
	sdir, mi := limitSwarm(t, 6<<20)

	t.Run("download", func(t *testing.T) {
		seeder, _ := seed(t, sdir, mi)
		leecher := newTestClient(t, "", memStorage, func(c *Config) { c.DownloadRate = limit })
		lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
		if err != nil {
			t.Fatal(err)
		}
		got := bytesOver(func() int64 { return lt.Stats().Downloaded }, time.Second, 2*time.Second)
		if got <= 0 || got > 3*2*limit {
			t.Fatalf("%d bytes in 2s under %d B/s", got, limit)
		}
		if l := leecher.Limits(); l.DownloadRate != limit || l.MaxConns != DefaultMaxConns {
			t.Fatalf("limits %+v", l)
		}
		leecher.SetLimits(Limits{})
		waitDone(t, lt)
	})

	t.Run("upload", func(t *testing.T) {
		seeder, _ := seed(t, sdir, mi, func(c *Config) { c.UploadRate = limit })
		leecher := newTestClient(t, "", memStorage)
		lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
		if err != nil {
			t.Fatal(err)
		}
		got := bytesOver(func() int64 { return lt.Stats().Downloaded }, time.Second, 2*time.Second)
		if got <= 0 || got > 3*2*limit {
			t.Fatalf("%d bytes in 2s under %d B/s", got, limit)
		}
		seeder.SetLimits(Limits{})
		waitDone(t, lt)
	})
}

func TestTorrentRateUnderClient(t *testing.T) {
	const limit = 128 << 10
	sdir, mi := limitSwarm(t, 6<<20)
	seeder, _ := seed(t, sdir, mi)
	leecher := newTestClient(t, "", memStorage)
	lt, err := leecher.AddTorrent(mi, &AddOptions{
		Peers:  []string{addrOf(seeder)},
		Limits: TorrentLimits{DownloadRate: limit},
	})
	if err != nil {
		t.Fatal(err)
	}
	downloaded := func() int64 { return lt.Stats().Downloaded }
	// The torrent's rate holds under an unlimited client.
	if got := bytesOver(downloaded, time.Second, 2*time.Second); got <= 0 || got > 3*2*limit {
		t.Fatalf("%d bytes in 2s under the torrent's %d B/s", got, limit)
	}
	// The client's rate holds under a faster torrent's.
	lt.SetLimits(TorrentLimits{DownloadRate: 64 << 20})
	leecher.SetLimits(Limits{DownloadRate: limit})
	if got := bytesOver(downloaded, time.Second, 2*time.Second); got <= 0 || got > 3*2*limit {
		t.Fatalf("%d bytes in 2s under the client's %d B/s", got, limit)
	}
	if l := lt.Limits(); l.DownloadRate != 64<<20 {
		t.Fatalf("torrent limits %+v", l)
	}
	leecher.SetLimits(Limits{})
	waitDone(t, lt)
}

func TestSetLimitsConnsAndSlots(t *testing.T) {
	sdir, mi := limitSwarm(t, 4<<20)
	hub, ht := seed(t, sdir, mi)
	// Leechers slow enough to stay connected and interested.
	for range 4 {
		l := newTestClient(t, "", memStorage, func(c *Config) { c.DownloadRate = 16 << 10 })
		if _, err := l.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(hub)}}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor := func(what string, ok func([]PeerInfo) bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			peers := ht.Peers()
			if ok(peers) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d peers %+v", what, len(peers), peers)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	unchoked := func(peers []PeerInfo) int {
		n := 0
		for _, p := range peers {
			if !p.AmChoking {
				n++
			}
		}
		return n
	}
	waitFor("4 unchoked peers", func(p []PeerInfo) bool { return len(p) == 4 && unchoked(p) == 4 })

	// Fewer upload slots rechoke at once: one, and the optimistic unchoke.
	hub.SetLimits(Limits{UploadSlots: 1})
	if p := ht.Peers(); unchoked(p) > 2 {
		t.Fatalf("%d unchoked after UploadSlots 1", unchoked(p))
	}
	// A torrent's own slots override the client's; negative is every
	// interested peer.
	ht.SetLimits(TorrentLimits{UploadSlots: -1})
	if p := ht.Peers(); unchoked(p) != 4 {
		t.Fatalf("%d unchoked with unlimited slots", unchoked(p))
	}

	// A lower MaxConns closes the excess; the leechers' dials back are
	// refused.
	hub.SetLimits(Limits{MaxConns: 2})
	waitFor("2 peers", func(p []PeerInfo) bool { return len(p) == 2 })
	ht.SetLimits(TorrentLimits{MaxConns: 1})
	waitFor("1 peer", func(p []PeerInfo) bool { return len(p) == 1 })
	time.Sleep(time.Second)
	if n := len(ht.Peers()); n > 1 {
		t.Fatalf("%d peers over the torrent's MaxConns 1", n)
	}
}

func TestLessUseful(t *testing.T) {
	now := time.Now()
	ranks := []peerRank{
		{flowing: true, perSec: 10, since: now.Add(-time.Minute)},
		{flowing: true, perSec: 10, since: now},
		{flowing: true, perSec: 1},
		{flowing: true, snubbed: true, perSec: 100},
		{flowing: false, perSec: 1000},
	}
	for i := 0; i+1 < len(ranks); i++ {
		if !lessUseful(&ranks[i+1], &ranks[i]) || lessUseful(&ranks[i], &ranks[i+1]) {
			t.Fatalf("rank %d not less useful than %d", i+1, i)
		}
	}
}

func TestTorrentSeedLimits(t *testing.T) {
	sdir, mi := limitSwarm(t, 120_000)

	t.Run("ratio", func(t *testing.T) {
		seeder := newTestClient(t, sdir)
		st, err := seeder.AddTorrent(mi, &AddOptions{Limits: TorrentLimits{SeedRatio: 0.5}})
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, st)
		leecher := newTestClient(t, "", memStorage)
		lt, _ := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
		waitDone(t, lt)
		waitState(t, st, StatePaused)
	})

	t.Run("unlimited", func(t *testing.T) {
		seeder := newTestClient(t, sdir, func(c *Config) { c.SeedRatio = 0.5 })
		st, err := seeder.AddTorrent(mi, &AddOptions{Limits: TorrentLimits{SeedRatio: -1}})
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, st)
		leecher := newTestClient(t, "", memStorage)
		lt, _ := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
		waitDone(t, lt)
		time.Sleep(2500 * time.Millisecond)
		if s := st.State(); s != StateSeeding {
			t.Fatalf("seeder %v past the client's ratio with its own unlimited", s)
		}
		// Back to the client's ratio, which it is over: it pauses at the
		// next tick.
		st.SetLimits(TorrentLimits{})
		waitState(t, st, StatePaused)
	})
}

func waitState(t *testing.T, tor *Torrent, s State) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for tor.State() != s {
		if time.Now().After(deadline) {
			t.Fatalf("%v, not %v, after uploading %d", tor.State(), s, tor.Stats().Uploaded)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
