package tracker

import (
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

func storeAddr(i int) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), uint16(i))
}

func storeAnn(m *MemoryStore, h metainfo.InfoHash, i int, seeder bool) (StoreResult, error) {
	return m.Announce(StoreAnnounce{
		InfoHash: h, PeerID: PeerID{byte(i), byte(i >> 8)}, V4: storeAddr(i), Seeder: seeder,
		NumWant: 50, WantV4: true,
	})
}

// rangeHash is the i-th torrent of the Range tests, spread over the shards.
func rangeHash(i int) metainfo.InfoHash {
	return metainfo.InfoHash{byte(i), byte(i >> 8), 0xAB}
}

func TestStoreRange(t *testing.T) {
	m := NewMemoryStore(MemoryStoreConfig{})
	want := map[metainfo.InfoHash]Stats{}
	for i := 0; i < 200; i++ {
		h := rangeHash(i)
		seeders, leechers := i%3, 1+i%4
		for j := 0; j < seeders+leechers; j++ {
			if _, err := storeAnn(m, h, j, j < seeders); err != nil {
				t.Fatal(err)
			}
		}
		want[h] = Stats{Complete: seeders, Incomplete: leechers}
	}
	got := map[metainfo.InfoHash]Stats{}
	m.Range(func(ih metainfo.InfoHash, s Stats) bool {
		if _, dup := got[ih]; dup {
			t.Errorf("%x seen twice", ih)
		}
		got[ih] = s
		// fn is called with no lock held: it may use the store.
		if s2, ok := m.Scrape(ih); !ok || s2 != s {
			t.Errorf("%x: Scrape %v %v, Range %v", ih, s2, ok, s)
		}
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("Range saw %d torrents, want %d", len(got), len(want))
	}
	for h, s := range want {
		if got[h] != s {
			t.Errorf("%x: %+v, want %+v", h, got[h], s)
		}
	}
	n := 0
	m.Range(func(metainfo.InfoHash, Stats) bool {
		n++
		return n < 5
	})
	if n != 5 {
		t.Errorf("Range went on after fn returned false: %d calls", n)
	}
	// A torrent whose peers expired is not listed, and is forgotten.
	m.SetConfig(MemoryStoreConfig{PeerTTL: time.Millisecond})
	time.Sleep(5 * time.Millisecond)
	m.Range(func(ih metainfo.InfoHash, s Stats) bool {
		t.Errorf("expired torrent %x listed", ih)
		return true
	})
	if m.Torrents() != 0 || m.Peers() != 0 {
		t.Errorf("torrents %d, peers %d after Range of expired swarms", m.Torrents(), m.Peers())
	}
}

// TestStoreRangeConcurrent ranges over swarms announced to meanwhile. Each
// torrent has a peer that stays and one that keeps switching between
// seeder and leecher, or leaving and coming back: Range must see two peers,
// or one while the second is away, never a torrent half changed.
func TestStoreRangeConcurrent(t *testing.T) {
	m := NewMemoryStore(MemoryStoreConfig{})
	const torrents = 64
	for i := 0; i < torrents; i++ {
		if _, err := storeAnn(m, rangeHash(i), 1, true); err != nil {
			t.Fatal(err)
		}
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; !stop.Load(); k++ {
				h := rangeHash((k*4 + w) % torrents)
				if k%5 == 4 {
					_, _ = m.Announce(StoreAnnounce{InfoHash: h, PeerID: PeerID{2}, Event: EventStopped})
					continue
				}
				if _, err := storeAnn(m, h, 2, k%2 == 0); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for r := 0; r < 200; r++ {
		seen := 0
		m.Range(func(ih metainfo.InfoHash, s Stats) bool {
			seen++
			if n := s.Complete + s.Incomplete; n != 1 && n != 2 || s.Complete < 1 {
				t.Errorf("%x: %+v", ih, s)
			}
			return true
		})
		if seen != torrents {
			t.Errorf("Range saw %d torrents, want %d", seen, torrents)
		}
	}
	stop.Store(true)
	wg.Wait()
}

func TestStoreSetConfig(t *testing.T) {
	m := NewMemoryStore(MemoryStoreConfig{MaxTorrents: 3, MaxPeersPerTorrent: 3})
	if c := m.Config(); c.MaxTorrents != 3 || c.MaxPeers != DefaultMaxPeers || c.PeerTTL != DefaultPeerTTL {
		t.Fatalf("Config %+v", c)
	}
	h := func(i int) metainfo.InfoHash { return metainfo.InfoHash{byte(i)} }
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			if _, err := storeAnn(m, h(i), j, false); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Lowered bounds drop nothing, but refuse what is new.
	m.SetConfig(MemoryStoreConfig{MaxTorrents: 1, MaxPeersPerTorrent: 1})
	if m.Torrents() != 3 || m.Peers() != 9 {
		t.Fatalf("lowering bounds dropped swarms: %d torrents, %d peers", m.Torrents(), m.Peers())
	}
	if _, err := storeAnn(m, h(4), 1, false); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("a new torrent past the lowered bound: %v", err)
	}
	res, err := storeAnn(m, h(1), 1, true) // a peer already kept announces again
	if err != nil || res.Complete != 1 || res.Incomplete != 2 {
		t.Fatalf("a kept peer's announce: %+v %v", res, err)
	}
	if res, _ := storeAnn(m, h(1), 9, false); res.Complete+res.Incomplete != 3 {
		t.Fatalf("a new peer recorded past the lowered bound: %+v", res)
	}

	// Raised bounds take effect at once.
	m.SetConfig(MemoryStoreConfig{MaxTorrents: 4, MaxPeersPerTorrent: 4})
	if _, err := storeAnn(m, h(4), 1, false); err != nil {
		t.Fatal(err)
	}
	if res, _ := storeAnn(m, h(1), 9, false); res.Complete+res.Incomplete != 4 {
		t.Fatalf("a new peer not recorded under the raised bound: %+v", res)
	}

	// A longer TTL leaves the peers as they are; a shorter one applies to
	// the peers kept.
	m.SetConfig(MemoryStoreConfig{MaxTorrents: 4, MaxPeersPerTorrent: 4, PeerTTL: 2 * time.Hour})
	if _, ok := m.Scrape(h(2)); !ok {
		t.Fatal("swarm lost on raising PeerTTL")
	}
	m.SetConfig(MemoryStoreConfig{MaxTorrents: 4, MaxPeersPerTorrent: 4, PeerTTL: 200 * time.Millisecond})
	if _, ok := m.Scrape(h(2)); !ok {
		t.Fatal("swarm lost at once on lowering PeerTTL")
	}
	time.Sleep(400 * time.Millisecond)
	for i := 1; i <= 4; i++ {
		if _, ok := m.Scrape(h(i)); ok {
			t.Errorf("torrent %d kept past the lowered PeerTTL", i)
		}
	}
	if m.Torrents() != 0 || m.Peers() != 0 {
		t.Errorf("torrents %d, peers %d", m.Torrents(), m.Peers())
	}
}

// TestStoreSetConfigConcurrent changes the bounds while peers announce,
// for the race detector, then checks the counts still add up.
func TestStoreSetConfigConcurrent(t *testing.T) {
	m := NewMemoryStore(MemoryStoreConfig{})
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; !stop.Load(); k++ {
				_, err := storeAnn(m, rangeHash(k%100), w*1000+k%50, k%3 == 0)
				if err != nil && !errors.Is(err, ErrStoreFull) {
					t.Error(err)
					return
				}
			}
		}()
	}
	for r := 0; r < 200; r++ {
		ttl := time.Hour
		if r%2 == 1 {
			ttl = 30 * time.Minute
		}
		m.SetConfig(MemoryStoreConfig{MaxTorrents: 10 + r%100, MaxPeersPerTorrent: 5 + r%20, PeerTTL: ttl})
		m.Range(func(metainfo.InfoHash, Stats) bool { return true })
	}
	stop.Store(true)
	wg.Wait()
	torrents, peers := 0, 0
	m.Range(func(_ metainfo.InfoHash, s Stats) bool {
		torrents++
		peers += s.Complete + s.Incomplete
		return true
	})
	if torrents != m.Torrents() || peers != m.Peers() {
		t.Errorf("Range saw %d torrents and %d peers; counts say %d and %d", torrents, peers, m.Torrents(), m.Peers())
	}
}
