package bittorrent

import (
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// allowFilter is a Config.Filter whose policy a test flips: it refuses
// the addresses in refused, or every address with refuseAll, until allow.
type allowFilter struct {
	allow     atomic.Bool
	refuseAll bool
	refused   map[netip.AddrPort]bool
}

func (f *allowFilter) filter(a netip.AddrPort) bool {
	if f.allow.Load() {
		return true
	}
	return !f.refuseAll && !f.refused[a]
}

func (f *allowFilter) option(c *Config) { c.Filter = f.filter }

// noPeersFor checks that tor makes no connection for d.
func noPeersFor(t *testing.T, tor *Torrent, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if n := tor.Stats().Peers; n > 0 {
			t.Fatalf("%d peers connected through the filter", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A peer the Filter refuses cannot connect over TCP or uTP, and is served
// once the Filter's answer changes.
func TestFilterInbound(t *testing.T) {
	for name, only := range map[string]clientOpt{
		"tcp": func(c *Config) { c.DisableUTP = true },
		"utp": func(c *Config) { c.DisableTCP = true; c.UTPDialTimeout = 200 * time.Millisecond },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 60_000, 5_000)
			mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
			f := &allowFilter{refuseAll: true}
			seeder, _ := seed(t, sdir, mi, only, f.option)
			leecher := newTestClient(t, ldir, only)
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			noPeersFor(t, lt, 1500*time.Millisecond)
			// A dial that fails after AddPeers would put the backoff back.
			for lt.Stats().HalfOpen > 0 {
				time.Sleep(10 * time.Millisecond)
			}
			f.allow.Store(true)
			lt.AddPeers(addrOf(seeder)) // dial again now, not after the backoff
			waitDone(t, lt)
			checkFiles(t, ldir, files)
		})
	}
}

// A peer the Filter refuses is not dialed, wherever its address comes
// from, nor kept as a candidate.
func TestFilterDial(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 60_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, _ := seed(t, sdir, mi)
	sa := netip.MustParseAddrPort(addrOf(seeder))
	f := &allowFilter{refused: map[netip.AddrPort]bool{sa: true}}
	leecher := newTestClient(t, ldir, f.option)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{sa.String()}})
	if err != nil {
		t.Fatal(err)
	}
	other := netip.MustParseAddrPort("127.0.0.2:1")
	for _, src := range []Source{SourceTracker, SourceDHT, SourcePEX, SourceLSD, SourceManual} {
		lt.addPeers(src, false, []netip.AddrPort{sa, netip.AddrPortFrom(netip.AddrFrom16(sa.Addr().As16()), sa.Port())})
	}
	lt.addPeers(SourcePEX, false, []netip.AddrPort{other})
	lt.AddPeers(sa.String())
	lt.mu.Lock()
	_, kept := lt.pool[sa]
	_, otherKept := lt.pool[other]
	lt.mu.Unlock()
	if kept || !otherKept {
		t.Fatalf("pool: refused kept %v, other kept %v", kept, otherKept)
	}
	noPeersFor(t, lt, time.Second)
	f.allow.Store(true)
	lt.AddPeers(sa.String())
	waitDone(t, lt)
	checkFiles(t, ldir, files)
}

// A peer a tracker announces is not dialed while the Filter refuses it.
func TestFilterTracker(t *testing.T) {
	httpURL, _ := runTracker(t)
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 60_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10, httpURL)
	trackers := func(c *Config) { c.DisableTrackers = false }
	seeder, _ := seed(t, sdir, mi, trackers)
	sa := netip.MustParseAddrPort(addrOf(seeder))
	f := &allowFilter{refused: map[netip.AddrPort]bool{sa: true}}
	leecher := newTestClient(t, ldir, trackers, f.option)
	lt, err := leecher.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	noPeersFor(t, lt, 2500*time.Millisecond)
	if lt.Stats().Candidates != 0 {
		t.Fatalf("refused tracker peer kept: %+v", lt.Stats())
	}
	f.allow.Store(true)
	waitDone(t, lt)
	checkFiles(t, ldir, files)
}

// Refilter closes the connections the Filter refuses once its policy
// changes, and the refused peer cannot come back.
func TestRefilter(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	testFiles(t, sdir, "content", 600_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	f := &allowFilter{refuseAll: true}
	f.allow.Store(true)
	seeder, st := seed(t, sdir, mi, f.option, func(c *Config) { c.UploadRate = 64 << 10 })
	leecher := newTestClient(t, ldir)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(transferTimeout)
	for st.Stats().Peers == 0 {
		if time.Now().After(deadline) {
			t.Fatal("never connected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	seeder.Refilter() // allowed still: nothing closes
	time.Sleep(200 * time.Millisecond)
	if st.Stats().Peers == 0 {
		t.Fatal("Refilter closed an allowed peer")
	}
	f.allow.Store(false)
	seeder.Refilter()
	deadline = time.Now().Add(5 * time.Second)
	for st.Stats().Peers > 0 {
		if time.Now().After(deadline) {
			t.Fatal("refused peer still connected after Refilter")
		}
		time.Sleep(10 * time.Millisecond)
	}
	noPeersFor(t, st, time.Second)
	select {
	case <-lt.Done():
		t.Fatal("download finished, so the peer was never cut off")
	default:
	}
}
