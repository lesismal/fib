package bittorrent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/tracker"
	fibhttp "github.com/lesismal/fib/http"
)

// runTrackerWith runs a tracker of cfg, and returns its HTTP announce URL.
func runTrackerWith(t *testing.T, cfg tracker.ServerConfig) string {
	t.Helper()
	srv := tracker.NewServer(cfg)
	ecfg := fib.DefaultConfig()
	ecfg.LogSettings = false
	engine, err := fib.New(ecfg, fib.Binding{Network: "tcp4", Addr: "127.0.0.1:0", Handler: fibhttp.NewHandler(srv)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	ta, _ := engine.BindingAddrs(0)
	return "http://" + ta[0].String() + "/announce"
}

// Trackers reports each tracker of the announce-list in its tier: the
// one refusing the torrent with its reason, the one nobody answers at
// with its error, the one answering with its interval, counts, scrape and
// next announce, under both hashes of a hybrid; a pause clears Next.
func TestTrackers(t *testing.T) {
	okURL := runTrackerWith(t, tracker.ServerConfig{Interval: time.Hour})
	refuseURL := runTrackerWith(t, tracker.ServerConfig{AllowInfoHash: func(metainfo.InfoHash) bool { return false }})
	deadURL := "http://127.0.0.1:1/announce"

	dir := t.TempDir()
	testFiles(t, dir, "content", 100_000)
	b := metainfo.Builder{Version: metainfo.Hybrid, PieceLength: 16 << 10,
		Trackers: [][]string{{refuseURL}, {}, {deadURL, deadURL}, {okURL}}}
	mi, err := b.BuildPath(filepath.Join(dir, "content"))
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(t, dir, func(c *Config) { c.DisableTrackers = false })
	tor, err := c.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, tor)
	find := func(list []TrackerInfo, u string) TrackerInfo {
		for _, ti := range list {
			if ti.URL == u {
				return ti
			}
		}
		t.Fatalf("no tracker %s in %+v", u, list)
		return TrackerInfo{}
	}
	// And the other two done trying: on Windows a dial of a refused port
	// takes a couple of seconds, so the dead tracker may still be updating.
	waitFor(t, "the answering tracker's announces and scrapes", func() bool {
		list := tor.Trackers()
		ok, dead, refused := find(list, okURL), find(list, deadURL), find(list, refuseURL)
		return !ok.Scraped.IsZero() && ok.V2 != nil && !ok.V2.Scraped.IsZero() &&
			!dead.Updating && dead.Fails > 0 && !refused.Updating && refused.Fails > 0
	})
	list := tor.Trackers()
	if len(list) != 3 {
		t.Fatalf("%d trackers, want 3: %+v", len(list), list)
	}
	refused, dead, ok := find(list, refuseURL), find(list, deadURL), find(list, okURL)
	if refused.Tier != 0 || dead.Tier != 1 || ok.Tier != 2 {
		t.Fatalf("tiers %d %d %d", refused.Tier, dead.Tier, ok.Tier)
	}
	var terr *tracker.Error
	if !errors.As(refused.Err, &terr) || !strings.Contains(refused.Err.Error(), "unregistered") || refused.Fails == 0 || refused.Last.IsZero() {
		t.Fatalf("refusing tracker: %+v", refused.TrackerStatus)
	}
	if refused.Seeders != -1 || refused.Downloaded != -1 {
		t.Fatalf("refusing tracker has counts: %+v", refused.TrackerStatus)
	}
	if dead.Err == nil || dead.Fails == 0 || dead.Updating {
		t.Fatalf("unreachable tracker: %+v", dead.TrackerStatus)
	}
	for _, st := range []TrackerStatus{ok.TrackerStatus, *ok.V2} {
		if st.Err != nil || st.Fails != 0 || st.Last.IsZero() || st.Interval != time.Hour {
			t.Fatalf("answering tracker: %+v", st)
		}
		// The torrent is the swarm's one seed, and was scraped after its
		// announce.
		if st.Seeders != 1 || st.Leechers != 0 || st.Downloaded != 0 {
			t.Fatalf("answering tracker's counts: %+v", st)
		}
		if !st.Next.After(time.Now().Add(50 * time.Minute)) {
			t.Fatalf("next announce at %v", st.Next)
		}
	}

	srcs := tor.Sources()
	if srcs[0].Source != SourceTracker || !srcs[0].Enabled || srcs[1].Source != SourceDHT || srcs[1].Enabled {
		t.Fatalf("sources: %+v", srcs)
	}

	tor.Pause()
	waitFor(t, "Next cleared", func() bool {
		ok := find(tor.Trackers(), okURL)
		return ok.Next.IsZero() && ok.V2.Next.IsZero()
	})
}

// A torrent of one hash has no V2 state; one without trackers has none.
func TestTrackersOneHash(t *testing.T) {
	dir := t.TempDir()
	testFiles(t, dir, "content", 10_000)
	mi := buildTorrent(t, dir, "content", metainfo.V1, 16<<10, "http://127.0.0.1:1/announce")
	c := newTestClient(t, dir)
	tor, err := c.AddTorrent(mi, &AddOptions{Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	list := tor.Trackers()
	if len(list) != 1 || list[0].V2 != nil || !list[0].Last.IsZero() || list[0].Seeders != -1 {
		t.Fatalf("trackers: %+v", list)
	}
	// DisableTrackers: the source is off.
	if s := tor.Sources(); s[0].Enabled {
		t.Fatalf("sources: %+v", s)
	}
}

// A web seed that fails reports its failures and its error.
func TestWebSeedInfo(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	testFiles(t, sdir, "content", 50_000)
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	b := metainfo.Builder{Version: metainfo.V1, PieceLength: 16 << 10, WebSeeds: []string{srv.URL + "/", "ftp://example.invalid/"}}
	mi, err := b.BuildPath(filepath.Join(sdir, "content"))
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(t, ldir)
	tor, err := c.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a web seed failure", func() bool {
		ws := tor.WebSeeds()
		return len(ws) == 1 && ws[0].Fails > 0 && ws[0].Active == 0
	})
	ws := tor.WebSeeds()[0]
	if ws.URL != srv.URL+"/" || ws.Err == nil || !strings.Contains(ws.Err.Error(), "404") || !ws.NextTry.After(time.Now()) {
		t.Fatalf("web seed: %+v", ws)
	}
}
