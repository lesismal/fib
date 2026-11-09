package bittorrent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/tracker"
)

// A torrent's trackers, peer sources and web seeds as they are now:
// Torrent.Trackers, Torrent.Sources and Torrent.WebSeeds.

// trackerScrapeInterval is how often a tracker that answers an announce
// is scraped at most, for the completed downloads its announces do not
// give.
const trackerScrapeInterval = 30 * time.Minute

// TrackerInfo is a tracker of a torrent and the state of its announces,
// as Torrent.Trackers returns it.
type TrackerInfo struct {
	// URL is the tracker's announce URL; Tier is the index of its tier in
	// the torrent's announce-list (BEP 12), empty tiers not counted.
	URL  string
	Tier int
	// TrackerStatus is the state of the announces under the torrent's v1
	// info hash, or under its only one.
	TrackerStatus
	// V2 is the state of the announces under the truncated v2 info hash of
	// a hybrid torrent, which it is announced under too; nil for a torrent
	// of one hash.
	V2 *TrackerStatus
}

// TrackerStatus is the state of a torrent's announces to a tracker under
// one info hash.
type TrackerStatus struct {
	// Updating is set while an announce to the tracker is under way.
	Updating bool
	// Last is when the last announce to the tracker ended, zero before
	// the first; Err is its error, nil when the tracker answered (a
	// refusal is a *tracker.Error, with the tracker's reason), and Warning
	// the warning the tracker's last answer carried. Fails counts the
	// announces to it that failed since the last one it answered.
	Last    time.Time
	Err     error
	Warning string
	Fails   int
	// Next is when the torrent announces next, zero while it does not
	// run. The announce goes through the tiers (BEP 12): this tracker is
	// asked only when those tried before it do not answer.
	Next time.Time
	// Interval and MinInterval are those of the tracker's last answer.
	Interval, MinInterval time.Duration
	// Peers is how many peers the tracker's last answer listed.
	Peers int
	// Seeders and Leechers are the counts of the tracker's last answer or
	// scrape, whichever came last, Downloaded the completed downloads of
	// its last scrape; -1 when not known. A tracker that gives no counts
	// in its answers reports zero.
	Seeders, Leechers, Downloaded int
	// Scraped is when the tracker last answered a scrape, which follows
	// an announce it answers, once in 30 minutes at most.
	Scraped time.Time
}

// unknownStatus is the state of a tracker not announced to yet.
var unknownStatus = TrackerStatus{Seeders: -1, Leechers: -1, Downloaded: -1}

// trackerEntry is what a torrent knows of a tracker: the state under each
// hash it is announced under (v1, or the only one, first), and when it was
// last scraped under each.
type trackerEntry struct {
	st        [2]TrackerStatus
	scrapeTry [2]time.Time
}

// trackerTable holds a torrent's trackerEntries, apart from t.mu: an
// entry per URL of the torrent's tiers, made at its first announce.
type trackerTable struct {
	mu sync.Mutex
	m  map[string]*trackerEntry
}

// entryLocked returns u's entry, made if new. tr.mu is held.
func (tr *trackerTable) entryLocked(u string) *trackerEntry {
	if tr.m == nil {
		tr.m = map[string]*trackerEntry{}
	}
	e := tr.m[u]
	if e == nil {
		e = &trackerEntry{st: [2]TrackerStatus{unknownStatus, unknownStatus}}
		tr.m[u] = e
	}
	return e
}

// recordingAnnouncer is the Announcer a torrent's tiers announce through:
// it records each tracker's announces under one hash's slot.
type recordingAnnouncer struct {
	t    *Torrent
	slot int
	a    tracker.Announcer
}

func (r recordingAnnouncer) Announce(ctx context.Context, u string, req *tracker.AnnounceRequest) (*tracker.AnnounceResponse, error) {
	tr := &r.t.tr
	tr.mu.Lock()
	tr.entryLocked(u).st[r.slot].Updating = true
	tr.mu.Unlock()
	resp, err := r.a.Announce(ctx, u, req)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	st := &tr.entryLocked(u).st[r.slot]
	st.Updating = false
	switch {
	case err == nil:
		st.Last, st.Err, st.Fails = time.Now(), nil, 0
		st.Warning = resp.Warning
		st.Interval, st.MinInterval = resp.Interval, resp.MinInterval
		st.Peers = len(resp.Peers)
		st.Seeders, st.Leechers = resp.Complete, resp.Incomplete
	case errors.Is(err, context.Canceled):
		// The torrent paused or was removed: nothing was learned.
	default:
		st.Last, st.Err = time.Now(), err
		st.Fails++
	}
	return resp, err
}

// setTrackerNext records when the announces under slot go next to the trackers
// of tiers, zero once they stop.
func (t *Torrent) setTrackerNext(tiers *tracker.Tiers, slot int, next time.Time) {
	tr := &t.tr
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, tier := range tiers.List() {
		for _, u := range tier {
			tr.entryLocked(u).st[slot].Next = next
		}
	}
}

// scrapeTracker scrapes u under h, after an announce u answered, unless
// it was scraped under slot within trackerScrapeInterval. It runs on the
// announce loop's goroutine; a failed scrape changes nothing but when the
// next is tried.
func (t *Torrent) scrapeTracker(ctx context.Context, u string, h metainfo.InfoHash, slot int) {
	cl := t.c
	tr := &t.tr
	now := time.Now()
	tr.mu.Lock()
	e := tr.entryLocked(u)
	due := e.scrapeTry[slot].IsZero() || now.Sub(e.scrapeTry[slot]) >= trackerScrapeInterval
	if due {
		e.scrapeTry[slot] = now
	}
	tr.mu.Unlock()
	if !due || cl.trackers == nil {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	res, err := cl.trackers.Scrape(sctx, u, []metainfo.InfoHash{h})
	cancel()
	if err != nil {
		return
	}
	s, ok := res[h]
	if !ok {
		return
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	st := &tr.entryLocked(u).st[slot]
	st.Seeders, st.Leechers, st.Downloaded = s.Complete, s.Incomplete, s.Downloaded
	st.Scraped = time.Now()
}

// Trackers returns the torrent's trackers in the order of its
// announce-list, tier by tier, a URL listed twice once, and the state of
// the announces to each. The list is the torrent's (its .torrent file's
// or magnet link's, and AddOptions.Trackers), and so is its size.
func (t *Torrent) Trackers() []TrackerInfo {
	t.mu.Lock()
	list := t.trackers
	hybrid := !t.ih.IsZero() && !t.ih2.IsZero()
	t.mu.Unlock()
	tr := &t.tr
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var out []TrackerInfo
	seen := map[string]bool{}
	tier := 0
	for _, urls := range list {
		n := 0
		for _, u := range urls {
			if u = strings.TrimSpace(u); u == "" || seen[u] {
				continue
			}
			seen[u] = true
			n++
			ti := TrackerInfo{URL: u, Tier: tier, TrackerStatus: unknownStatus}
			e := tr.m[u]
			if e != nil {
				ti.TrackerStatus = e.st[0]
			}
			if hybrid {
				v2 := unknownStatus
				if e != nil {
					v2 = e.st[1]
				}
				ti.V2 = &v2
			}
			out = append(out, ti)
		}
		if n > 0 {
			tier++
		}
	}
	return out
}

func (s Source) String() string {
	names := []string{"tracker", "dht", "pex", "lsd", "manual", "incoming", "other"}
	var parts []string
	for i, n := range names {
		if s&(1<<i) != 0 {
			parts = append(parts, n)
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "+")
}

// SourceInfo is a source of a torrent's peers, as Torrent.Sources returns
// it.
type SourceInfo struct {
	Source Source
	// Enabled reports the source used for the torrent: turned on in the
	// client's Config, and not one a private torrent (BEP 27) goes
	// without; for SourceTracker, with trackers to announce to.
	Enabled bool
	// Candidates is how many of the peer addresses the torrent keeps came
	// from the source (among others, maybe), Connected how many of those
	// it is connected to.
	Candidates, Connected int
}

// Sources returns the sources of the torrent's peers: its trackers, the
// DHT, PEX, local service discovery, AddPeers (and a magnet link's x.pe),
// and Config.PeerSources, in that order, with the peers each found. The
// connections peers opened to this side are in no source's count.
func (t *Torrent) Sources() []SourceInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	cl := t.c
	priv := t.isPrivate()
	out := []SourceInfo{
		{Source: SourceTracker, Enabled: !cl.cfg.DisableTrackers && cl.trackers != nil && len(t.trackers) > 0},
		{Source: SourceDHT, Enabled: !priv && (cl.dht4 != nil || cl.dht6 != nil)},
		{Source: SourcePEX, Enabled: !priv && !cl.cfg.DisablePEX},
		{Source: SourceLSD, Enabled: !priv && cl.lsd != nil},
		{Source: SourceManual, Enabled: true},
		{Source: SourceOther, Enabled: !priv && len(cl.cfg.PeerSources) > 0},
	}
	for _, cand := range t.pool {
		for i := range out {
			if cand.sources&out[i].Source != 0 {
				out[i].Candidates++
				if cand.connected {
					out[i].Connected++
				}
			}
		}
	}
	return out
}

// WebSeedInfo is a web seed (BEP 19) of a torrent, as Torrent.WebSeeds
// returns it.
type WebSeedInfo struct {
	URL string
	// Active is how many pieces are being fetched from it.
	Active int
	// Fails counts its failed fetches and the pieces it sent that failed
	// their check; at 5 it is no longer used (Dropped). Err is the last
	// failure, nil after a fetch that worked; NextTry, when after now, is
	// when it is used again after a failure.
	Fails   int
	Dropped bool
	Err     error
	NextTry time.Time
}

// WebSeeds returns the torrent's web seeds of HTTP and HTTPS URLs, the
// only ones used, in the order of its url-list.
func (t *Torrent) WebSeeds() []WebSeedInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seeds != nil {
		out := make([]WebSeedInfo, 0, len(t.seeds))
		for _, ws := range t.seeds {
			out = append(out, WebSeedInfo{
				URL: ws.url, Active: ws.inFlight, Fails: ws.fails,
				Dropped: ws.fails >= webSeedMaxFails, Err: ws.err, NextTry: ws.nextTry,
			})
		}
		return out
	}
	var out []WebSeedInfo
	for _, u := range t.webSeeds {
		if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
			out = append(out, WebSeedInfo{URL: u})
		}
	}
	return out
}
