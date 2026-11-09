package bittorrent

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// allEvents records every event a client's OnEvent hears.
type allEvents struct {
	mu  sync.Mutex
	evs []Event
}

func (l *allEvents) option(c *Config) {
	c.OnEvent = func(e Event) {
		l.mu.Lock()
		l.evs = append(l.evs, e)
		l.mu.Unlock()
	}
}

// of returns the events of type typ of tor heard so far.
func (l *allEvents) of(tor *Torrent, typ EventType) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Event
	for _, e := range l.evs {
		if e.Type == typ && (tor == nil || e.Torrent == tor) {
			out = append(out, e)
		}
	}
	return out
}

// waitChecked waits for the n-th EventChecked of tor.
func (l *allEvents) waitChecked(t *testing.T, tor *Torrent, n int) Event {
	t.Helper()
	waitFor(t, "EventChecked", func() bool { return len(l.of(tor, EventChecked)) >= n })
	return l.of(tor, EventChecked)[n-1]
}

// corrupt flips a byte of file rel below dir, in place.
func corrupt(t *testing.T, dir, rel string, off int64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, rel), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

// A recheck of good data changes nothing: the seed goes through
// StateChecking back to seeding with every piece, checked, and keeps its
// peer and its counters. A stopped torrent is checked and stays stopped.
func TestRecheckGood(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 200_000, 100_000)
	mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 16<<10)
	n := mi.Info.PieceCount()
	evs := &allEvents{}
	seeder := newTestClient(t, sdir, evs.option)
	st, err := seeder.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, st)
	evs.waitChecked(t, st, 1)

	// A leecher downloading slowly, from the seed under its recheck.
	leecher := newTestClient(t, ldir)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}, Limits: TorrentLimits{DownloadRate: 100_000}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the leecher downloading", func() bool { return len(st.Peers()) == 1 && lt.Stats().PiecesHave > 0 })
	before := st.Stats()
	conns := len(evs.of(st, EventPeerConnected))
	verified := len(evs.of(st, EventPieceVerified))

	if err := st.Recheck(); err != nil {
		t.Fatal(err)
	}
	if s := st.State(); s != StateChecking {
		t.Fatalf("state %v during the recheck", s)
	}
	if e := evs.waitChecked(t, st, 2); e.Err != nil {
		t.Fatalf("recheck: %v", e.Err)
	}
	s := st.Stats()
	if s.State != StateSeeding || s.PiecesHave != n || s.Trusted != 0 || s.HashFails != 0 {
		t.Fatalf("after the recheck: %+v", s)
	}
	if s.Uploaded < before.Uploaded || s.Uploaded == 0 {
		t.Fatalf("uploaded %d, was %d", s.Uploaded, before.Uploaded)
	}
	if len(st.Peers()) != 1 || len(evs.of(st, EventPeerConnected)) != conns || len(evs.of(st, EventPeerDisconnected)) != 0 {
		t.Fatalf("connections changed: %d peers, %+v", len(st.Peers()), evs.of(st, EventPeerDisconnected))
	}
	if v := len(evs.of(st, EventPieceVerified)) - verified; v != n {
		t.Fatalf("%d pieces verified by the recheck, want %d", v, n)
	}
	if _, err := st.ResumeData(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.Done():
	default:
		t.Fatal("not done after the recheck")
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)
	if s := lt.Stats(); s.HashFails != 0 {
		t.Fatalf("leecher: %+v", s)
	}

	// Stopped: checked, and stopped again, its storage closed.
	st.Stop()
	if err := st.Recheck(); err != nil {
		t.Fatal(err)
	}
	evs.waitChecked(t, st, 3)
	if s := st.Stats(); s.State != StateStopped || s.PiecesHave != n || s.Trusted != 0 {
		t.Fatalf("stopped torrent after the recheck: %+v", s)
	}
	st.mu.Lock()
	open := st.storage != nil
	st.mu.Unlock()
	if open {
		t.Fatal("storage of a stopped torrent left open")
	}
	st.Start()
	waitDone(t, st)
	checkFiles(t, sdir, files)

	// Twice at once: one more check at least, the last one complete.
	if err := st.Recheck(); err != nil {
		t.Fatal(err)
	}
	if err := st.Recheck(); err != nil {
		t.Fatal(err)
	}
	evs.waitChecked(t, st, 4)
	waitFor(t, "the checks over", func() bool { return st.State() == StateSeeding })
	if s := st.Stats(); s.PiecesHave != n || s.Trusted != 0 || s.HashFails != 0 {
		t.Fatalf("after two rechecks: %+v", s)
	}

	if err := st.Remove(false); err != nil {
		t.Fatal(err)
	}
	if err := st.Recheck(); err == nil {
		t.Fatal("recheck of a removed torrent")
	}
}

// A file corrupted behind the client's back: the recheck loses the
// piece, which the torrent downloads again from a peer, the counters
// going on from where they were.
func TestRecheckCorrupt(t *testing.T) {
	gdir, xdir := t.TempDir(), t.TempDir()
	files := testFiles(t, gdir, "content", 100_000, 300_000)
	testFiles(t, xdir, "content", 100_000, 300_000)
	mi := buildTorrent(t, gdir, "content", metainfo.V1, 16<<10)
	good, _ := seed(t, gdir, mi)
	evs := &allEvents{}
	c := newTestClient(t, xdir, evs.option)
	xt, err := c.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(good)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, xt)
	evs.waitChecked(t, xt, 1)
	if d := xt.Stats().Downloaded; d != 0 {
		t.Fatalf("downloaded %d with the data there", d)
	}

	var rel string
	for r := range files {
		if len(files[r]) == 300_000 {
			rel = r
		}
	}
	fi := fileIndex(t, mi.Info, rel[len("content/"):])
	off := int64(70_000)
	corrupt(t, xdir, rel, off)
	bad := int((mi.Info.FileList()[fi].Offset + off) / mi.Info.PieceLength)

	if err := xt.Recheck(); err != nil {
		t.Fatal(err)
	}
	if e := evs.waitChecked(t, xt, 2); e.Err != nil {
		t.Fatal(e.Err)
	}
	fails := evs.of(xt, EventHashFailed)
	if len(fails) != 1 || fails[0].Piece != bad {
		t.Fatalf("hash failures %+v, want piece %d", fails, bad)
	}
	waitDone(t, xt)
	checkFiles(t, xdir, files)
	s := xt.Stats()
	if s.HashFails != 1 || s.Downloaded != mi.Info.PieceSize(bad) || s.PiecesHave != mi.Info.PieceCount() || s.State != StateSeeding {
		t.Fatalf("after the recheck: %+v", s)
	}
}

// A recheck during a download: requests stop while it runs, it keeps the
// pieces downloaded, and the download goes on to the end.
func TestRecheckDuringDownload(t *testing.T) {
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 600_000, 200_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, _ := seed(t, sdir, mi)
	evs := &allEvents{}
	leecher := newTestClient(t, ldir, evs.option)
	lt, err := leecher.AddTorrent(mi, &AddOptions{
		Peers:  []string{addrOf(seeder)},
		Limits: TorrentLimits{DownloadRate: 400_000},
	})
	if err != nil {
		t.Fatal(err)
	}
	evs.waitChecked(t, lt, 1)
	waitFor(t, "some pieces", func() bool { return lt.Stats().PiecesHave >= 5 })
	if err := lt.Recheck(); err != nil {
		t.Fatal(err)
	}
	if s := lt.State(); s != StateChecking {
		t.Fatalf("state %v", s)
	}
	if e := evs.waitChecked(t, lt, 2); e.Err != nil {
		t.Fatal(e.Err)
	}
	if s := lt.Stats(); s.HashFails != 0 || s.PiecesHave < 5 {
		t.Fatalf("after the recheck: %+v", s)
	}
	if d := evs.of(lt, EventPeerDisconnected); len(d) != 0 {
		t.Fatalf("peer dropped during the recheck: %v", d[0].Err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)
}

// A recheck asked for during the first check runs once that is over; a
// torrent paused during a recheck stays paused after it.
func TestRecheckDuringCheck(t *testing.T) {
	dir := t.TempDir()
	testFiles(t, dir, "content", 2_000_000)
	mi := buildTorrent(t, dir, "content", metainfo.V1, 16<<10)
	n := mi.Info.PieceCount()
	evs := &allEvents{}
	c := newTestClient(t, dir, evs.option)
	tor, err := c.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tor.Recheck(); err != nil {
		t.Fatal(err)
	}
	evs.waitChecked(t, tor, 2)
	waitDone(t, tor)
	if s := tor.Stats(); s.PiecesHave != n || s.Trusted != 0 || s.State != StateSeeding {
		t.Fatalf("after the checks: %+v", s)
	}

	if err := tor.Recheck(); err != nil {
		t.Fatal(err)
	}
	tor.Pause()
	evs.waitChecked(t, tor, 3)
	if s := tor.Stats(); s.State != StatePaused || s.PiecesHave != n || s.Trusted != 0 {
		t.Fatalf("paused during the recheck: %+v", s)
	}
}
