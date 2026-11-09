package bittorrent

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/tracker"
	fibhttp "github.com/lesismal/fib/http"
)

// TestEventQueueBound fills the queue of a handler that blocks: piece
// events are dropped, the oldest first for the events never dropped, and
// an EventDropped counts them after the rest.
func TestEventQueueBound(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var got []Event
	c := newTestClient(t, t.TempDir(), func(cfg *Config) {
		cfg.EventQueue = 8
		cfg.OnEvent = func(e Event) {
			if e.Type == EventError && e.Err == errBlock {
				close(entered)
				<-release
				return
			}
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		}
	})
	c.emit(Event{Type: EventError, Err: errBlock})
	<-entered
	for i := range 100 {
		c.emit(Event{Type: EventPieceVerified, Piece: i})
	}
	c.emit(Event{Type: EventCompleted})
	for i := 100; i < 200; i++ {
		c.emit(Event{Type: EventPieceVerified, Piece: i})
	}
	c.emit(Event{Type: EventStateChanged})
	// The events never dropped go past the bound.
	for i := range 10 {
		c.emit(Event{Type: EventError, Piece: i})
	}
	close(release)

	// Pieces 0..7 filled the queue and 8..99 were dropped; Completed
	// dropped piece 0, 100..199 were dropped, StateChanged dropped piece
	// 1, the errors piece 2..7 and then nothing.
	const drops = 92 + 1 + 100 + 1 + 6
	want := []EventType{EventCompleted, EventStateChanged}
	for range 10 {
		want = append(want, EventError)
	}
	want = append(want, EventDropped)
	deadline := time.Now().Add(transferTimeout)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= len(want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("heard %d events, want %d", n, len(want))
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("heard %d events, want %d: %v", len(got), len(want), got)
	}
	for i, e := range got {
		if e.Type != want[i] {
			t.Fatalf("event %d is %v, want %v", i, e.Type, want[i])
		}
	}
	for i := range 10 {
		if got[2+i].Piece != i {
			t.Fatalf("errors out of order: %v", got)
		}
	}
	if d := got[len(got)-1].Dropped; d != drops {
		t.Fatalf("EventDropped says %d, want %d", d, drops)
	}
	if d := c.DroppedEvents(); d != drops {
		t.Fatalf("DroppedEvents %d, want %d", d, drops)
	}
}

var errBlock = errors.New("block")

// TestReaderCloseInterruptsRead closes a reader whose Read waits for a
// piece no peer has: Close returns, and so does the Read.
func TestReaderCloseInterruptsRead(t *testing.T) {
	sdir := t.TempDir()
	testFiles(t, sdir, "movie.bin", 100_000)
	mi := buildTorrent(t, sdir, "movie.bin", metainfo.V1, 16<<10)
	c := newTestClient(t, "", func(c *Config) { c.Storage = NewMemoryStorage() })
	tor, err := c.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := tor.NewReader()
	read := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 1000))
		read <- err
	}()
	// The Read has set its window, and waits.
	deadline := time.Now().Add(transferTimeout)
	for {
		tor.mu.Lock()
		_, waiting := tor.readers[r]
		tor.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Read never reached its wait")
		}
		time.Sleep(5 * time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	hang := time.After(transferTimeout)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-hang:
		t.Fatal("Close waits for the Read")
	}
	select {
	case err := <-read:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Read gave %v, want net.ErrClosed", err)
		}
	case <-hang:
		t.Fatal("Read not interrupted")
	}
	tor.mu.Lock()
	_, kept := tor.readers[r]
	urgent := len(tor.urgent)
	tor.mu.Unlock()
	if kept || urgent != 0 {
		t.Fatalf("closed reader kept its window: %v, %d urgent", kept, urgent)
	}
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read after Close gave %v", err)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

// eventStore records the events of the announces it keeps.
type eventStore struct {
	*tracker.MemoryStore
	mu      sync.Mutex
	events  map[tracker.PeerID][]tracker.Event
	started chan struct{} // closed on the first started announce
	once    sync.Once
}

func (s *eventStore) Announce(a tracker.StoreAnnounce) (tracker.StoreResult, error) {
	s.mu.Lock()
	s.events[a.PeerID] = append(s.events[a.PeerID], a.Event)
	s.mu.Unlock()
	if a.Event == tracker.EventStarted {
		s.once.Do(func() { close(s.started) })
	}
	return s.MemoryStore.Announce(a)
}

func (s *eventStore) count(id [20]byte, ev tracker.Event) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.events[id] {
		if e == ev {
			n++
		}
	}
	return n
}

// gatedStorage holds the reads of a check until gate closes.
type gatedStorage struct {
	Storage
	gate <-chan struct{}
}

func (s gatedStorage) ReadAt(file int, p []byte, off int64) (int, error) {
	<-s.gate
	return s.Storage.ReadAt(file, p, off)
}

// TestSeedDoesNotAnnounceCompleted has a seed announce started while it
// checks its data, then a leecher download from it: only the leecher
// announces completed, and the tracker counts one download.
func TestSeedDoesNotAnnounceCompleted(t *testing.T) {
	store := &eventStore{
		MemoryStore: tracker.NewMemoryStore(tracker.MemoryStoreConfig{}),
		events:      map[tracker.PeerID][]tracker.Event{},
		started:     make(chan struct{}),
	}
	// An hour: the seed's announce after its check must not wait for it.
	srv := tracker.NewServer(tracker.ServerConfig{Store: store, Interval: time.Hour})
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	engine, err := fib.New(cfg, fib.Binding{Network: "tcp4", Addr: "127.0.0.1:0", Handler: fibhttp.NewHandler(srv)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	ta, _ := engine.BindingAddrs(0)
	url := "http://" + ta[0].String() + "/announce"

	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 150_000, 10_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10, url)
	trackers := func(c *Config) { c.DisableTrackers = false }
	gated := func(c *Config) {
		open := NewFileStorage(sdir, FileStorageOptions{})
		c.Storage = func(info *metainfo.Info) (Storage, error) {
			st, err := open(info)
			if err != nil {
				return nil, err
			}
			return gatedStorage{Storage: st, gate: store.started}, nil
		}
	}
	seeder, _ := seed(t, sdir, mi, trackers, gated)
	// Its started announce went out during the check, with bytes left; the
	// check over, it tells the tracker it is a seed at once.
	deadline := time.Now().Add(transferTimeout)
	for {
		if st, _ := store.Scrape(mi.Info.InfoHashV1()); st.Complete == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the tracker never counted the seed complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	leecher := newTestClient(t, ldir, trackers)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)
	deadline = time.Now().Add(transferTimeout)
	for store.count(leecher.peerID, tracker.EventCompleted) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the leecher never announced completed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A completed announce of the seed's would have gone out when its
	// check ended, before the leecher's; the wait makes room for it.
	time.Sleep(500 * time.Millisecond)
	if n := store.count(seeder.peerID, tracker.EventStarted); n != 1 {
		t.Fatalf("seed announced started %d times", n)
	}
	if n := store.count(seeder.peerID, tracker.EventCompleted); n != 0 {
		t.Fatalf("seed announced completed %d times", n)
	}
	if n := store.count(leecher.peerID, tracker.EventCompleted); n != 1 {
		t.Fatalf("leecher announced completed %d times", n)
	}
	st, _ := store.Scrape(mi.Info.InfoHashV1())
	if st.Downloaded != 1 {
		t.Fatalf("tracker counts %d downloads, want 1", st.Downloaded)
	}
}
