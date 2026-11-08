package bittorrent

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// watchedStorage is a Storage that records whether it was called on a
// connection's receive path: a fib worker running a round, or the reading
// goroutine of a net.Conn (uTP) feeding a peer.Conn; and whether it was
// used during or after Close. Delay, when set, makes every call that slow.
type watchedStorage struct {
	Storage
	delay time.Duration

	reads, writes atomic.Int64
	active        atomic.Int64
	closed        atomic.Bool
	mu            sync.Mutex
	bad           string
}

func (s *watchedStorage) fail(why string) {
	buf := make([]byte, 64<<10)
	stack := string(buf[:runtime.Stack(buf, false)])
	s.mu.Lock()
	if s.bad == "" {
		s.bad = why + "\n" + stack
	}
	s.mu.Unlock()
}

func (s *watchedStorage) begin() {
	s.active.Add(1)
	if s.closed.Load() {
		s.fail("used after Close")
	}
	buf := make([]byte, 64<<10)
	stack := string(buf[:runtime.Stack(buf, false)])
	if strings.Contains(stack, "github.com/lesismal/fib/core.") || strings.Contains(stack, "bittorrent/peer.(*Conn).input") {
		s.fail("called on a connection's receive path")
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
}

func (s *watchedStorage) ReadAt(file int, p []byte, off int64) (int, error) {
	s.begin()
	defer s.active.Add(-1)
	s.reads.Add(1)
	return s.Storage.ReadAt(file, p, off)
}

func (s *watchedStorage) WriteAt(file int, p []byte, off int64) (int, error) {
	s.begin()
	defer s.active.Add(-1)
	s.writes.Add(1)
	return s.Storage.WriteAt(file, p, off)
}

func (s *watchedStorage) Close() error {
	if s.active.Load() > 0 {
		s.fail("closed while in use")
	}
	s.closed.Store(true)
	return s.Storage.Close()
}

func (s *watchedStorage) badStack() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bad
}

// watch wraps the storage opener's storages in watchedStorage, which it
// collects.
type watch struct {
	opener StorageOpener
	delay  time.Duration
	mu     sync.Mutex
	all    []*watchedStorage
}

func (w *watch) open(info *metainfo.Info) (Storage, error) {
	st, err := w.opener(info)
	if err != nil {
		return nil, err
	}
	ws := &watchedStorage{Storage: st, delay: w.delay}
	w.mu.Lock()
	w.all = append(w.all, ws)
	w.mu.Unlock()
	return ws, nil
}

func (w *watch) check(t *testing.T, name string) (reads, writes int64) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ws := range w.all {
		if s := ws.badStack(); s != "" {
			t.Fatalf("%s: storage %s", name, s)
		}
		reads += ws.reads.Load()
		writes += ws.writes.Load()
	}
	return reads, writes
}

// TestStorageOffWorkers checks that neither the leecher's writes and piece
// checks nor the seeder's upload reads run on a fib worker or a uTP
// connection's reading goroutine.
func TestStorageOffWorkers(t *testing.T) {
	for _, utp := range []bool{false, true} {
		name := map[bool]string{false: "tcp", true: "utp"}[utp]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			transport := func(c *Config) { c.DisableUTP = true }
			if utp {
				transport = func(c *Config) { c.DisableTCP = true }
			}
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 300_000, 200_000)
			mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 32<<10)
			sw := &watch{opener: NewFileStorage(sdir, FileStorageOptions{})}
			seeder := newTestClient(t, sdir, transport, func(c *Config) { c.Storage = sw.open })
			st, err := seeder.AddTorrent(mi, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, st)
			lw := &watch{opener: NewFileStorage(ldir, FileStorageOptions{})}
			leecher := newTestClient(t, ldir, transport, func(c *Config) { c.Storage = lw.open })
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, lt)
			checkFiles(t, ldir, files)
			if r, _ := sw.check(t, "seeder"); r == 0 {
				t.Fatal("seeder never read")
			}
			if _, w := lw.check(t, "leecher"); w == 0 {
				t.Fatal("leecher never wrote")
			}
		})
	}
}

// TestDiskBackpressure gives the leecher a slow storage and a small disk
// queue: reads from the seeder are held while the queue is full, so the
// bytes queued stay near DiskQueueBytes, and the download completes.
func TestDiskBackpressure(t *testing.T) {
	for _, utp := range []bool{false, true} {
		name := map[bool]string{false: "tcp", true: "utp"}[utp]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			transport := func(c *Config) { c.DisableUTP = true }
			if utp {
				transport = func(c *Config) { c.DisableTCP = true }
			}
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 1_000_000)
			mi := buildTorrent(t, sdir, "content", metainfo.V1, 64<<10)
			seeder, _ := seed(t, sdir, mi, transport)
			const limit = 64 << 10
			lw := &watch{opener: NewFileStorage(ldir, FileStorageOptions{}), delay: 5 * time.Millisecond}
			leecher := newTestClient(t, ldir, transport, func(c *Config) {
				c.Storage = lw.open
				c.DiskWorkers = 1
				c.DiskQueueBytes = limit
			})
			var peak atomic.Int64
			stop := make(chan struct{})
			sampled := make(chan struct{})
			go func() {
				defer close(sampled)
				for {
					if q := leecher.disk.queuedBytes(); q > peak.Load() {
						peak.Store(q)
					}
					select {
					case <-stop:
						return
					case <-time.After(time.Millisecond):
					}
				}
			}()
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, lt)
			close(stop)
			<-sampled
			checkFiles(t, ldir, files)
			lw.check(t, "leecher")
			t.Logf("peak queued %d bytes, reads held %d times", peak.Load(), leecher.disk.holds.Load())
			if leecher.disk.holds.Load() == 0 {
				t.Fatal("reads never held")
			}
			// Past the bound by what reads already delivered when it was
			// reached: one read round of the one connection.
			if p := peak.Load(); p > limit+256<<10 {
				t.Fatalf("peak queued %d bytes, bound %d", p, limit)
			}
		})
	}
}

// TestCloseWithQueuedWrites stops or removes a torrent, or closes its
// client, with writes queued for a slow storage: the storage is closed
// only once the write under way is over, none is written after, and the
// queue drains.
func TestCloseWithQueuedWrites(t *testing.T) {
	for _, how := range []string{"remove", "stop", "client"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			testFiles(t, sdir, "content", 2_000_000)
			mi := buildTorrent(t, sdir, "content", metainfo.V1, 64<<10)
			seeder, _ := seed(t, sdir, mi)
			lw := &watch{opener: NewFileStorage(ldir, FileStorageOptions{}), delay: 20 * time.Millisecond}
			leecher := newTestClient(t, ldir, func(c *Config) {
				c.Storage = lw.open
				c.DiskWorkers = 1
			})
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(transferTimeout)
			for leecher.disk.queuedBytes() < 256<<10 {
				if time.Now().After(deadline) {
					t.Fatalf("no writes queued: %+v", lt.Stats())
				}
				time.Sleep(time.Millisecond)
			}
			switch how {
			case "remove":
				err = lt.Remove(false)
			case "stop":
				lt.Stop()
			case "client":
				err = leecher.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			for leecher.disk.queuedBytes() > 0 {
				if time.Now().After(deadline) {
					t.Fatalf("%d bytes still queued", leecher.disk.queuedBytes())
				}
				time.Sleep(time.Millisecond)
			}
			lw.check(t, "leecher")
		})
	}
}
