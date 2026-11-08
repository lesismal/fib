package bittorrent

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

// readCounter counts the reads of the storages countingOpener opens, by
// file.
type readCounter struct {
	mu    sync.Mutex
	reads map[int]int
}

func (rc *readCounter) total() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	n := 0
	for _, c := range rc.reads {
		n += c
	}
	return n
}

func (rc *readCounter) of(file int) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.reads[file]
}

type countingStorage struct {
	Storage
	rc *readCounter
}

func (s *countingStorage) ReadAt(file int, p []byte, off int64) (int, error) {
	s.rc.mu.Lock()
	s.rc.reads[file]++
	s.rc.mu.Unlock()
	return s.Storage.ReadAt(file, p, off)
}

func (s *countingStorage) Stat(file int) (int64, time.Time, error) {
	return s.Storage.(StatStorage).Stat(file)
}

// noStatStorage hides the StatStorage of the storage it wraps.
type noStatStorage struct{ Storage }

// countingOpener wraps the storages open opens in ones that count their
// reads; with noStat, they are no StatStorage.
func countingOpener(open StorageOpener, noStat bool) (StorageOpener, *readCounter) {
	rc := &readCounter{reads: map[int]int{}}
	return func(info *metainfo.Info) (Storage, error) {
		st, err := open(info)
		if err != nil {
			return nil, err
		}
		if noStat {
			return &noStatStorage{&countingStorage{st, rc}}, nil
		}
		return &countingStorage{st, rc}, nil
	}, rc
}

// errorLog records the errors a client's events report.
type errorLog struct {
	mu   sync.Mutex
	errs []error
}

func (l *errorLog) option(c *Config) {
	c.OnEvent = func(e Event) {
		if e.Type == EventError {
			l.mu.Lock()
			l.errs = append(l.errs, e.Err)
			l.mu.Unlock()
		}
	}
}

// waitResumeErrors waits until n EventErrors of ErrResumeData have been
// heard: events reach OnEvent on a goroutine of their own, so a count
// read right after the check may not hold the latest yet.
func (l *errorLog) waitResumeErrors(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for l.resumeErrors() < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d reports of unused resume data, want %d", l.resumeErrors(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *errorLog) resumeErrors() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, err := range l.errs {
		if errors.Is(err, ErrResumeData) {
			n++
		}
	}
	return n
}

var versionNames = map[metainfo.Version]string{metainfo.V1: "v1", metainfo.V2: "v2", metainfo.Hybrid: "hybrid"}

// downloadForResume downloads mi from seeder into ldir, stops the torrent
// and returns its resume data.
func downloadForResume(t *testing.T, mi *metainfo.MetaInfo, seeder *Client, ldir string) []byte {
	t.Helper()
	leecher := newTestClient(t, ldir)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	if g := lt.ResumeGen(); g != uint64(mi.Info.PieceCount()) {
		t.Fatalf("ResumeGen %d, want %d", g, mi.Info.PieceCount())
	}
	lt.Stop()
	data, err := lt.ResumeData()
	if err != nil {
		t.Fatal(err)
	}
	leecher.Close()
	return data
}

func TestResumeRoundTrip(t *testing.T) {
	for _, v := range []metainfo.Version{metainfo.V1, metainfo.V2, metainfo.Hybrid} {
		t.Run(versionNames[v], func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 100_000, 300_000, 0, 65_536, 1234)
			mi := buildTorrent(t, sdir, "content", v, 32<<10)
			seeder, _ := seed(t, sdir, mi)
			data := downloadForResume(t, mi, seeder, ldir)
			if len(data) > 1024 {
				t.Fatalf("resume data of %d bytes", len(data))
			}

			var log errorLog
			c := newTestClient(t, ldir, log.option)
			open, rc := countingOpener(NewFileStorage(ldir, FileStorageOptions{}), false)
			tor, err := c.AddTorrent(mi, &AddOptions{Storage: open, Resume: data})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, tor)
			if n := rc.total(); n != 0 {
				t.Fatalf("%d reads: resumed pieces were hashed", n)
			}
			if !tor.HaveAll() || tor.Stats().Downloaded != 0 {
				t.Fatalf("not resumed: %+v", tor.Stats())
			}
			if n := log.resumeErrors(); n != 0 {
				t.Fatalf("good resume data reported unused: %v", log.errs)
			}
			checkFiles(t, ldir, files)
		})
	}
}

// fileIndex is the index in info's FileList of the file at rel below the
// torrent's directory.
func fileIndex(t *testing.T, info *metainfo.Info, rel string) int {
	t.Helper()
	for i, f := range info.FileList() {
		if filepath.Join(f.Path...) == rel {
			return i
		}
	}
	t.Fatalf("no file %s", rel)
	return -1
}

func TestResumeChangedFiles(t *testing.T) {
	// Hybrid: padding aligns the files to pieces, so a piece reads one
	// file only.
	dir := t.TempDir()
	testFiles(t, dir, "content", 100_000, 200_000, 50_000, 70_000)
	mi := buildTorrent(t, dir, "content", metainfo.Hybrid, 16<<10)
	info := mi.Info
	c := newTestClient(t, dir)
	tor, err := c.AddTorrent(mi, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, tor)
	data, err := tor.ResumeData()
	if err != nil {
		t.Fatal(err)
	}
	if err := tor.Remove(false); err != nil {
		t.Fatal(err)
	}

	touched := fileIndex(t, info, filepath.Join("dir0", "file0.bin"))
	truncated := fileIndex(t, info, filepath.Join("dir1", "file1.bin"))
	removed := fileIndex(t, info, filepath.Join("dir0", "file2.bin"))
	kept := fileIndex(t, info, filepath.Join("dir1", "file3.bin"))
	path := func(i int) string {
		return filepath.Join(append([]string{dir, "content"}, info.FileList()[i].Path...)...)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(path(touched), later, later); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path(truncated), 6*16<<10); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path(removed)); err != nil {
		t.Fatal(err)
	}

	open, rc := countingOpener(NewFileStorage(dir, FileStorageOptions{}), false)
	tor, err = c.AddTorrent(mi, &AddOptions{Storage: open, Resume: data, Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	waitChecked(t, tor)
	if rc.of(touched) == 0 || rc.of(truncated) == 0 {
		t.Fatalf("changed files not checked: %v", rc.reads)
	}
	if rc.of(removed) != 0 || rc.of(kept) != 0 {
		t.Fatalf("unchanged or missing files read: %v", rc.reads)
	}
	for f, want := range map[int]func(i, b int) bool{
		touched:   func(i, b int) bool { return true },
		truncated: func(i, b int) bool { return i < b+6 },
		removed:   func(i, b int) bool { return false },
		kept:      func(i, b int) bool { return true },
	} {
		b, e := info.FilePieces(f)
		for i := b; i < e; i++ {
			if tor.HavePiece(i) != want(i, b) {
				t.Errorf("file %d piece %d: have %v", f, i, tor.HavePiece(i))
			}
		}
	}
}

// waitChecked waits for the end of tor's check.
func waitChecked(t *testing.T, tor *Torrent) {
	t.Helper()
	deadline := time.Now().Add(transferTimeout)
	for {
		tor.mu.Lock()
		checking := tor.checking || tor.info == nil
		tor.mu.Unlock()
		if !checking {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("check not done")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestResumeRejected(t *testing.T) {
	dir := t.TempDir()
	testFiles(t, dir, "content", 100_000, 30_000)
	mi := buildTorrent(t, dir, "content", metainfo.Hybrid, 16<<10)
	otherDir := t.TempDir()
	testFiles(t, otherDir, "other", 50_000, 20_000)
	other := buildTorrent(t, otherDir, "other", metainfo.V1, 16<<10)

	var log errorLog
	c := newTestClient(t, dir, log.option)
	resumeOf := func(mi *metainfo.MetaInfo, opener StorageOpener) []byte {
		tor, err := c.AddTorrent(mi, &AddOptions{Storage: opener})
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, tor)
		data, err := tor.ResumeData()
		if err != nil {
			t.Fatal(err)
		}
		_ = tor.Remove(false)
		return data
	}
	good := resumeOf(mi, nil)
	foreign := resumeOf(other, NewFileStorage(otherDir, FileStorageOptions{}))
	var b resumeBlob
	if err := bencode.Unmarshal(good, &b); err != nil {
		t.Fatal(err)
	}
	b.Version = resumeVersion + 1
	newer, _ := bencode.Marshal(&b)
	b.Version = resumeVersion
	b.Pieces = b.Pieces[1:]
	short, _ := bencode.Marshal(&b)
	b = resumeBlob{}
	_ = bencode.Unmarshal(good, &b)
	b.V2 = make([]byte, 32)
	wrongV2, _ := bencode.Marshal(&b)

	for name, data := range map[string][]byte{
		"garbage": []byte("not resume data"),
		"foreign": foreign,
		"newer":   newer,
		"short":   short,
		"wrongv2": wrongV2,
		"empty":   []byte("de"),
	} {
		t.Run(name, func(t *testing.T) {
			before := log.resumeErrors()
			open, rc := countingOpener(NewFileStorage(dir, FileStorageOptions{}), false)
			tor, err := c.AddTorrent(mi, &AddOptions{Storage: open, Resume: data})
			if err != nil {
				t.Fatal(err)
			}
			// Removed however the subtest ends, so that a failure does
			// not fail the next ones with ErrTorrentExists.
			defer func() { _ = tor.Remove(false) }()
			waitDone(t, tor)
			log.waitResumeErrors(t, before+1)
			time.Sleep(50 * time.Millisecond) // a second report would arrive by now
			if log.resumeErrors() != before+1 {
				t.Fatalf("%d reports, want %d", log.resumeErrors(), before+1)
			}
			if rc.total() < mi.Info.PieceCount() {
				t.Fatalf("%d reads: no full check", rc.total())
			}
		})
	}
}

func TestResumeMagnet(t *testing.T) {
	for _, v := range []metainfo.Version{metainfo.V1, metainfo.V2, metainfo.Hybrid} {
		t.Run(versionNames[v], func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 200_000, 70_000)
			mi := buildTorrent(t, sdir, "content", v, 16<<10)
			seeder, _ := seed(t, sdir, mi)
			data := downloadForResume(t, mi, seeder, ldir)

			c := newTestClient(t, ldir)
			m := mi.Magnet()
			m.Peers = []string{addrOf(seeder)}
			before := c.Torrents()
			if len(before) != 0 {
				t.Fatal("torrents left")
			}
			open, rc := countingOpener(NewFileStorage(ldir, FileStorageOptions{}), false)
			tor, err := c.AddMagnet(m.String(), &AddOptions{Storage: open, Resume: data})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tor.ResumeData(); err == nil {
				select {
				case <-tor.GotInfo():
				default:
					t.Fatal("resume data before the info dictionary")
				}
			}
			waitDone(t, tor)
			if n := rc.total(); n != 0 {
				t.Fatalf("%d reads: resumed pieces were hashed", n)
			}
			if d := tor.Stats().Downloaded; d != 0 {
				t.Fatalf("downloaded %d bytes again", d)
			}
			checkFiles(t, ldir, files)
		})
	}
}

func TestResumeMemoryStorage(t *testing.T) {
	sdir := t.TempDir()
	testFiles(t, sdir, "content", 100_000, 40_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V2, 16<<10)
	seeder, _ := seed(t, sdir, mi)
	mem := NewMemoryStorage()
	var log errorLog
	c := newTestClient(t, t.TempDir(), func(cfg *Config) { cfg.Storage = mem }, log.option)
	tor, err := c.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, tor)
	data, err := tor.ResumeData()
	if err != nil {
		t.Fatal(err)
	}
	_ = tor.Remove(false)

	for i, noStat := range []bool{false, true} {
		t.Run(strconv.FormatBool(noStat), func(t *testing.T) {
			open, rc := countingOpener(mem, noStat)
			tor, err := c.AddTorrent(mi, &AddOptions{Storage: open, Resume: data})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, tor)
			if noStat {
				log.waitResumeErrors(t, 1)
				if rc.total() == 0 {
					t.Fatal("no check without file stamps")
				}
				if _, err := tor.ResumeData(); err == nil {
					t.Fatal("resume data without file stamps")
				}
			} else if rc.total() != 0 {
				t.Fatalf("%d reads: resumed pieces were hashed", rc.total())
			}
			if log.resumeErrors() != i {
				t.Fatalf("%d reports, want %d", log.resumeErrors(), i)
			}
			_ = tor.Remove(false)
		})
	}

	// A file written since is checked again.
	m, _ := mem(mi.Info)
	m.(*MemoryStorage).Corrupt(0, []byte{0xff}, 10)
	open, rc := countingOpener(mem, false)
	tor, err = c.AddTorrent(mi, &AddOptions{Storage: open, Resume: data, Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	waitChecked(t, tor)
	if rc.of(0) == 0 {
		t.Fatal("changed file not checked")
	}
	if tor.HavePiece(0) {
		t.Fatal("corrupt piece taken as had")
	}
}
