package bittorrent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Storage keeps a torrent's data: the files of its metainfo.Info's
// FileList, addressed by their index there. The client never asks it for
// a padding file (BEP 47), whose bytes are zeros it supplies itself, nor
// for the gaps between the files of a v2 torrent, which are no file's. Its
// methods are called from many goroutines at once, for different blocks.
type Storage interface {
	// ReadAt reads len(p) bytes of file at off. Fewer, with an error, say
	// the bytes are not there: a file not yet written, or shorter.
	ReadAt(file int, p []byte, off int64) (int, error)
	// WriteAt writes p to file at off, creating the file as needed: an
	// empty p creates an empty file.
	WriteAt(file int, p []byte, off int64) (int, error)
	// Close releases what the storage holds open; the torrent may open it
	// again later.
	Close() error
}

// StorageRemover is a Storage that can delete what it stored, for
// Torrent.Remove.
type StorageRemover interface {
	Remove() error
}

// StorageOpener opens the storage of a torrent once its info dictionary is
// known.
type StorageOpener func(info *metainfo.Info) (Storage, error)

// FileStorageOptions tunes the storage NewFileStorage opens.
type FileStorageOptions struct {
	// Preallocate sets each file to its full length when it is first
	// opened for writing. It truncates, which most file systems keep
	// sparse: it reserves no blocks, but the file has its final size from
	// the start, as some users expect.
	Preallocate bool
	// MaxOpenFiles bounds the file handles kept open per torrent;
	// DefaultMaxOpenFiles when zero.
	MaxOpenFiles int
}

// DefaultMaxOpenFiles is FileStorageOptions.MaxOpenFiles when zero.
const DefaultMaxOpenFiles = 64

// NewFileStorage returns the opener of storage in files below dir, laid out
// as every client lays them out: a single-file torrent's file is
// dir/name, a multi-file torrent's files are below dir/name. Each path
// component is made safe for the operating system (see SanitizePath);
// padding files are never written.
func NewFileStorage(dir string, opts FileStorageOptions) StorageOpener {
	return func(info *metainfo.Info) (Storage, error) {
		if opts.MaxOpenFiles <= 0 {
			opts.MaxOpenFiles = DefaultMaxOpenFiles
		}
		files := info.FileList()
		s := &fileStorage{opts: opts, paths: make([]string, len(files)), lengths: make([]int64, len(files)), open: map[int]*openFile{}}
		root := dir
		taken := map[string]int{}
		for i, f := range files {
			s.lengths[i] = f.Length
			if f.IsPad() {
				continue
			}
			comps := append([]string{info.Name}, f.Path...)
			p := filepath.Join(root, SanitizePath(comps))
			p = dedupePath(p, taken)
			s.paths[i] = p
		}
		s.top = filepath.Join(root, sanitizeComponent(info.Name))
		return s, nil
	}
}

// dedupePath gives p a suffix when another file of the torrent already has
// a path that the file system would take for the same: equal, or equal
// but for case on Windows and macOS, whose file systems fold case.
func dedupePath(p string, taken map[string]int) string {
	key := func(s string) string {
		if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
			return strings.ToLower(s)
		}
		return s
	}
	if _, ok := taken[key(p)]; !ok {
		taken[key(p)] = 1
		return p
	}
	ext := filepath.Ext(p)
	base := strings.TrimSuffix(p, ext)
	for n := taken[key(p)]; ; n++ {
		q := fmt.Sprintf("%s.%d%s", base, n, ext)
		if _, ok := taken[key(q)]; !ok {
			taken[key(p)] = n + 1
			taken[key(q)] = 1
			return q
		}
	}
}

// SanitizePath joins the components of a torrent's path into a relative
// path that is safe on the operating system it runs on: metainfo already
// refuses "..", "." and empty components, and this replaces the
// characters a file name cannot hold. On Windows those are the control
// characters and <>:"/\|?*, names may not end with a dot or a space, and
// the device names CON, PRN, AUX, NUL, COM1-9 and LPT1-9, with any
// extension, get an underscore appended. Elsewhere only '/' and NUL are
// replaced, and a backslash stays a character of the name, as other
// clients keep it.
func SanitizePath(components []string) string {
	out := make([]string, len(components))
	for i, c := range components {
		out[i] = sanitizeComponent(c)
	}
	return filepath.Join(out...)
}

var windowsReserved = map[string]bool{
	"CON": true, "PRN": true, "AUX": true, "NUL": true,
	"COM1": true, "COM2": true, "COM3": true, "COM4": true, "COM5": true, "COM6": true, "COM7": true, "COM8": true, "COM9": true,
	"LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true, "LPT5": true, "LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true,
}

func sanitizeComponent(c string) string {
	return sanitizeFor(c, runtime.GOOS == "windows")
}

func sanitizeFor(c string, windows bool) string {
	if !utf8.ValidString(c) {
		c = strings.ToValidUTF8(c, "_")
	}
	var b strings.Builder
	for _, r := range c {
		switch {
		case r == '/' || r == 0:
			b.WriteByte('_')
		case windows && (r < 32 || strings.ContainsRune(`<>:"\|?*`, r)):
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	s := b.String()
	if windows {
		s = strings.TrimRight(s, ". ")
		stem := s
		if i := strings.IndexByte(stem, '.'); i >= 0 {
			stem = stem[:i]
		}
		if windowsReserved[strings.ToUpper(strings.TrimRight(stem, " "))] {
			s += "_"
		}
	}
	if s == "" || s == "." || s == ".." {
		s = "_"
	}
	return s
}

type openFile struct {
	f    *os.File
	rw   bool
	used uint64
}

type fileStorage struct {
	opts    FileStorageOptions
	paths   []string
	lengths []int64
	top     string

	mu     sync.Mutex
	open   map[int]*openFile
	clock  uint64
	stale  []*os.File
	closed bool
}

// file returns the handle of file i, opened for writing when write.
func (s *fileStorage) file(i int, write bool) (*os.File, error) {
	if i < 0 || i >= len(s.paths) || s.paths[i] == "" {
		return nil, fmt.Errorf("bittorrent: no file %d in storage", i)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Closed for good: what still reads or writes (a check, an
		// upload) must not open files again, which on Windows would keep
		// them from being deleted. A torrent started again opens a new
		// storage.
		return nil, errStorageClosed
	}
	s.clock++
	if of, ok := s.open[i]; ok && (of.rw || !write) {
		of.used = s.clock
		return of.f, nil
	}
	p := s.paths[i]
	var f *os.File
	var err error
	if write {
		if err = os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return nil, err
		}
		f, err = os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
		if err == nil && s.opts.Preallocate {
			if st, serr := f.Stat(); serr == nil && st.Size() < s.lengths[i] {
				err = f.Truncate(s.lengths[i])
			}
		}
	} else {
		f, err = os.Open(p)
	}
	if err != nil {
		if f != nil {
			f.Close()
		}
		return nil, err
	}
	if old, ok := s.open[i]; ok {
		// A read-only handle this replaces may be in use by a reader: it
		// is closed with the storage (Windows will not delete an open
		// file), or by the collector.
		s.stale = append(s.stale, old.f)
	}
	s.open[i] = &openFile{f: f, rw: write, used: s.clock}
	if len(s.open) > s.opts.MaxOpenFiles {
		s.evictLocked(i)
	}
	return f, nil
}

// evictLocked closes the least recently used handle other than keep's.
// A read or write in progress on it fails with an error the caller
// retries.
func (s *fileStorage) evictLocked(keep int) {
	victim, oldest := -1, ^uint64(0)
	for i, of := range s.open {
		if i != keep && of.used < oldest {
			victim, oldest = i, of.used
		}
	}
	if victim >= 0 {
		s.open[victim].f.Close()
		delete(s.open, victim)
	}
}

func (s *fileStorage) ReadAt(file int, p []byte, off int64) (int, error) {
	for try := 0; ; try++ {
		f, err := s.file(file, false)
		if err != nil {
			return 0, err
		}
		n, err := f.ReadAt(p, off)
		if err != nil && errors.Is(err, os.ErrClosed) && try < 3 {
			continue
		}
		return n, err
	}
}

func (s *fileStorage) WriteAt(file int, p []byte, off int64) (int, error) {
	for try := 0; ; try++ {
		f, err := s.file(file, true)
		if err != nil {
			return 0, err
		}
		if len(p) == 0 {
			return 0, nil
		}
		n, err := f.WriteAt(p, off)
		if err != nil && errors.Is(err, os.ErrClosed) && try < 3 {
			continue
		}
		return n, err
	}
}

var errStorageClosed = errors.New("bittorrent: storage closed")

func (s *fileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var first error
	for i, of := range s.open {
		if err := of.f.Close(); err != nil && first == nil {
			first = err
		}
		delete(s.open, i)
	}
	for _, f := range s.stale {
		f.Close()
	}
	s.stale = nil
	return first
}

// Remove deletes the torrent's files, and the directory of a multi-file
// torrent when nothing else is left in it.
func (s *fileStorage) Remove() error {
	s.Close()
	var first error
	for _, p := range s.paths {
		if p == "" {
			continue
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) && first == nil {
			first = err
		}
	}
	// Remove the directories left empty, deepest first.
	dirs := map[string]bool{}
	for _, p := range s.paths {
		for d := filepath.Dir(p); strings.HasPrefix(d, s.top) && len(d) >= len(s.top); d = filepath.Dir(d) {
			dirs[d] = true
		}
	}
	for len(dirs) > 0 {
		deepest := ""
		for d := range dirs {
			if len(d) > len(deepest) {
				deepest = d
			}
		}
		delete(dirs, deepest)
		_ = os.Remove(deepest) // fails, as it should, when not empty
	}
	return first
}

// NewMemoryStorage returns the opener of storage in memory, which tests
// and short-lived transfers use: nothing touches the disk. The returned
// opener gives the same storage back for the same info hash, so that a
// torrent removed and added again finds its data.
func NewMemoryStorage() StorageOpener {
	var mu sync.Mutex
	stores := map[metainfo.InfoHash]*MemoryStorage{}
	return func(info *metainfo.Info) (Storage, error) {
		key := info.InfoHashV1()
		if key.IsZero() {
			key = info.InfoHashV2().Truncate()
		}
		mu.Lock()
		defer mu.Unlock()
		if s, ok := stores[key]; ok {
			return s, nil
		}
		s := &MemoryStorage{files: make([][]byte, len(info.FileList()))}
		stores[key] = s
		return s, nil
	}
}

// MemoryStorage is a Storage in memory.
type MemoryStorage struct {
	mu    sync.RWMutex
	files [][]byte
}

// ReadAt implements Storage.
func (m *MemoryStorage) ReadAt(file int, p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if file < 0 || file >= len(m.files) {
		return 0, fmt.Errorf("bittorrent: no file %d in storage", file)
	}
	f := m.files[file]
	if off >= int64(len(f)) {
		return 0, io.EOF
	}
	n := copy(p, f[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt implements Storage.
func (m *MemoryStorage) WriteAt(file int, p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if file < 0 || file >= len(m.files) {
		return 0, fmt.Errorf("bittorrent: no file %d in storage", file)
	}
	f := m.files[file]
	if end := off + int64(len(p)); end > int64(len(f)) {
		if end <= int64(cap(f)) {
			f = f[:end]
		} else {
			nf := make([]byte, end, max(end, int64(cap(f))*2))
			copy(nf, f)
			f = nf
		}
		m.files[file] = f
	}
	return copy(f[off:], p), nil
}

// Close implements Storage; the data stays.
func (m *MemoryStorage) Close() error { return nil }

// Remove forgets the data.
func (m *MemoryStorage) Remove() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.files {
		m.files[i] = nil
	}
	return nil
}

// Corrupt overwrites the stored bytes of file at off with p, for tests
// that need bad data: unlike WriteAt it never grows the file.
func (m *MemoryStorage) Corrupt(file int, p []byte, off int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if file >= 0 && file < len(m.files) && off < int64(len(m.files[file])) {
		copy(m.files[file][off:], p)
	}
}
