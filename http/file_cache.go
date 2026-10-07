//go:build linux || darwin || windows

package http

import (
	"bytes"
	"errors"
	"mime"
	stdhttp "net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FileCacheConfig configures a FileCache.
type FileCacheConfig struct {
	// Root is the directory the files are served from.
	Root string
	// Precompressed serves a file's twin compressed on disk, name.br or
	// name.gz, when the request's Accept-Encoding takes that coding, with
	// Content-Encoding set and the original's Content-Type: brotli where
	// both are accepted as much.
	Precompressed bool
	// MaxFileBytes is the largest file kept in memory; a larger one is
	// served from disk each time. Zero means DefaultMaxCachedFile.
	MaxFileBytes int64
	// MaxBytes bounds what the cache holds altogether, past which further
	// files are served from disk until changes free room. Zero means
	// DefaultMaxCacheBytes.
	MaxBytes int64
}

// The defaults FileCacheConfig's zero values stand for.
const (
	DefaultMaxCachedFile = 1 << 20
	DefaultMaxCacheBytes = 64 << 20
)

// FileCache serves the files under a directory from memory, following the
// disk: a file is read the first time it is asked for, and read again once it
// has changed. On Linux an inotify watch on each directory a cached file is
// in tells the cache of a change, created, written, replaced, moved or removed,
// within moments of it, and a request reads nothing from the disk; elsewhere,
// or where inotify cannot be had, each request checks the file's size,
// modification time and inode, one system call, and rereads it when any
// moved.
//
// A file served from memory leaves on a GET or HEAD without conditions or a
// range, with its Content-Type, Content-Length and Last-Modified, and without
// allocating; a conditional or ranged request goes through
// net/http.ServeContent over the same bytes. The cache is safe for concurrent
// use.
type FileCache struct {
	root          string
	precompressed bool
	maxFile       int64
	maxBytes      int64

	mu      sync.RWMutex
	entries map[fileKey]*cachedFile
	bytes   int64
	// generation moves on with every change the watch reports, so that a
	// file read while one arrived is not cached as if it were current.
	generation atomic.Uint64
	watch      *fileWatch
}

// fileKey names a cached file: a path relative to the root, and which of its
// compressed twins, if any, the entry is of, so that looking a twin up takes
// no string built for it.
type fileKey struct {
	name string
	twin twin
}

// twin is a file's compressed twin on disk, or none.
type twin uint8

const (
	noTwin twin = iota
	brTwin
	gzTwin
)

// suffix is the twin's file name suffix.
func (t twin) suffix() string { return [...]string{"", ".br", ".gz"}[t] }

// cachedFile is one file as last read, or the knowledge that it does not
// exist, which a twin that is not on disk is.
type cachedFile struct {
	missing bool
	// watched says a watch on the file's directory reports its changes, so
	// that the entry stands until one does; otherwise each use checks it.
	watched bool
	// name is the file's path relative to the root, data its contents, or
	// nil for one too large to keep, and header what a response carrying
	// it says of it; see fileHeader.
	name    string
	data    []byte
	header  stdhttp.Header
	modTime time.Time
	stamp   fileStamp
}

// fileStamp is what a stat tells of a file's identity and content.
type fileStamp struct {
	size    int64
	modTime time.Time
	inode   uint64
}

// NewFileCache returns a cache of the files under config.Root.
func NewFileCache(config FileCacheConfig) (*FileCache, error) {
	root, err := filepath.Abs(config.Root)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("http: FileCache root is not a directory")
	}
	fc := &FileCache{root: root, precompressed: config.Precompressed, maxFile: config.MaxFileBytes,
		maxBytes: config.MaxBytes, entries: make(map[fileKey]*cachedFile)}
	if fc.maxFile <= 0 {
		fc.maxFile = DefaultMaxCachedFile
	}
	if fc.maxBytes <= 0 {
		fc.maxBytes = DefaultMaxCacheBytes
	}
	fc.watch = newFileWatch(fc.root, fc.changed)
	return fc, nil
}

// Close stops watching the directories. The cache keeps serving, checking
// each file as it goes.
func (fc *FileCache) Close() error {
	fc.mu.Lock()
	w := fc.watch
	fc.watch = nil
	fc.mu.Unlock()
	if w != nil {
		w.close()
	}
	fc.changed("")
	return nil
}

// changed drops what the cache holds of name, a path relative to the root,
// or of every file for "".
func (fc *FileCache) changed(name string) {
	fc.generation.Add(1)
	fc.mu.Lock()
	if name == "" {
		clear(fc.entries)
		fc.bytes = 0
	} else {
		keys := [...]fileKey{{name, noTwin}, {name, brTwin}, {name, gzTwin}, {}}
		// A twin that changed is its original's twin.
		if original, ok := strings.CutSuffix(name, ".br"); ok {
			keys[3] = fileKey{original, brTwin}
		} else if original, ok := strings.CutSuffix(name, ".gz"); ok {
			keys[3] = fileKey{original, gzTwin}
		}
		for _, key := range keys {
			if e := fc.entries[key]; e != nil {
				fc.bytes -= int64(len(e.data))
				delete(fc.entries, key)
			}
		}
	}
	fc.mu.Unlock()
}

// ServeFile answers r with the file name under the root, a slash-separated
// path that may not leave it, or with 404 when there is no such file.
func (fc *FileCache) ServeFile(c *Context, r *stdhttp.Request, name string) {
	if !cleanName(name) {
		name = strings.TrimPrefix(path.Clean("/"+name), "/")
	}
	if name == "" || strings.HasSuffix(name, ".br") && fc.precompressed || strings.HasSuffix(name, ".gz") && fc.precompressed {
		c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		return
	}
	var file *cachedFile
	if fc.precompressed {
		accept := r.Header["Accept-Encoding"]
		br, gz := acceptQ(accept, "br"), acceptQ(accept, "gzip")
		twins := [2]struct {
			twin twin
			q    float64
		}{{brTwin, br}, {gzTwin, gz}}
		if gz > br {
			twins[0], twins[1] = twins[1], twins[0]
		}
		for _, t := range twins {
			if t.q <= 0 {
				continue
			}
			if f := fc.lookup(fileKey{name, t.twin}); f != nil && !f.missing {
				file = f
				break
			}
		}
	}
	if file == nil {
		file = fc.lookup(fileKey{name, noTwin})
	}
	if file == nil || file.missing {
		c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		return
	}
	if file.data == nil {
		// Too large to keep: served from the disk.
		fc.serveFromDisk(c, r, name, file)
		return
	}
	if (r.Method == stdhttp.MethodGet || r.Method == stdhttp.MethodHead) && !hasConditions(r) {
		_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Header: file.header, Body: file.data})
		return
	}
	h := c.Header()
	for key, values := range file.header {
		h[key] = values
	}
	stdhttp.ServeContent(c, r, name, file.modTime, bytes.NewReader(file.data))
}

// cleanName reports whether name is a relative slash-separated path that
// path.Clean leaves as it is and that stays within the root, as the names
// requests carry nearly always are.
func cleanName(name string) bool {
	if name == "" || name[0] == '/' {
		return false
	}
	for name != "" {
		segment, rest, more := strings.Cut(name, "/")
		if segment == "" || segment == "." || segment == ".." || more && rest == "" {
			return false
		}
		name = rest
	}
	return true
}

// hasConditions reports whether r asks for anything but the whole file as it
// is: a range, or a condition on its validators.
func hasConditions(r *stdhttp.Request) bool {
	for _, key := range [...]string{"Range", "If-Modified-Since", "If-None-Match", "If-Match", "If-Unmodified-Since", "If-Range"} {
		if _, ok := r.Header[key]; ok {
			return true
		}
	}
	return false
}

// serveFromDisk serves a file too large to keep, as net/http.ServeContent
// does, with the headers file carries.
func (fc *FileCache) serveFromDisk(c *Context, r *stdhttp.Request, name string, file *cachedFile) {
	f, err := os.Open(fc.path(file.name))
	if err != nil {
		c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		return
	}
	h := c.Header()
	for key, values := range file.header {
		h[key] = values
	}
	if h.Get("Content-Encoding") != "" && r.Header.Get("Range") == "" {
		// ServeContent leaves Content-Length out once Content-Encoding is
		// set; a whole file knows its length.
		h.Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	}
	stdhttp.ServeContent(c, r, name, info.ModTime(), f)
}

// path is name's path on the disk.
func (fc *FileCache) path(name string) string {
	return filepath.Join(fc.root, filepath.FromSlash(name))
}

// lookup returns what the cache holds of key, reading the file when it holds
// nothing current. It returns nil for a directory.
func (fc *FileCache) lookup(key fileKey) *cachedFile {
	fc.mu.RLock()
	e, watch := fc.entries[key], fc.watch
	fc.mu.RUnlock()
	if e != nil && e.watched && watch != nil {
		return e
	}
	name := key.name + key.twin.suffix()
	full := fc.path(name)
	if e != nil {
		// Unwatched: the entry stands while the file's stamp does.
		stamp, exists, err := statStamp(full)
		if err == nil && exists == !e.missing && (e.missing || stamp == e.stamp) {
			return e
		}
	}
	return fc.load(key, name, full, watch)
}

// load reads name, the file key stands for, and caches it, unless a change arrived while it was being
// read, or it is too large.
func (fc *FileCache) load(key fileKey, name, full string, watch *fileWatch) *cachedFile {
	generation := fc.generation.Load()
	// Watched from before it is read, so that no change between the two goes
	// unseen.
	watched := watch != nil && watch.add(fc.root, path.Dir(name))
	stamp, exists, err := statStamp(full)
	if err != nil {
		return nil
	}
	e := &cachedFile{missing: !exists, watched: watched, name: name, stamp: stamp}
	if exists {
		e.modTime = stamp.modTime
		if stamp.size > fc.maxFile {
			// ServeContent finds the Content-Type of a file served from the
			// disk, from its name or, failing that, from its bytes.
			e.header = fileHeader(name, stamp, nil)
			delete(e.header, "Content-Type")
			return e
		}
		data, err := os.ReadFile(full)
		if err != nil {
			return nil
		}
		// A file that is empty still has its contents, which nil would
		// mistake for a file too large to keep.
		e.data = append(make([]byte, 0, len(data)), data...)
		e.header = fileHeader(name, stamp, e.data)
	}
	fc.mu.Lock()
	if fc.generation.Load() == generation && fc.bytes+int64(len(e.data)) <= fc.maxBytes {
		if old := fc.entries[key]; old != nil {
			fc.bytes -= int64(len(old.data))
		}
		fc.entries[key] = e
		fc.bytes += int64(len(e.data))
	}
	fc.mu.Unlock()
	return e
}

// fileHeader is what a response carrying name, or its twin, says of it: the
// Content-Type of the original's extension, or, as net/http.ServeContent
// does, of the bytes of a file whose extension says none, the
// Content-Encoding of a twin's, and the modification time. The length the
// server works out from the body it is given. The header is shared by every
// response the file goes out in, which only read it.
func fileHeader(name string, stamp fileStamp, data []byte) stdhttp.Header {
	h := make(stdhttp.Header, 4)
	original := name
	switch {
	case strings.HasSuffix(name, ".br"):
		original = strings.TrimSuffix(name, ".br")
		h["Content-Encoding"] = []string{"br"}
		h["Vary"] = []string{"Accept-Encoding"}
	case strings.HasSuffix(name, ".gz"):
		original = strings.TrimSuffix(name, ".gz")
		h["Content-Encoding"] = []string{"gzip"}
		h["Vary"] = []string{"Accept-Encoding"}
	}
	contentType := mime.TypeByExtension(path.Ext(original))
	if contentType == "" {
		contentType = "application/octet-stream"
		if original == name {
			contentType = stdhttp.DetectContentType(data)
		}
	}
	h["Content-Type"] = []string{contentType}
	h["Last-Modified"] = []string{stamp.modTime.UTC().Format(stdhttp.TimeFormat)}
	return h
}

// acceptQ is the q value fields give coding, 0 when they refuse it or name
// neither it nor "*".
func acceptQ(fields []string, coding string) float64 {
	q, wildcard := -1.0, -1.0
	for _, field := range fields {
		for field != "" {
			var member string
			member, field, _ = strings.Cut(field, ",")
			name, params, _ := strings.Cut(member, ";")
			name = strings.TrimSpace(name)
			value := 1.0
			for params != "" {
				var param string
				param, params, _ = strings.Cut(params, ";")
				if key, v, ok := strings.Cut(strings.TrimSpace(param), "="); ok && strings.EqualFold(key, "q") {
					if parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
						value = parsed
					}
				}
			}
			switch {
			case strings.EqualFold(name, coding):
				q = value
			case name == "*":
				wildcard = value
			}
		}
	}
	if q < 0 {
		q = max(wildcard, 0)
	}
	return q
}
