//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/lesismal/fib/filewatch"
)

// replaceKeepingStamp puts data in name's place, with the size and
// modification time of the file it replaces, as rsync -t or cp -p leave a
// new release's files.
func replaceKeepingStamp(t *testing.T, dir, name, data string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Windows has no inode or status change time to tell such a file by")
	}
	full := filepath.Join(dir, name)
	info, err := os.Stat(full)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != info.Size() {
		t.Fatalf("%q is not as long as %s", data, name)
	}
	tmp := filepath.Join(dir, ".tmp")
	if err := os.WriteFile(tmp, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(tmp, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, full); err != nil {
		t.Fatal(err)
	}
}

func cachedBody(t *testing.T, fc *FileCache, name string) string {
	t.Helper()
	f := fc.lookup(fileKey{name, noTwin})
	if f == nil || f.missing {
		t.Fatalf("%s: %+v", name, f)
	}
	return string(f.data)
}

// With no watch, a file is trusted for CheckInterval, and stat'ed once it
// has passed: a file replaced by one of the same size and modification time
// is told apart by its stamp and read anew.
func TestFileCacheCheckInterval(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("AAAA"), 0o644); err != nil {
		t.Fatal(err)
	}
	const interval = 200 * time.Millisecond
	fc, err := NewFileCache(FileCacheConfig{Root: dir, CheckInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	_ = fc.Close() // no watch: only the interval tells
	if fc.check != interval {
		t.Fatalf("check %v", fc.check)
	}
	if got := cachedBody(t, fc, "a.txt"); got != "AAAA" {
		t.Fatalf("got %q", got)
	}
	start := time.Now()
	replaceKeepingStamp(t, dir, "a.txt", "BBBB")
	if got := cachedBody(t, fc, "a.txt"); got != "AAAA" && time.Since(start) < interval/2 {
		t.Fatalf("within the interval: got %q", got)
	}
	time.Sleep(interval)
	if got := cachedBody(t, fc, "a.txt"); got != "BBBB" {
		t.Fatalf("past the interval, the replaced file is still %q", got)
	}
}

// A negative CheckInterval checks the file on every request, with no watch:
// the next request after a replacement, of the same size and time, has the
// new file.
func TestFileCacheCheckEveryRequest(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("AAAA"), 0o644); err != nil {
		t.Fatal(err)
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, CheckInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	if fc.check != 0 || fc.watch != nil {
		t.Fatalf("check %v, watch %v", fc.check, fc.watch)
	}
	if got := cachedBody(t, fc, "a.txt"); got != "AAAA" {
		t.Fatalf("got %q", got)
	}
	replaceKeepingStamp(t, dir, "a.txt", "BBBB")
	if got := cachedBody(t, fc, "a.txt"); got != "BBBB" {
		t.Fatalf("the next request after a replacement got %q", got)
	}
	if fc2, _ := NewFileCache(FileCacheConfig{Root: dir}); fc2.check != DefaultCheckInterval {
		t.Fatalf("default check %v", fc2.check)
	} else {
		_ = fc2.Close()
	}
}

// A watch reports a replacement at once, though the file keeps its size and
// time and CheckInterval is long.
func TestFileCacheWatchSeesSameStampReplacement(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("AAAA"), 0o644); err != nil {
		t.Fatal(err)
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, CheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	if fc.watch == nil {
		t.Skip("no watch here")
	}
	cachedBody(t, fc, "a.txt")
	replaceKeepingStamp(t, dir, "a.txt", "BBBB")
	waitUntil(t, func() bool { return cachedBody(t, fc, "a.txt") == "BBBB" })
}

// A directory renamed away, and another put in its place, is reported for
// its files too, which the cache then reads from the new directory.
func TestFileCacheWatchSeesReplacedDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, sub := range []string{"assets", "next"} {
		if err := os.MkdirAll(filepath.Join(dir, sub, "css"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, sub, "css", "a.css"), []byte(sub), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, CheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	if fc.watch == nil {
		t.Skip("no watch here")
	}
	if got := cachedBody(t, fc, "assets/css/a.css"); got != "assets" {
		t.Fatalf("got %q", got)
	}
	if err := os.Rename(filepath.Join(dir, "assets"), filepath.Join(dir, "old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "next"), filepath.Join(dir, "assets")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return cachedBody(t, fc, "assets/css/a.css") == "next" })
}

// A root reached through a "current" link, pointed at another release, is
// served from the new release.
func TestFileCacheWatchSeesRootLinkSwap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symbolic links need privileges on Windows")
	}
	base := t.TempDir()
	for _, release := range []string{"r1", "r2"} {
		if err := os.Mkdir(filepath.Join(base, release), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(base, release, "a.txt"), []byte(release), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current := filepath.Join(base, "current")
	if err := os.Symlink("r1", current); err != nil {
		t.Fatal(err)
	}
	fc, err := NewFileCache(FileCacheConfig{Root: current, CheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	if fc.watch == nil {
		t.Skip("no watch here")
	}
	if got := cachedBody(t, fc, "a.txt"); got != "r1" {
		t.Fatalf("got %q", got)
	}
	tmp := filepath.Join(base, "tmp")
	if err := os.Symlink("r2", tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, current); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return cachedBody(t, fc, "a.txt") == "r2" })
}

// event drops what each kind of event names: a file, a directory and what is
// under it, or everything.
func TestFileCacheEvents(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "d/b.txt", "d/e/c.txt", "dx.txt"} {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, CheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	_ = fc.Close()
	names := []string{"a.txt", "d/b.txt", "d/e/c.txt", "dx.txt"}
	load := func() {
		for _, name := range names {
			cachedBody(t, fc, name)
		}
	}
	cached := func() map[string]bool {
		fc.mu.RLock()
		defer fc.mu.RUnlock()
		m := map[string]bool{}
		for key := range fc.entries {
			m[key.name] = true
		}
		return m
	}
	load()
	fc.event(filewatch.Event{Name: "d", Dir: true})
	if got := cached(); got["d/b.txt"] || got["d/e/c.txt"] || !got["a.txt"] || !got["dx.txt"] {
		t.Fatalf("after d changed: %v", got)
	}
	fc.event(filewatch.Event{Name: "a.txt"})
	if got := cached(); got["a.txt"] || !got["dx.txt"] {
		t.Fatalf("after a.txt changed: %v", got)
	}
	load()
	fc.event(filewatch.Event{All: true})
	if got := cached(); len(got) != 0 || fc.bytes != 0 {
		t.Fatalf("after everything changed: %v, %d bytes", got, fc.bytes)
	}
}

func waitUntil(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
