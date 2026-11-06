//go:build linux

package filewatch

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

type recorder struct {
	t      *testing.T
	w      *Watcher
	events chan Event
}

func newRecorder(t *testing.T, root string) *recorder {
	t.Helper()
	r := &recorder{t: t, events: make(chan Event, 1024)}
	w, err := New(root, func(ev Event) { r.events <- ev })
	if err != nil {
		t.Fatal(err)
	}
	r.w = w
	t.Cleanup(func() { _ = w.Close() })
	return r
}

// expect waits for an event equal to want, passing over others.
func (r *recorder) expect(want Event) {
	r.t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev := <-r.events:
			if ev == want {
				return
			}
		case <-timeout:
			r.t.Fatalf("no event %+v", want)
		}
	}
}

// drain forgets the events reported so far, once the watch has been quiet
// for a moment.
func (r *recorder) drain() {
	for {
		select {
		case <-r.events:
		case <-time.After(50 * time.Millisecond):
			return
		}
	}
}

func (r *recorder) watched(rel string) bool {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	_, ok := r.w.dirs[rel]
	return ok
}

func mkdir(t *testing.T, name string) {
	t.Helper()
	if err := os.MkdirAll(name, 0o755); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, name, data string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestWatchReportsFileChanges(t *testing.T) {
	root := t.TempDir()
	mkdir(t, filepath.Join(root, "a"))
	writeFile(t, filepath.Join(root, "a", "f.txt"), "one")
	r := newRecorder(t, root)
	if !r.w.Watch("a") || !r.watched("") || !r.watched("a") {
		t.Fatal("a and the root are not watched")
	}
	writeFile(t, filepath.Join(root, "a", "f.txt"), "two")
	r.expect(Event{Name: "a/f.txt"})
	tmp := filepath.Join(root, "a", "tmp")
	writeFile(t, tmp, "three")
	if err := os.Rename(tmp, filepath.Join(root, "a", "f.txt")); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{Name: "a/f.txt"})
	if err := os.Remove(filepath.Join(root, "a", "f.txt")); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{Name: "a/f.txt"})
	writeFile(t, filepath.Join(root, "top.txt"), "x")
	r.expect(Event{Name: "top.txt"})
}

// A directory on the way to a watched one, renamed, is reported as a Dir
// event, and its watches go with it, so that the directory now at the path
// is watched afresh.
func TestWatchReportsMovedAncestors(t *testing.T) {
	root := t.TempDir()
	mkdir(t, filepath.Join(root, "a", "b"))
	r := newRecorder(t, root)
	if !r.w.Watch("a/b") {
		t.Fatal("a/b is not watched")
	}
	if err := os.Rename(filepath.Join(root, "a"), filepath.Join(root, "x")); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{Name: "a", Dir: true})
	if r.watched("a") || r.watched("a/b") {
		t.Fatal("a moved away and is still watched by its old path")
	}
	mkdir(t, filepath.Join(root, "a", "b"))
	r.drain()
	if !r.w.Watch("a/b") {
		t.Fatal("the new a/b is not watched")
	}
	writeFile(t, filepath.Join(root, "a", "b", "f"), "x")
	r.expect(Event{Name: "a/b/f"})
	// The old a/b, now x/b, reports nothing.
	writeFile(t, filepath.Join(root, "x", "b", "g"), "x")
	writeFile(t, filepath.Join(root, "a", "b", "h"), "x")
	for ev := range r.events {
		if ev.Name == "a/b/g" {
			t.Fatal("the moved directory still reports as a/b")
		}
		if ev.Name == "a/b/h" {
			break
		}
	}
}

// A directory that does not exist yet is as good as watched: its coming is
// reported by the one above it.
func TestWatchMissingDirectory(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "file"), "x")
	r := newRecorder(t, root)
	if !r.w.Watch("no/such") || !r.w.Watch("file/sub") {
		t.Fatal("a missing directory is not watched")
	}
	mkdir(t, filepath.Join(root, "no"))
	r.expect(Event{Name: "no", Dir: true})
}

// A symbolic link on the way to the root swapped for another, as a deploy
// does to a "current" link, is reported as All, and the root watched afresh.
func TestWatchReportsRootLinkSwap(t *testing.T) {
	base := t.TempDir()
	for _, release := range []string{"r1", "r2"} {
		mkdir(t, filepath.Join(base, release))
	}
	current := filepath.Join(base, "current")
	if err := os.Symlink("r1", current); err != nil {
		t.Fatal(err)
	}
	r := newRecorder(t, current)
	if !r.w.Watch("") {
		t.Fatal("the root is not watched")
	}
	tmp := filepath.Join(base, "next")
	if err := os.Symlink("r2", tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, current); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{All: true})
	r.drain()
	if !r.w.Watch("") {
		t.Fatal("the new root is not watched")
	}
	writeFile(t, filepath.Join(base, "r2", "f"), "x")
	r.expect(Event{Name: "f"})
	// And the next swap is seen too.
	if err := os.Symlink("r1", tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, current); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{All: true})
}

// A symbolic link to a directory under the root, swapped, is reported as a
// Dir event, though the link is no directory itself.
func TestWatchReportsDirectoryLinkSwap(t *testing.T) {
	root := t.TempDir()
	mkdir(t, filepath.Join(root, "v1"))
	mkdir(t, filepath.Join(root, "v2"))
	if err := os.Symlink("v1", filepath.Join(root, "assets")); err != nil {
		t.Fatal(err)
	}
	r := newRecorder(t, root)
	if !r.w.Watch("assets") {
		t.Fatal("assets is not watched")
	}
	tmp := filepath.Join(root, "next")
	if err := os.Symlink("v2", tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(root, "assets")); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{Name: "assets", Dir: true})
	if r.watched("assets") {
		t.Fatal("the old assets is still watched")
	}
}

func TestWatchReportsRootRemoved(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	mkdir(t, root)
	r := newRecorder(t, root)
	r.w.Watch("")
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{All: true})
}

func TestWatchClose(t *testing.T) {
	root := t.TempDir()
	r := newRecorder(t, root)
	r.w.Watch("")
	if err := r.w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.w.Close(); err != nil {
		t.Fatal(err)
	}
	if r.w.Watch("") {
		t.Fatal("a closed Watcher watches")
	}
	writeFile(t, filepath.Join(root, "f"), "x")
	select {
	case ev := <-r.events:
		t.Fatalf("a closed Watcher reported %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestNewMissingRoot(t *testing.T) {
	if _, err := New(filepath.Join(t.TempDir(), "nope", "x"), func(Event) {}); err == nil {
		t.Fatal("a root that does not exist was watched")
	}
}
