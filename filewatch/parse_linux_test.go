//go:build linux

package filewatch

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

// rawEvent is an inotify event as the kernel writes it, its name padded.
func rawEvent(wd int32, mask uint32, name string) []byte {
	n := 0
	if name != "" {
		n = (len(name) + 1 + 15) &^ 15
	}
	buf := make([]byte, syscall.SizeofInotifyEvent+n)
	ev := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[0]))
	ev.Wd, ev.Mask, ev.Len = wd, mask, uint32(n)
	copy(buf[syscall.SizeofInotifyEvent:], name)
	return buf
}

// parse turns what the kernel could write into events: an overflow into
// All, once however many there are; an event of a watch already dropped
// into nothing; a truncated event into nothing.
func TestParse(t *testing.T) {
	root := t.TempDir()
	r := newRecorder(t, root)
	r.w.Watch("")
	wd := r.w.dirs[""]
	var buf []byte
	buf = append(buf, rawEvent(-1, syscall.IN_Q_OVERFLOW, "")...)
	buf = append(buf, rawEvent(-1, syscall.IN_Q_OVERFLOW, "")...)
	buf = append(buf, rawEvent(wd+100, syscall.IN_MODIFY, "x")...)
	buf = append(buf, rawEvent(wd, syscall.IN_ATTRIB, "")...)
	buf = append(buf, rawEvent(wd, syscall.IN_MODIFY, "f")...)
	buf = append(buf, rawEvent(wd, syscall.IN_MODIFY, "truncated")[:syscall.SizeofInotifyEvent+2]...)
	got := r.w.parse(buf, nil)
	want := []Event{{All: true}, {Name: "f"}}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
	// The kernel dropping the root's watch drops it here too.
	got = r.w.parse(rawEvent(wd, syscall.IN_IGNORED, ""), nil)
	if len(got) != 1 || !got[0].All || r.watched("") {
		t.Fatalf("after the root's watch went: %+v", got)
	}
}

// Links on the way to the root are followed as the system follows them:
// relative, absolute, through "..", and refused past the bound.
func TestWatchLinks(t *testing.T) {
	base := t.TempDir()
	mkdir(t, filepath.Join(base, "real", "site"))
	if err := os.Symlink(filepath.Join(base, "real"), filepath.Join(base, "abs")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../real/./site", filepath.Join(base, "real", "rel")); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "abs", "rel")
	r := newRecorder(t, root)
	links := 0
	for _, n := range r.w.nodes {
		links += len(n.links)
	}
	if links != 2 {
		t.Fatalf("%d links watched, want 2", links)
	}
	if !r.w.Watch("") {
		t.Fatal("the root is not watched")
	}
	writeFile(t, filepath.Join(base, "real", "site", "f"), "x")
	r.expect(Event{Name: "f"})
	// A link that leads to itself is refused.
	loop := filepath.Join(base, "loop")
	if err := os.Symlink("loop", loop); err != nil {
		t.Fatal(err)
	}
	if _, err := New(filepath.Join(loop, "x"), func(Event) {}); err == nil {
		t.Fatal("a link loop was watched")
	}
}

// The directory holding a link on the way to the root going is reported as
// All, and the links watched afresh.
func TestWatchLinkDirectoryGone(t *testing.T) {
	base := t.TempDir()
	mkdir(t, filepath.Join(base, "site"))
	mkdir(t, filepath.Join(base, "links"))
	if err := os.Symlink(filepath.Join(base, "site"), filepath.Join(base, "links", "current")); err != nil {
		t.Fatal(err)
	}
	r := newRecorder(t, filepath.Join(base, "links", "current"))
	if err := os.Rename(filepath.Join(base, "links"), filepath.Join(base, "moved")); err != nil {
		t.Fatal(err)
	}
	r.expect(Event{All: true})
}
