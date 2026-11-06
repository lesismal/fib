package filewatch

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func stampOf(t *testing.T, name string) Stamp {
	t.Helper()
	_, s, err := Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A file replaced by another of the same size and modification time, or
// written over in place and given its old modification time back, has
// another stamp; one left alone keeps its stamp.
func TestStampTellsReplacedFiles(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "a.txt")
	mtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(name, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(name, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	write(full, "AAAA")
	first := stampOf(t, full)
	if again := stampOf(t, full); again != first {
		t.Fatalf("an unchanged file's stamp moved: %+v, then %+v", first, again)
	}
	if first.Size != 4 || first.ModTime != mtime.UnixNano() {
		t.Fatalf("stamp %+v", first)
	}

	// Status change times move on with the kernel's coarse clock.
	time.Sleep(50 * time.Millisecond)
	tmp := filepath.Join(dir, "tmp")
	write(tmp, "BBBB")
	if err := os.Rename(tmp, full); err != nil {
		t.Fatal(err)
	}
	replaced := stampOf(t, full)
	if runtime.GOOS == "windows" {
		// Windows has neither an inode nor a status change time, and NTFS
		// tunneling may give a file renamed into another's place that
		// file's creation time: only size and times tell there.
		return
	}
	if replaced == first {
		t.Fatalf("a file replaced by one of the same size and time kept its stamp %+v", first)
	}
	if replaced.Inode == first.Inode {
		t.Errorf("a replaced file kept its inode %d", first.Inode)
	}
	time.Sleep(50 * time.Millisecond)
	write(full, "CCCC")
	if rewritten := stampOf(t, full); rewritten == replaced {
		t.Fatalf("a file written in place, its time set back, kept its stamp %+v", replaced)
	} else if rewritten.Inode != replaced.Inode || rewritten.Change == replaced.Change {
		t.Errorf("written in place: inode %d to %d, change %d to %d", replaced.Inode, rewritten.Inode, replaced.Change, rewritten.Change)
	}
}

func TestStatMissing(t *testing.T) {
	if _, _, err := Stat(filepath.Join(t.TempDir(), "nope")); !os.IsNotExist(err) {
		t.Fatalf("err %v", err)
	}
}

// A nil Watcher watches nothing, and closes.
func TestNilWatcher(t *testing.T) {
	var w *Watcher
	if w.Watch("") {
		t.Fatal("a nil Watcher watches")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
