//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"fmt"
	"testing"
)

// What the cache remembers of names with no file is bounded: requests for
// ever more names that are not there forget the older ones rather than
// growing the cache without end, as they would where a watch keeps every
// entry until its directory changes.
func TestFileCacheMissingEntriesBounded(t *testing.T) {
	fc, _, _ := newTestFileCache(t, true)
	for i := range maxMissingEntries + 500 {
		if f := fc.lookup(fileKey{fmt.Sprintf("nope-%d.txt", i), noTwin}); f == nil || !f.missing {
			t.Fatalf("lookup %d: %+v", i, f)
		}
	}
	if f := fc.lookup(fileKey{"plain.txt", noTwin}); f == nil || f.missing {
		t.Fatal("plain.txt is missing")
	}
	count := func() (missing, all int) {
		fc.mu.RLock()
		defer fc.mu.RUnlock()
		for _, e := range fc.entries {
			if e.missing {
				missing++
			}
		}
		if missing != fc.missing {
			t.Fatalf("%d missing entries, counted %d", missing, fc.missing)
		}
		return missing, len(fc.entries)
	}
	if missing, all := count(); missing != maxMissingEntries || all != maxMissingEntries+1 {
		t.Fatalf("%d missing entries of %d, want %d of %d", missing, all, maxMissingEntries, maxMissingEntries+1)
	}
	// A change forgets what it names, and its count with it.
	fc.mu.RLock()
	var name string
	for key, e := range fc.entries {
		if e.missing {
			name = key.name
			break
		}
	}
	fc.mu.RUnlock()
	fc.changed(name)
	if missing, _ := count(); missing != maxMissingEntries-1 {
		t.Fatalf("%d missing entries after a change, want %d", missing, maxMissingEntries-1)
	}
	fc.changed("")
	if missing, all := count(); missing != 0 || all != 0 {
		t.Fatalf("%d missing entries of %d after everything changed", missing, all)
	}
}
