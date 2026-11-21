//go:build linux

package filewatch

import (
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkWatchWatched is Watch of a directory watched already, from
// every CPU at once, as a cache asks for the directory of each path it
// looks up.
func BenchmarkWatchWatched(b *testing.B) {
	root := b.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o755); err != nil {
		b.Fatal(err)
	}
	w, err := New(root, func(Event) {})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	if !w.Watch("a/b") {
		b.Fatal("not watched")
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !w.Watch("a/b") {
				b.Fatal("not watched")
			}
		}
	})
}
