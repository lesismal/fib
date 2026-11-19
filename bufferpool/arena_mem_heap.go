//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || windows) || race

package bufferpool

// arenaMemory makes the arena on the Go heap. Under the race detector it
// always does: memory outside the heap is not instrumented, so races on
// arena buffers would go unseen.
func arenaMemory(n int) ([]byte, bool) {
	return make([]byte, n), false
}

// arenaRelease has nothing to do: the collector frees the heap.
func arenaRelease([]byte) {}
