//go:build fib_arena

package bufferpool

// Built with -tags fib_arena, every program and test starts with an Arena
// installed, so that the whole of fib can be tested on it: go test -tags
// fib_arena ./... The arena is small, so that classes run out of slots and
// fall back to the pool on the way.
func init() {
	a, err := NewArenaBytes(64<<20, nil)
	if err != nil {
		panic(err)
	}
	SetAllocator(a)
}
