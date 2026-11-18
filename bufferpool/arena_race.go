//go:build race

package bufferpool

// raceEnabled sends the arena past its per-P stacks: the race detector cannot
// see that pinning a P is what makes them its own, and the shared stacks'
// locks are what it can see hand a slot over.
const raceEnabled = true
