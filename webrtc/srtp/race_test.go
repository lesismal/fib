//go:build race

package srtp

// raceEnabled says the race detector is on, whose instrumentation
// allocates where a plain build does not.
const raceEnabled = true
