//go:build race

package rtp

// raceEnabled says the race detector is on, whose instrumentation
// allocates where a plain build does not.
const raceEnabled = true
