//go:build race

package http

// raceEnabled says the race detector is on, under which sync.Pool drops some
// of what is put in it, so that counts of allocations say nothing.
const raceEnabled = true
