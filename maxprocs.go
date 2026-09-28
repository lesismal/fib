package fib

import (
	"os"
	"runtime"
	"sync"
)

// GOMAXPROCS follows the event loops the process runs. An engine's loops and
// its workers share the process's Ps, and a loop that is woken for events
// takes one from the workers until it waits again, so fib raises GOMAXPROCS
// by one for every loop its engines run, up to twice the CPUs: an engine with
// one poller on 8 CPUs, its own loop and the poller's, runs with 10. The
// value is set as an engine starts and again as one closes, and goes back to
// the CPU count once none is left.
//
// Twice the CPUs is also what the workers want: they make their connections'
// syscalls themselves and hold their P meanwhile, and a 100k-connection echo
// went from 330k to 415k echoes/s, and from 55k to 71k accepted connections/s,
// on GOMAXPROCS at twice the cores rather than at the cores.
var maxProcs = struct {
	sync.Mutex
	// loops is how many event loops the running engines have.
	loops int
	// manual says SetAutoMaxProcs(false) has taken GOMAXPROCS out of fib's
	// hands.
	manual bool
}{}

// maxProcsFromEnv says the GOMAXPROCS environment variable set GOMAXPROCS,
// which fib then leaves as it is.
var _, maxProcsFromEnv = os.LookupEnv("GOMAXPROCS")

// SetAutoMaxProcs says whether fib sets GOMAXPROCS from the event loops its
// engines run, which it does by default, unless the GOMAXPROCS environment
// variable is set: runtime.NumCPU plus one for every loop, and at most twice
// runtime.NumCPU.
//
// SetAutoMaxProcs(false) stops it, and sets GOMAXPROCS to twice
// runtime.NumCPU once, which serves fib's workers well whatever loops run; a
// program that wants a value of its own sets it after that call.
// SetAutoMaxProcs(true) starts it again, and sets GOMAXPROCS for the engines
// running at once.
func SetAutoMaxProcs(enabled bool) {
	maxProcs.Lock()
	defer maxProcs.Unlock()
	maxProcs.manual = !enabled
	if !enabled {
		runtime.GOMAXPROCS(2 * runtime.NumCPU())
		return
	}
	tuneMaxProcsLocked()
}

// AutoMaxProcs reports whether fib sets GOMAXPROCS; see SetAutoMaxProcs.
func AutoMaxProcs() bool {
	maxProcs.Lock()
	defer maxProcs.Unlock()
	return !maxProcs.manual && !maxProcsFromEnv
}

// addLoops counts loops an engine started, or with a negative n closed, and
// sets GOMAXPROCS for the new total.
func addLoops(n int) {
	maxProcs.Lock()
	defer maxProcs.Unlock()
	maxProcs.loops += n
	tuneMaxProcsLocked()
}

// tuneMaxProcsLocked sets GOMAXPROCS for the loops running, unless it is not
// fib's to set. Callers hold maxProcs.
func tuneMaxProcsLocked() {
	if maxProcs.manual || maxProcsFromEnv {
		return
	}
	if want := autoMaxProcs(runtime.NumCPU(), maxProcs.loops); runtime.GOMAXPROCS(0) != want {
		runtime.GOMAXPROCS(want)
	}
}

// autoMaxProcs is the GOMAXPROCS for loops event loops on cpus CPUs.
func autoMaxProcs(cpus, loops int) int {
	return cpus + min(max(loops, 0), cpus)
}
