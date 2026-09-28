//go:build linux || darwin || windows

package fib

import (
	"runtime"
	"testing"
)

// GOMAXPROCS gets a P for every loop on top of the CPUs, and at most twice
// the CPUs.
func TestAutoMaxProcsValue(t *testing.T) {
	for _, c := range []struct{ cpus, loops, want int }{
		{3, 0, 3}, {3, 2, 5}, {3, 3, 6}, {3, 9, 6}, {8, 2, 10}, {64, 9, 73},
	} {
		if got := autoMaxProcs(c.cpus, c.loops); got != c.want {
			t.Errorf("%d CPUs, %d loops: GOMAXPROCS %d, want %d", c.cpus, c.loops, got, c.want)
		}
	}
}

// An engine counts its loops toward GOMAXPROCS as it starts and takes them
// back as it closes, and SetAutoMaxProcs(false) leaves GOMAXPROCS at twice
// the CPUs until it is turned on again.
func TestEnginesSetMaxProcs(t *testing.T) {
	if maxProcsFromEnv {
		t.Skip("GOMAXPROCS is set in the environment, which fib leaves alone")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	cpus := runtime.NumCPU()
	maxProcs.Lock()
	before := maxProcs.loops
	maxProcs.Unlock()

	config := DefaultConfig()
	config.Name = "maxprocs"
	config.IOPollerCount = 2
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := autoMaxProcs(cpus, before+server.loops); runtime.GOMAXPROCS(0) != want {
		t.Fatalf("GOMAXPROCS %d with the engine's %d loops, want %d", runtime.GOMAXPROCS(0), server.loops, want)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if want := autoMaxProcs(cpus, before); runtime.GOMAXPROCS(0) != want {
		t.Fatalf("GOMAXPROCS %d once the engine closed, want %d", runtime.GOMAXPROCS(0), want)
	}

	SetAutoMaxProcs(false)
	defer SetAutoMaxProcs(true)
	if AutoMaxProcs() || runtime.GOMAXPROCS(0) != 2*cpus {
		t.Fatalf("SetAutoMaxProcs(false): auto %v, GOMAXPROCS %d, want false, %d", AutoMaxProcs(), runtime.GOMAXPROCS(0), 2*cpus)
	}
	runtime.GOMAXPROCS(cpus)
	server, err = Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOMAXPROCS(0) != cpus {
		t.Fatalf("an engine set GOMAXPROCS to %d while fib was not to, want %d", runtime.GOMAXPROCS(0), cpus)
	}
	_ = server.Close()
	SetAutoMaxProcs(true)
	if want := autoMaxProcs(cpus, before); !AutoMaxProcs() || runtime.GOMAXPROCS(0) != want {
		t.Fatalf("SetAutoMaxProcs(true): auto %v, GOMAXPROCS %d, want true, %d", AutoMaxProcs(), runtime.GOMAXPROCS(0), want)
	}
}
