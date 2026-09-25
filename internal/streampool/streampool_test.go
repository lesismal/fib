package streampool

import (
	"testing"
	"time"
)

// The ceiling is the widest any engine of the name asks for, the fallback
// when none does, and moves the built pool with it.
func TestCeilingFollowsTheWidestEngine(t *testing.T) {
	const engine = "ceiling"
	pool := Get(engine, 100, 64)
	if Get(engine, 1, 1) != pool {
		t.Fatal("a second Get built another pool")
	}
	if got := pool.Name(); got != "ceiling-streams" {
		t.Fatalf("Name() = %q, want the engine's name with -streams", got)
	}
	if got := Ceiling(engine); got != 100 {
		t.Fatalf("Ceiling() = %d with no engine, want the fallback 100", got)
	}
	releaseNarrow := Require(engine, 40)
	defer releaseNarrow()
	releaseWide := Require(engine, 400)
	releaseOther := Require("ceiling-other", 4000)
	defer releaseOther()
	if got := Ceiling(engine); got != 400 {
		t.Fatalf("Ceiling() = %d, want the widest engine's 400", got)
	}
	releaseWide()
	releaseWide()
	if got := Ceiling(engine); got != 40 {
		t.Fatalf("Ceiling() = %d once the wide engine left, want 40", got)
	}
	if shared.pools[engine].ceiling != 40 {
		t.Fatalf("the built pool's ceiling = %d, want 40", shared.pools[engine].ceiling)
	}
}

// Each engine name has a pool of its own, and the last engine of a name to
// go stops that name's pool, while a Get after it builds a new one.
func TestPoolPerEngineName(t *testing.T) {
	release := Require("per-name-a", 8)
	a := Get("per-name-a", 4, 16)
	b := Get("per-name-b", 4, 16)
	if a == b {
		t.Fatal("two engine names were given one pool")
	}
	release()
	// Stopping does not wait for the pool, so it refuses work a moment later.
	deadline := time.Now().Add(5 * time.Second)
	for a.Go(func() {}) {
		if time.Now().After(deadline) {
			t.Fatal("the pool of a name whose last engine left kept taking work")
		}
		time.Sleep(time.Millisecond)
	}
	if Get("per-name-a", 4, 16) == a {
		t.Fatal("the pool of a name whose last engine left was handed out again")
	}
}
