package streampool

import "testing"

// The ceiling is the widest any engine asks for, the fallback when none
// does, and moves the built pool with it.
func TestCeilingFollowsTheWidestEngine(t *testing.T) {
	pool := Get(100, 64)
	if Get(1, 1) != pool {
		t.Fatal("a second Get built another pool")
	}
	if got := Ceiling(); got != 100 {
		t.Fatalf("Ceiling() = %d with no engine, want the fallback 100", got)
	}
	releaseNarrow := Require(40)
	releaseWide := Require(400)
	if got := Ceiling(); got != 400 {
		t.Fatalf("Ceiling() = %d, want the widest engine's 400", got)
	}
	releaseWide()
	releaseWide()
	if got := Ceiling(); got != 40 {
		t.Fatalf("Ceiling() = %d once the wide engine left, want 40", got)
	}
	releaseNarrow()
	if got := Ceiling(); got != 100 {
		t.Fatalf("Ceiling() = %d once every engine left, want the fallback 100", got)
	}
	if shared.ceiling != 100 {
		t.Fatalf("the built pool's ceiling = %d, want 100", shared.ceiling)
	}
}
