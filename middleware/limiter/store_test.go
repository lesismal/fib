//go:build linux || darwin || windows

package limiter

import "testing"

// The store is given the time, so what a window counts can be checked exactly
// rather than by waiting for one to pass. The units are the store's own: a
// window of 1000 and times within it.
const testWindow = 1000

func takeAt(t *testing.T, s *store, now int64, limit int) bool {
	t.Helper()
	_, allowed, _, _ := s.take("k", now, limit)
	return allowed
}

func TestStoreFixedWindow(t *testing.T) {
	s := newStore(testWindow, false)
	for i := 0; i < 4; i++ {
		if !takeAt(t, s, 1000+int64(i), 4) {
			t.Fatalf("request %d filling the window was refused", i)
		}
	}
	if takeAt(t, s, 1500, 4) {
		t.Fatal("a fifth request in the window was allowed")
	}
	// A fixed window forgets the one before it the moment it starts.
	if !takeAt(t, s, 2000, 4) {
		t.Fatal("the first request of the next window was refused")
	}
}

func TestStoreSlidingWindow(t *testing.T) {
	// Four requests at the start of the window that runs from 1000 to 2000.
	s := newStore(testWindow, true)
	for i := 0; i < 4; i++ {
		if !takeAt(t, s, 1000+int64(i), 4) {
			t.Fatalf("request %d filling the window was refused", i)
		}
	}
	// In the next window the four still count in proportion to how much of
	// the last window the window ending now covers: at 1000-offset they count
	// whole, and they have faded enough to let a request through only once
	// three of the four are behind the trailing edge.
	for _, tt := range []struct {
		now     int64
		allowed bool
	}{
		{2000, false}, // 1 + 4.0 = 5.0
		{2200, false}, // 1 + 3.2 = 4.2
		{2249, false}, // 1 + 3.004 = 4.004
		{2250, true},  // 1 + 3.0 = 4.0
		{2500, true},  // 1 + 2.0 = 3.0
	} {
		s := newStore(testWindow, true)
		for i := 0; i < 4; i++ {
			takeAt(t, s, 1000+int64(i), 4)
		}
		if allowed := takeAt(t, s, tt.now, 4); allowed != tt.allowed {
			t.Errorf("at %d into the next window: allowed %v, want %v", tt.now-2000, allowed, tt.allowed)
		}
	}
	// Two windows on, the one that was filled counts for nothing.
	if !takeAt(t, s, 3000, 4) {
		t.Error("a request two windows on was refused")
	}
}

// A request past the bound is not counted, so that a client that keeps
// knocking is not kept out for longer than the window it filled.
func TestStoreRefusedRequestsAreNotCounted(t *testing.T) {
	s := newStore(testWindow, true)
	for i := 0; i < 2; i++ {
		if !takeAt(t, s, 1000+int64(i), 2) {
			t.Fatalf("request %d filling the window was refused", i)
		}
	}
	// Each refusal reports the count it would have made, and leaves the one
	// the key has where it was.
	for i := 0; i < 5; i++ {
		used, allowed, _, _ := s.take("k", 1100+int64(i), 2)
		if allowed || used != 3 {
			t.Fatalf("request %d past the bound: used %d, allowed %v", i, used, allowed)
		}
	}
	// Halfway through the next window the two count for one between them,
	// which leaves room; the seven they would have come to do not.
	if !takeAt(t, s, 2500, 2) {
		t.Error("a request halfway through the next window was refused")
	}
}

func TestStoreGiveBack(t *testing.T) {
	s := newStore(testWindow, false)
	_, _, window, _ := s.take("k", 1000, 1)
	if takeAt(t, s, 1100, 1) {
		t.Fatal("a second request in the window was allowed")
	}
	s.give("k", window)
	if !takeAt(t, s, 1200, 1) {
		t.Error("the request given back did not free a place")
	}
	// A refund for a window that has passed leaves the one running alone.
	s.take("k", 2000, 1)
	s.give("k", window)
	if takeAt(t, s, 2100, 1) {
		t.Error("a refund for a window that had passed freed a place in this one")
	}
}
