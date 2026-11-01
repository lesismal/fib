package sidepool

import (
	"testing"
	"time"
)

// Each pool is built once, on first use, and stays the same pool after; Go
// runs its function on the client pool.
func TestPools(t *testing.T) {
	for name, get := range map[string]func() any{
		"Client":    func() any { return Client() },
		"Handshake": func() any { return Handshake() },
		"File":      func() any { return File() },
	} {
		if p := get(); p == nil || p != get() {
			t.Errorf("%s() = %v, then %v", name, p, get())
		}
	}
	if Client() == Handshake() || Client() == File() || Handshake() == File() {
		t.Error("two pools are the same")
	}
	ran := make(chan struct{})
	Go(func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("Go did not run its function")
	}
}
