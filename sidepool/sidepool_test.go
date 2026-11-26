package sidepool

import (
	"slices"
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

// Configure sizes a pool not built yet, Current and Custom report it, and a
// pool already built keeps its size and is reported as kept.
func TestConfigure(t *testing.T) {
	t.Cleanup(func() {
		configMu.Lock()
		configured = Sizing{}
		configMu.Unlock()
	})
	def := DefaultSizing()
	if def.Handshake.Workers <= 0 || def.Notify.Queue != notifyQueue {
		t.Fatalf("DefaultSizing() = %+v", def)
	}
	Notify() // built now, with the default
	if file.built != (PoolSize{}) {
		t.Skip("fib-file was built by an earlier test")
	}
	kept := Configure(Sizing{File: PoolSize{Workers: 7}, Notify: PoolSize{Workers: 3}})
	if !slices.Equal(kept, []string{"fib-notify"}) {
		t.Fatalf("kept %v, want [fib-notify]", kept)
	}
	cur := Current()
	if cur.File != (PoolSize{Workers: 7, Queue: queueSize}) {
		t.Fatalf("Current().File = %+v, want 7 workers and the default queue", cur.File)
	}
	if cur.Notify != def.Notify {
		t.Fatalf("Current().Notify = %+v, want the size it was built with, %+v", cur.Notify, def.Notify)
	}
	if got := Custom(); !slices.Equal(got, []string{"fib-file"}) {
		t.Fatalf("Custom() = %v, want [fib-file]", got)
	}
}
