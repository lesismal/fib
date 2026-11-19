package bufferpool

import (
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func quiesced(a *Arena) bool { return a.state.Load() >= arenaQuiesced }

// An arena SetAllocator replaces serves nothing more, takes its slots back
// whatever is installed, and releases its memory once the last one is back.
func TestReplacedArenaReleasesOnceItsSlotsAreBack(t *testing.T) {
	a := newArena(t, map[int]int{1024: 64, 4096: 8})
	SetAllocator(a)
	t.Cleanup(func() { SetAllocator(nil) })
	var out [][]byte
	for range 40 {
		out = append(out, Get(1000))
	}
	out = append(out, Get(4000), Get(4000))
	for _, b := range out {
		if !a.holds(b) {
			t.Fatal("a buffer did not come from the installed arena")
		}
	}
	// Some come back before the switch, into the per-P stacks and depots.
	for _, b := range out[:10] {
		Put(b)
	}
	out = out[10:]

	SetAllocator(nil)
	if b := a.Get(1000); a.holds(b) {
		t.Fatal("a retired arena served a slot")
	}
	if b := Get(1000); a.holds(b) {
		t.Fatal("Get after the switch came from the retired arena")
	}
	waitFor(t, "the retired arena to count its slots", func() bool { return quiesced(a) })
	if a.Released() || a.outstanding.Load() != int64(len(out)) {
		t.Fatalf("released %v with %d out, %d held", a.Released(), a.outstanding.Load(), len(out))
	}

	// One comes back as an interior slice, which still counts as back.
	Put(out[0][8:])
	for _, b := range out[1 : len(out)-1] {
		Put(b)
	}
	if a.Released() || a.outstanding.Load() != 1 {
		t.Fatalf("released %v with %d out before the last slot came back", a.Released(), a.outstanding.Load())
	}
	// The pool must not have kept any of them.
	for range 200 {
		if b := Get(1000); a.holds(b) {
			t.Fatal("the pool handed out a slot of the retired arena")
		}
	}
	Put(out[len(out)-1])
	if !a.Released() {
		t.Fatalf("not released with %d out", a.outstanding.Load())
	}
	if p := retired.Load(); p != nil {
		for _, r := range *p {
			if r == a {
				t.Fatal("a released arena is still listed as retired")
			}
		}
	}
}

func TestReplacedArenaWithNothingOutIsReleased(t *testing.T) {
	a := newArena(t, map[int]int{2048: 16})
	SetAllocator(a)
	t.Cleanup(func() { SetAllocator(nil) })
	Put(Get(2000))
	other := newArena(t, map[int]int{2048: 16})
	SetAllocator(other)
	waitFor(t, "the replaced arena's release", a.Released)
	if other.Released() || other.state.Load() != arenaActive {
		t.Fatal("the installed arena was retired")
	}
	// Installing the same arena again does not retire it.
	SetAllocator(other)
	if other.state.Load() != arenaActive {
		t.Fatal("installing the arena again retired it")
	}
}

// Goroutines take and return buffers through Get and Put while the
// allocator changes under them: every buffer keeps its contents while held,
// and each replaced arena is released once they have all returned theirs.
func TestArenaSwitchUnderLoad(t *testing.T) {
	arenas := []*Arena{
		newArena(t, map[int]int{1024: 256, 4096: 64}),
		newArena(t, map[int]int{1024: 256, 4096: 64}),
		newArena(t, map[int]int{1024: 256, 4096: 64}),
	}
	SetAllocator(arenas[0])
	t.Cleanup(func() { SetAllocator(nil) })
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			var held [][]byte
			for r := 0; ; r++ {
				select {
				case <-stop:
					for _, h := range held {
						Put(h)
					}
					return
				default:
				}
				size := 1000
				if r%3 == 0 {
					size = 4000
				}
				b := Get(size)
				for k := range b {
					b[k] = byte(g)
				}
				held = append(held, b)
				if len(held) == 16 {
					for _, h := range held {
						for k := range h {
							if h[k] != byte(g) {
								t.Errorf("goroutine %d found %d in its buffer", g, h[k])
								return
							}
						}
						Put(h)
					}
					held = held[:0]
				}
			}
		})
	}
	for _, a := range arenas[1:] {
		time.Sleep(20 * time.Millisecond)
		SetAllocator(a)
	}
	time.Sleep(20 * time.Millisecond)
	SetAllocator(nil)
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	for k, a := range arenas {
		waitFor(t, "every arena's release", a.Released)
		if n := a.outstanding.Load(); n != 0 {
			t.Errorf("arena %d released with %d out", k, n)
		}
	}
}
