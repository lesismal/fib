package bufferpool

import (
	"os"
	"sync"
	"testing"
	"unsafe"
)

// The other tests in the package are about the sync.Pool and its reserve, so
// they run without the arena; the tests here make their own.
func TestMain(m *testing.M) {
	setArena(nil)
	os.Exit(m.Run())
}

func withArena(t *testing.T, counts map[int]int) *arena {
	t.Helper()
	setArena(counts)
	t.Cleanup(func() { setArena(nil) })
	return arenaP.Load()
}

func inArena(a *arena, b []byte) bool {
	off := uintptr(unsafe.Pointer(unsafe.SliceData(b))) - a.base
	return off < a.size
}

func TestArenaServesItsSlotsThenThePool(t *testing.T) {
	a := withArena(t, map[int]int{1024: 4, 2048: 2})
	if got := ArenaBytes(); got != 4*1024+2*2048 {
		t.Fatalf("ArenaBytes %d", got)
	}
	var out [][]byte
	for range 4 {
		b := Get(1000)
		if len(b) != 1000 || cap(b) != 1024 || !inArena(a, b) {
			t.Fatalf("Get(1000) len %d cap %d in arena %v", len(b), cap(b), inArena(a, b))
		}
		out = append(out, b)
	}
	if b := Get(1000); inArena(a, b) || cap(b) != 1024 {
		t.Fatalf("a fifth 1 KiB buffer came from the arena")
	}
	if ArenaFree(1024) != 0 || ArenaFree(2048) != 2 {
		t.Fatalf("free %d / %d", ArenaFree(1024), ArenaFree(2048))
	}
	for _, b := range out {
		Put(b)
	}
	if ArenaFree(1024) != 4 {
		t.Fatalf("free after Put %d", ArenaFree(1024))
	}
	// Distinct slots, none overlapping.
	seen := map[uintptr]bool{}
	for range 4 {
		p := uintptr(unsafe.Pointer(unsafe.SliceData(Get(1024))))
		if seen[p] {
			t.Fatal("a slot was handed out twice")
		}
		seen[p] = true
	}
}

func TestArenaTakesBackOnlyWholeSlots(t *testing.T) {
	a := withArena(t, map[int]int{1024: 2})
	b := Get(1024)
	Put(b[100:])
	if a.lost.Load() != 1 || ArenaFree(1024) != 1 {
		t.Fatalf("an interior slice: lost %d free %d", a.lost.Load(), ArenaFree(1024))
	}
	// An empty slice at a slot's end has the slot's address but no room.
	Put(b[1024:])
	if ArenaFree(1024) != 1 {
		t.Fatalf("an empty slice at the end went back: free %d", ArenaFree(1024))
	}
	// A slot cut shorter is still the slot.
	Put(b[:10:100])
	if ArenaFree(1024) != 2 {
		t.Fatalf("a shortened slot: free %d", ArenaFree(1024))
	}
}

func TestArenaReplacedWhileSlotsAreOut(t *testing.T) {
	a := withArena(t, map[int]int{1024: 2})
	b := Get(1024)
	if !inArena(a, b) {
		t.Fatal("not from the arena")
	}
	withArena(t, map[int]int{4096: 1})
	Put(b) // goes to the sync.Pool now
	c := Get(1024)
	if cap(c) != 1024 {
		t.Fatalf("cap %d", cap(c))
	}
}

func TestPooledReleasesTheArena(t *testing.T) {
	withArena(t, map[int]int{1024: 2})
	SetAllocator(Pooled)
	t.Cleanup(func() { SetAllocator(nil) })
	if ArenaBytes() != 0 {
		t.Fatal("the arena is still there")
	}
	b := Get(1000)
	if cap(b) != 1024 {
		t.Fatalf("cap %d", cap(b))
	}
	Put(b)
}

func TestBuiltinUsesTheArena(t *testing.T) {
	a := withArena(t, map[int]int{1024: 1})
	b := Builtin.Get(1000)
	if !inArena(a, b) {
		t.Fatal("Builtin.Get did not take from the arena")
	}
	Builtin.Put(b)
	if ArenaFree(1024) != 1 {
		t.Fatal("Builtin.Put did not give back")
	}
}

// Every goroutine marks the slot it holds and checks nobody else wrote it,
// through the per-P stacks and the shared ones and past the arena's end.
func TestArenaUnderLoad(t *testing.T) {
	withArena(t, map[int]int{1024: 300})
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			held := make([][]byte, 0, 40)
			for r := range 3000 {
				b := Get(1024)
				for k := range b {
					b[k] = byte(g)
				}
				held = append(held, b)
				if len(held) == cap(held) || r%7 == 0 {
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
			for _, h := range held {
				Put(h)
			}
		})
	}
	wg.Wait()
	if n := ArenaFree(1024); n != 300 {
		t.Fatalf("free %d after all were returned", n)
	}
}

func BenchmarkArenaGetPut(b *testing.B) {
	setArena(defaultArena())
	defer setArena(nil)
	for range b.N {
		Put(Get(1500))
	}
}

func BenchmarkArenaGetPutParallel(b *testing.B) {
	setArena(defaultArena())
	defer setArena(nil)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Put(Get(1500))
		}
	})
}

func BenchmarkPoolGetPut(b *testing.B) {
	for range b.N {
		Put(Get(1500))
	}
}

func BenchmarkPoolGetPutParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Put(Get(1500))
		}
	})
}
