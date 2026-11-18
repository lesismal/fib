package bufferpool

import (
	"math"
	"sync"
	"testing"
	"unsafe"
)

func newArena(t testing.TB, counts map[int]int) *Arena {
	t.Helper()
	a, err := NewArena(counts)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (a *Arena) holds(b []byte) bool {
	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))-a.base < a.size
}

func TestArenaServesItsSlotsThenThePool(t *testing.T) {
	a := newArena(t, map[int]int{1024: 4, 2048: 2, 1500: 1})
	if got := a.Bytes(); got != 4*1024+3*2048 {
		t.Fatalf("Bytes %d", got)
	}
	if a.Slots(1000) != 4 || a.Slots(2048) != 3 || a.Slots(64) != 0 {
		t.Fatalf("Slots %d %d %d", a.Slots(1000), a.Slots(2048), a.Slots(64))
	}
	var out [][]byte
	for range 4 {
		b := a.Get(1000)
		if len(b) != 1000 || cap(b) != 1024 || !a.holds(b) {
			t.Fatalf("Get(1000) len %d cap %d from the arena %v", len(b), cap(b), a.holds(b))
		}
		out = append(out, b)
	}
	if b := a.Get(1000); a.holds(b) || len(b) != 1000 || cap(b) != 1024 {
		t.Fatalf("a fifth 1 KiB buffer: from the arena %v, len %d cap %d", a.holds(b), len(b), cap(b))
	}
	if b := a.Get(100); a.holds(b) || cap(b) != 128 {
		t.Fatal("a class without slots was not served by the pool")
	}
	if b := a.Get(MaxSize + 1); len(b) != MaxSize+1 {
		t.Fatal("a size past MaxSize")
	}
	if a.Get(0) != nil {
		t.Fatal("Get(0)")
	}
	if a.Free(1024) != 0 || a.Free(2048) != 3 {
		t.Fatalf("free %d / %d", a.Free(1024), a.Free(2048))
	}
	for _, b := range out {
		a.Put(b)
	}
	if a.Free(1024) != 4 {
		t.Fatalf("free after Put %d", a.Free(1024))
	}
	seen := map[uintptr]bool{}
	for range 4 {
		p := uintptr(unsafe.Pointer(unsafe.SliceData(a.Get(1024))))
		if seen[p] {
			t.Fatal("a slot was handed out twice")
		}
		seen[p] = true
	}
}

func TestArenaTakesBackOnlyWholeSlots(t *testing.T) {
	a := newArena(t, map[int]int{1024: 2})
	b := a.Get(1024)
	a.Put(b[100:])
	if a.Lost() != 1 || a.Free(1024) != 1 {
		t.Fatalf("an interior slice: lost %d free %d", a.Lost(), a.Free(1024))
	}
	a.Put(b[1024:])
	if a.Free(1024) != 1 || a.Lost() != 1 {
		t.Fatalf("an empty slice at the end: free %d lost %d", a.Free(1024), a.Lost())
	}
	a.Put(b[:10:100])
	if a.Free(1024) != 2 {
		t.Fatalf("a shortened slot: free %d", a.Free(1024))
	}
}

func TestArenaSendsOtherBuffersToThePool(t *testing.T) {
	a := newArena(t, map[int]int{1024: 1})
	other := newArena(t, map[int]int{1024: 1})
	b := other.Get(1024)
	a.Put(b) // another arena's slot is just a buffer here
	a.Put(make([]byte, 1024))
	if a.Free(1024) != 1 || a.Lost() != 0 {
		t.Fatalf("free %d lost %d", a.Free(1024), a.Lost())
	}
}

func TestArenaAsTheAllocator(t *testing.T) {
	a := newArena(t, map[int]int{1024: 1})
	SetAllocator(a)
	t.Cleanup(func() { SetAllocator(nil) })
	b := Get(1000)
	if !a.holds(b) {
		t.Fatal("Get did not take from the arena")
	}
	b = Append(b[:0], make([]byte, 3000))
	if len(b) != 3000 || a.Free(1024) != 1 {
		t.Fatalf("growing a slot past it: len %d, free %d", len(b), a.Free(1024))
	}
	Put(b)
}

func TestNewArenaRejectsTooMuch(t *testing.T) {
	if _, err := NewArena(map[int]int{1: math.MaxInt32, 32: math.MaxInt32, 64: math.MaxInt32}); err == nil {
		t.Fatal("a class with more slots than a uint32 can number")
	}
	if a := newArena(t, map[int]int{MaxSize + 1: 4, 1024: -1}); a.Bytes() != 0 {
		t.Fatalf("Bytes %d", a.Bytes())
	} else if b := a.Get(1024); cap(b) != 1024 {
		t.Fatal("an empty arena does not serve from the pool")
	}
}

func TestDefaultArenaFollowsTheMemory(t *testing.T) {
	for _, c := range []struct {
		memory int64
		want   int64
	}{
		{0, MinDefaultArenaBytes},
		{128 << 20, MinDefaultArenaBytes},
		{8 << 30, 1 << 30},
		{512 << 30, MaxDefaultArenaBytes},
	} {
		withMemory(t, c.memory)
		// A 32-bit int gives the arena at most half its range.
		want := min(c.want, math.MaxInt/2)
		if got := DefaultArenaBytes(); int64(got) != want {
			t.Errorf("memory %d: DefaultArenaBytes %d, want %d", c.memory, got, want)
		}
	}
}

func TestNewArenaBytesSplitsTheTotal(t *testing.T) {
	a := newArenaBytes(t, 11<<20, ArenaWeightsEven())
	if a.Bytes() != 11<<20 {
		t.Fatalf("Bytes %d", a.Bytes())
	}
	for size := MinSize; size <= 64<<10; size <<= 1 {
		if got := a.Slots(size) * size; got != 1<<20 {
			t.Errorf("class %d: %d bytes", size, got)
		}
	}
	if a.Slots(128<<10) != 0 {
		t.Error("a class without weight has slots")
	}
	// Whole slots only: a total too small for the biggest classes leaves
	// them out rather than rounding up.
	if m := ArenaCounts(11*1024, ArenaWeightsEven()); m[64<<10] != 0 || m[1024] != 1 || m[64] != 16 {
		t.Errorf("ArenaCounts(11 KiB) = %v", m)
	}
	if len(ArenaCounts(0, nil)) != 0 {
		t.Error("ArenaCounts(0)")
	}
	withMemory(t, 512<<20) // a 64 MiB default, small enough for any test host
	if a := newArenaBytes(t, 0, nil); a.Bytes() > DefaultArenaBytes() || a.Bytes() < DefaultArenaBytes()-DefaultArenaBytes()/100 {
		t.Errorf("NewArenaBytes(0): %d, default %d", a.Bytes(), DefaultArenaBytes())
	}
}

func TestArenaWeights(t *testing.T) {
	const total = 24 << 20
	// The server preset's weights add up to 24, so each unit is 1 MiB.
	server := ArenaCounts(total, nil)
	if server[16<<10]*(16<<10) != 6<<20 || server[64]*64 != 1<<20 || server[64<<10]*(64<<10) != 3<<20 {
		t.Errorf("nil weights are not the server preset: %v", server)
	}
	for name, w := range map[string]ArenaWeights{
		"server": ArenaWeightsServer(), "messages": ArenaWeightsMessages(),
		"streaming": ArenaWeightsStreaming(), "even": ArenaWeightsEven(),
	} {
		m := ArenaCounts(total, w)
		bytes := 0
		for size, n := range m {
			bytes += size * n
		}
		if bytes > total || bytes < total-total/100 || len(m) != 11 {
			t.Errorf("%s: %d bytes in %d classes", name, bytes, len(m))
		}
	}
	// Sizes round up to their class and add up there; weights that are not
	// above zero, and sizes past MaxSize, are left out.
	m := ArenaCounts(4<<20, ArenaWeights{
		1000: 1, 1024: 1, 128 << 10: 2, 64: 0, 128: -1, 256: math.NaN(), 512: math.Inf(1), MaxSize + 1: 9,
	})
	if len(m) != 2 || m[1024]*1024 != 2<<20 || m[128<<10]*(128<<10) != 2<<20 {
		t.Errorf("own weights: %v", m)
	}
	if len(ArenaCounts(1<<20, ArenaWeights{64: -1})) != 0 {
		t.Error("weights all left out")
	}
}

func newArenaBytes(t testing.TB, total int, w ArenaWeights) *Arena {
	t.Helper()
	a, err := NewArenaBytes(total, w)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// Every goroutine marks the slots it holds and checks nobody else wrote them,
// through the per-P stacks and the shared ones and past the arena's end.
func TestArenaUnderLoad(t *testing.T) {
	a := newArena(t, map[int]int{1024: 300})
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			held := make([][]byte, 0, 40)
			for r := range 3000 {
				b := a.Get(1024)
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
						a.Put(h)
					}
					held = held[:0]
				}
			}
			for _, h := range held {
				a.Put(h)
			}
		})
	}
	wg.Wait()
	if n := a.Free(1024); n != 300 {
		t.Fatalf("free %d after all were returned", n)
	}
}

func BenchmarkArenaGetPut(b *testing.B) {
	a := newArena(b, map[int]int{2048: 1024})
	for range b.N {
		a.Put(a.Get(1500))
	}
}

func BenchmarkArenaGetPutParallel(b *testing.B) {
	a := newArena(b, map[int]int{2048: 1024})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a.Put(a.Get(1500))
		}
	})
}

func BenchmarkArenaInstalledGetPut(b *testing.B) {
	SetAllocator(newArena(b, map[int]int{2048: 1024}))
	defer SetAllocator(nil)
	for range b.N {
		Put(Get(1500))
	}
}

func BenchmarkArenaInstalledGetPutParallel(b *testing.B) {
	SetAllocator(newArena(b, map[int]int{2048: 1024}))
	defer SetAllocator(nil)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Put(Get(1500))
		}
	})
}
