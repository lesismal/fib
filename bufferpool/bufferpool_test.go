package bufferpool

import (
	"bytes"
	"math/bits"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestAlignRoundsUpToAPowerOfTwo(t *testing.T) {
	cases := []struct{ size, want int }{
		{-1, 0},
		{0, 0},
		{1, MinSize},
		{MinSize - 1, MinSize},
		{MinSize, MinSize},
		{MinSize + 1, 2 * MinSize},
		{1000, 1024},
		{1024, 1024},
		{1025, 2048},
		{MaxSize, MaxSize},
		{MaxSize + 1, MaxSize + 1},
	}
	for _, c := range cases {
		if got := Align(c.size); got != c.want {
			t.Fatalf("Align(%d) = %d, want %d", c.size, got, c.want)
		}
	}
	for size := 1; size <= MaxSize; size *= 3 {
		got := Align(size)
		if got < size {
			t.Fatalf("Align(%d) = %d, want at least the size asked for", size, got)
		}
		if bits.OnesCount(uint(got)) != 1 {
			t.Fatalf("Align(%d) = %d, want a power of two", size, got)
		}
		if got > MinSize && got/2 >= size {
			t.Fatalf("Align(%d) = %d, want the smallest class that fits", size, got)
		}
	}
}

func TestGetHasTheRequestedLengthAndAlignedCapacity(t *testing.T) {
	if got := Get(0); got != nil {
		t.Fatalf("Get(0) = %v, want nil", got)
	}
	for _, size := range []int{1, 7, MinSize, MinSize + 1, 4097, 64 << 10, MaxSize} {
		buf := Get(size)
		if len(buf) != size {
			t.Fatalf("len(Get(%d)) = %d, want %d", size, len(buf), size)
		}
		if cap(buf) != Align(size) {
			t.Fatalf("cap(Get(%d)) = %d, want %d", size, cap(buf), Align(size))
		}
		Put(buf)
	}
}

// A buffer larger than the largest class is served, but never pooled: holding
// one for a rare request would cost more than making it again.
func TestOversizedBuffersAreServedButNotPooled(t *testing.T) {
	size := MaxSize + 1
	buf := Get(size)
	if len(buf) != size || cap(buf) != size {
		t.Fatalf("Get(%d) has len %d cap %d, want both %d", size, len(buf), cap(buf), size)
	}
	address := unsafe.SliceData(buf)
	Put(buf)
	// A class holds addresses and works the length out from the class, so an
	// oversized buffer kept by one could not be told apart from an ordinary
	// one afterwards. What has to hold is that Put turned it away.
	for i := range classes {
		if p, _ := classes[i].pool.Get().(*byte); p != nil {
			if p == address {
				t.Fatalf("class %d pooled a buffer larger than MaxSize", i)
			}
			classes[i].pool.Put(p)
		}
		classes[i].mu.Lock()
		for _, held := range classes[i].free {
			if held == address {
				t.Fatalf("class %d reserved a buffer larger than MaxSize", i)
			}
		}
		classes[i].mu.Unlock()
	}
}

// reused reports whether one of the buffers Put just took came back out of the
// pool. A collection between the two empties the pool, so the check is given a
// few attempts before it is believed.
func reused(t *testing.T, size int) bool {
	t.Helper()
	const attempts = 5
	for range attempts {
		const buffers = 8
		want := map[uintptr]bool{}
		for range buffers {
			buf := Get(size)
			want[uintptr(unsafe.Pointer(unsafe.SliceData(buf)))] = true
			Put(buf)
		}
		for range buffers {
			if want[uintptr(unsafe.Pointer(unsafe.SliceData(Get(size))))] {
				return true
			}
		}
	}
	return false
}

func TestPutMakesTheBufferAvailableAgain(t *testing.T) {
	for _, size := range []int{MinSize, 1500, 16 << 10} {
		if !reused(t, size) {
			t.Fatalf("a buffer of %d bytes was never handed back out", size)
		}
	}
}

// A request is served by any buffer in its class, so one taken at a size that
// rounds up to the same class serves the next size too.
func TestAClassServesEverySizeThatRoundsUpToIt(t *testing.T) {
	Put(Get(1024))
	buf := Get(600)
	if cap(buf) < 1024 {
		t.Fatalf("cap = %d, want a buffer from the 1024-byte class", cap(buf))
	}
}

// A buffer the pool did not make — grown by append, or made by the caller —
// still belongs to the class below its capacity, and must never be handed out
// as if it held a whole class more than it does. It is trimmed to its class on
// the way in, so a buffer's capacity always says exactly which class it is in.
func TestUnalignedBuffersAreKeptAtTheClassBelow(t *testing.T) {
	Put(make([]byte, 0, 100))
	for range 16 {
		if buf := Get(MinSize); cap(buf) != MinSize {
			t.Fatalf("cap = %d, want exactly %d", cap(buf), MinSize)
		}
	}
	// Smaller than the smallest class, so there is no class it could be kept
	// at without over-promising. It is dropped instead.
	Put(make([]byte, 0, MinSize-1))
	for range 16 {
		if buf := Get(1); cap(buf) != MinSize {
			t.Fatalf("cap = %d, want exactly %d", cap(buf), MinSize)
		}
	}
}

func TestGrowKeepsTheBytesAndAddsRoom(t *testing.T) {
	buf := append(Get(4)[:0], "abcd"...)
	grown := Grow(buf, 5000)
	if !bytes.Equal(grown, []byte("abcd")) {
		t.Fatalf("grown = %q, want %q", grown, "abcd")
	}
	if cap(grown) != Align(5000) {
		t.Fatalf("cap = %d, want %d", cap(grown), Align(5000))
	}
	same := Grow(grown, cap(grown))
	if unsafe.SliceData(same) != unsafe.SliceData(grown) {
		t.Fatal("Grow replaced a buffer that already had the room")
	}
	Put(same)
}

func TestJoinPutsPartsIntoOneBuffer(t *testing.T) {
	buf := Join(nil, []byte("hello, "), []byte("world"))
	if !bytes.Equal(buf, []byte("hello, world")) {
		t.Fatalf("buf = %q, want %q", buf, "hello, world")
	}
	buf = Append(buf, []byte("!"))
	if !bytes.Equal(buf, []byte("hello, world!")) {
		t.Fatalf("buf = %q, want %q", buf, "hello, world!")
	}
	if got := Join(nil); got != nil {
		t.Fatalf("Join(nil) = %v, want nil", got)
	}
	if got := Append(nil, nil); got != nil {
		t.Fatalf("Append(nil, nil) = %v, want nil", got)
	}
	Put(buf)
}

// Join must take a buffer that fits every part at once, not grow once per
// part and leave the earlier ones behind.
func TestJoinGrowsOnceForEveryPart(t *testing.T) {
	part := make([]byte, 700)
	if got := cap(Join(nil, part, part)); got != 2048 {
		t.Fatalf("cap = %d, want 2048", got)
	}
}

// Growing piece by piece must move up the classes, not stay at the size of the
// first piece and copy on every append.
func TestAppendGrowsToTheClassThatFits(t *testing.T) {
	part := make([]byte, 700)
	buf := Append(nil, part)
	if cap(buf) != 1024 {
		t.Fatalf("cap = %d, want 1024", cap(buf))
	}
	buf = Append(buf, part)
	if cap(buf) != 2048 {
		t.Fatalf("cap = %d, want 2048", cap(buf))
	}
	Put(buf)
}

// Every buffer handed out is the caller's alone: two in flight at once must
// never share an array.
func TestConcurrentUseHandsOutDistinctBuffers(t *testing.T) {
	var wg sync.WaitGroup
	for worker := range runtime.GOMAXPROCS(0) * 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mark := byte(worker)
			for range 2000 {
				buf := Get(1500)
				for i := range buf {
					buf[i] = mark
				}
				runtime.Gosched()
				for i := range buf {
					if buf[i] != mark {
						t.Errorf("byte %d is %d, want %d: the buffer is shared", i, buf[i], mark)
						return
					}
				}
				Put(buf)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkGetPut(b *testing.B) {
	for _, size := range []int{64, 1500, 16 << 10} {
		b.Run(sizeName(size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				Put(Get(size))
			}
		})
	}
}

func BenchmarkGetPutParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Put(Get(1500))
		}
	})
}

func BenchmarkMake(b *testing.B) {
	for _, size := range []int{64, 1500, 16 << 10} {
		b.Run(sizeName(size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				sink = make([]byte, size)
			}
		})
	}
}

func BenchmarkJoinParts(b *testing.B) {
	head := make([]byte, 120)
	body := make([]byte, 900)
	b.Run("pooled", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			Put(Join(nil, head, body))
		}
	})
	b.Run("append", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			sink = append(append(make([]byte, 0, len(head)+len(body)), head...), body...)
		}
	})
}

var sink []byte

func sizeName(size int) string { return strconv.Itoa(size) + "B" }

// collect runs a collection and waits for the cleanup that counts it, since a
// cleanup runs on its own goroutine once the collection has swept the tick.
func collect(t *testing.T) {
	t.Helper()
	before := gcEpoch.Load()
	deadline := time.Now().Add(5 * time.Second)
	for gcEpoch.Load() == before {
		runtime.GC()
		if time.Now().After(deadline) {
			t.Fatal("no collection was ever counted: the pool cannot tell its sync.Pool has been emptied")
		}
		time.Sleep(time.Millisecond)
	}
}

// A collection empties every sync.Pool, so without a reserve behind them the
// whole working set is made again afterwards. This is the measurement the
// reserve exists for: once it has settled, a collection costs nothing.
func TestTheWorkingSetSurvivesACollection(t *testing.T) {
	const (
		size    = 16 << 10
		working = 256
	)
	// Start from a clean pool: the classes are shared, and what the tests
	// before this one left reserved is budget this one cannot have.
	SetRetainedBytes(0)
	SetRetainedBytes(DefaultRetainedBytes)
	held := make([][]byte, working)
	remade := func() uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := range held {
			held[i] = Get(size)
		}
		runtime.ReadMemStats(&after)
		for _, buf := range held {
			Put(buf)
		}
		return after.Mallocs - before.Mallocs
	}
	// The target is learned from what a class has had to make, and a reserve
	// is filled by the returns that follow a collection, so it takes a round
	// or two of both to settle.
	var last uint64
	for range 8 {
		collect(t)
		last = remade()
		if last <= working/8 {
			return
		}
	}
	t.Fatalf("a collection still costs %d of a working set of %d", last, working)
}

// The reserve is bounded, and the bound is what the caller says it is.
func TestRetentionStaysWithinItsBudget(t *testing.T) {
	t.Cleanup(func() { SetRetainedBytes(DefaultRetainedBytes) })
	const size = 64 << 10
	SetRetainedBytes(8 * size)
	held := make([][]byte, 512)
	for range 4 {
		collect(t)
		for i := range held {
			held[i] = Get(size)
		}
		for _, buf := range held {
			Put(buf)
		}
		if got := RetainedBytes(); got > 8*size {
			t.Fatalf("retention reached %d bytes, want at most %d", got, 8*size)
		}
	}

	// Lowering the budget gives back what is already reserved, which is what
	// makes it a way to release the pool's memory rather than only a limit on
	// what it takes next.
	SetRetainedBytes(0)
	if got := RetainedBytes(); got != 0 {
		t.Fatalf("retention is %d bytes after being set to zero", got)
	}
	for i := range classes {
		classes[i].mu.Lock()
		free := len(classes[i].free)
		classes[i].mu.Unlock()
		if free != 0 {
			t.Fatalf("class %d still holds %d buffers", i, free)
		}
	}
}

// With the reserves off the pool is the sync.Pool alone, which still has to
// hand out buffers of the right shape.
func TestTheReservesCanBeTurnedOff(t *testing.T) {
	t.Cleanup(func() { SetRetainedBytes(DefaultRetainedBytes) })
	SetRetainedBytes(0)
	collect(t)
	for range 64 {
		buf := Get(1500)
		if len(buf) != 1500 || cap(buf) != 2048 {
			t.Fatalf("Get(1500) has len %d cap %d", len(buf), cap(buf))
		}
		Put(buf)
	}
	if got := RetainedBytes(); got != 0 {
		t.Fatalf("retention grew to %d bytes with the reserves off", got)
	}
}
