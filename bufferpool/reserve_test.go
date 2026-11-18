package bufferpool

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A negative budget means no reserves, as zero does, rather than a budget
// every class is already over.
func TestNegativeRetainedBytesTurnsTheReservesOff(t *testing.T) {
	t.Cleanup(restoreBudget)
	SetRetainedBytes(-1)
	if got := budget.Load(); got != 0 {
		t.Fatalf("budget %d after SetRetainedBytes(-1), want 0", got)
	}
	if got := RetainedBytes(); got != 0 {
		t.Fatalf("%d bytes still retained", got)
	}
}

// An empty reserve hands out nothing, and a class whose burst of returns is
// spent keeps nothing more until the next collection starts another, however
// far below its target it is.
func TestReserveTakeAndKeepAtTheirLimits(t *testing.T) {
	c := &class{size: MinSize}
	c.shards.Store(&reserveShards{mask: 3, list: make([]reserveShard, 4)})
	if p := c.take(); p != nil {
		t.Fatal("an empty reserve handed out a buffer")
	}
	c.target.Store(4)
	buf := make([]byte, MinSize)
	if c.keep(&buf[0]) {
		t.Fatal("a class with no burst left kept a buffer")
	}
	if c.refill.Load() > 0 {
		t.Fatalf("refill went up to %d", c.refill.Load())
	}
	c.refill.Store(1)
	if !c.keep(&buf[0]) {
		t.Fatal("a class within its burst and target did not keep a buffer")
	}
	if p := c.take(); p != &buf[0] {
		t.Fatal("the reserve did not hand back the buffer it kept")
	}
}

// holdAcross takes n buffers of class i, holds them across collections with one
// returned after each, the way requests queued for longer than collections are
// apart come back, then returns the rest and lets two collections take the
// sync.Pool and its victim. It reports how many of the next n takes had to
// make a buffer.
func holdAcross(t *testing.T, i, n, collections int) (remade int) {
	t.Helper()
	c := &classes[i]
	size := classSize(i)
	bufs := make([][]byte, n)
	for k := range bufs {
		bufs[k] = Get(size)
	}
	for k := 0; k < collections; k++ {
		collect(t)
		Put(bufs[k])
	}
	for _, buf := range bufs[collections:] {
		Put(buf)
	}
	collect(t)
	collect(t)
	for k := range bufs {
		if c.held.Load() == 0 {
			remade++
		}
		bufs[k] = Get(size)
	}
	for _, buf := range bufs {
		Put(buf)
	}
	return remade
}

// Buffers held for longer than collections are apart are still out when a
// burst ends unfilled, and the class keeps its target for them: the next
// round of takes is served from the reserve rather than made again.
func TestBuffersHeldAcrossCollectionsKeepTheirReserve(t *testing.T) {
	t.Cleanup(restoreBudget)
	SetRetainedBytes(0)
	SetRetainedBytes(64 << 20)
	i := classUp(256 << 10)
	if remade := holdAcross(t, i, 64, 3); remade > 0 {
		t.Fatalf("%d of 64 buffers made again after being held across 3 collections; target %d",
			remade, classes[i].target.Load())
	}
}

// With no window the class gives back at the first unfilled burst, which is
// what the window is there to stop.
func TestNoWindowGivesBackAtTheFirstUnfilledBurst(t *testing.T) {
	t.Cleanup(func() {
		restoreBudget()
		SetRetainedFor(DefaultRetainedFor)
	})
	SetRetainedBytes(0)
	SetRetainedBytes(64 << 20)
	SetRetainedFor(0)
	i := classUp(512 << 10)
	if remade := holdAcross(t, i, 64, 3); remade < 32 {
		t.Fatalf("only %d of 64 made again with no window: the window is not what keeps them", remade)
	}
}

// A class that sees no returns any more has no burst to give back at, so the
// collections give its reserve back once the window has passed.
func TestAnIdleClassGivesItsReserveBack(t *testing.T) {
	t.Cleanup(func() {
		restoreBudget()
		SetRetainedFor(DefaultRetainedFor)
	})
	SetRetainedBytes(0)
	SetRetainedBytes(64 << 20)
	i := classUp(1 << 20)
	c := &classes[i]
	bufs := make([][]byte, 16)
	for k := range bufs {
		bufs[k] = Get(classSize(i))
	}
	collect(t)
	for _, buf := range bufs {
		Put(buf)
	}
	if c.held.Load() == 0 {
		t.Fatal("the reserve kept nothing to begin with")
	}
	// The window is shortened only now, so that it cannot run out between
	// the misses and the returns that fill the reserve.
	SetRetainedFor(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	for k := 0; k < 4 && c.held.Load() > 0; k++ {
		collect(t)
	}
	if held, target := c.held.Load(), c.target.Load(); held != 0 || target != 0 {
		t.Fatalf("an idle class still holds %d buffers against a target of %d", held, target)
	}
}

// Lowering the budget while misses raise targets leaves the budget counting
// exactly what the targets add up to.
func TestTrimKeepsTheBudgetInStepWithTheTargets(t *testing.T) {
	t.Cleanup(restoreBudget)
	SetRetainedBytes(0)
	var wg sync.WaitGroup
	var done atomic.Int32
	n := runtime.GOMAXPROCS(0)
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			defer done.Add(1)
			c := &classes[g%4]
			for range 20000 {
				c.reserveMore()
			}
		}(g)
	}
	for done.Load() < int32(n) {
		setRetainedBytes(1 << 20)
		setRetainedBytes(1 << 10)
	}
	wg.Wait()
	var sum int64
	for i := range classes {
		sum += classes[i].target.Load() * classes[i].size
	}
	if got := committed.Load(); got != sum {
		t.Fatalf("the budget counts %d bytes, the targets add up to %d", got, sum)
	}
}

// reserved is what the class's reserve holds, across its shards.
func (c *class) reserved() []*byte {
	var all []*byte
	set := c.shards.Load()
	for k := range set.list {
		sh := &set.list[k]
		sh.mu.Lock()
		all = append(all, sh.free...)
		sh.mu.Unlock()
	}
	return all
}

func TestReserveShardsRoundAndKeepWhatIsHeld(t *testing.T) {
	t.Cleanup(func() { setReserveShards(defaultReserveShards()) })
	SetRetainedBytes(64 << 20)
	t.Cleanup(restoreBudget)
	const size = 16 << 10
	c := &classes[classUp(size)]
	bufs := make([][]byte, 64)
	for k := range bufs {
		bufs[k] = Get(size)
	}
	collect(t)
	for _, buf := range bufs {
		Put(buf)
	}
	before := len(c.reserved())
	if before == 0 {
		t.Fatal("the reserve kept nothing to begin with")
	}
	for _, tc := range []struct{ n, want int }{{5, 8}, {0, 1}, {1 << 20, 1024}, {64, 64}} {
		if got := SetReserveShards(tc.n); got != tc.want || ReserveShards() != tc.want {
			t.Fatalf("SetReserveShards(%d) gave %d shards, want %d", tc.n, got, tc.want)
		}
		if got := len(c.reserved()); got != before {
			t.Fatalf("%d shards hold %d buffers, want the %d held before", tc.want, got, before)
		}
		if held := c.held.Load(); held != int64(before) {
			t.Fatalf("held says %d, the lists hold %d", held, before)
		}
	}
}

// Returns and takes racing SetReserveShards lose nothing: every buffer the
// reserve counts is in one of its lists.
func TestReserveShardsCanChangeUnderLoad(t *testing.T) {
	t.Cleanup(func() { setReserveShards(defaultReserveShards()) })
	SetRetainedBytes(64 << 20)
	t.Cleanup(restoreBudget)
	const size = 8 << 10
	c := &classes[classUp(size)]
	var wg sync.WaitGroup
	var stop atomic.Bool
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mine := make([][]byte, 32)
			for !stop.Load() {
				for i := range mine {
					mine[i] = Get(size)
				}
				for _, buf := range mine {
					Put(buf)
				}
				runtime.Gosched()
			}
		}()
	}
	for k := 0; k < 200; k++ {
		setReserveShards(1 << (k % 7))
		if k%20 == 0 {
			runtime.GC()
		}
	}
	stop.Store(true)
	wg.Wait()
	if held, got := c.held.Load(), len(c.reserved()); held != int64(got) {
		t.Fatalf("held says %d, the lists hold %d", held, got)
	}
}

// restoreBudget puts the budget back to the default, without logging it.
func restoreBudget() {
	bytes, _ := defaultRetainedBytes()
	setRetainedBytes(bytes)
}

// withMemory has the package read memory as the process's limit for the test.
func withMemory(t *testing.T, memory int64) {
	saved := memoryLimit
	memoryLimit = func() int64 { return memory }
	t.Cleanup(func() { memoryLimit = saved; restoreBudget() })
}

func TestTheDefaultBudgetFollowsTheMemory(t *testing.T) {
	for _, tc := range []struct {
		memory int64
		want   int64
	}{
		{0, DefaultRetainedBytes},            // unreadable
		{128 << 20, DefaultRetainedBytes},    // an eighth is below the floor
		{8 << 30, 1 << 30},                   // an 8 GiB container
		{64 << 30, 8 << 30},                  // .51
		{512 << 30, MaxDefaultRetainedBytes}, // capped
	} {
		withMemory(t, tc.memory)
		if got, _ := defaultRetainedBytes(); int64(got) != min(tc.want, math.MaxInt) {
			t.Errorf("memory %d: default budget %d, want %d", tc.memory, got, tc.want)
		}
	}
}

func TestSetRetainedPercent(t *testing.T) {
	withMemory(t, 1<<30)
	got, err := SetRetainedPercent(25)
	if err != nil || got != 256<<20 || RetainedBytesLimit() != 256<<20 {
		t.Fatalf("25%% of 1 GiB set %d (limit %d), %v", got, RetainedBytesLimit(), err)
	}
	if got, err := SetRetainedPercent(0); err != nil || got != 0 {
		t.Fatalf("0%% set %d, %v", got, err)
	}
	for _, bad := range []float64{-1, 100.5, math.NaN()} {
		if _, err := SetRetainedPercent(bad); err == nil {
			t.Fatalf("%v%% was accepted", bad)
		}
	}
	if RetainedBytesLimit() != 0 {
		t.Fatal("a rejected percent changed the budget")
	}
	withMemory(t, 0)
	if _, err := SetRetainedPercent(10); err == nil {
		t.Fatal("a percent of unreadable memory was accepted")
	}
}

func TestTheDefaultShardsFollowTheProcs(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	for _, tc := range []struct{ procs, want int }{{1, 16}, {2, 16}, {8, 32}, {16, 64}, {32, 128}, {256, 128}} {
		runtime.GOMAXPROCS(tc.procs)
		if got := defaultReserveShards(); got != tc.want {
			t.Errorf("GOMAXPROCS %d: %d shards, want %d", tc.procs, got, tc.want)
		}
	}
}
