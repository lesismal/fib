package bufferpool

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lesismal/fib/log"
)

// The arena is one large allocation cut into fixed slots per class. Get takes
// a slot before it goes to the class's sync.Pool, and Put recognises a slot by
// its address and gives it back to the arena. The arena is one object with no
// pointers in it, so the collector neither scans it nor frees it: a slot that
// is out across any number of collections is still there when it comes back,
// and nothing is made again.
//
// Free slots are kept as their index within the class, not as pointers, so
// keeping them costs the collector nothing either. Each P has a small stack
// of them per class, taken and returned with the P pinned and no lock, as a
// sync.Pool's private slot is; past that, slots move in batches to and from
// shared stacks with a lock each.
//
// A slot the program never gives back is lost to the arena until it is
// replaced, since the collector never takes it back: the arena serves only as
// many buffers at once as it has slots, and Get goes on to the sync.Pool
// after that.

const (
	// magSize is how many free slots a P keeps per class.
	magSize = 64
	// magBatch is how many move between a P and the shared stacks at once.
	magBatch = magSize / 2
	// depotShards is how many shared stacks a class has.
	depotShards = 8

	// DefaultArenaClassBytes is how much of the default arena each class up
	// to DefaultArenaMaxSize has.
	DefaultArenaClassBytes = 4 << 20
	// DefaultArenaMaxSize is the largest class the default arena has slots
	// for.
	DefaultArenaMaxSize = 64 << 10
)

type arena struct {
	mem  []byte
	base uintptr
	size uintptr
	cls  [numClasses]arenaClass
	// local is one entry per P, as GOMAXPROCS was when the arena was made.
	local []arenaLocal
	// lost counts returns the arena could not take back: a slice that does
	// not start at a slot.
	lost atomic.Int64
}

type arenaClass struct {
	start, end uintptr
	count      int
	depot      [depotShards]depotShard
}

type depotShard struct {
	mu   sync.Mutex
	free []uint32
	n    atomic.Int32
	_    [64]byte
}

type magazine struct {
	n int
	s [magSize]uint32
}

type arenaLocal struct {
	m [numClasses]magazine
	_ [64]byte
}

var arenaP atomic.Pointer[arena]

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

// SetArena replaces the arena with one holding counts[size] slots of each
// size, rounded up to its class, and returns the arena's bytes. Sizes past
// MaxSize are ignored. An empty map removes the arena.
//
// It may be called at any time: slots of the arena it replaces are returned
// to the sync.Pool when they are Put, as any other buffer of their class.
func SetArena(counts map[int]int) int {
	bytes := setArena(counts)
	log.Info("fib: bufferpool arena", "bytes", bytes, "classes", len(counts))
	return bytes
}

func setArena(counts map[int]int) int {
	var per [numClasses]int
	total := 0
	for size, n := range counts {
		if size <= 0 || size > MaxSize || n <= 0 {
			continue
		}
		i := classUp(size)
		per[i] += n
	}
	for i, n := range per {
		total += n * classSize(i)
	}
	if total == 0 {
		arenaP.Store(nil)
		return 0
	}
	a := &arena{mem: make([]byte, total)}
	a.base = uintptr(unsafe.Pointer(unsafe.SliceData(a.mem)))
	a.size = uintptr(total)
	a.local = make([]arenaLocal, runtime.GOMAXPROCS(0))
	off := uintptr(0)
	for i, n := range per {
		c := &a.cls[i]
		c.start, c.count = off, n
		off += uintptr(n * classSize(i))
		c.end = off
		if n == 0 {
			continue
		}
		for s := range c.depot {
			c.depot[s].free = make([]uint32, 0, n/depotShards+1)
		}
		for k := 0; k < n; k++ {
			d := &c.depot[k%depotShards]
			d.free = append(d.free, uint32(k))
		}
		for s := range c.depot {
			c.depot[s].n.Store(int32(len(c.depot[s].free)))
		}
	}
	arenaP.Store(a)
	return total
}

// ArenaBytes is the size of the arena, or 0 when there is none.
func ArenaBytes() int {
	if a := arenaP.Load(); a != nil {
		return int(a.size)
	}
	return 0
}

// ArenaFree is how many slots of size's class are not out.
func ArenaFree(size int) int {
	a := arenaP.Load()
	if a == nil || size <= 0 || size > MaxSize {
		return 0
	}
	i := classUp(size)
	n := 0
	for s := range a.cls[i].depot {
		n += int(a.cls[i].depot[s].n.Load())
	}
	for p := range a.local {
		n += a.local[p].m[i].n
	}
	return n
}

func defaultArena() map[int]int {
	m := map[int]int{}
	for size := MinSize; size <= DefaultArenaMaxSize; size <<= 1 {
		m[size] = DefaultArenaClassBytes / size
	}
	return m
}

func (a *arena) slot(i int, idx uint32) []byte {
	size := classSize(i)
	p := unsafe.Add(unsafe.Pointer(unsafe.SliceData(a.mem)), a.cls[i].start+uintptr(idx)*uintptr(size))
	return unsafe.Slice((*byte)(p), size)
}

// get takes a free slot of class i, or returns nil when the class has none.
func (a *arena) get(i int) []byte {
	if raceEnabled {
		var one [1]uint32
		if a.cls[i].pop(one[:]) == 0 {
			return nil
		}
		return a.slot(i, one[0])
	}
	pid := procPin()
	if pid < len(a.local) {
		m := &a.local[pid].m[i]
		if m.n > 0 {
			m.n--
			idx := m.s[m.n]
			procUnpin()
			return a.slot(i, idx)
		}
	}
	procUnpin()
	return a.getSlow(i)
}

func (a *arena) getSlow(i int) []byte {
	var batch [magBatch]uint32
	n := a.cls[i].pop(batch[:])
	if n == 0 {
		return nil
	}
	n--
	idx := batch[n]
	if n > 0 {
		pid := procPin()
		if pid < len(a.local) {
			m := &a.local[pid].m[i]
			k := min(n, magSize-m.n)
			copy(m.s[m.n:], batch[n-k:n])
			m.n += k
			n -= k
		}
		procUnpin()
		if n > 0 {
			a.cls[i].push(batch[:n])
		}
	}
	return a.slot(i, idx)
}

// put gives back the slot at offset off, or counts it lost when off is not
// the start of a slot. Put has already turned away a capacity below MinSize,
// which is also what keeps out an empty slice taken at a slot's end: its
// address would be that slot's start.
func (a *arena) put(off uintptr, capacity int) {
	i := 0
	if capacity >= MinSize {
		i = min(classDown(capacity), numClasses-1)
	}
	for ; i < numClasses; i++ {
		if off < a.cls[i].end {
			break
		}
	}
	if i == numClasses || off < a.cls[i].start || a.cls[i].count == 0 {
		a.lost.Add(1)
		return
	}
	rel := off - a.cls[i].start
	if rel&uintptr(classSize(i)-1) != 0 {
		a.lost.Add(1)
		return
	}
	idx := uint32(rel >> (uint(i) + minShift))
	if raceEnabled {
		a.cls[i].push([]uint32{idx})
		return
	}
	pid := procPin()
	if pid < len(a.local) {
		m := &a.local[pid].m[i]
		if m.n < magSize {
			m.s[m.n] = idx
			m.n++
			procUnpin()
			return
		}
		var batch [magBatch + 1]uint32
		m.n -= magBatch
		copy(batch[:], m.s[m.n:m.n+magBatch])
		batch[magBatch] = idx
		procUnpin()
		a.cls[i].push(batch[:])
		return
	}
	procUnpin()
	a.cls[i].push([]uint32{idx})
}

func (c *arenaClass) pop(out []uint32) int {
	start := int(rand.Uint32())
	for k := 0; k < depotShards; k++ {
		d := &c.depot[(start+k)%depotShards]
		if d.n.Load() == 0 {
			continue
		}
		d.mu.Lock()
		n := min(len(out), len(d.free))
		copy(out, d.free[len(d.free)-n:])
		d.free = d.free[:len(d.free)-n]
		d.n.Store(int32(len(d.free)))
		d.mu.Unlock()
		if n > 0 {
			return n
		}
	}
	return 0
}

func (c *arenaClass) push(in []uint32) {
	d := &c.depot[rand.Uint32()%depotShards]
	d.mu.Lock()
	d.free = append(d.free, in...)
	d.n.Store(int32(len(d.free)))
	d.mu.Unlock()
}
