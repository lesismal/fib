package bufferpool

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lesismal/fib/log"
)

// Arena is an Allocator that serves buffers from one large allocation cut
// into fixed slots per class, made once and kept for the program's life.
// Install it with SetAllocator: NewArena sets the slots of each class,
// NewArenaBytes a total it splits between the classes by ArenaWeights (one of
// the presets, or a program's own), and either one with nothing given makes
// an eighth of the memory the process may use, split by ArenaWeightsServer.
//
// It is for a program that holds many buffers across collections — requests
// queued for seconds while collections come every few hundred milliseconds,
// or many HTTP/2 and HTTP/3 streams in flight at once. The arena is one object
// with no pointers in it, so the collector neither scans it nor frees it: a
// slot that is out across any number of collections is still there when it
// comes back, nothing is made again, and a collection has one object to mark
// where the package's own pool has one per buffer. Free slots are kept as
// their index within the class, not as pointers, so keeping them costs the
// collector nothing either; a slot that has never been out is not kept at all
// but taken from a count of the slots handed out so far, so the indexes held
// grow with the slots a program has used rather than with the arena.
//
// Each P has a small stack of free slots per class, taken and returned with
// the P pinned and no lock, as a sync.Pool's private slot is; past that, slots
// move in batches to and from shared stacks with a lock each. When a class
// has no free slot, or has none at all, Get goes to the package's own pool,
// and Put sends there every buffer that is not one of the arena's slots.
//
// What it costs:
//
//   - The memory is the arena's for good. It counts towards the live heap, so
//     towards GOGC's goal and GOMEMLIMIT, from the start; the operating system
//     backs a page only once a slot on it has been used, but does not get it
//     back after.
//   - A slot that is never Put is lost to the arena, since the collector never
//     takes it back: fib leaves some buffers to the collector rather than
//     returning them (docs/bufferpool-allocator.md lists where), and each one
//     that was a slot leaves the arena a slot smaller. Put of a slice that
//     does not start at a slot — buf[k:] — loses that slot too. Lost counts
//     the second kind; Free shows both.
//   - Get and Put go through the Allocator interface, which costs a few
//     nanoseconds on a pair that costs about ten.
type Arena struct {
	mem  []byte
	base uintptr
	size uintptr
	cls  [numClasses]arenaClass
	// local is one entry per P, for as many Ps as GOMAXPROCS or the CPU
	// count allowed when the arena was made; a P past them goes to the
	// shared stacks.
	local []arenaLocal
	lost  atomic.Int64
}

type arenaClass struct {
	start, end uintptr
	count      int
	// fresh is how many slots, from index 0 up, have been handed out at least
	// once; the ones past it have never been out and are in no depot. It runs
	// past count once they all have.
	fresh atomic.Uint64
	_     [64]byte
	depot [depotShards]depotShard
}

type depotShard struct {
	mu   sync.Mutex
	free []uint32
	n    atomic.Int64
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

const (
	// magSize is how many free slots a P keeps per class.
	magSize = 64
	// magBatch is how many move between a P and the shared stacks at once.
	magBatch = magSize / 2
	// depotShards is how many shared stacks a class has.
	depotShards = 8

	// DefaultArenaDivisor makes the default arena an eighth of the memory the
	// process may use, read as the default retention budget reads it, and
	// bounded the same way. The package's own pool still serves what the
	// arena cannot, with that budget of its own.
	DefaultArenaDivisor = 8
	// MinDefaultArenaBytes and MaxDefaultArenaBytes bound the default arena;
	// MinDefaultArenaBytes is also what it is when the memory cannot be read.
	MinDefaultArenaBytes = DefaultRetainedBytes
	MaxDefaultArenaBytes = MaxDefaultRetainedBytes

	// maxArenaSlots keeps a slot's index within a uint32.
	maxArenaSlots = math.MaxUint32
)

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

// DefaultArenaBytes is the size of the arena NewArena(nil) makes: an eighth
// of the memory the process may use (see SetRetainedPercent), at least 32 MiB
// and at most 16 GiB, as the default retention budget is.
func DefaultArenaBytes() int {
	total := int64(MinDefaultArenaBytes)
	if memory := memoryLimit(); memory > 0 {
		total = min(max(memory/DefaultArenaDivisor, MinDefaultArenaBytes), MaxDefaultArenaBytes)
	}
	// A 32-bit int gives the arena at most half its range.
	return int(min(total, math.MaxInt/2))
}

// ArenaWeights says how an arena's bytes are split between classes: each
// size, rounded up to its class, gets its weight's share of the total. Sizes
// past MaxSize and weights that are not above zero are left out, so a class
// with no weight has no slots and is served by the package's own pool.
//
// The presets below are starting points drawn from the sizes fib itself asks
// for, not measurements of any one program; a program that knows what it
// holds sets its own weights, or its own counts with NewArena.
type ArenaWeights map[int]float64

// ArenaWeightsServer is the default, shaped after the buffers a fib server
// takes: a 16 KiB read buffer for each read in progress, on plain and TLS
// connections alike, the largest share; 32 KiB for TLS records with their
// overhead and response copies, and 64 KiB for streamed bodies, sendfile and
// QUIC send buffers next; 2 KiB for QUIC datagrams; and the small classes for
// headers, frames and messages built from them.
func ArenaWeightsServer() ArenaWeights {
	return ArenaWeights{
		64: 1, 128: 1, 256: 1, 512: 1, 1 << 10: 2, 2 << 10: 2, 4 << 10: 2, 8 << 10: 2,
		16 << 10: 6, 32 << 10: 3, 64 << 10: 3,
	}
}

// ArenaWeightsMessages is for programs that hold many small messages at once
// — WebSocket, RPC, chat, queued requests: most of the bytes go to 64 B to
// 4 KiB, with the 16 KiB read buffers still given their share.
func ArenaWeightsMessages() ArenaWeights {
	return ArenaWeights{
		64: 2, 128: 2, 256: 3, 512: 3, 1 << 10: 4, 2 << 10: 3, 4 << 10: 2, 8 << 10: 1,
		16 << 10: 4, 32 << 10: 1, 64 << 10: 1,
	}
}

// ArenaWeightsStreaming is for programs that move large bodies — HTTP/2 and
// HTTP/3 streams, TLS, files, media: most of the bytes go to 16 to 64 KiB,
// with 2 KiB for QUIC datagrams.
func ArenaWeightsStreaming() ArenaWeights {
	return ArenaWeights{
		64: 0.5, 128: 0.5, 256: 0.5, 512: 0.5, 1 << 10: 1, 2 << 10: 2, 4 << 10: 1, 8 << 10: 1,
		16 << 10: 4, 32 << 10: 4, 64 << 10: 6,
	}
}

// ArenaWeightsEven gives each class from 64 B to 64 KiB the same bytes, so a
// class has half the slots of the one below it.
func ArenaWeightsEven() ArenaWeights {
	w := ArenaWeights{}
	for size := MinSize; size <= 64<<10; size <<= 1 {
		w[size] = 1
	}
	return w
}

// ArenaCounts splits total bytes between classes by weights and returns the
// slots each gets, for NewArena; nil weights are ArenaWeightsServer. A class
// gets whole slots only, so the sum may fall a little short of total.
func ArenaCounts(total int, weights ArenaWeights) map[int]int {
	if weights == nil {
		weights = ArenaWeightsServer()
	}
	var per [numClasses]float64
	var sum float64
	for size, w := range weights {
		if size <= 0 || size > MaxSize || !(w > 0) || math.IsInf(w, 0) {
			continue
		}
		per[classUp(size)] += w
		sum += w
	}
	counts := map[int]int{}
	if total <= 0 || sum == 0 {
		return counts
	}
	for i, w := range per {
		if c := int(float64(total) * (w / sum) / float64(classSize(i))); c > 0 {
			counts[classSize(i)] = c
		}
	}
	return counts
}

// NewArenaBytes makes an Arena of about total bytes, split between classes
// by weights as ArenaCounts splits it; nil weights are ArenaWeightsServer. A
// total of zero or less is DefaultArenaBytes.
func NewArenaBytes(total int, weights ArenaWeights) (*Arena, error) {
	if total <= 0 {
		total = DefaultArenaBytes()
	}
	return NewArena(ArenaCounts(total, weights))
}

// NewArena makes an Arena with counts[size] slots of each size, rounded up to
// its class; sizes of two or more requests that round to one class add up.
// Sizes above MaxSize and counts of zero or less are left out. A nil or empty
// map makes the default arena: DefaultArenaBytes, split by
// ArenaWeightsServer. It returns an error when
// the arena would not fit in an int, or a class would have more than 2^32-1
// slots.
func NewArena(counts map[int]int) (*Arena, error) {
	if len(counts) == 0 {
		counts = ArenaCounts(DefaultArenaBytes(), nil)
	}
	var per [numClasses]int64
	var total int64
	for size, n := range counts {
		if size <= 0 || size > MaxSize || n <= 0 {
			continue
		}
		i := classUp(size)
		per[i] += int64(n)
		if per[i] > maxArenaSlots {
			return nil, fmt.Errorf("bufferpool: an arena class of %d bytes with %d slots", classSize(i), per[i])
		}
	}
	for i, n := range per {
		total += n * int64(classSize(i))
		if total > math.MaxInt || total < 0 {
			return nil, fmt.Errorf("bufferpool: an arena of more than %d bytes", math.MaxInt)
		}
	}
	a := &Arena{local: make([]arenaLocal, max(runtime.GOMAXPROCS(0), runtime.NumCPU()))}
	if total > 0 {
		a.mem = make([]byte, total)
		a.base = uintptr(unsafe.Pointer(unsafe.SliceData(a.mem)))
		a.size = uintptr(total)
	}
	off := uintptr(0)
	for i, n := range per {
		c := &a.cls[i]
		c.start, c.count = off, int(n)
		off += uintptr(n) * uintptr(classSize(i))
		c.end = off
	}
	log.Info("fib: bufferpool arena", "bytes", total, "classes", a.describe())
	return a, nil
}

// describe lists the classes with slots, as size:count.
func (a *Arena) describe() string {
	s := ""
	for i := range a.cls {
		if a.cls[i].count > 0 {
			if s != "" {
				s += " "
			}
			s += fmt.Sprintf("%d:%d", classSize(i), a.cls[i].count)
		}
	}
	return s
}

// Bytes is the size of the arena.
func (a *Arena) Bytes() int { return int(a.size) }

// Slots is how many slots the class serving size has.
func (a *Arena) Slots(size int) int {
	if size <= 0 || size > MaxSize {
		return 0
	}
	return a.cls[classUp(size)].count
}

// Free is how many slots of the class serving size are not out. It is read
// without stopping Get and Put, so while they run it is approximate.
func (a *Arena) Free(size int) int {
	if size <= 0 || size > MaxSize {
		return 0
	}
	c := &a.cls[classUp(size)]
	n := c.count - int(min(c.fresh.Load(), uint64(c.count)))
	for s := range c.depot {
		n += int(c.depot[s].n.Load())
	}
	if !raceEnabled {
		for p := range a.local {
			n += a.local[p].m[classUp(size)].n
		}
	}
	return n
}

// Lost is how many slots Put could not take back because the slice it was
// given does not start at a slot.
func (a *Arena) Lost() int64 { return a.lost.Load() }

// Get returns a slot of the class serving size, or a buffer from the
// package's own pool when the class has none free.
func (a *Arena) Get(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size <= MaxSize {
		i := classUp(size)
		if a.cls[i].count > 0 {
			if b := a.get(i); b != nil {
				return b[:size]
			}
		}
	}
	return getPooled(size)
}

// Put gives a slot back to the arena and any other buffer to the package's
// own pool.
func (a *Arena) Put(buf []byte) {
	capacity := cap(buf)
	// A capacity below MinSize is turned away first, which also keeps out an
	// empty slice taken at a slot's end: its address is that slot's start.
	if capacity < MinSize || capacity > MaxSize {
		return
	}
	if off := uintptr(unsafe.Pointer(unsafe.SliceData(buf))) - a.base; off < a.size {
		a.put(off, capacity)
		return
	}
	putPooled(buf)
}

func (a *Arena) slot(i int, idx uint32) []byte {
	size := classSize(i)
	p := unsafe.Add(unsafe.Pointer(unsafe.SliceData(a.mem)), a.cls[i].start+uintptr(idx)*uintptr(size))
	return unsafe.Slice((*byte)(p), size)
}

// get takes a free slot of class i, or returns nil when the class has none.
func (a *Arena) get(i int) []byte {
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

// getSlow takes a batch from the shared stacks, keeps one and gives the rest
// to whichever P the goroutine is on by then. The locks are taken unpinned,
// since a pinned goroutine must not block.
func (a *Arena) getSlow(i int) []byte {
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
// the start of a slot. The class is found from off: a slot cut shorter is
// still its class's slot.
func (a *Arena) put(off uintptr, capacity int) {
	i := min(classDown(capacity), numClasses-1)
	for i < numClasses && off >= a.cls[i].end {
		i++
	}
	if i == numClasses || a.cls[i].count == 0 || off < a.cls[i].start {
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
	for k := range depotShards {
		d := &c.depot[(start+k)%depotShards]
		if d.n.Load() == 0 {
			continue
		}
		d.mu.Lock()
		n := min(len(out), len(d.free))
		copy(out, d.free[len(d.free)-n:])
		d.free = d.free[:len(d.free)-n]
		d.n.Store(int64(len(d.free)))
		d.mu.Unlock()
		if n > 0 {
			return n
		}
	}
	return c.takeFresh(out)
}

// takeFresh hands out slots that have never been out, up to len(out), once
// the depots have none. Taking from the depots first keeps a program on the
// slots it has touched, so the system backs pages only as the working set
// grows.
func (c *arenaClass) takeFresh(out []uint32) int {
	if c.fresh.Load() >= uint64(c.count) {
		return 0
	}
	k := uint64(len(out))
	first := c.fresh.Add(k) - k
	if first >= uint64(c.count) {
		return 0
	}
	n := int(min(k, uint64(c.count)-first))
	for j := range n {
		out[j] = uint32(first) + uint32(j)
	}
	return n
}

func (c *arenaClass) push(in []uint32) {
	d := &c.depot[rand.Uint32()%depotShards]
	d.mu.Lock()
	d.free = append(d.free, in...)
	d.n.Store(int64(len(d.free)))
	d.mu.Unlock()
}
