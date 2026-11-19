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
// an eighth of the memory the process may use, at most 1 GiB, split by
// ArenaWeightsServer.
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
//   - With ArenaMapped, the default, on Linux, the BSDs, macOS and Windows
//     the memory is mapped outside the Go heap, so it counts towards neither
//     GOGC's goal nor GOMEMLIMIT: a program that sets GOMEMLIMIT leaves room
//     for the arena itself. With ArenaHeap, elsewhere, under the race detector
//     (which does not instrument memory outside the heap), or when the mapping
//     fails, it is made on the heap: it counts as live heap from the start,
//     raising the collector's goal by its size. Either way the operating
//     system backs a page only once a slot on it has been used.
//   - An arena SetAllocator replaces is retired: it serves nothing more, Put
//     still takes its slots back whatever is installed, and once every slot
//     it handed out has come back its memory is released — unmapped, or left
//     to the collector when it was made on the heap. A slot that is never Put
//     keeps it from being released. Released says when it has been.
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
	// mapped is set when mem was mapped outside the Go heap.
	mapped bool

	// state is arenaActive until SetAllocator replaces the arena; see retire.
	state atomic.Int32
	// outstanding is how many slots are out, counted once the arena has
	// quiesced, and counted down as they come back.
	outstanding atomic.Int64
	// retireMu orders a retired arena's lost slots with quiesce's count.
	retireMu sync.Mutex
}

// An arena's states. Active serves and takes back slots. Retiring serves
// nothing and waits for Get and Put that began before it to finish. Quiesced
// has counted the slots still out and counts them down as they come back.
// Released has given its memory back.
const (
	arenaActive int32 = iota
	arenaRetiring
	arenaQuiesced
	arenaReleased
)

// ArenaMemory is where NewArena and NewArenaBytes make an arena's memory.
type ArenaMemory uint8

const (
	// ArenaMapped maps the memory outside the Go heap where the platform
	// allows it (Linux, the BSDs and macOS with mmap, Windows with
	// VirtualAlloc), and makes it on the heap elsewhere, under the race
	// detector, or when the mapping fails. It is the zero value.
	ArenaMapped ArenaMemory = iota
	// ArenaHeap makes the memory one []byte on the Go heap.
	ArenaHeap
)

type arenaClass struct {
	start, end uintptr
	count      int
	// freshMu orders takeFresh with quiesce's count.
	freshMu sync.Mutex
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
	// process may use, read as the default retention budget reads it. The
	// package's own pool still serves what the arena cannot, with that budget
	// of its own.
	DefaultArenaDivisor = 8
	// MinDefaultArenaBytes and MaxDefaultArenaBytes bound the default arena;
	// MinDefaultArenaBytes is also what it is when the memory cannot be read.
	// MaxDefaultArenaBytes keeps a large machine from getting an arena sized
	// to its memory rather than to what a program holds: 1 GiB is 65k 16 KiB
	// read buffers, or a million 1 KiB messages, out at once, and the pool
	// serves whatever is past it. It bounds the default only: NewArenaBytes
	// and NewArena make an arena of any size they are given.
	MinDefaultArenaBytes = DefaultRetainedBytes
	MaxDefaultArenaBytes = 1 << 30

	// maxArenaSlots keeps a slot's index within a uint32.
	maxArenaSlots = math.MaxUint32
)

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

// DefaultArenaBytes is the size of the arena NewArena(nil, ArenaMapped) makes: an eighth
// of the memory the process may use (see SetRetainedPercent), at least 32 MiB
// and at most MaxDefaultArenaBytes, 1 GiB.
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
// by weights as ArenaCounts splits it, with its memory where memory says;
// nil weights are ArenaWeightsServer. A total of zero or less is
// DefaultArenaBytes.
func NewArenaBytes(total int, weights ArenaWeights, memory ArenaMemory) (*Arena, error) {
	if total <= 0 {
		total = DefaultArenaBytes()
	}
	return NewArena(ArenaCounts(total, weights), memory)
}

// NewArena makes an Arena with counts[size] slots of each size, rounded up to
// its class; sizes of two or more requests that round to one class add up.
// Sizes above MaxSize and counts of zero or less are left out. A nil or empty
// map makes the default arena: DefaultArenaBytes, split by
// ArenaWeightsServer. Its memory is where memory says. It returns an error
// when the arena would not fit in an int, or a class would have more than
// 2^32-1 slots.
func NewArena(counts map[int]int, memory ArenaMemory) (*Arena, error) {
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
		if memory == ArenaHeap {
			a.mem = make([]byte, total)
		} else {
			a.mem, a.mapped = arenaMemory(int(total))
		}
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
	log.Info("fib: bufferpool arena", "bytes", total, "outsideHeap", a.mapped, "classes", a.describe())
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

// OutsideHeap says whether the arena's memory was mapped outside the Go heap
// (see the Arena type) rather than made on it, as ArenaHeap asks for and as
// it is where mapping is not available.
func (a *Arena) OutsideHeap() bool { return a.mapped }

// Released says whether the arena has given its memory back, which it does
// once SetAllocator has replaced it and every slot it handed out has come
// back.
func (a *Arena) Released() bool { return a.state.Load() == arenaReleased }

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
		if a.pop(i, one[:]) == 0 {
			return nil
		}
		return a.slot(i, one[0])
	}
	pid := procPin()
	// Read pinned: retire waits out every pinned section that began before
	// the state changed, so one that sees it active finishes before the
	// slots out are counted.
	if a.state.Load() != arenaActive {
		procUnpin()
		return nil
	}
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
	n := a.pop(i, batch[:])
	if n == 0 {
		return nil
	}
	n--
	idx := batch[n]
	if n > 0 {
		pid := procPin()
		// The batch left the depots before the arena may have been retired;
		// once it has, the per-P stacks are counted and the rest goes back
		// through push.
		if pid < len(a.local) && a.state.Load() == arenaActive {
			m := &a.local[pid].m[i]
			k := min(n, magSize-m.n)
			copy(m.s[m.n:], batch[n-k:n])
			m.n += k
			n -= k
		}
		procUnpin()
		if n > 0 {
			a.push(i, batch[:n])
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
		a.putLost()
		return
	}
	rel := off - a.cls[i].start
	if rel&uintptr(classSize(i)-1) != 0 {
		a.putLost()
		return
	}
	idx := uint32(rel >> (uint(i) + minShift))
	if raceEnabled {
		a.push(i, []uint32{idx})
		return
	}
	pid := procPin()
	if a.state.Load() != arenaActive {
		// Retired: past the per-P stacks, which quiesce counts once.
		procUnpin()
		a.push(i, []uint32{idx})
		return
	}
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
		a.push(i, batch[:])
		return
	}
	procUnpin()
	a.push(i, []uint32{idx})
}

// putLost counts a slot Put could not take back. Once the arena has quiesced
// it still came back, so it counts down what is out.
func (a *Arena) putLost() {
	if a.state.Load() == arenaActive {
		a.lost.Add(1)
		return
	}
	a.retireMu.Lock()
	quiesced := a.state.Load() >= arenaQuiesced
	if !quiesced {
		a.lost.Add(1)
	}
	a.retireMu.Unlock()
	if quiesced {
		a.returned(1)
	}
}

// pop takes up to len(out) free slots of class i: from the depots, or ones
// never out. A retired arena gives none: its depot locks order that with
// quiesce's count.
func (a *Arena) pop(i int, out []uint32) int {
	c := &a.cls[i]
	// Unsigned, so that the shard stays in range where int is 32 bits.
	start := rand.Uint32()
	for k := range uint32(depotShards) {
		d := &c.depot[(start+k)%depotShards]
		if d.n.Load() == 0 {
			continue
		}
		d.mu.Lock()
		if a.state.Load() != arenaActive {
			d.mu.Unlock()
			return 0
		}
		n := min(len(out), len(d.free))
		copy(out, d.free[len(d.free)-n:])
		d.free = d.free[:len(d.free)-n]
		d.n.Store(int64(len(d.free)))
		d.mu.Unlock()
		if n > 0 {
			return n
		}
	}
	return a.takeFresh(i, out)
}

// takeFresh hands out slots that have never been out, up to len(out), once
// the depots have none. Taking from the depots first keeps a program on the
// slots it has touched, so the system backs pages only as the working set
// grows.
func (a *Arena) takeFresh(i int, out []uint32) int {
	c := &a.cls[i]
	if c.fresh.Load() >= uint64(c.count) {
		return 0
	}
	c.freshMu.Lock()
	defer c.freshMu.Unlock()
	if a.state.Load() != arenaActive {
		return 0
	}
	first := c.fresh.Load()
	if first >= uint64(c.count) {
		return 0
	}
	n := int(min(uint64(len(out)), uint64(c.count)-first))
	for j := range n {
		out[j] = uint32(first) + uint32(j)
	}
	c.fresh.Store(first + uint64(n))
	return n
}

// push gives slots of class i back to a depot. Once the arena has quiesced
// they count down what is out instead: the depot lock orders that with
// quiesce, which holds every one while it counts.
func (a *Arena) push(i int, in []uint32) {
	d := &a.cls[i].depot[rand.Uint32()%depotShards]
	d.mu.Lock()
	if a.state.Load() >= arenaQuiesced {
		d.mu.Unlock()
		a.returned(len(in))
		return
	}
	d.free = append(d.free, in...)
	d.n.Store(int64(len(d.free)))
	d.mu.Unlock()
}

// retire stops the arena serving and, once Get and Put that began before
// have finished, counts the slots still out; see quiesce.
func (a *Arena) retire() {
	if !a.state.CompareAndSwap(arenaActive, arenaRetiring) {
		return
	}
	addRetired(a)
	go a.quiesce()
}

// quiesce waits until no Get or Put that saw the arena active can still be
// using its per-P stacks, then counts the slots out: handed out, less those
// free in the depots and per-P stacks, less those Put back but lost.
//
// A pinned section cannot be preempted, and a collection stops the world,
// which waits for every running one to end: after runtime.GC returns, each
// pinned section is either finished or began after the state changed and so
// left the arena alone. The depot and fresh locks, all held while counting,
// order the unpinned paths: pop and takeFresh give nothing once retired, and
// push counts down instead of filling a depot once quiesced.
func (a *Arena) quiesce() {
	runtime.GC()
	a.retireMu.Lock()
	for i := range a.cls {
		a.cls[i].freshMu.Lock()
		for s := range a.cls[i].depot {
			a.cls[i].depot[s].mu.Lock()
		}
	}
	out := -a.lost.Load()
	for i := range a.cls {
		c := &a.cls[i]
		out += int64(min(c.fresh.Load(), uint64(c.count)))
		for s := range c.depot {
			out -= int64(len(c.depot[s].free))
			c.depot[s].free = nil
			c.depot[s].n.Store(0)
		}
		for p := range a.local {
			out -= int64(a.local[p].m[i].n)
			a.local[p].m[i].n = 0
		}
	}
	a.outstanding.Store(out)
	a.state.Store(arenaQuiesced)
	for i := range a.cls {
		for s := range a.cls[i].depot {
			a.cls[i].depot[s].mu.Unlock()
		}
		a.cls[i].freshMu.Unlock()
	}
	a.retireMu.Unlock()
	if out <= 0 {
		a.release()
	}
}

// returned counts down n slots that came back to a quiesced arena, and
// releases it when the last one has.
func (a *Arena) returned(n int) {
	if a.outstanding.Add(-int64(n)) == 0 {
		a.release()
	}
}

// release gives the memory back: unmaps it, or drops it from the retired
// list so that the collector can free it once nothing else refers to it.
func (a *Arena) release() {
	if !a.state.CompareAndSwap(arenaQuiesced, arenaReleased) {
		return
	}
	removeRetired(a)
	if a.mapped {
		arenaRelease(a.mem)
	}
	log.Info("fib: bufferpool arena released", "bytes", a.size, "outsideHeap", a.mapped)
}

// retired lists the arenas SetAllocator replaced that still have slots out,
// so that Put sends those slots back to them whatever is installed. It is
// nil while there are none, which keeps Put's check to one load.
var (
	retired   atomic.Pointer[[]*Arena]
	retiredMu sync.Mutex
)

func addRetired(a *Arena) {
	retiredMu.Lock()
	defer retiredMu.Unlock()
	var list []*Arena
	if p := retired.Load(); p != nil {
		list = append(list, *p...)
	}
	list = append(list, a)
	retired.Store(&list)
}

func removeRetired(a *Arena) {
	retiredMu.Lock()
	defer retiredMu.Unlock()
	p := retired.Load()
	if p == nil {
		return
	}
	var list []*Arena
	for _, r := range *p {
		if r != a {
			list = append(list, r)
		}
	}
	if len(list) == 0 {
		retired.Store(nil)
		return
	}
	retired.Store(&list)
}

// putRetired gives buf back to the retired arena it came from, if any, and
// says whether it did.
func putRetired(buf []byte) bool {
	p := retired.Load()
	if p == nil {
		return false
	}
	capacity := cap(buf)
	if capacity < MinSize || capacity > MaxSize {
		return false
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	for _, a := range *p {
		if off := addr - a.base; off < a.size {
			a.put(off, capacity)
			return true
		}
	}
	return false
}
