// Package bufferpool recycles byte buffers in aligned size classes.
//
// Every buffer it hands out has a capacity that is a power of two, so a
// request is served from one of a small number of classes rather than from an
// allocation shaped to the request. The alignment is what makes the pool worth
// having twice over.
//
// It is what makes recycling possible at all: buffers sized to their requests
// are all slightly different, and a pool of them either hands back one that is
// too small or keeps a class per distinct size. Rounding a request up to its
// class means any buffer in that class can serve it, so a class needs to
// retain only as many buffers as are in flight at once.
//
// It is also what makes the underlying allocation exact. Go's allocator rounds
// every object up to one of its own size classes, and a request that falls
// just past one — 4200 bytes, say — is served by a 4864-byte object with the
// remainder wasted. Every power of two from 64 bytes up is one of those size
// classes exactly, and past 32 KiB, where the allocator switches to whole
// pages, a power of two is an exact multiple of the 8 KiB page. So an aligned
// buffer never pays for rounding, and the class index is one instruction
// (bits.Len) rather than a search.
//
// The pool is a package-level one rather than a type, so that buffers are
// shared across every part of a program that takes part: a connection's read
// buffer, the reply built from it and a TLS record are all the same buffers
// moving between classes, and a second pool would only mean each of them
// retaining idle buffers the others could have used. For the same reason a
// program that wants its buffers from somewhere else installs one Allocator
// for all of them with SetAllocator, and Get and Put go to it instead.
//
// # What it costs the collector
//
// Nothing is allocated to keep a buffer. A class holds the address of each of
// its buffers and works the length out from the class, so recycling adds no
// object of its own to the heap, and buffers hold no pointers, so the
// collector never scans one.
//
// What is left is that a sync.Pool is emptied at every collection, and what it
// empties is the program's working set: measured here, a thousand idle 16 KiB
// buffers cost 16.8MB of fresh allocation after each collection, which is
// itself part of what brings on the next one. So each class keeps a reserve
// behind its pool, in an ordinary slice the collector leaves alone, and the
// pool falls back to it when a collection has just emptied the pool.
//
// A reserve is refilled in a burst once a collection has been noticed — the
// package has the collector count them for it — rather than a little on every
// return. That is what keeps it off the ordinary path: a return between
// collections reads one counter and goes to the sync.Pool, and the reserve's
// locks are taken only for the burst that follows a collection, which is as
// many operations as the reserve holds buffers and saves that many
// allocations. The reserve is split into shards with a lock each, so that a
// burst taken and returned from every core at once is not one queue. A
// reserve grows only towards what its class has actually had to make, gives
// back what a burst could not fill, and stops at what SetRetainedBytes allows.
//
// It gives back only once its class has gone a while without coming up short.
// Buffers a program holds for longer than collections are apart — requests
// queued for seconds while collections come every few hundred milliseconds —
// are still out when the next collection comes, and a burst that could not be
// filled then says only that they have not come back yet. So a class keeps
// its target for SetRetainedFor's window after it last had to make a buffer or
// ran its reserve dry, and a class that has seen no returns at all for that
// long gives its reserve back from the collection itself, since it has no
// returns left to do it.
//
// The bytes in a buffer Get returns are whatever the last user left there.
// Callers write before they read, as they would with any recycled buffer.
package bufferpool

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/log"
)

const (
	// minShift puts the smallest class at 64 bytes, which is a cache line on
	// every platform this runs on. Below that the pool costs more than the
	// allocation it saves: Go allocates tiny objects from a per-P tiny
	// allocator that is already faster than an atomic pool operation.
	minShift = 6
	// maxShift puts the largest class at 64 MiB. A buffer past it is left to
	// the collector rather than pooled, since retaining one that big for a
	// rare request costs more than making it again.
	maxShift = 26

	// MinSize is the capacity of the smallest class: a request for fewer
	// bytes than this still comes back with this much room.
	MinSize = 1 << minShift
	// MaxSize is the capacity of the largest class. Get serves a larger
	// request with a fresh allocation, and Put drops a larger buffer.
	MaxSize = 1 << maxShift

	numClasses = maxShift - minShift + 1
)

// class is one size class: a sync.Pool for the traffic, and behind it a
// reserve holding what a collection would otherwise take away.
type class struct {
	// pool is the fast path. The runtime shards it per P, so an ordinary take
	// and return costs no lock. It holds each buffer's address rather than
	// the buffer: a sync.Pool stores an any, a slice header does not fit in
	// one and would be copied to the heap on the way in, while a pointer fits
	// exactly — and every buffer in a class is the same length, so the
	// address is all there is to keep.
	pool sync.Pool

	// shards is the reserve, split into lists with a lock each, so that the
	// takes and returns that follow a collection — as many as the reserve
	// holds buffers — do not all queue on one lock. held is what the lists
	// hold between them, so that a return can see the reserve is full and a
	// take that it is empty without taking a lock; the shards have no
	// capacity of their own, so none is ever full while the class has room.
	// held is raised before a return is stored and lowered after a take has
	// removed one, so it may run a buffer or two ahead of the lists but never
	// behind them.
	shards atomic.Pointer[reserveShards]
	held   atomic.Int64
	// size is this class's capacity, kept here so that a class can price its
	// own target against the budget.
	size int64
	// target is how many buffers this class is entitled to keep. It rises by
	// one each time a request had to be served by making a buffer and falls
	// back to what a burst managed to keep when it could not fill it, so it
	// settles at what the program keeps in flight, and stops at what is left
	// of the retention budget.
	target atomic.Int64
	// refill is how many more returns may go to the reserve instead of the
	// pool. It is set to target when a collection is first noticed and then
	// spent, which is what confines refilling to a burst after a collection.
	refill atomic.Int64
	// epoch is the collection this class last began refilling for.
	epoch atomic.Uint64
	// short is when this class last came up short — a Get that had to make a
	// buffer, or a take that emptied the reserve — in nanoseconds since
	// start. Both are paths that already cost far more than reading the
	// clock, and nothing on the ordinary path touches it.
	short atomic.Int64
	// pad keeps one class's counters off the cache line of the next one: the
	// class a server reads into and the class it replies from are neighbours
	// here, and they are busiest at the same moment.
	_ [64]byte
}

var classes [numClasses]class

// reserveShards is one class's reserve: a power of two of lists, so that a
// shard is picked with a mask.
type reserveShards struct {
	mask int
	list []reserveShard
}

type reserveShard struct {
	mu   sync.Mutex
	free []*byte
	// n mirrors len(free), so that a take can pass an empty shard without
	// its lock.
	n atomic.Int64
	// retired marks a shard SetReserveShards has replaced and emptied: a
	// return that picked it before the swap tries again with the new ones.
	retired bool
	_       [64]byte
}

// The default split of a class's reserve is DefaultReserveShardsPerP lists
// for each P, at least MinDefaultReserveShards and at most
// MaxDefaultReserveShards. Several per P, since what contends for a reserve
// is every goroutine returning at once, not only the running ones: a
// goroutine preempted holding a shard's lock stalls the others that picked
// it, and more shards make that less likely. Each shard costs about 2.4 KiB
// across the classes, so the most the default spends is about 300 KiB.
const (
	DefaultReserveShardsPerP = 4
	MinDefaultReserveShards  = 16
	MaxDefaultReserveShards  = 128
	// maxReserveShards bounds what SetReserveShards accepts.
	maxReserveShards = 1024
)

// defaultReserveShards is the split for the program's GOMAXPROCS.
func defaultReserveShards() int {
	return min(max(DefaultReserveShardsPerP*runtime.GOMAXPROCS(0), MinDefaultReserveShards), MaxDefaultReserveShards)
}

// shardsMu serializes SetReserveShards.
var shardsMu sync.Mutex

// SetReserveShards splits each class's reserve into n lists, rounded up to a
// power of two and at most 1024, each with a lock of its own, and returns how
// many it set. More shards spread the takes and returns that follow a
// collection over more locks, which pays once a reserve holds many thousands
// of buffers taken and returned from many goroutines at once; each shard
// costs about a cache line per class. What the reserves hold is moved to the
// new lists, so it may be called at any time, though it is meant for a
// program's start. It logs the new split.
func SetReserveShards(n int) int {
	n = setReserveShards(n)
	log.Info("fib: bufferpool reserve shards", "shards", n)
	return n
}

func setReserveShards(n int) int {
	n = 1 << bits.Len(uint(min(max(n, 1), maxReserveShards)-1))
	shardsMu.Lock()
	defer shardsMu.Unlock()
	for i := range classes {
		c := &classes[i]
		next := &reserveShards{mask: n - 1, list: make([]reserveShard, n)}
		prev := c.shards.Swap(next)
		if prev == nil {
			continue
		}
		k := 0
		for j := range prev.list {
			old := &prev.list[j]
			old.mu.Lock()
			free := old.free
			old.free, old.retired = nil, true
			old.n.Store(0)
			old.mu.Unlock()
			for _, p := range free {
				sh := &next.list[k&next.mask]
				k++
				sh.mu.Lock()
				sh.free = append(sh.free, p)
				sh.n.Store(int64(len(sh.free)))
				sh.mu.Unlock()
			}
		}
	}
	return n
}

// ReserveShards reports how many lists each class's reserve is split into.
func ReserveShards() int { return classes[0].shards.Load().mask + 1 }

// gcEpoch counts collections, so that a class can tell its pool has been
// emptied. Reading it is one atomic load of a line every core already has,
// which is what keeps refilling off the ordinary path.
var gcEpoch atomic.Uint64

// collectionTick is the object whose collection is the signal. It is well
// clear of the 16 bytes below which the runtime packs several objects into one
// tiny block: a tick in such a block is freed only once everything sharing it
// is, so a small one silently never ticks at all.
type collectionTick struct{ _ [32]byte }

// watchCollections arranges for gcEpoch to count collections: the tick it
// allocates is unreachable the moment this returns, so its cleanup runs once
// the next collection has swept it, and sets up the one after.
func watchCollections() {
	runtime.AddCleanup(new(collectionTick), func(struct{}) {
		epoch := gcEpoch.Add(1)
		releaseIdle(epoch)
		watchCollections()
	}, struct{}{})
}

// start is the origin of the clock the classes stamp, so that a stamp is a
// monotonic reading rather than the wall clock.
var start = time.Now()

func now() int64 { return int64(time.Since(start)) }

// releaseIdle gives back the reserve of every class that has seen no return
// for two whole collections and has not come up short within the window.
// Such a class would otherwise keep its reserve for good: a reserve shrinks
// at the start of a burst, and a burst starts with a return.
func releaseIdle(epoch uint64) {
	t := now()
	window := retainFor.Load()
	for i := range classes {
		c := &classes[i]
		if c.target.Load() == 0 || epoch-c.epoch.Load() < 2 || t-c.short.Load() < window {
			continue
		}
		c.aim(0)
		c.shrink()
	}
}

// DefaultRetainedFor is how long a class keeps its target after it last came
// up short, until SetRetainedFor says otherwise.
const DefaultRetainedFor = time.Minute

var retainFor atomic.Int64

// SetRetainedFor sets how long a class keeps what it is entitled to hold after
// the last time it had to make a buffer or ran its reserve dry. It should be
// longer than a buffer is held — a request waiting in a queue, a response
// waiting on a slow peer — so that buffers still out when a collection comes
// are waited for rather than written off. Zero gives back at the first
// collection a burst could not fill. The window only delays giving back:
// SetRetainedBytes bounds what is held either way.
func SetRetainedFor(d time.Duration) {
	if d < 0 {
		d = 0
	}
	retainFor.Store(int64(d))
}

// The default budget is DefaultRetainedPercent of the memory the process may
// use — GOMEMLIMIT where it is set and lower, else the container's limit, else
// the machine's memory — at least DefaultRetainedBytes and at most
// MaxDefaultRetainedBytes, until SetRetainedBytes or SetRetainedPercent says
// otherwise. Where the memory cannot be read it is DefaultRetainedBytes.
//
// It is a ceiling, not an allocation: a reserve grows only towards what its
// class has had to make, so a program that cycles less than the budget keeps
// less than it. What the reserves do keep is live heap, which the collector's
// goal doubles under GOGC=100, so an eighth of the memory held costs up to a
// quarter of it at the peak.
const (
	DefaultRetainedPercent  = 12.5
	DefaultRetainedBytes    = 32 << 20
	MaxDefaultRetainedBytes = 16 << 30
)

// memoryLimit is what the process may use, GOMEMLIMIT included, or zero when
// it cannot be read. It is a variable so that a test can stand in for the
// machine.
var memoryLimit = func() int64 {
	limit := sys.MemoryLimit()
	if soft := debug.SetMemoryLimit(-1); soft > 0 && soft < math.MaxInt64 && (limit <= 0 || soft < limit) {
		limit = soft
	}
	return limit
}

// defaultRetainedBytes is the budget for the memory the process may use, and
// that memory, zero when it could not be read.
func defaultRetainedBytes() (bytes int, memory int64) {
	memory = memoryLimit()
	if memory <= 0 {
		return DefaultRetainedBytes, 0
	}
	return clampInt(min(max(int64(float64(memory)*DefaultRetainedPercent/100), DefaultRetainedBytes), MaxDefaultRetainedBytes)), memory
}

// clampInt converts n to an int, at most math.MaxInt, which a 32-bit platform
// reaches well below the budgets a large machine is given.
func clampInt(n int64) int { return int(min(n, math.MaxInt)) }

// budget is the ceiling SetRetainedBytes sets, and committed is what the
// classes' targets add up to, which is what the reserves may grow to hold.
var (
	budget    atomic.Int64
	committed atomic.Int64
)

func init() {
	for i := range classes {
		classes[i].size = int64(classSize(i))
	}
	bytes, memory := defaultRetainedBytes()
	budget.Store(int64(bytes))
	retainFor.Store(int64(DefaultRetainedFor))
	shards := setReserveShards(defaultReserveShards())
	// At Debug, so that every program importing the package does not print
	// a line at its start; what a program sets itself is logged at Info.
	log.Debug("fib: bufferpool defaults", "retainedBytes", bytes, "memory", memory,
		"reserveShards", shards, "retainedFor", DefaultRetainedFor)
	watchCollections()
}

// SetRetainedBytes bounds how much the pool may keep across collections, in
// bytes. Zero turns the reserves off and leaves the sync.Pool in front of
// them, so that buffers are recycled while they are in use and made again
// after each collection. Lowering it gives back what is already reserved,
// largest class first, so SetRetainedBytes(0) releases the pool's memory.
//
// A reserve only grows towards what its class has had to make, so a program
// that needs less than the budget keeps less than it.
//
// It logs the new budget.
func SetRetainedBytes(bytes int) {
	bytes = setRetainedBytes(bytes)
	log.Info("fib: bufferpool retained bytes", "retainedBytes", bytes)
}

func setRetainedBytes(bytes int) int {
	if bytes < 0 {
		bytes = 0
	}
	budget.Store(int64(bytes))
	if committed.Load() > int64(bytes) {
		trim()
	}
	return bytes
}

// SetRetainedPercent sets the budget to percent of the memory the process may
// use — GOMEMLIMIT where it is set and lower, else the container's limit, else
// the machine's memory — and returns the budget in bytes. Zero turns the
// reserves off, as SetRetainedBytes(0) does. It fails, leaving the budget as
// it was, for a percent outside 0 to 100 or where the memory cannot be read.
// It logs the new budget.
//
// Mind that what the reserves keep is live heap, which the collector's goal
// doubles under GOGC=100: a quarter of the memory held can cost half of it at
// the peak.
func SetRetainedPercent(percent float64) (int, error) {
	if !(percent >= 0 && percent <= 100) {
		return 0, fmt.Errorf("bufferpool: retained percent %v is outside 0 to 100", percent)
	}
	memory := memoryLimit()
	if memory <= 0 {
		return 0, errors.New("bufferpool: the memory the process may use could not be read")
	}
	bytes := setRetainedBytes(clampInt(int64(float64(memory) * percent / 100)))
	log.Info("fib: bufferpool retained bytes", "retainedBytes", bytes, "percent", percent, "memory", memory)
	return bytes, nil
}

// RetainedBytes reports what the reserves are currently entitled to hold.
func RetainedBytes() int { return int(committed.Load()) }

// RetainedBytesLimit reports the budget: what SetRetainedBytes or
// SetRetainedPercent last set, or the default for the process's memory.
func RetainedBytesLimit() int { return clampInt(budget.Load()) }

// reserveMore lets this class keep one more buffer, if the budget has room for
// one. It is called where a request had to be served by making a buffer, which
// is the evidence that the class holds less than the program keeps in flight.
func (c *class) reserveMore() {
	if committed.Add(c.size) > budget.Load() {
		committed.Add(-c.size)
		return
	}
	c.target.Add(1)
	c.short.Store(now())
}

// aim moves this class's target, giving the difference back to the budget or
// taking it, so that committed always says what the classes may hold.
func (c *class) aim(target int64) {
	committed.Add((target - c.target.Swap(target)) * c.size)
}

// trim gives back what the classes hold above the budget, largest class first,
// since a buffer there is worth many of a small one.
func trim() {
	for i := numClasses - 1; i >= 0; i-- {
		c := &classes[i]
		// At most as many steps as the target had when trim came to it: misses
		// raising it again meanwhile would otherwise keep this going as long
		// as they come.
		for steps := c.target.Load(); steps > 0 && committed.Load() > budget.Load(); steps-- {
			target := c.target.Load()
			if target == 0 {
				break
			}
			// A compare-and-swap, since a miss may raise the target at the
			// same moment: storing over it would lose that rise while the
			// budget still counts it, and the budget would stay that much
			// short for good.
			if c.target.CompareAndSwap(target, target-1) {
				committed.Add(-c.size)
			}
		}
		c.shrink()
	}
}

// shrink drops what the reserve holds above the class's target.
func (c *class) shrink() {
	set := c.shards.Load()
	for k := range set.list {
		excess := c.held.Load() - c.target.Load()
		if excess <= 0 {
			return
		}
		sh := &set.list[k]
		sh.mu.Lock()
		drop := min(excess, int64(len(sh.free)))
		for j := int64(0); j < drop; j++ {
			last := len(sh.free) - 1
			sh.free[last] = nil
			sh.free = sh.free[:last]
		}
		sh.n.Store(int64(len(sh.free)))
		sh.mu.Unlock()
		c.held.Add(-drop)
	}
}

// Align returns the capacity a buffer of size bytes is given: size rounded up
// to a power of two, and at least MinSize. A size larger than MaxSize is
// returned unchanged, since it is allocated as asked rather than pooled.
func Align(size int) int {
	if size <= MinSize {
		if size <= 0 {
			return 0
		}
		return MinSize
	}
	if size > MaxSize {
		return size
	}
	return 1 << uint(bits.Len(uint(size-1)))
}

// classUp is the class that can serve size bytes: the request rounded up.
// Callers check that size is within MinSize..MaxSize first.
func classUp(size int) int {
	if size <= MinSize {
		return 0
	}
	return bits.Len(uint(size-1)) - minShift
}

// classDown is the class a buffer of capacity bytes belongs to: the capacity
// rounded down, so that every buffer in a class really does hold at least the
// class size. A buffer whose capacity is not aligned — one grown by append, or
// one the caller made itself — is kept at the next class down rather than
// turned away, since a buffer with spare room is still a buffer.
func classDown(capacity int) int { return bits.Len(uint(capacity)) - 1 - minShift }

// classSize is the capacity of class i.
func classSize(i int) int { return MinSize << uint(i) }

// Get returns a buffer of len size, with the capacity Align reports. A size of
// zero or less returns nil, so that Get(len(x)) on an empty x costs nothing.
//
// With an Allocator set, Get returns what the allocator's Get returns for
// sizes above zero.
func Get(size int) []byte {
	if size <= 0 {
		return nil
	}
	if a := allocator.Load(); a != nil {
		return a.Get(size)
	}
	if size > MaxSize {
		return make([]byte, size)
	}
	i := classUp(size)
	c := &classes[i]
	if p, _ := c.pool.Get().(*byte); p != nil {
		return unsafe.Slice(p, classSize(i))[:size]
	}
	// An empty pool is what every collection leaves behind. The reserve is
	// what the collection could not take.
	if c.held.Load() > 0 {
		if p := c.take(); p != nil {
			return unsafe.Slice(p, classSize(i))[:size]
		}
	}
	c.reserveMore()
	return make([]byte, size, classSize(i))
}

// getPooled is Get from the package's own pool, for a size above zero, for
// Builtin. Get keeps its own copy of the body rather than calling this: the
// call alone measured 1.7ns on a Get+Put pair that costs 11.5.
func getPooled(size int) []byte {
	if size > MaxSize {
		return make([]byte, size)
	}
	i := classUp(size)
	c := &classes[i]
	if p, _ := c.pool.Get().(*byte); p != nil {
		return unsafe.Slice(p, classSize(i))[:size]
	}
	// An empty pool is what every collection leaves behind. The reserve is
	// what the collection could not take.
	if c.held.Load() > 0 {
		if p := c.take(); p != nil {
			return unsafe.Slice(p, classSize(i))[:size]
		}
	}
	c.reserveMore()
	return make([]byte, size, classSize(i))
}

// take removes a buffer from the reserve, starting at a random shard and
// moving past empty ones, or reports that there was none.
func (c *class) take() *byte {
	set := c.shards.Load()
	start := int(rand.Uint32())
	for k := 0; k <= set.mask; k++ {
		sh := &set.list[(start+k)&set.mask]
		if sh.n.Load() == 0 {
			continue
		}
		sh.mu.Lock()
		last := len(sh.free) - 1
		if last < 0 {
			sh.mu.Unlock()
			continue
		}
		p := sh.free[last]
		sh.free[last] = nil
		sh.free = sh.free[:last]
		sh.n.Store(int64(last))
		sh.mu.Unlock()
		if c.held.Add(-1) <= 0 {
			// Running the reserve dry is coming up short as much as a miss
			// is.
			c.short.Store(now())
		}
		return p
	}
	return nil
}

// Put returns buf to the pool. The caller must not use buf, or any slice of
// it, afterwards. A buffer too small or too large to belong to a class is
// dropped, so Put may be called on any buffer without the caller having to
// know where it came from.
//
// With an Allocator set, Put hands every buffer with a capacity to the
// allocator's Put, which decides what to do with one it did not hand out.
func Put(buf []byte) {
	if a := allocator.Load(); a != nil {
		if cap(buf) > 0 {
			a.Put(buf)
		}
		return
	}
	capacity := cap(buf)
	if capacity < MinSize || capacity > MaxSize {
		return
	}
	i := classDown(capacity)
	c := &classes[i]
	// Trimmed to the class size, so that every buffer in a class is exactly
	// that size and what Get hands out is always Align of what was asked for,
	// whatever shape the buffer had before it came here.
	size := classSize(i)
	p := unsafe.SliceData(buf[:size:size])
	if gcEpoch.Load() != c.epoch.Load() {
		c.beginRefill()
	}
	if c.refill.Load() > 0 && c.keep(p) {
		return
	}
	c.pool.Put(p)
}

// putPooled is Put to the package's own pool, for Builtin. Put keeps its own
// copy of the body, as Get does.
func putPooled(buf []byte) {
	capacity := cap(buf)
	if capacity < MinSize || capacity > MaxSize {
		return
	}
	i := classDown(capacity)
	c := &classes[i]
	// Trimmed to the class size, so that every buffer in a class is exactly
	// that size and what Get hands out is always Align of what was asked for,
	// whatever shape the buffer had before it came here.
	size := classSize(i)
	p := unsafe.SliceData(buf[:size:size])
	if gcEpoch.Load() != c.epoch.Load() {
		c.beginRefill()
	}
	if c.refill.Load() > 0 && c.keep(p) {
		return
	}
	c.pool.Put(p)
}

// beginRefill starts this class's burst of returns into the reserve, once per
// collection. The swap settles which caller noticed it first.
//
// A burst that still had allowance left when the next collection came around
// ran out of returns before it filled the reserve, which says the class no
// longer keeps that many buffers in flight. Its target drops to what the burst
// did manage to keep, and the difference goes back to the budget for a class
// that can use it. That is what stops a target raised by one busy stretch from
// holding memory for the rest of the program's life.
//
// Unless the class came up short within the window SetRetainedFor sets. Then
// the returns the burst was waiting for are more likely still out — held by
// requests that take longer than collections are apart — than gone, and
// giving back now would leave the reserve empty for when they are taken
// again.
func (c *class) beginRefill() {
	epoch := gcEpoch.Load()
	if c.epoch.Swap(epoch) == epoch {
		return
	}
	if c.refill.Load() > 0 && now()-c.short.Load() >= retainFor.Load() {
		c.aim(c.held.Load())
	}
	c.refill.Store(c.target.Load())
}

// keep puts a buffer in the reserve and reports whether it took it. The burst
// is spent whether or not a return finds room, so a class the program no
// longer keeps that many of in flight stops taking the lock instead of
// reaching for a target it cannot fill.
func (c *class) keep(p *byte) bool {
	// Spent, it is left below zero rather than set back to it: a burst that
	// began between the two would be wiped, and the reserve left unfilled for
	// a whole collection. Put stops calling here once it reads zero or less,
	// so it goes only as far below as there are returns racing it.
	if c.refill.Add(-1) < 0 {
		return false
	}
	if c.held.Add(1) > c.target.Load() {
		c.held.Add(-1)
		return false
	}
	for {
		set := c.shards.Load()
		sh := &set.list[int(rand.Uint32())&set.mask]
		sh.mu.Lock()
		if !sh.retired {
			sh.free = append(sh.free, p)
			sh.n.Store(int64(len(sh.free)))
			sh.mu.Unlock()
			return true
		}
		sh.mu.Unlock()
	}
}

// Grow returns a buffer holding buf's bytes with room for capacity of them in
// total. buf is recycled when a larger one has to replace it, so the caller
// must not use it afterwards. A buffer that already has the room is returned
// as it is, which is the case worth keeping cheap: it is what every append to
// a buffer that has reached its working size does, so the test for it is all
// this function is, and the replacement is a call of its own.
func Grow(buf []byte, capacity int) []byte {
	if capacity <= cap(buf) {
		return buf
	}
	return regrow(buf, capacity)
}

// regrow moves buf's bytes into a buffer from the class that holds capacity of
// them and returns buf to the pool.
func regrow(buf []byte, capacity int) []byte {
	grown := Get(capacity)[:len(buf)]
	copy(grown, buf)
	Put(buf)
	return grown
}

// Append appends data to buf, taking a larger buffer from the pool and
// returning buf to it when data no longer fits, and returns the result. buf
// may be nil. It is small enough to inline, so appending to a buffer that has
// reached its working size costs a capacity test and the copy.
//
// Growth goes straight to the class that fits what is being appended, so a
// buffer filled piece by piece moves up its classes once each rather than
// doubling from wherever append happened to leave it.
func Append(buf, data []byte) []byte {
	return append(Grow(buf, len(buf)+len(data)), data...)
}

// Join appends each of parts to buf and returns the result, growing buf
// through the pool once for all of them rather than once per part. buf may be
// nil, which makes Join(nil, first, second) the pooled way to put parts
// together into one buffer.
func Join(buf []byte, parts ...[]byte) []byte {
	need := len(buf)
	for _, part := range parts {
		need += len(part)
	}
	if need > cap(buf) {
		buf = regrow(buf, need)
	}
	for _, part := range parts {
		buf = append(buf, part...)
	}
	return buf
}
