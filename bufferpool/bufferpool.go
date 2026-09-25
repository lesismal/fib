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
// retaining idle buffers the others could have used.
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
// lock is taken only for the burst that follows a collection, which is as many
// operations as the reserve holds buffers and saves that many allocations. A
// reserve grows only towards what its class has actually had to make, gives
// back what a burst could not fill, and stops at what SetRetainedBytes allows.
//
// The bytes in a buffer Get returns are whatever the last user left there.
// Callers write before they read, as they would with any recycled buffer.
package bufferpool

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
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

	// mu guards free, the reserve. held mirrors len(free) so that a return
	// can see the reserve is full without taking the lock.
	mu   sync.Mutex
	free []*byte
	held atomic.Int64
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
	// pad keeps one class's counters off the cache line of the next one: the
	// class a server reads into and the class it replies from are neighbours
	// here, and they are busiest at the same moment.
	_ [64]byte
}

var classes [numClasses]class

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
		gcEpoch.Add(1)
		watchCollections()
	}, struct{}{})
}

// DefaultRetainedBytes is how much the reserves may hold between them until
// SetRetainedBytes says otherwise. It bounds what the pool keeps while nothing
// is asking for it, which is the price of not making the working set again
// after every collection.
const DefaultRetainedBytes = 32 << 20

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
	budget.Store(DefaultRetainedBytes)
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
func SetRetainedBytes(bytes int) {
	if bytes < 0 {
		bytes = 0
	}
	budget.Store(int64(bytes))
	if committed.Load() > int64(bytes) {
		trim()
	}
}

// RetainedBytes reports what the reserves are currently entitled to hold.
func RetainedBytes() int { return int(committed.Load()) }

// reserveMore lets this class keep one more buffer, if the budget has room for
// one. It is called where a request had to be served by making a buffer, which
// is the evidence that the class holds less than the program keeps in flight.
func (c *class) reserveMore() {
	if committed.Add(c.size) > budget.Load() {
		committed.Add(-c.size)
		return
	}
	c.target.Add(1)
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
		for committed.Load() > budget.Load() {
			target := c.target.Load()
			if target == 0 {
				break
			}
			c.target.Store(target - 1)
			committed.Add(-c.size)
		}
		if c.held.Load() <= c.target.Load() {
			continue
		}
		c.mu.Lock()
		for int64(len(c.free)) > c.target.Load() {
			last := len(c.free) - 1
			c.free[last] = nil
			c.free = c.free[:last]
		}
		c.held.Store(int64(len(c.free)))
		c.mu.Unlock()
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
func Get(size int) []byte {
	if size <= 0 {
		return nil
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

// take removes a buffer from the reserve, or reports that there was none.
func (c *class) take() *byte {
	c.mu.Lock()
	last := len(c.free) - 1
	if last < 0 {
		c.mu.Unlock()
		return nil
	}
	p := c.free[last]
	c.free[last] = nil
	c.free = c.free[:last]
	c.held.Store(int64(last))
	c.mu.Unlock()
	return p
}

// Put returns buf to the pool. The caller must not use buf, or any slice of
// it, afterwards. A buffer too small or too large to belong to a class is
// dropped, so Put may be called on any buffer without the caller having to
// know where it came from.
func Put(buf []byte) {
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
func (c *class) beginRefill() {
	epoch := gcEpoch.Load()
	if c.epoch.Swap(epoch) == epoch {
		return
	}
	if c.refill.Load() > 0 {
		c.aim(c.held.Load())
	}
	c.refill.Store(c.target.Load())
}

// keep puts a buffer in the reserve and reports whether it took it. The burst
// is spent whether or not a return finds room, so a class the program no
// longer keeps that many of in flight stops taking the lock instead of
// reaching for a target it cannot fill.
func (c *class) keep(p *byte) bool {
	if c.refill.Add(-1) < 0 {
		c.refill.Store(0)
		return false
	}
	if c.held.Load() >= c.target.Load() {
		return false
	}
	c.mu.Lock()
	if int64(len(c.free)) >= c.target.Load() {
		c.mu.Unlock()
		return false
	}
	c.free = append(c.free, p)
	c.held.Store(int64(len(c.free)))
	c.mu.Unlock()
	return true
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
