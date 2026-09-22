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
// The bytes in a buffer Get returns are whatever the last user left there.
// Callers write before they read, as they would with any recycled buffer.
package bufferpool

import (
	"math/bits"
	"sync"
)

const (
	// minShift puts the smallest class at 64 bytes, which is a cache line on
	// every platform this runs on. Below that the pool costs more than the
	// allocation it saves: Go allocates tiny objects from a per-P tiny
	// allocator that is already faster than two atomic pool operations.
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

// classes holds one pool per size class. Each entry stores *[]byte rather than
// []byte: sync.Pool stores an any, and a slice header does not fit in one, so
// storing slices directly would copy each to the heap on the way in.
var classes [numClasses]sync.Pool

// headers recycles the *[]byte handles themselves, which is what keeps a
// Get/Put round trip free of allocation entirely. Taking a handle out of a
// class pool leaves it empty and reusable, and the next Put needs one; without
// this they would be allocated and collected at exactly the rate buffers are
// recycled, which is the cost the pool exists to avoid. A pointer does fit in
// an any, so moving handles through a pool of their own allocates nothing.
//
// It pays for itself where it matters. On one goroutine the extra pool
// operation costs about 2ns against letting each handle escape to the heap;
// with every core taking buffers at once, which is what a server does, the
// round trip measured 7.1ns against 10.5ns, because the allocation the handle
// would need is what contends.
var headers sync.Pool

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
	handle, _ := classes[i].Get().(*[]byte)
	if handle == nil {
		return make([]byte, size, classSize(i))
	}
	buf := *handle
	// Emptying the handle before parking it is what lets it be reused without
	// keeping the buffer alive through it.
	*handle = nil
	headers.Put(handle)
	return buf[:size]
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
	handle, _ := headers.Get().(*[]byte)
	if handle == nil {
		handle = new([]byte)
	}
	// Trimmed to the class size, so that every buffer in a class is exactly
	// that size and what Get hands out is always Align of what was asked for,
	// whatever shape the buffer had before it came here.
	size := classSize(i)
	*handle = buf[:size:size]
	classes[i].Put(handle)
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
