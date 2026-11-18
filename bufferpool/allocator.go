package bufferpool

import "sync/atomic"

// Allocator is what Get and Put use instead of the package's own pool once
// SetAllocator has installed one.
//
// Get is called with a size above zero and must return a buffer of that len
// and a capacity of at least that much. Put must accept any buffer: fib
// returns buffers it did not take from Get, and leaves some it did take to the
// collector rather than returning them, so an allocator that has to know which
// buffers are its own — one handing out memory from outside the Go heap, say —
// has to keep track itself, and ignore a buffer that is not one of them. See
// docs/bufferpool-allocator.md for where fib does each.
//
// Both are called from many goroutines at once.
type Allocator interface {
	Get(size int) []byte
	Put(buf []byte)
}

// installed holds the Allocator, since an atomic pointer cannot hold an
// interface itself.
type installed struct{ Allocator }

// allocator is the Allocator SetAllocator installed, or nil for the package's
// own pool. Get and Put load it first; while it is nil that is one load of a
// line every core already has and a branch that always goes the same way, so
// the package's own pool costs what it did before there was a choice.
var allocator atomic.Pointer[installed]

// SetAllocator makes Get and Put use a, or the package's own pool again when a
// is nil. Get, Grow, Append and Join then take their buffers from a, and Put
// returns them to it.
//
// It is meant to be called once, before anything has taken a buffer: a buffer
// taken before the change is returned to whichever allocator is in place when
// it is Put, so changing it while buffers are out hands one allocator's buffers
// to the other.
//
// Align, MinSize and MaxSize keep describing the package's own pool, which fib
// uses to size what it asks for, and SetRetainedBytes and RetainedBytes apply
// to that pool only.
//
// Builtin is the package's own pool as an Allocator, so that a program can
// hold either kind in one place; SetAllocator(Builtin) is SetAllocator(nil).
//
// NewArena makes the other Allocator the package provides: one allocation cut
// into slots, for a program that holds many buffers across collections. It
// falls back to the package's own pool, so the retention settings apply to
// what it cannot serve.
func SetAllocator(a Allocator) {
	if a == nil || a == Builtin {
		allocator.Store(nil)
		return
	}
	allocator.Store(&installed{a})
}

// Builtin is the package's own pool as an Allocator. Its Get and Put go to that
// pool whatever SetAllocator has installed.
var Builtin Allocator = builtin{}

type builtin struct{}

func (builtin) Get(size int) []byte {
	if size <= 0 {
		return nil
	}
	return getPooled(size)
}

func (builtin) Put(buf []byte) { putPooled(buf) }
