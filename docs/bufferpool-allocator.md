# bufferpool: custom allocators and strict Get/Put pairing

[中文](bufferpool-allocator.zh-CN.md)

[`bufferpool`](../bufferpool) keeps returning and taking plain `[]byte`, and a user can install an allocator of their own with `SetAllocator` (section 1). The rest is an analysis with no implementation behind it. The question: with a manually managed allocator installed (jemalloc through cgo, say), can fib make sure that

- **Put accepts only buffers this pool handed out**, and turns everything else away;
- **every Get is matched by exactly one Put**, with nothing leaked?

The analysis is of `b070cc6` on 2026-11-17. Numbers were measured on Apple Silicon, 14 cores, Go 1.27.3.

## Conclusions

1. **A pluggable allocator costs nothing by itself.** `Get` and `Put` check an `atomic.Pointer` for nil first and fall through to the built-in pool when it is, which measures the same as before the check existed. This part is implemented.
2. **Only the allocator can tell whether a buffer is its own.** Given just a `[]byte`, fib has no way to know where it came from.
3. **fib does not pair Get and Put strictly today.** Its contract is that any buffer may be Put and that one never Put is left to the collector, and many paths rely on that. A manually managed allocator needs the contract changed first and every such path fixed.
4. **jemalloc through cgo makes each Get+Put pair several times slower than the current pool**, and most of the GC cost it would save is already saved by the current design.

## 1. The allocator hook: nil fallback (implemented)

```go
type Allocator interface {
    Get(size int) []byte
    Put(buf []byte)
}

// SetAllocator is meant to be called once, before anything takes a buffer; nil restores the built-in pool.
func SetAllocator(a Allocator)
```

The implementation is in [allocator.go](../bufferpool/allocator.go):

- **Default path**: `Get` and `Put` each load one `atomic.Pointer` first and use the built-in pool when it is nil; nothing else changed.
- **Get**: a `size <= 0` still returns nil without reaching the allocator. Every other size goes to the allocator, including those above `MaxSize`. The slice it returns must have len size and cap at least size.
- **Put**: a buffer with no capacity is ignored. Every other one goes to the allocator, which decides what to do with a buffer it did not hand out (sections 2 and 3).
- **Grow, Append, Join**: they reach the same allocator through `Get` and `Put`; their fast path, when the buffer already has room, does not.
- **Align, MinSize, MaxSize**: they still describe the built-in pool, which fib uses to size its requests. `SetRetainedBytes` and `RetainedBytes` apply to the built-in pool only.
- **When to switch**: install the allocator before start-up. A buffer still out when the allocator changes is Put to whichever allocator is in place by then.

Cost of a Get+Put pair, measured while choosing the design:

| Variant | Single thread | 14 cores parallel |
| --- | --- | --- |
| Current code | 8.15 ns | 1.02 ns |
| In-package nil check, plain global | 7.74 ns | 1.12 ns |
| In-package nil check, `atomic.Pointer` | 8.16 ns | 1.11 ns |
| Package-level interface variable, default called through it too | about +1.5 ns | about +0.2 ns |

The first three differ by noise. The last adds an indirect call, and `Get`'s body no longer inlines into its callers. So `atomic.Pointer` it is: just as fast, and quiet under `-race`.

The package's own benchmarks before and after the change (mean of 8 runs each):

| Benchmark | Before | After |
| --- | --- | --- |
| `GetPut/64B` | 7.54 ns | 7.62 ns |
| `GetPut/1500B` | 7.64 ns | 7.64 ns |
| `GetPut/16384B` | 7.58 ns | 7.70 ns |
| `GetPutParallel` | 1.03 ns | 0.86 ns |
| `JoinParts/pooled` | 24.02 ns | 24.07 ns |

## 2. Put accepting only the pool's own buffers

The check has to live inside the allocator:

- **Address registry**: record every address handed out in a sharded map; Put looks it up first and ignores a buffer it does not find, leaving it to the collector. This catches foreign buffers and double Puts, at the cost of a lookup per Put, roughly 20–50 ns (estimated, not measured).
- **Address range**: reserve one contiguous range and do the size classes inside it, for example a dedicated jemalloc arena with custom extent hooks, or a region mmapped directly. Put then only compares the address against the range, in O(1).
- **Never ask `malloc_usable_size` or the like about a foreign pointer.** Calling it on memory jemalloc did not allocate is undefined behaviour.
- **Interior pointers must be refused** (a slice cut down as `buf[k:]`): freeing one crashes outright. The run below found no place where fib Puts one.

## 3. Strict pairing: where fib stands

### Method

A scratch copy of fib had a pairing tracker added to `bufferpool`, switched on by an environment variable:

- Get records the buffer's base address with the call stack;
- Put clears the record. A Put with no record is classified as a foreign buffer, an interior pointer or a double Put;
- every fresh allocation gets a `runtime.AddCleanup`. A buffer collected while still recorded as outstanding is a leak.

All 88 packages were then run with `-short`, and all passed.

### Results

| Problem | Production sites | Examples |
| --- | --- | --- |
| Get without Put, left to the collector | 48 | `core.(*Engine).deliverDatagram`: datagrams still queued when a connection closes are not given back; `http.(*Parser).appendBuffer` |
| Put of a buffer that did not come from the pool | 29 | `http.(*h2ServerConn).sendPooledLocked`, `tls.(*layer).releaseInLocked`, `websocket.(*Parser).releaseInflated` |
| Suspected double Put | 2 | `http.(*clientRequest).releaseData`, `http3.(*clientStream).send` |
| Put of an interior pointer | 0 | — |

Leaks are found only when a buffer is collected, so the count is a lower bound. Some come from tests abandoning connections, but each one is a path where a Get goes unpaired.

Both suspected double Puts are at sites that also report foreign buffers. They are probably a false positive in the tracker: between the collection of a buffer and its cleanup running, a fresh Go allocation can take the same address and then look like a second Put of it. This is inferred, not confirmed.

### The typical pattern

`fastFramedRequest` in package `http` ([client_codec.go](../http/client_codec.go)):

1. it takes `bufferpool.Get(size)` and then writes into it with a plain `append`;
2. `size` does not count the length of the URL path, so a long path makes `append` reallocate on the Go heap;
3. the pool's buffer is left to the collector, and what `releaseData` later Puts is the Go-heap one.

Under the current contract that is not a bug. With jemalloc, the first buffer is a real leak and the second is a Go-heap pointer handed to free, which crashes the process.

### What strict pairing would take

1. **A new contract**: `Put` accepts only a buffer the pool handed out whose backing array has not been replaced, and every Get is Put exactly once on every path, close, error and panic paths included.
2. **Fixing the sites above**: every Get-then-append moves to `bufferpool.Append`/`Grow`, queues and caches are released when a connection closes, and buffers that did not come from the pool are no longer Put. That is a full audit of about 270 call sites across 55 files.
3. **The tracker as a fib debug feature** (behind a build tag, say), kept running in CI so the pairing does not regress.

One risk fib cannot remove from the inside: a buffer passed to a user callback (OnData's data, a WebSocket message) and kept past the callback is today merely overwritten by its next user; with manually managed memory it is a use after free, which can segfault or corrupt memory.

## 4. The cost of cgo and jemalloc

jemalloc is not installed on the test machine, so the system malloc stood in for it. The cost is dominated by the cgo call itself; jemalloc's own work is also in the tens of nanoseconds.

| | Single thread | 14 cores parallel |
| --- | --- | --- |
| Empty cgo call | 13.8 ns | — |
| cgo malloc+free pair | 49.6 ns | 9.5 ns |
| bufferpool Get+Put pair | about 7.5 ns | about 1 ns |

What jemalloc saves is GC pressure on the Go heap, and the current design already saves most of it:

- the pool keeps `*byte`, so Put boxes no slice header, and Get and Put allocate nothing;
- buffers hold no pointers, so the collector never scans them;
- the working set remade after a collection empties the `sync.Pool` is covered by each class's reserve.

If the goal is less GC cost, the better options, in order, are:

1. tune `SetRetainedBytes` and `GOMEMLIMIT` first;
2. mmap memory outside the Go heap directly from Go and do the size classes there, with no cgo and the current pool still in front;
3. if jemalloc it must be, put a per-P cache on the Go side in front of it to spread the cost of the cgo calls.

## Appendix: why not pass a wrapper object

Another approach is to follow [valyala/bytebufferpool](https://github.com/valyala/bytebufferpool) and pass a `*ByteBuffer`. It does not pay:

- **No GC gain.** What bytebufferpool avoids is the slice header boxed by `sync.Pool.Put([]byte)`, which fib's `*byte` already avoids. A wrapper adds a heap object holding a pointer, which the collector does have to scan.
- **Slower.** Under parallel mixed-size load, a re-implementation of bytebufferpool's design ran 5–8 times slower than fib's pool: a contended shared counter, no size classes, and buffers past the calibrated maximum dropped. A `*Buffer` wrapped around fib's classes adds about 1.4–2 ns per Get+Put pair.
- **The wrapper's real value is the fields it can carry**: an owner pointer (so Put returns to the right allocator), a reference count (zero-copy broadcast), metadata. When one of those is needed, a wrapper with an owner can be introduced on that path alone, without touching every call site.
