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
5. **No fd-sharded `GetBy`/`PutBy`.** sync.Pool is already sharded per P and has no global lock, so sharding again by fd gains nothing, and a buffer given back under a different fd than it was taken for becomes 20 to 170 times slower.
6. **An arena allocator for programs that hold many buffers across collections** (section 7): one allocation cut into slots, opt-in through `SetAllocator(NewArena(...))`; the package's own pool stays the default.

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

## 5. Sharding by fd: `GetBy`/`PutBy`

The idea is two keyed methods:

```go
func GetBy(key uintptr, size int) []byte
func PutBy(key uintptr, buf []byte)
```

Each connection passes its fd, and a custom allocator carrying several sets of sync.Pools picks one by `fd % N`, to avoid contention on "the one global sync.Pool".

### The premise does not hold

sync.Pool has no global lock; it is already sharded per P, that is per CPU:

- each P has a private slot plus a lock-free shared queue;
- the fast path of Get and Put pins the goroutine to its P and touches the local slot, with no lock and almost never a CAS;
- the only global lock is taken the first time a Pool is used on a P.

Per CPU is already the right way to shard. Sharding again by fd means one P serving many fds goes through many Pools in turn, which widens the cache footprint and lowers the hit rate.

Nothing else shared in bufferpool is contended either: counters such as `gcEpoch` are read-mostly, and the reserve's lock is taken only in the refill burst after a collection.

### Measurements

A scratch copy got `GetBy`/`PutBy` (passing straight through to `Get`/`Put` with no allocator installed) and an fd-sharded allocator with one sync.Pool per size class in each shard. The load is 14 cores in parallel, 10,000 fds in random rotation, sizes mixed between 200B and 16KB, each cell the mean of 5 runs:

| Variant | Holding 1 at a time | Holding 64 at once | Holding 64, given back under another fd |
| --- | --- | --- | --- |
| fib `Get`/`Put` (current) | 1.40 ns | 1.49 ns | 1.49 ns |
| fib `GetBy`/`PutBy`, no allocator | 1.41 ns | 1.51 ns | 1.52 ns |
| Unsharded, one sync.Pool per class | 1.30 ns | 1.45 ns | 1.51 ns |
| 16 fd shards | 2.68 ns | 3.00 ns | 28.7 ns |
| 256 fd shards | 1.68 ns | 2.71 ns | 256.9 ns, a fresh allocation every time |

Single-threaded, `GetBy`/`PutBy` with no allocator take 9.56 ns against 9.07 ns for `Get`/`Put`. The API itself costs little; the sharding is the problem.

### Gain against cost

- **Gain: none.** Even when Get and Put use the same fd, sharding is only slower, since the contention it targets does not exist.
- **Much slower across fds.** In fib a buffer is often given back on a connection other than the one that took it: a read buffer becomes a reply or a TLS record, HTTP/2 and HTTP/3 multiplex, a broadcast sends one buffer to many connections. Given back under another fd, a buffer lands in another shard; the next Get on the first shard misses, sync.Pool searches the other Ps' queues, and failing that allocates. That is the last column above.
- **More idle buffers.** Every collection empties every sync.Pool, so more shards means more sets to build up again, and idle memory multiplies. This was not quantified.
- **A large change.** About 270 call sites would have to pass an fd down, `Grow`/`Append`/`Join` would each need a keyed version, and layers such as the http parser, tls and h2 do not have the fd to begin with.

This is the same lesson as the earlier rejected fd-sharded task dispatch (fnet's approach, which measured 1–3% slower in fib): per CPU is already the best cut, and cutting again by fd only adds cross-shard cost.

If the real need is to count or cap memory per connection, that accounting belongs at the Connection, not in a split bufferpool.

## 6. Long-held buffers and storms: the reserve fixes and a sharded reserve

Raised in [lesismal/fib#1](https://github.com/lesismal/fib/issues/1): a WebSocket service whose requests wait 3–8 s in a business queue while collections come every 200–600 ms. Measured with the issue author's cloud-service simulator (16 event loops → 16 workers, waves of 1.6M requests every 12 s, a collection forced every 400 ms, 1 KiB buffers, 1 GiB budget), changed to use only the public API, on a Ryzen 7800X3D (16 threads) with the host otherwise idle, 3 rounds each.

### Four fixes to the package's own pool

1. **A target collapsed when buffers were held across collections.** A burst left unfilled at the next collection dropped the target to what the reserve held, but the buffers it was waiting for were still out. Holding 500 buffers across two collections took the target from 500 to 1. A class now keeps its target for `SetRetainedFor` (default 1 minute) after it last had to make a buffer or ran its reserve dry.
2. **A class with no more returns never gave its reserve back**, since giving back started on a return. The collection watcher now gives back the reserve of a class that has seen no return for two collections, once the window has passed.
3. **`keep` could wipe a burst that had just begun** by storing 0 into `refill` after another goroutine set it. The count is now left below zero instead.
4. **`trim` lowered a target with a load and a store**, losing a concurrent miss's rise while the budget still counted it. It now uses a compare-and-swap, and stops after as many steps as the target had, so misses arriving as fast as it lowers cannot keep it going.

### The reserve split into shards

With the fixes the reserve holds the working set, and every take and return after a collection went through its one lock: on the simulator that cost the wave rate 11%. Each class's reserve is now split into shards, each a list with its own lock. The default is 4 per GOMAXPROCS, at least 16 and at most 128; `SetReserveShards(n)` sets another count, rounded up to a power of two and at most 1024, and returns the count it set. The class still keeps one count and one target, so no shard runs out of room while the class has some, and the ordinary path never reaches the shards: a take goes to the reserve only when a collection has emptied the sync.Pool, and a return only in the burst that follows. `SetReserveShards` moves what the reserve holds into the new shards, so it may be called at any time.

### A budget that follows the memory

The fixed 32 MiB default was far below what a service holding requests across collections needs: HTTP/2 and HTTP/3 streams in flight, or a queue like the one in the issue, keep hundreds of megabytes out. The default budget is now 1/8 of the memory the process may use, at least 32 MiB and at most 16 GiB. That memory is the smaller of the machine's (or the cgroup's) limit and `GOMEMLIMIT` when one is set; when neither can be read the budget stays 32 MiB. The budget is live heap: under `GOGC=100` a full reserve of R can cost up to 2R at the peak, so 1/8 keeps that within a quarter of the memory.

- `SetRetainedPercent(p)` sets the budget to p% of that memory and returns the bytes it set, or an error when p is outside 0–100 or the memory cannot be read.
- `RetainedBytesLimit()` returns the budget in force.
- `SetRetainedBytes`, `SetRetainedPercent` and `SetReserveShards` each log what they set at Info through `fib/log`. The defaults chosen at start-up are logged at Debug, so a program that changes nothing prints nothing.

A separate `Sharded` allocator with every size configurable was built first and dropped: the same algorithm behind `SetAllocator` cost 2–2.5 ns more per Get+Put through the interface, and its only other use, separate budgets per instance, had no caller.

Two of the issue author's own versions were checked first. v1 is fix 1 alone. v2 adds 16 shards, but every Put scans them even when no refill is under way: on the idle host the ordinary Get+Put went from 11.5 ns to 26.6 ns, and from 1.39 ns to 3.28 ns on 16 threads. The shards here are behind the class-wide refill count, as the single list was.

A cap on the target was tried and dropped: limiting it to what the class had drawn past its sync.Pool over two collections undercounts whenever a burst spans more collections than that (a 200-buffer wave in a small heap spanned three), and an undercount means making buffers again, which is worse than a target that only overstates the budget's bookkeeping.

### Results

| GOMAXPROCS=16 | Remade per wave (waves 1–5, mean) | Wave rate | Sustained 30 s | Peak heap | Get+Put, 1 thread | Get+Put, 16 threads |
| --- | --- | --- | --- | --- | --- | --- |
| Before the fixes | 15.4% | 2.79 M/s | 3.18 M/s, 0.21–0.29% remade | 559–566 MB | 11.6 ns | 1.33 ns |
| Fixed, 1 shard | 4.5% | 2.39 M/s | 3.07 M/s, 0–0.02% | 454–503 MB | 11.3 ns | 1.32 ns |
| Fixed, default (16 shards) | 2.8% | 2.74 M/s | 3.16 M/s, 0% | 319–482 MB | 11.3 ns | 1.31 ns |
| Fixed, 64 shards | 2.7% | 2.78 M/s | 3.17 M/s, 0% | 484–501 MB | 11.4 ns | 1.33 ns |

| GOMAXPROCS=2 | Remade per wave | Wave rate | Sustained 30 s |
| --- | --- | --- | --- |
| Before the fixes | 15.8% | 2.75 M/s | 3.09–3.24 M/s, 0.29% |
| Fixed, 1 shard | 3.8% | 2.48 M/s | 3.05 M/s, 0% |
| Fixed, default (2 shards) | 3.4% | 2.58 M/s | 3.06 M/s, 0% |
| Fixed, 64 shards | 3.2% | 2.73 M/s | 3.15 M/s, 0% |

Get+Put times are medians of 10 runs. A wave's first collection after the target has been learned remakes nothing at all; what is left is the first wave or two, while the targets are still being learned.

- The fixes cut what each wave makes again by more than four fifths, and the peak heap falls with it, since a buffer made again is a fresh allocation the collector has to catch up with.
- One lock costs the wave rate 11–14%; the default shards give nearly all of it back on 16 threads. On 2 threads the default is 2 shards, and 64 do better there (2.73 against 2.58 M/s), since 8 event loops and 2 workers still return at once; a program in that shape can raise it.
- The ordinary Get+Put costs what it did before the fixes, on one thread and on 16.
- Retention at the end is higher (480–680 MB against 145 MB of the 1 GiB budget), which is the point: the reserve now keeps the working set the queue holds across collections. `SetRetainedBytes` bounds it as before.

## 7. An arena allocator

The pool and its reserve keep the working set across collections, but every buffer is still an object of its own: a collection marks each one, and the simulator in section 6 forces one every 400 ms. `Arena` takes the other route. It is one allocation with no pointers in it, cut into fixed slots per class and kept until another allocator replaces it; free slots are kept as their index (a `uint32`), but a slot that has never been out is kept nowhere: each class counts the slots handed out so far and takes the next ones from that count once its shared stacks are empty, so the indexes held grow with the slots a program has used, not with the arena (a 6.3 GiB arena split by `ArenaWeightsMessages` has 18 million slots, whose indexes made up front took 72 MB and 40–55 ms); each P has a stack of 64 per class taken with the P pinned and no lock, and past that slots move in batches of 32 to and from 8 shared stacks with a lock each. A class with no free slot, or no slots at all, is served by the package's own pool, and `Put` sends there every buffer that is not a slot. It is an `Allocator`, installed with `SetAllocator`; the package's own pool stays the default.

```go
arena, err := bufferpool.NewArena(nil, bufferpool.ArenaMapped)                         // an eighth of the memory, at most 1 GiB, split by ArenaWeightsServer
arena, err := bufferpool.NewArenaBytes(2<<30, bufferpool.ArenaWeightsStreaming(), bufferpool.ArenaMapped) // a total, split by weight
arena, err := bufferpool.NewArena(map[int]int{1024: 300000}, bufferpool.ArenaMapped)     // slots per class
arena, err := bufferpool.NewArenaBytes(0, nil, bufferpool.ArenaHeap)                    // on the Go heap, not mapped
bufferpool.SetAllocator(arena)
```

- **Size.** By default an eighth of the memory the process may use, 32 MiB to 1 GiB (`MaxDefaultArenaBytes`). The cap keeps a large machine from getting an arena sized to its memory rather than to what the program holds: on a 61 GiB host the eighth was 6.3 GiB, 18 million slots, for a WebSocket server whose buffers in flight came to a few hundred MB. It bounds the default only: `NewArenaBytes` and `NewArena` make an arena of any size given, past 1 GiB too. `NewArenaBytes(total, weights, memory)` splits a total between classes by `ArenaWeights` (size → weight); `ArenaCounts` shows the split.
- **Presets.** `ArenaWeightsServer` (the default) follows the buffers a fib server takes: the 16 KiB read buffers the largest share, then 32 KiB (TLS records, response copies) and 64 KiB (streamed bodies, sendfile, QUIC send buffers), 2 KiB for QUIC datagrams. `ArenaWeightsMessages` favours 64 B to 4 KiB, `ArenaWeightsStreaming` 16 to 64 KiB, `ArenaWeightsEven` gives each class from 64 B to 64 KiB the same bytes. They are starting points drawn from fib's own sizes, not measurements of a program.
- **Where the memory is.** The constructors' last parameter says. With `ArenaMapped` on Linux, the BSDs and macOS the arena is an anonymous `mmap` (with `MAP_NORESERVE` on Linux), on Windows a `VirtualAlloc`: outside the Go heap, so it counts towards neither GOGC's goal nor `GOMEMLIMIT` (`OutsideHeap` says so), and a program that sets `GOMEMLIMIT` leaves room for the arena itself. `ArenaHeap` makes it one `[]byte` on the Go heap instead. With `ArenaMapped` on other platforms, when the mapping fails, and under the race detector (which does not instrument memory outside the heap) it is made on the heap and counts as live heap from the start, which raises the collector's goal by the arena's size. Either way the system backs a page only once a slot on it is used.
- **Switching and release.** `SetAllocator` retires the arena it replaces, for instance a mapped arena replaced by one on the heap or by the package's own pool. A retired arena serves nothing more, even if installed again; `Put` sends its slots back to it whatever is installed, so no other allocator keeps them. Once every slot it handed out is back it releases its memory: a mapped arena is unmapped (`munmap`, `VirtualFree`), one on the heap is dropped for the collector, and `Released` reports it. A slot that is never `Put` keeps the arena from being released, so a buffer still in use never points into memory given back. To count the slots out it first waits for any `Get` or `Put` that began before the switch to leave the per-P stacks: those run pinned, which a collection's stop-the-world waits out, so the retired arena runs one `runtime.GC()` in the background. The check a retired arena adds to the hot path cost 0.45 ns on a single-thread Get+Put of the installed arena (7.73 to 8.19 ns), nothing measurable on 16 threads.
- **What it costs.** A slot never `Put` is lost to the arena, since the collector never takes it back — fib leaves some buffers to the collector (section 3) — and so is one returned as a slice that does not start at it (`buf[k:]`); `Lost` counts the second kind, `Free` shows both. Under the race detector the per-P stacks are skipped, since it cannot see that pinning a P makes them its own.

### Results

The simulator of section 6 on the same host, otherwise idle, 3 rounds each, with the arena installed through `SetAllocator`. "Default" is the arena `NewArena(nil)` made when this was measured: 1/16 of the host's 64 GiB, split evenly, 369k slots of 1 KiB; the default is now 1/8 split by `ArenaWeightsServer`. The other two give the 1 KiB class 256k and 512k slots; the simulator holds about 300k at its peak.

| GOMAXPROCS=16 | Remade per wave | Wave rate | Sustained 30 s | Process CPU | Fresh allocations | Peak RSS |
| --- | --- | --- | --- | --- | --- | --- |
| Pool (the default) | 2.6% | 2.78 M/s | 3.17–3.18 M/s | 47.6–49.3 s | 510–523k | 502–522 MB |
| Arena, default | 0% | 2.88 M/s | 3.23–3.26 M/s | 9.1–9.5 s | 5.5k | 446–449 MB |
| Arena, 256k slots | 0% | 2.92 M/s | 3.25 M/s | 9.6–10.1 s | 39–52k | 304–310 MB |
| Arena, 512k slots | 0% | 2.93 M/s | 3.25–3.26 M/s | 9.2–9.5 s | 5.6k | 521–558 MB |

| GOMAXPROCS=2 | Remade per wave | Wave rate | Sustained 30 s | Process CPU | Peak RSS |
| --- | --- | --- | --- | --- | --- |
| Pool | 3.1% | 2.72 M/s | 3.13–3.14 M/s | 15.2–15.9 s | 565–572 MB |
| Arena, default | 0% | 2.84 M/s | 3.19–3.20 M/s | 5.3–5.6 s | 447 MB |
| Arena, 256k slots | 0.2% | 2.83 M/s | 3.18–3.19 M/s | 5.7–5.8 s | 302–303 MB |
| Arena, 512k slots | 0% | 2.84 M/s | 3.17–3.18 M/s | 5.4–5.5 s | 509–557 MB |

The peak heap with the default arena reads 4213 MB: the arena was made on the heap then, so all of it counted as live heap, though the system backed only the 449 MB used.

- **The collector's work is what drops.** Process CPU falls to a fifth on 16 threads and a third on 2: each forced collection marks one object instead of some 500k buffers, and the buffers are not made again (510k fresh allocations against 5.5k).
- **Throughput rises a little:** the wave rate by 4–5%, the sustained rate by 2–3%, and no wave makes a buffer again.
- **An arena smaller than the working set still gets nearly all of it.** With 256k slots the rest goes to the pool, and the peak RSS is the lowest of all, since the pool's reserve no longer keeps a copy of what the arena holds.
- **The ordinary Get+Put is faster than the pool's** (1500 B, median of 10): 9.9 ns against 11.2 on one thread, 1.25 ns against 1.33 on 16, 5.0 against 5.9 on 2. `Get` and `Put` call an installed `*Arena` directly rather than through the interface: through it the same pair measured 11.8, 1.48 and 5.88 ns.
- **`go test -tags fib_arena ./...`** runs every test with a 64 MiB arena installed; CI runs it, and the main packages again with the race detector.

## Appendix: why not pass a wrapper object

Another approach is to follow [valyala/bytebufferpool](https://github.com/valyala/bytebufferpool) and pass a `*ByteBuffer`. It does not pay:

- **No GC gain.** What bytebufferpool avoids is the slice header boxed by `sync.Pool.Put([]byte)`, which fib's `*byte` already avoids. A wrapper adds a heap object holding a pointer, which the collector does have to scan.
- **Slower.** Under parallel mixed-size load, a re-implementation of bytebufferpool's design ran 5–8 times slower than fib's pool: a contended shared counter, no size classes, and buffers past the calibrated maximum dropped. A `*Buffer` wrapped around fib's classes adds about 1.4–2 ns per Get+Put pair.
- **The wrapper's real value is the fields it can carry**: an owner pointer (so Put returns to the right allocator), a reference count (zero-copy broadcast), metadata. When one of those is needed, a wrapper with an owner can be introduced on that path alone, without touching every call site.
