# Hacks

[English](21-hacks.md) | [简体中文](21-hacks.zh-CN.md)

fib's speed comes from three habits borrowed from C epoll servers: one round of a connection runs
serially on one worker, handlers must never wait, and every write in a round is corked and sent
once. This chapter is a curated tour of what those habits let you do — the tricks, a minimal
example each, the trap in each, and a link to the matching numbered section of the
[hack guide](../hacks.md), which has the long explanation. It is for readers who already write fib
handlers; start with [03-handlers.md](03-handlers.md) otherwise.

The tour, in order: [1](#1-async-responses-retain-and-release) async responses ·
[2](#2-large-bodies-onbody) large bodies · [3](#3-multiplex-the-handler-pool) multiplex ·
[4](#4-one-write-per-round) one write per round ·
[5](#5-borrowed-objects-reuse-and-zero-copy) borrowed objects ·
[6](#6-read-backpressure-holdreads) read backpressure · [7](#7-the-write-side) the write side ·
[8](#8-reads-deadlines-and-cancellation) reads and deadlines ·
[9](#9-websocket-frames) WebSocket frames ·
[10](#10-one-engine-server-and-client) one engine, server and client ·
[11](#11-one-handler-three-protocols) one handler, three protocols ·
[12](#12-static-files-filecache-and-sendfile) static files · [13](#13-udp-batches) UDP batches ·
[14](#14-pools-processes-and-buffers) pools and processes ·
[15](#15-the-cheat-sheet) the cheat sheet · [16](#16-anti-patterns) anti-patterns.

## 1. Async responses: Retain and Release

**The trick.** A response is reference counted: serving a request holds one reference, and the
handler can take another with `Retain` and give it back later with `Release`. Between the two the
handler returns and its worker goes back to the pool, so a slow backend costs no goroutine.
`Context.Async(fn)` wraps the pair for a pool of your own: it takes the `Retain` when called and
returns a func that runs `fn` once and then releases, so don't `Retain` again yourself, and make sure
the func runs (a func never called is a `Retain` never released).

```go
pool.Go(c.Async(func() { // Async retains now; the func runs fn, then releases even if fn panics
	_ = c.Respond(200, "application/json", result)
}))
```

**The trap.** Every `Retain` needs exactly one `Release` on every path, including the error and
cancellation paths; nothing times a `Retain` out, so a missing `Release` holds the connection (and,
on HTTP/1, every request pipelined behind it) until the peer or a connection timeout ends it. After
the last `Release` the request object may already belong to another request, so copy what you keep
first.

Below is the whole pattern in a program, including the copy of a borrowed query value:

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		name := strings.Clone(c.Query("name")) // a borrowed substring: copy it to keep it
		c.Retain()                             // the response waits for a Release now
		go func() {                            // real code: your own pool, not a goroutine per request
			defer c.Release()
			time.Sleep(10 * time.Millisecond) // a slow backend
			_ = c.Respond(200, "text/plain", []byte("hello "+name))
		}()
	})

	engine, err := fib.New(config, fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/?name=fib", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(resp.StatusCode, string(body))
}
```

Several branches may retain one response; the last one to `Release` finishes it, after writing.
Use `OnCancel` to hear that the peer is gone and to release the reference the branch took — with
`sync.Once`, because the normal and the cancelled path can both run.

See [§1](../hacks.md#1-async-responses-retain--release) and
[§3](../hacks.md#3-cancellation-oncancel--err--idempotent-cleanup).

## 2. Large bodies: OnBody

**The trick.** Set `Config.StreamRequestBody` on the HTTP handler and the handler runs as soon as
the header is in; the body arrives through `c.OnBody` in order, one call at a time, on the worker
that reads it. The body is never buffered whole and no goroutine is spent; if the callback is slow
the connection stops reading and TCP slows the sender down, so backpressure is free.

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamRequestBody = true
httpConfig.StreamRequestBodyThreshold = 0 // stream every body
httpConfig.MaxStreamedBodyBytes = 64 << 30
httpConfig.ReadBodyTimeout = time.Hour // the 5-minute default would cut a long upload off

app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	if c.Request.ContentLength > limit {
		_ = c.Respond(413, "text/plain", []byte("too large"))
		return // no OnBody: with Expect: 100-continue the client never starts uploading
	}
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {
			return // connection gone, over the limit or cancelled: the last call
		}
		_, _ = f.Write(data) // data is valid only during this call
		if fin {
			_ = c.Respond(200, "text/plain", []byte("stored"))
		}
	})
})
```

**The trap.** A streamed body is still bounded by the read timeouts while it arrives:
`ReadBodyTimeout` (5 minutes in `DefaultConfig`, from the end of the header) and `ReadTimeout` (off
by default, from the first byte). Raise them for large uploads, or lift them for one request with
`c.SetReadDeadline`. `Request.Body` cannot be read once `OnBody` has taken it over, and the
callback must not block — it holds the worker.

The program below streams a 2MiB upload into a server that only counts bytes:

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"sync/atomic"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	httpConfig := fibhttp.DefaultConfig()
	httpConfig.StreamRequestBody = true
	httpConfig.StreamRequestBodyThreshold = 0
	httpConfig.MaxStreamedBodyBytes = 1 << 30
	httpConfig.ReadBodyTimeout = 0 // no limit: the body streams while the handler runs

	var received atomic.Int64
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.ContentLength > 8<<20 {
			_ = c.Respond(413, "text/plain", []byte("too large"))
			return
		}
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				return
			}
			received.Add(int64(len(data)))
			if fin {
				_ = c.Respond(200, "text/plain", fmt.Appendf(nil, "got %d bytes", received.Load()))
			}
		})
	})

	engine, err := fib.New(config, fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandlerWithConfig(httpConfig, app),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	payload := bytes.Repeat([]byte("x"), 2<<20)
	// A reader net/http does not recognise as a buffer: the client streams it.
	body := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
	req, err := stdhttp.NewRequest(stdhttp.MethodPost, "http://127.0.0.1:8080/upload", body)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	answer, _ := io.ReadAll(resp.Body)
	fmt.Println(string(answer))
}
```

See [§2](../hacks.md#2-large-bodies-and-onbody-no-buffering-no-goroutine-built-in-backpressure).

## 3. Multiplex: the handler pool

**The trick.** On HTTP/2 and HTTP/3 one connection carries many requests, and their handlers run
on the engine's handler pool (`Engine.HandlerPool()`), not on the worker reading the connection.
`StreamPool.MaxConcurrentHandlers` caps how many of one connection's requests run at a time: the
Nth runs on the connection's own reader, which reads nothing more until it returns, so the peer's
flow control carries the limit and the server queues nothing. Zero, the default, leaves only
`MaxConcurrentStreams` as the bound.

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamPool.MaxConcurrentHandlers = 8 // per connection; 1 = one at a time
handler := fibhttp.NewHandlerWithConfig(httpConfig, app)
```

**The trap.** A blocking handler on a multiplexed connection hurts its own stream pool, and a
retained request suspends only that stream — but a handler must still not wait, because the worker
it holds is also reading every other stream on the connection. Backpressure cannot be done by
stopping reads on h2/h3; the stream window does it.

See [§5](../hacks.md#5-multiplex-http2-http3-the-handler-pool-and-maxconcurrenthandlers).

## 4. One write per round

**The trick.** Parse and send in loops inside one `OnData`; fib corks every send of the round and
writes them together at the end, so a read carrying N pipelined requests costs one `writev`, not N
writes. Nothing to enable — it is the default `CoalesceWrites`.

```go
Data: func(c *fib.Connection, data []byte) {
	for len(data) > 0 {
		msg, n, ok := parse(data)
		if !ok {
			break // partial: keep the remainder yourself and join it in the next OnData
		}
		data = data[n:]
		_ = c.Send(answer(msg)) // queued; the round flushes once at its end
	}
},
```

**The trap.** A `Flush` per message throws the benefit away. And the write watermark must exceed
the replies of one read, not one message: a round keeps reading in `ReadBufferSize` (16KiB by
default) pieces while the socket has data, checks what its cork holds against
`WriteBufferHighWatermark` after each, and flushes every 256KiB it reads; keep the watermark (64-256KiB
by default) well above one read's replies, or every pipeline pauses reads for nothing.

See [§4](../hacks.md#4-pipeline-one-write-per-round) and
[§8](../hacks.md#8-read-backpressure-holdreads).

## 5. Borrowed objects: reuse and zero copy

**The trick.** Request, Header, URL and Context are reused by default (`ReuseRequests`,
`ReuseHeaders`, `ReuseURLs`, `ReuseContexts`), which is where much of fib's throughput at high
concurrency comes from. The price is the fasthttp rule: a reused object belongs to the handler
until it has returned and released every `Retain`. `c.Query`, `c.Body()` and `c.Param` then borrow
in place, allocating nothing.

```go
q := c.Query("q")    // a substring of the raw query, no map, no allocation
b := c.Body()        // the server's own buffer for a whole body
_ = c.RespondCopy(200, "application/json", scratchJSON(q, b)) // the copy is made at the call
```

**The trap.** Borrowed memory must not outlive the handler. Handing `c.Body()`, `c.Request` or a
`c.Query` value to a goroutine that outlives the response reads another request's data and
**nothing panics**. Copy with `strings.Clone`, `append([]byte(nil), ...)` or `TakeBody`; use
`RespondCopy`/`WriteResponseCopy` when a handler wants its own scratch buffer back at the call.

See [§6](../hacks.md#6-object-reuse-using-context-like-fasthttp) and
[§7](../hacks.md#7-zero-copy-reads-contextbody--contextquery).

## 6. Read backpressure: HoldReads

**The trick.** On the write side fib pauses reads at the watermark by itself; on the read side the
application decides. `HoldReads(true)` stops the connection reading (a round in progress stops
before its next read), the data stays in the kernel buffer and TCP's window slows the peer;
`HoldReads(false)` resumes, and whatever piled up is delivered without another byte from the peer.
It may be called from any goroutine; holds do not nest, the last call wins. It is cheap because the connection stops being
scheduled instead of holding a worker.

```go
Data: func(c *fib.Connection, data []byte) {
	q.push(append([]byte(nil), data...)) // data is valid only during this callback
	if q.size() >= highWater {
		c.HoldReads(true) // from inside OnData it stops this round's read loop too
	}
},
// From the goroutine that drains the queue, once below the low watermark:
//     c.HoldReads(false)
```

**The trap.** HoldReads is not a sleep: blocking in the callback occupies a worker, while holding
reads returns the connection to the loop. It has no effect on UDP (the datagrams are already
read), and `Stats().ReadsPausedByBudget` non-zero means another connection's backlog paused this
one through `MaxPendingBytes`.

See [§8](../hacks.md#8-read-backpressure-holdreads).

## 7. The write side

**The trick.** Choose the send that matches the data: `Send` for the normal case, `SendParts` for a
frame header plus payload (`writev`, no join), `SendOwned`/`SendShared` for a buffer handed over or
shared by many connections, `SendFile` for a file the kernel sends itself. `Cork`/`Flush` merge
several replies written **outside** a round.

```go
c.Cork()
for _, reply := range replies { // from a goroutine of your own, not from OnData
	_ = c.Send(reply)
}
_ = c.Flush() // required: nothing else writes what a Cork holds
```

**The trap.** Every `Cork` needs its `Flush`, and a pinned cork means the peer waits. `SendFile`
takes the file range in order with the sends around it; a file shorter than declared closes the
connection. `MaxWriteQueueBytes` (or `Connection.SetWriteQueueLimit`) is the hard cap for a peer
that never reads: past it the send fails with `ErrSendQueueFull` and closes the connection.

See [§9](../hacks.md#9-the-write-side-cork--flush--sendparts--sendfile).

## 8. Reads, deadlines and cancellation

**The trick.** `*fib.Connection` is a `net.Conn`, but `Read` never blocks: it returns
`fib.ErrWouldBlock` when the socket has nothing for you, so a handler can drain what the round
already read. A deadline does not fail the call, it closes the connection; using it as a rolling
timeout costs nothing extra when the instant is unchanged. In the dark corners:
`CloseWrite` half-closes after the queue drains, `CloseRead` stops `OnData` without ending the
write side, `SetAttachment` hangs per-connection state on the connection, `IsAccepted`/`IsDialed`
tell the two ends apart.

```go
Data: func(c *fib.Connection, data []byte) {
	for {
		n, err := c.Read(buf)
		if errors.Is(err, fib.ErrWouldBlock) {
			break
		}
		// ... handle buf[:n], or stop on any other error
	}
},
```

**The trap.** Never wrap a `Connection` in `bufio.Reader` or hand it to code that expects blocking
reads. And do not close your own connection inside `OnData` and then wait for `OnClose` or
`OnCancel` in the same round: `OnClose` waits for the round to return, so this deadlocks. Use
`OnCancel` with `sync.Once` to release what a cancelled request still holds.

See [§10](../hacks.md#10-read-does-not-block-a-deadline-closes-the-connection) and
[§3](../hacks.md#3-cancellation-oncancel--err--idempotent-cleanup).

## 9. WebSocket frames

**The trick.** `OnMessage` reassembles the whole message; setting `HandlerFuncs.Frame` instead
delivers frame by frame, so a message larger than `MaxMessageBytes` can be streamed and the limit
applies to a single frame. It is `OnBody` for WebSocket.

```go
handler := websocket.HandlerFuncs{
	Frame: func(c *websocket.Connection, op websocket.Opcode, fin bool, data []byte) {
		_, _ = f.Write(data) // data is valid only during the call
		if fin {
			_ = f.Close()
		}
	},
}
```

**The trap.** With `Frame` set, `Message` is no longer called for Text or Binary, and each call
carries the message's own opcode plus `fin` — an unfragmented message is one call with `fin=true`.
A permessage-deflate message is still reassembled, because its frames are pieces of one DEFLATE
stream. A custom `Ping` handler replaces the default pong: call `c.Pong` yourself.

See [§11](../hacks.md#11-websocket-receiving-by-frame-onframe).

## 10. One engine, server and client

**The trick.** All of fib's clients share the server's event loop, worker pool and backpressure, so
a gateway uses one engine for both directions. `DialWithHandler` gives each dialed connection its
own handler, and `fib.New(config)` with no bindings is a client-only engine.

**The trap.** Callbacks may run on any goroutine, so do not block in them, and do not `Wait()` on a
client future inside a handler. Close the client before the engine.

The program below is a TCP echo server and its client, one engine, one handler type:

```go
package main

import (
	"fmt"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	echo := fib.HandlerFuncs{
		Data: func(c *fib.Connection, data []byte) {
			_ = c.Send(data) // corked and flushed once at the end of the round
		},
	}
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:9000", Handler: echo})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	got := make(chan string, 1)
	client := fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("ping")) },
		Data: func(c *fib.Connection, data []byte) { got <- string(data) },
	}
	err = engine.DialWithHandler("tcp", "127.0.0.1:9000", 3*time.Second, client,
		func(c *fib.Connection, err error) {
			if err != nil {
				got <- "dial: " + err.Error()
			}
		})
	if err != nil {
		panic(err)
	}
	fmt.Println("client got:", <-got)
}
```

See [§12](../hacks.md#12-one-engine-as-both-server-and-client).

## 11. One handler, three protocols

**The trick.** One `fibhttp.Handler` serves HTTP/1.x, HTTP/2 (ALPN, h2c prior knowledge or
`Upgrade: h2c`) and HTTP/3 unchanged; `c.Request.Proto` says which. `Context.Upgrade(protocol,
header, tunnelHandler)` answers 101 (200 on h2/h3) and hands the connection or stream to a
`TunnelHandler`, which is exactly how WebSocket upgrades and CONNECT tunnels are built.

```go
func(c *fibhttp.Context) {
	tunnel, err := c.Upgrade("myproto", stdhttp.Header{"X-Ver": {"1"}}, myTunnelHandler)
	if err != nil {
		return
	}
	_ = tunnel // OnTunnelOpen has run; the raw bytes follow
}
```

**The trap.** A response written through `Write`/`Flush` streams on all three protocols, paced by
the client: once 64KB of it is unsent, `Write` waits (bounded by `WriteTimeout`), so a handler that
streams a large body holds its goroutine while the client is slow — stream from a retained
goroutine of your own, or use `FileCache`/`SendFile` for files. `c.Header()` set before answering
goes out with `Write` and with `Respond`, `String`, `JSON` and `WriteResponse` alike (a whole
response's own fields of the same name win); changed after the response has begun, it has no
effect. `http.Hijacker` is not implemented, and on h2/h3 you must not call
`Context.Conn.Send`; go through `Respond`/`WriteResponse`.

See [§13](../hacks.md#13-one-handler-for-h1--h2--h3-and-upgrade--tunnel).

## 12. Static files: FileCache and sendfile

**The trick.** `http.NewFileCache` holds a file in memory after the first request and follows the
disk (inotify on Linux), and picks `name.br`/`name.gz` for `Precompressed` so compression happens
at deploy time. Files larger than its `MaxFileBytes` go from disk through `SendFile` each time; so
does plain `http.ServeFile`, because `Context` implements `io.ReaderFrom`.

```go
files, err := fibhttp.NewFileCache(fibhttp.FileCacheConfig{
	Root:          "/data/static",
	Precompressed: true,
})
if err != nil {
	panic(err)
}
app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	files.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
})
```

**The trap.** Where no watch reports (non-Linux, a symlinked file), an entry is trusted for
`CheckInterval` (5s by default) before it is stat'ed again; a negative interval stats on every
request and disables the watch. The cache is bounded by `MaxBytes`, and a file over `MaxFileBytes`
is read from disk on every request, so it is not a cache miss but a sendfile.

See [§14](../hacks.md#14-filecache-and-sendfile-static-files-without-touching-the-disk-or-user-space).

## 13. UDP: batches

**The trick.** On a listening UDP socket each peer is a `Connection`. A handler that implements
`DatagramsHandler` receives the datagrams that queued before its worker ran **together**, so a
reliable-UDP protocol acks a batch at once and sends fewer datagrams; `SendBatch` turns them into
one `sendmmsg`/`sendmsg_x`. `Engine.OpenUDPPeer` opens the connection of a peer the server must
speak to first (ICE, STUN).

```go
type acker struct{ fib.HandlerFuncs }

func (a acker) OnDatagrams(c *fib.Connection, datagrams [][]byte) {
	_ = c.SendBatch(acksFor(datagrams)) // one system call for the batch
}
```

**The trap.** Each datagram is the handler's own copy, a `bufferpool` buffer it may give back with
`bufferpool.Put` once nothing refers to it; the `[][]byte` holding them must not be kept after
`OnDatagrams` returns. UDP sends never queue — when the socket has no room the datagram is dropped
and the send returns an error. With `ReusePort` (the default) a peer `OpenUDPPeer` opened may reply
to another poller's socket, which opens a second connection for the same address, and a peer whose
NAT rebinds arrives as a new peer: key peer state by address, not by connection.

See [§15](../hacks.md#15-udp-datagramshandler-and-sendbatch).

## 14. Pools, processes and buffers

**The trick.** Four smaller levers with one theme: don't let the engine's scarce workers run your
work, and don't share a heap when a process of its own is cheaper.

- **A pool of your own** for async handler work, never the engine's pool: an engine worker submits
  to your pool, and if it were the same pool a full queue could leave every worker stuck
  submitting. `SharedTaskPool` (on by default) shares workers between engines of one `Name`, and
  the first engine's settings win. `Config.SetTaskPool(p)` is the other direction: it makes the
  engine run its connections on `p`, which the caller owns and stops after the engine has closed.
- **prefork** (Linux) runs the same program in several children, each with its own heap and
  collector, sharing the listening port through `SO_REUSEPORT`. Initialization the children need
  goes in `serve`; code before `Run` runs in the master and every child.
- **IOPollers** split the engine across event loops; more pollers compete with workers for Ps and
  are usually slower, so touch `IOPollerCount` and `EngineLoop` only with a profile.
- **GOMAXPROCS** stays at its default. Raising it is slower and uses more memory.
- **bufferpool** is fib's own buffer pool; `Get`/`Put` allocate nothing, a reserve survives GC
  (`SetRetainedBytes`), and your buffers can flow through the same pool as fib's.

```go
jobs := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
	Name: "jobs", MinWorkers: 16, MaxWorkers: 4096, QueueSize: 10000,
})
defer jobs.Stop()

app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	jobs.Go(c.Async(func() { // async work on a pool of its own, not the engine's
		_ = c.Respond(200, "text/plain", slowQuery())
	}))
})

buf := bufferpool.Get(4096) // len 4096; contents are the previous user's
defer bufferpool.Put(buf)   // after Put, not even a sub-slice may be used
```

**The trap.** `ModeInline` is not accepted by an engine (a connection round would run on the event
loop), and a prefork child shares nothing but the port with its siblings, so in-memory state exists
once per child and a resource that exists once per machine is divided by `prefork.Children()`.

See [§16](../hacks.md#16-pools-your-own-separate-ones-and-avoiding-deadlock),
[§17](../hacks.md#17-process-level-tricks-prefork-iopollers-gomaxprocs) and
[§18](../hacks.md#18-bufferpool-your-buffers-can-use-the-same-pool).

## 15. The cheat sheet

**The trick.** When in doubt, ask which goroutine runs this. The condensed version:

| Call | From where |
| --- | --- |
| `Connection.Send` / `SendParts` / `SendFile` / `Close` | any goroutine; never blocks |
| `Connection.HoldReads`, `Cork`/`Flush`, `Set*Deadline` | any goroutine; every `Cork` needs its `Flush` |
| `Context.Respond` / `Write` / `Finish` | any goroutine, before the last `Release`; the two write forms don't mix |
| `Context.Retain` / `Release` | any goroutine; paired; no-ops on a finished request |
| `Context.OnBody` / `OnCancel` | in the handler or later; callbacks must not block |
| `Context.Conn.Send` on h2/h3 | don't; use `Respond`/`WriteResponse` |
| `OnOpen` (TCP) | event loop: must be quick |
| `OnData` / `OnClose` | worker; serial per connection |
| inside `SyscallConn().Control(f)` | holds the connection lock: don't call this connection's methods |

See [§19](../hacks.md#19-cheat-sheet-what-may-be-called-from-which-goroutine).

## 16. Anti-patterns

**The trick.** The list is the cheapest debugging tool in the guide; these are the ones that bite:

1. Blocking in a handler, `OnData` or an `OnBody` callback (`time.Sleep`, synchronous RPC,
   `Wait()`, slow disk I/O) — it occupies a worker.
2. `Retain` without `Release` on every path; a `defer` or `sync.Once` keeps it at exactly one.
3. Touching `r`, `c` or `c.Body()` after `Release`, or handing them to a longer-lived goroutine.
4. Calling `Respond` after `Release`, or keeping `data` from `OnData`/`OnBody`/`OnFrame`.
5. A `bufio.Reader` on a `Connection`, or expecting `Read` to block.
6. A write watermark smaller than one round's replies.
7. `StreamRequestBody` with the default `ReadBodyTimeout` (5 minutes), so large uploads time out.
8. Async work on the engine's own pool; raising `GOMAXPROCS`; closing your own connection inside
   `OnData` and then waiting for `OnClose`.

See [§20](../hacks.md#20-anti-patterns).

## Pitfalls

- **These are edges, not defaults.** Reach for `Retain`, `HoldReads`, `SendParts` or a custom pool
  when a profile or a protocol demands it; the plain handler of
  [03-handlers.md](03-handlers.md) is still the right shape for most code.
- **The tricks do not compose blindly.** Borrowed memory plus an async branch, or a `Cork` held
  across goroutines, are how the same two APIs that make fib fast corrupt data and deadlock.
- **The backend differs by platform.** IOPollers need epoll/kqueue (Windows keeps one loop),
  `SendFile` uses `sendfile(2)` on Linux, macOS, FreeBSD and DragonFly and `TransmitFile` on Windows
  but reads in chunks on NetBSD/OpenBSD, `SendBatch` is one call only on Linux and macOS, and
  `SpliceTo` is Linux-only. Verify on the system you deploy to.
- **Measure, don't guess.** Every "X times faster" in this chapter's guide comes with a benchmark
  and a machine; reproduce it on yours before making it a policy.

## See also

- [The hack guide](../hacks.md) — the full explanation of every section this chapter condenses,
  with the benchmark conditions
- [Handlers: sync and async](03-handlers.md) — `Retain`/`Release`, `OnBody`, the handler pool
- [HTTP/1.x](06-http1.md) — corking, sendfile, `FileCache`, streaming responses
- [Raw TCP, UDP and Unix sockets](19-raw-io.md) — the `Connection` API used throughout this chapter
- [Performance and operations](22-performance.md) — pool modes, sizes, prefork and pprof for real
- [flows.html](../flows.html) — the diagrams of a round, a cork and a handler pool
- [Architecture](../../README.md#architecture) — the three rules in the README, and the benchmark
  tables
