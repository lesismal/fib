# Raw TCP, UDP and Unix sockets

[English](19-raw-io.md) | [简体中文](19-raw-io.zh-CN.md)

This chapter is fib at its lowest level: an `Engine` with a TCP, UDP or Unix binding and a
`fib.Handler` that sees bytes, with no HTTP, WebSocket or RPC in between. It shows the handler model,
the `Connection` API (writes, corking, reads, deadlines, half close, sendfile and `SpliceTo`), UDP
with one connection per peer, Unix sockets and the dial side of the same engine. It is the layer
every other protocol in this tutorial is built on, and the right one when the protocol is yours, or
when the framing and backpressure must be exactly what you decide.

## The model

A binding listens; every accepted connection (or, on UDP, every peer address) is a `*fib.Connection`.
The connection's callbacks run one round at a time on a worker from the engine's pool: `OnData` gets
a slice of what this round read, `OnOpen` runs on the event-loop goroutine before any round, and
`OnClose` runs in the connection's last round, after its last `OnData`. Two consequences shape every
handler. First, data passed to `OnData` is only valid during the call — copy what you keep. Second,
a handler must not block: while it runs, its worker is not serving other connections, and the next
round of the same connection waits. Writes made in a round are corked and reach the socket in one
write when the round ends, which is fib's pipelining. `Read` on a connection does not block; it
returns `fib.ErrWouldBlock` when the socket has nothing more right now.

## A TCP echo server and client

One engine can serve a binding and dial it at the same time; `DialWithHandler` gives the dialed
connection a handler of its own, so a program can be server and client without a second engine:

```go
package main

import (
	"fmt"
	"log"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:9000",
		Handler: fib.HandlerFuncs{
			Open: func(c *fib.Connection) { log.Printf("open %s", c.RemoteAddr()) },
			Data: func(c *fib.Connection, b []byte) {
				// b is valid only during this call; Send copies what it needs.
				if err := c.Send(b); err != nil {
					c.Close()
				}
			},
			Close: func(c *fib.Connection, err error) { log.Printf("close: %v", err) },
		},
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() {
		if err := engine.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	got := make(chan string, 1)
	err = engine.DialWithHandler("tcp", "127.0.0.1:9000", 5*time.Second, fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("hello\n")) },
		Data: func(c *fib.Connection, b []byte) {
			got <- string(b)
			c.Close()
		},
	}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("echo: %s", <-got)
}
```

`fib.HandlerFuncs` fills in only the callbacks a handler needs; the rest are no-ops. A handler
with state is a struct implementing `OnOpen`, `OnData`, `OnPriorityData` and `OnClose`. The done
callback of a dial runs exactly once, on the event loop, with `*net.OpError` on failure, so it must
not block either.

## The Connection API

- `Send(data)` copies `data` before returning, tries the socket first and queues the rest; it never
  waits. `SendParts(header, body)` sends two slices without joining them, and `SendOwned` /
  `SendShared` avoid the copy when ownership is clear (`SendShared` for one buffer sent to many
  connections).
- `Cork()` and `Flush()` control batching by hand, for replies produced outside the connection's
  round. Inside `OnData` writes are corked automatically until the callback returns.
- `Read` takes what the socket has, and `SendFile(f, offset, count)` sends a file range by
  `sendfile(2)`/`TransmitFile`, reading it as the peer keeps up.
- `SetReadDeadline`, `SetWriteDeadline` and `SetDeadline` close the connection when they pass rather
  than failing a call; `OnClose` receives `os.ErrDeadlineExceeded`.
- `HoldReads(true)` stops reading a connection the application cannot keep up with, leaving the data
  in the kernel and the peer slowed by TCP; `HoldReads(false)` redelivers what already arrived.
- `Close` drops unsent output, `CloseAfterSend` finishes it first, `CloseWrite` shuts the sending
  side only (the peer reads to EOF, `OnData` keeps running), and `CloseRead` the receiving side.
- `SetHandler` hands the connection to another handler, which is how `mux` switches protocol after
  sniffing the first bytes; `ShuttingDown` reports that the engine or binding is draining.
- `SetAttachment`/`Attachment` hang per-connection state on the connection; `SendBatch` and
  `SetUDPIdleTimeout` are the UDP-only members.

Corking is the one call worth learning by heart, because the usual mistake is to send several
replies from a goroutine and get one write per reply:

```go
func replyAll(c *fib.Connection, replies [][]byte) error {
	c.Cork()
	for _, r := range replies {
		if err := c.Send(r); err != nil {
			_ = c.Flush()
			return err
		}
	}
	return c.Flush() // nothing else writes what a Cork holds
}
```

`SpliceTo` goes one step further: it moves bytes between two connections through a pipe, so they
never pass through user space. It is Linux-only, and only for plain TCP or Unix connections:

```go
// proxyNext moves the next n bytes of src to dst through splice(2).
// Elsewhere, or between connections it cannot splice, it returns fib.ErrSpliceUnsupported.
func proxyNext(src, dst *fib.Connection, n int64) error {
	return src.SpliceTo(dst, fib.Splice{
		Begin: func() int64 { return n },
		Read:  func(moved int64, err error) { log.Printf("moved %d bytes: %v", moved, err) },
		Written: func(err error) {
			if err != nil {
				dst.Close() // a splice that cannot finish cuts dst's stream
			}
		},
	})
}
```

## Reads, deadlines and backpressure

`OnData` sees what the round's read loop got; a handler that must consume the rest of the socket
reads it itself, and a rolling idle timeout is a deadline reset on every round:

```go
func echoWithIdleTimeout() fib.Handler {
	return fib.HandlerFuncs{
		Data: func(c *fib.Connection, b []byte) {
			// OnData gave what this round read; take what is left in the socket.
			buf := make([]byte, 4096)
			for {
				n, err := c.Read(buf)
				if errors.Is(err, fib.ErrWouldBlock) {
					break // nothing more now; the next OnData delivers the rest
				}
				if err != nil {
					return // io.EOF, or the reason the connection closed
				}
				_ = c.Send(buf[:n])
			}
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
		},
	}
}
```

Because `Read` never waits, it must not be given to `bufio.Reader` or any code written for a
blocking socket. The idiomatic backpressure for a handler that buffers input is `HoldReads`: hold
when the buffer is over its high watermark, release from whichever goroutine drains it, and the TCP
window slows the peer instead of memory growing (section 8 of [the hack guide](../hacks.md)).

## UDP datagrams

On a UDP binding each peer address becomes a connection of its own: the first datagram opens it
(`OnOpen`), later ones go to the same connection, and it closes after `Config.UDPIdleTimeout` (60
seconds by default) of silence, with `ErrUDPIdleTimeout` in `OnClose`. A handler implementing
`fib.DatagramsHandler` receives the datagrams already queued for that peer as one burst and can
answer the whole burst with one `SendBatch` — the ACK coalescing QUIC and HTTP/3 use:

```go
package main

import (
	"fmt"
	"log"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "udp",
		Addr:    "127.0.0.1:9001",
		Handler: burstEcho{},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	reply := make(chan string, 1)
	client := fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("ping")) },
		Data: func(c *fib.Connection, b []byte) {
			reply <- string(b)
			c.Close()
		},
	}
	if err := engine.DialWithHandler("udp", "127.0.0.1:9001", 5*time.Second, client, nil); err != nil {
		log.Fatal(err)
	}
	fmt.Println(<-reply)
}

// burstEcho answers each burst of datagrams with one batch.
type burstEcho struct{}

func (burstEcho) OnOpen(c *fib.Connection)                   {}
func (burstEcho) OnClose(c *fib.Connection, err error)       {}
func (burstEcho) OnPriorityData(c *fib.Connection, b []byte) {}
func (burstEcho) OnData(c *fib.Connection, b []byte)         { answer(c, [][]byte{b}) }
func (burstEcho) OnDatagrams(c *fib.Connection, d [][]byte)  { answer(c, d) }

func answer(c *fib.Connection, dgrams [][]byte) {
	if err := c.SendBatch(dgrams); err != nil { // one sendmmsg for the whole burst
		log.Print(err)
	}
}
```

Two details separate UDP from TCP: a datagram is dropped when the socket has no room, with the error
returned and the connection left open (UDP sends never queue), and `OpenUDPPeer(binding, addr)`
lets a binding speak first to a peer that has sent nothing. It waits for the socket's event loop, so
it must not be called from a callback that loop runs. With `ReusePort` on, key peers by address:
a reply may arrive on another poller and open a second connection for the same peer.

## Unix sockets

A Unix binding is the same as a TCP one with `Network: "unix"` and the socket path as `Addr`. fib
removes the path when the engine or binding closes, and the path must not exist when it starts, so a
crashed process leaves a file the next one has to remove. The connections behave exactly like TCP
ones; there are no addresses, so `RemoteAddr` is the empty address:

```go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	const sock = "/tmp/fib-admin.sock"
	_ = os.Remove(sock) // a crashed process may have left the path behind

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "unix",
		Addr:    sock,
		Handler: fib.HandlerFuncs{
			Data: func(c *fib.Connection, b []byte) {
				_ = c.SendParts([]byte("admin: "), b)
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	replies := make(chan string, 1)
	err = engine.DialWithHandler("unix", sock, 5*time.Second, fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("status")) },
		Data: func(c *fib.Connection, b []byte) {
			replies <- string(b)
			c.Close()
		},
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(<-replies)
}
```

## Dialing out

`Engine.Dial(network, addr, timeout, done)` is the short form. `DialWithHandler` names the handler
of the dialed connection, which is what lets one engine carry a server and its own clients.
`DialWith` takes a `DialOptions`: the local address, and a port range for a protocol that must leave
from a known one. None of them blocks the caller — name resolution runs on a client pool and the
result arrives through the done callback:

```go
func dialFromRange(engine *fib.Engine, addr string, handler fib.Handler) error {
	return engine.DialWith(fib.DialOptions{
		Network:   "tcp",
		Addr:      addr,
		Timeout:   5 * time.Second,
		Handler:   handler,
		LocalAddr: netip.MustParseAddrPort("127.0.0.1:0"), // the port comes from the range
		PortMin:   20000,
		PortMax:   20099,
	}, func(c *fib.Connection, err error) {
		if err != nil {
			log.Printf("dial failed: %v", err) // *net.OpError
			return
		}
		_ = c.Send([]byte("hello"))
	})
}
```

The same options dial `udp` (the socket is connected, so one `Send` is one datagram and `OnData` is
one received datagram) and `unix`.

## When raw is the right layer

Use raw TCP, UDP or Unix when the protocol is your own — a binary framing, a game protocol, a
database wire protocol, an internal RPC with requirements no generic one meets — or when you need
what the higher layers hide: corking and `SendParts` for exact write batching, `HoldReads` for
application-driven backpressure, `SendFile`/`SpliceTo` for zero-copy relaying, `CloseWrite` for
protocols that half-close, `SetHandler` for sniffing. For everything with a standard shape, use the
layer that already parses, frames and secures it: [03-handlers.md](03-handlers.md) and
[06-http1.md](06-http1.md) for HTTP, [09-websocket.md](09-websocket.md), [11-grpc.md](11-grpc.md),
[12-arpc.md](12-arpc.md), [15-multiprotocol.md](15-multiprotocol.md). Those are all `fib.Handler`s
over this same layer, so a raw handler can sit beside them on one engine and even share a port.

## Pitfalls

- **`OnData`'s slice is borrowed.** It is only valid during the call; keep a copy, or hand it to
  something that copies (`Send` does). The same holds for UDP datagrams in `OnDatagrams`.
- **`OnOpen` runs on the event loop.** Blocking there stalls every connection on that loop; do
  handshakes and I/O in `OnData`, or hand work to a pool.
- **Never block a handler.** One connection runs one round at a time, and blocking occupies a
  worker the other connections share. Long work belongs on a goroutine or a task pool; the connection
  can be kept alive meanwhile (chapter [03-handlers.md](03-handlers.md)).
- **`Read` is not `net.Conn.Read`.** It returns `fib.ErrWouldBlock` instead of waiting; a deadline
  closes the connection instead of failing a call.
- **UDP has no `Read`.** The event loop reads the datagrams and delivers them whole; a datagram the
  socket cannot take is dropped, with an error returned and the connection still open.
- **A silent UDP peer disappears.** `UDPIdleTimeout` (60 s by default) or `SetUDPIdleTimeout` decides
  when; a peer that must outlive it needs traffic or a negative timeout.
- **Unix socket paths persist.** A process killed with a live socket leaves the file; remove it
  before listening, and remember fib removes it on a clean close.
- **`SpliceTo` is Linux-only** and needs plain TCP or Unix connections on both sides; check its
  `error` and fall back to a `Read`/`Send` loop elsewhere.

## See also

- [The hack guide](../hacks.md) — §8 `HoldReads`, §9 cork and sendfile, §10 `Read` and deadlines, §15 UDP
- [Handlers: sync and async](03-handlers.md) — the execution model in full
- [Engine and configuration](02-engine.md) — `UDPIdleTimeout`, buffers, `ReusePort`, `IOPollers`
- [The README](../../README.md#raw-tcp) — raw TCP in four lines, and where each protocol runs
- [BitTorrent](18-bittorrent.md) — a full protocol stack built on this layer
- [Clients](20-clients.md) — dialing with the engine, callbacks and futures
