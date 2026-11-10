# KCP

[English](14-kcp.md) | [简体中文](14-kcp.zh-CN.md)

KCP is a reliable ARQ protocol that trades bandwidth for latency: it retransmits sooner than TCP,
on selective and cumulative acknowledgements, and can turn congestion control off. fib implements
it on its UDP bindings, wire-compatible with [skywind3000/kcp](https://github.com/skywind3000/kcp)
and with [xtaci/kcp-go](https://github.com/xtaci/kcp-go) and therefore with kcptun, including
kcp-go's crypt and FEC framing. This chapter shows a session as a `net.Conn`, a `net/http` server
on KCP, message sessions, the tuning knobs, FEC and crypt, and the sans-IO `KCP` type for a
transport of your own.

## The layer model

KCP in fib is three things stacked on a UDP connection:

- **`KCP`** is the protocol without I/O: `Input` takes the peer's packets, `Send` queues messages,
  `Recv` returns the peer's, and `Flush` sends through a function of the caller's when `NextFlush`
  says. It is the reference implementation's segment format, usable without any of fib's sockets.
- **`Crypt` and the FEC shards** frame those packets as kcp-go does: its CFB block ciphers, XOR
  table, AEADs or "none", and its Reed-Solomon data and parity shards. `Config.Crypt`,
  `Config.DataShards` and `Config.ParityShards` turn them on.
- **`Session`, `Server`, `Dial`** put a conversation on a fib UDP connection. `Server` is the
  `fib.Handler` of a UDP binding and a `net.Listener` of its sessions; `Dial` starts a client
  session from a binding of its own.

A `Server` keys sessions by peer address, as kcp-go's `Listener` does. A datagram from an address
without a session starts one only when its first segment is the start of a conversation — data
with sequence number 0; a peer that starts another conversation that way from the same address
replaces its session. KCP has no handshake and no close: a conversation is a 32-bit ID both sides
know, and it ends when either stops answering — from `Config.IdleTimeout` (`ErrIdle`, one minute
by default) or when one segment goes `Config.DeadLink` transmissions without an acknowledgement
(`ErrDeadLink`).

A session is a `net.Conn` — `Read` and `Write` carry a stream of bytes as a kcp-go
`UDPSession`'s do — or, with `Config.OnMessage`, messages arrive whole at a callback and `Send`
sends messages. Its KCP is driven by the datagrams fib reads, by its own timer and by the
application's calls, serialized by a mutex; what each sends goes out in one batch once it
releases the mutex. Unlike the reference implementation, which flushes every conversation every
interval whether it has anything to send or not, a session here flushes only when something is
due: after each burst of datagrams, after `Write` and `Send`, and when its timer reports the
earliest retransmission timeout or window probe. An idle session has no timer but its idle
timeout.

## A KCP echo server and client

A complete program: one engine, a KCP listener on it, an echo session loop, and a client dialing
through the same engine. Both sides run kcptun's "fast3" settings:

```go
package main

import (
	"io"
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/kcp"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	// kcptun's "fast3": lower RTO, faster resends, no congestion control.
	// Both sides should agree.
	fast3 := kcp.Config{NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true}

	srv, err := kcp.Listen(engine, "127.0.0.1:4000", kcp.ServerConfig{Config: fast3})
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() {
		for {
			conn, err := srv.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	conn, err := kcp.Dial(engine, "127.0.0.1:4000", fast3)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello kcp")); err != nil {
		panic(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		panic(err)
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	log.Printf("echo: %s", buf[:n])
}
```

`kcp.Listen` adds a UDP binding to a running engine and returns a `*kcp.Server`, which is a
`net.Listener`: `Accept` hands out sessions, and `Close` closes them and removes the binding.
`kcp.Dial` adds a client binding of its own, starts a conversation of a random ID, and returns at
once — there is no handshake to wait for, and the server hears of the session with the first
segment; `kcp.DialConv` is the same with a conversation ID of the caller's. Neither may be called
from a callback that the engine's event loop runs. A session reports `RTT()` and `Stats()`
(segments sent, retransmitted, FEC-recovered), and `Server.Sessions()` lists the open ones.

## net/http on KCP

Because a session is a `net.Conn` and the server a `net.Listener`, a standard `net/http` server
runs on KCP unchanged; on the client side a `net/http.Transport` only needs a `DialContext` that
returns a session:

```go
package main

import (
	"context"
	"io"
	"log"
	"net"
	nethttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/kcp"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	srv, err := kcp.Listen(engine, "127.0.0.1:4001", kcp.ServerConfig{})
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	mux := nethttp.NewServeMux()
	mux.HandleFunc("/", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		_, _ = io.WriteString(w, "hello from net/http over KCP\n")
	})
	go func() { _ = nethttp.Serve(srv, mux) }()

	client := &nethttp.Client{Transport: &nethttp.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return kcp.Dial(engine, addr, kcp.Config{})
		},
	}}
	rsp, err := client.Get("http://127.0.0.1:4001/")
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: %s", rsp.Status, body)
}
```

The listener and the dialer are what KCP replaces; HTTP, TLS or any other stream protocol above
them does not know the transport changed. With no crypt and no FEC, a session sends KCP packets
with no extra framing, as kcp-go without a `BlockCrypt` does, so it drops straight onto a UDP
path.

## Server configuration

`kcp.NewServer` builds the handler when the binding should be created with the engine rather than
added later, and `ServerConfig` says how sessions are admitted and what they default to:

```go
server, err := kcp.NewServer(kcp.ServerConfig{
	Config: kcp.Config{}, // every session's configuration
	Accept: func(remote netip.AddrPort, conv uint32) bool {
		return remote.Addr().IsLoopback() // a cheap filter before a session exists
	},
	OnSession: func(s *kcp.Session) {
		// Runs before the session's first datagram is read, on a fib worker:
		// do not block, do not Read or Write; Send may.
		_ = s.Send([]byte("welcome"))
	},
	MaxSessions:   1024,
	AcceptBacklog: 128,
})
if err != nil {
	panic(err)
}
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Network: "udp", Addr: "127.0.0.1:4000", Handler: server})
if err != nil {
	panic(err)
}
defer engine.Close()
go func() { _ = engine.Run() }()
```

Without `OnSession`, sessions wait for `Server.Accept`, which is the `net.Listener` path of the
two programs above. `AcceptBacklog` bounds how many sessions wait for it, and a new one past it is
dropped for the peer to send again; `MaxSessions` bounds the open sessions. The binding's
engine must already be running when `Listen` or `Dial` is called.

## Message sessions

A message session replaces `Read` and `Write` with `Config.OnMessage` and `Session.Send`: each
message arrives whole, on a fib worker, one at a time per session, and `Send` queues one message
without blocking:

```go
server, err := kcp.NewServer(kcp.ServerConfig{Config: kcp.Config{
	OnMessage: func(s *kcp.Session, msg []byte) {
		_ = s.Send(append([]byte("echo: "), msg...)) // never blocks
	},
	OnClose: func(s *kcp.Session, err error) {
		log.Printf("session %d closed: %v", s.Conv(), err)
	},
}})
if err != nil {
	panic(err)
}
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Network: "udp", Addr: "127.0.0.1:4002", Handler: server})
// ...
conn, err := kcp.Dial(engine, "127.0.0.1:4002", kcp.Config{
	OnMessage: func(s *kcp.Session, msg []byte) {
		log.Printf("message: %s", msg)
	},
})
if err != nil {
	panic(err)
}
if err := conn.Send([]byte("hello")); err != nil {
	panic(err)
}
```

The callback must not call `Write`, which can wait for acknowledgements that only the worker
reading this session delivers; `Send` is the one to use, and `WaitSend` says how much it has
queued. With `OnMessage`, the session's flow control belongs to the application: messages are
taken out of KCP as they complete, not as the application reads. `OnClose` reports why a session
ended: nil for `Close`, `ErrIdle`, `ErrDeadLink`, `ErrReplaced`.

## Tuning

The knobs are the reference implementation's, named as kcp-go names them:

```go
cfg := kcp.Config{
	// kcptun's fast3; less aggressive settings are the reference defaults.
	NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true,
	// Windows in segments, both sides; the default is 128/128 and kcp-go's is 32/32.
	SendWindow: 256, RecvWindow: 256,
	// The largest datagram, framing (crypt, FEC) included.
	MTU: 1400,
	// A quiet session ends after IdleTimeout; a negative value never ends it.
	IdleTimeout: time.Minute,
	// Transmissions of one segment without an acknowledgement before the link is dead.
	DeadLink: 20,
	// Segments queued or in flight before Write blocks; twice the send window when zero.
	WriteQueue: 512,
	// Pack Write's bytes into whole segments, as kcptun does, instead of a message per Write.
	StreamMode: true,
}
srv, err := kcp.Listen(engine, "127.0.0.1:4000", kcp.ServerConfig{Config: cfg})
if err != nil {
	panic(err)
}
```

`NoDelay`, `Interval`, `Resend` and `NoCongestion` are `KCP.NoDelay`'s knobs: interval enters the
RTO calculation, `Resend` retransmits early once that many later segments were acknowledged, and
`NoCongestion` leaves only the windows. Both ends should agree on them. The windows and `MTU` must
match the peer's expectations of what the path carries; a `Config` whose FEC or crypt framing
makes the datagram larger than the path MTU keeps retransmitting.

## FEC and crypt

`Config.Crypt` is kcp-go's `BlockCrypt`, and `Config.DataShards`/`ParityShards` its forward error
correction. Both must match the peer's; FEC shards from a peer that sends them are read even when
the session has none, but their parity is not used:

```go
key := bytes.Repeat([]byte{0x2a}, 32) // already derived; kcptun derives with PBKDF2-SHA1
crypt, err := kcp.NewCrypt("aes", key) // "null", "none", "xor", "aes", "aes-128", "aes-192", "3des", "aes-128-gcm"
if err != nil {
	panic(err)
}
cfg := kcp.Config{
	Crypt:        crypt,
	DataShards:   10, // kcptun's default FEC
	ParityShards: 3,
}
srv, err := kcp.Listen(engine, "127.0.0.1:4000", kcp.ServerConfig{Config: cfg})
if err != nil {
	panic(err)
}
```

`NewCrypt` takes kcptun's cipher names (`"null"` gives a nil `*Crypt`: no crypt at all, while
`"none"` keeps kcp-go's nonce and CRC header unencrypted) and a key that is already derived: kcptun derives it with
`pbkdf2.Key(sha1.New, passphrase, []byte("kcp-go"), 4096, 32)`, so a fib server serves kcptun
clients only with that derivation in front. For the ciphers the standard library does not have —
blowfish, cast5, twofish, tea, xtea, sm4 — `NewBlockCrypt` wraps any `cipher.Block` of 8 or 16
byte blocks from wherever it is implemented; `NewAESCrypt`, `NewAESGCMCrypt`, `NewTripleDESCrypt`,
`NewXORCrypt` and `NewNoneCrypt` are the rest. Salsa20 is not supported: it is not a block cipher.

## Sans-IO KCP

`kcp.New` is the protocol alone, for a transport of the application's own. The caller supplies the
time and moves the packets. A complete program, two ends of one conversation wired back to back in
memory:

```go
package main

import (
	"log"
	"time"

	"github.com/lesismal/fib/kcp"
)

func main() {
	// Two ends of conversation 7, wired back to back: what one outputs is
	// queued for the other's Input. A real transport would write it to a
	// socket instead; output's packet is valid only for the call.
	var toB, toA [][]byte
	a := kcp.New(7, func(packet []byte) { toB = append(toB, append([]byte(nil), packet...)) })
	b := kcp.New(7, func(packet []byte) { toA = append(toA, append([]byte(nil), packet...)) })
	a.NoDelay(1, 10, 2, true) // kcptun's fast3
	b.NoDelay(1, 10, 2, true)

	start := time.Now()
	clock := func() uint32 { return uint32(time.Since(start).Milliseconds()) }

	if err := a.Send([]byte("hello sans-IO")); err != nil {
		panic(err)
	}
	// flush runs Flush when NextFlush says something is due.
	flush := func(k *kcp.KCP, now uint32) {
		if at, due := k.NextFlush(now); due && int32(at-now) <= 0 {
			k.Flush(now)
		}
	}
	deliver := func(k *kcp.KCP, packets *[][]byte, now uint32) {
		for _, p := range *packets {
			if err := k.Input(p, now); err != nil {
				log.Print(err)
			}
		}
		*packets = (*packets)[:0]
	}
	for i := 0; i < 100; i++ {
		now := clock()
		flush(a, now)
		deliver(b, &toB, now)
		flush(b, now) // b's acknowledgements
		deliver(a, &toA, now)
		if size := b.PeekSize(); size > 0 {
			buf := make([]byte, size)
			n, err := b.Recv(buf)
			if err != nil {
				panic(err)
			}
			log.Printf("b received %q; a still has %d segments in flight", buf[:n], a.WaitSend())
			return
		}
		time.Sleep(time.Millisecond)
	}
	log.Print("nothing arrived")
}
```

`Input` copies the data of the segments it keeps; `NextFlush` says when `Flush` next has something to
do; `Send` keeps the message it is given until it is sent, up to 255 segments, unless stream mode
is on. A `KCP` is not safe for concurrent use and times are milliseconds on any clock, wrapping
at 2^32. This is the piece to use under an application-level relay that cannot lend fib a UDP
socket.

## Compatibility with kcp-go and kcptun

`kcp.Dial` is kcp-go's `DialWithOptions`, `kcp.Listen` its `ListenWithOptions`,
`ServerConfig.Config` is `DialWithOptions`' options, and the knobs are `SetNoDelay`,
`SetWindowSize`, `SetMtu` and `SetStreamMode` by another name. A fib server takes kcp-go and
kcptun clients and a fib client talks to their servers, for every crypt the two share and with
FEC. The `kcp/interop` module, a module of its own so that kcp-go is never fib's dependency, tests
it in both roles through a relay that drops one datagram in 17:

```sh
cd kcp/interop && go test -race ./...
```

What differs: kcp-go's out-of-band packets (type 0xf3) are dropped; there is no FEC auto-tuning,
so shards must match; and the defaults differ — windows 128/128 rather than 32/32, an idle
timeout where kcp-go has none, and a dead link that closes the session where kcp-go keeps it. The
full mapping is in [docs/kcp.md](../kcp.md#compatibility-with-kcp-go).

## Pitfalls

- **KCP has no handshake and no close.** A session is usable at once, and `Close` sends what is
  due and stops without waiting for unacknowledged data. The peer only learns that a session ended
  from its own idle timeout or dead link, or from the application's protocol.
- **Send your own keepalive if the application may be quiet.** `Config.IdleTimeout` defaults to
  one minute; kcptun keeps a quiet link alive through smux. Set a longer timeout, or send
  heartbeats at the application level.
- **A timed-out session cannot be revived.** A new conversation starts only with a segment whose
  sequence number is 0, so a client whose session the server timed out gets no answer for
  mid-conversation segments and dead-links; it must dial again.
- **Both sides must agree on tuning, crypt and FEC.** FEC shards must match; a crypt of a
  different cipher or key garbles every datagram; `NoDelay`, `Interval`, `Resend` and
  `NoCongestion` are documented as an agreement, not a hint.
- **`OnMessage` must not `Write`.** `Write` can wait for acknowledgements that only the worker
  reading the session delivers, so it would deadlock; use `Send` and watch `WaitSend`.
- **`OnSession` must not block or read.** It runs on a fib worker before the first datagram is
  read; `Send` may be called, `Read` and `Write` may not.
- **`Write` blocks; `Send` does not.** `Write` waits while `Config.WriteQueue` segments are
  queued or in flight (twice the send window by default), so a sender that outruns its peer
  should use `Send` and `WaitSend`.
- **One session per peer address.** A peer whose address changes (NAT rebinding, mobile handover)
  looks like a new peer: the old session idles out, and the new one starts only from sequence
  number 0.
- **Idle sessions still hold their place.** An idle session has no timer, but its entry stays until
  `IdleTimeout` — with a UDP binding of its own for a dialed session, while a `Server`'s sessions
  share its binding; bound them with `MaxSessions` and a sensible idle timeout.
- **Do not call `Listen` or `Dial` from an engine callback.** Both wait for the engine's event
  loop, which that callback would be holding.

## See also

- [docs/kcp.md](../kcp.md), the reference page with the layer and compatibility tables.
- The [`kcp` package](../../kcp) and the interop module [`kcp/interop`](../../kcp/interop).
- The root README's [KCP section](../../README.md#kcp).
- [Chapter 19: Raw TCP, UDP and Unix sockets](19-raw-io.md) for the UDP layer KCP runs on.
- [Chapter 22: Performance and operations](22-performance.md) for tuning and profiling a server.
- [xtaci/kcp-go](https://github.com/xtaci/kcp-go) and
  [xtaci/kcptun](https://github.com/xtaci/kcptun) for the compatible upstreams.
