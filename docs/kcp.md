# KCP

[中文](kcp.zh-CN.md)

Package [`kcp`](../kcp) is KCP, the ARQ protocol of
[skywind3000/kcp](https://github.com/skywind3000/kcp), on fib's UDP bindings. Its segments are the
reference implementation's, and its datagrams are those of
[xtaci/kcp-go](https://github.com/xtaci/kcp-go), crypt and FEC included, so a fib server takes
kcp-go and kcptun clients and a fib client talks to their servers. It has no dependency: the
ciphers come from the standard library, and the Reed-Solomon code is written here, giving the
parity `klauspost/reedsolomon` gives.

## Layers

| Layer | What it is |
| --- | --- |
| `KCP` | The protocol without I/O: `Input` takes the peer's packets, `Send` queues messages, `Recv` returns the peer's, `Flush` sends through a function of the caller's when `NextFlush` says. |
| `Crypt`, FEC | kcp-go's framing: the nonce and CRC32 header with its CFB block ciphers or XOR table, or an AEAD's nonce and tag; data and parity shards of a Reed-Solomon code. |
| `Session`, `Server`, `Dial` | A conversation on a fib UDP connection, a `net.Conn` or a message callback; the `fib.Handler` (and `net.Listener`) that makes a session of each peer; a client from a binding of its own. |

## Flushing when something is due

The reference implementation and kcp-go flush every conversation every `interval` (10 to 100 ms),
whether it has anything to send or not. Here a session flushes:

- after each burst of datagrams fib reads for it, so acknowledgements go out at once, together, in
  one `sendmmsg` on Linux;
- after `Write` and `Send`, and after `Read` reopens a closed receive window;
- when its timer fires, at the earliest retransmission timeout or window probe.

`KCP.NextFlush` returns no time at all for a conversation with nothing in flight, so an idle session
has no timer but its idle timeout. `Config.Interval` still enters the RTO, as in the reference.

## Compatibility with kcp-go

| kcp-go | fib |
| --- | --- |
| `DialWithOptions(addr, block, ds, ps)` | `kcp.Dial(engine, addr, kcp.Config{Crypt: c, DataShards: ds, ParityShards: ps})` |
| `ListenWithOptions(addr, block, ds, ps)` | `kcp.Listen(engine, addr, kcp.ServerConfig{Config: ...})`, or a `kcp.Server` in a `fib.Binding` |
| `SetNoDelay(nodelay, interval, resend, nc)` | `Config.NoDelay`, `Interval`, `Resend`, `NoCongestion` |
| `SetWindowSize(snd, rcv)` | `Config.SendWindow`, `RecvWindow` (128 and 128 when zero) |
| `SetMtu(mtu)` | `Config.MTU`, the whole datagram (1400 when zero) |
| `SetStreamMode(true)` | `Config.StreamMode` |
| `block == nil` | `Config.Crypt == nil` |
| `NewNoneBlockCrypt`, `NewSimpleXORBlockCrypt`, `NewAESBlockCrypt`, `NewTripleDESBlockCrypt`, `NewAESGCMCrypt` | `NewNoneCrypt`, `NewXORCrypt`, `NewAESCrypt`, `NewTripleDESCrypt`, `NewAESGCMCrypt`; `NewCrypt` takes kcptun's names |
| Blowfish, CAST5, Twofish, TEA, XTEA, SM4 | `NewBlockCrypt(block)` with any `cipher.Block` of 8 or 16 byte blocks, e.g. from `golang.org/x/crypto` |
| Salsa20 | not supported: it is not a block cipher |

`kcp/interop`, a module of its own so that kcp-go is never fib's dependency, runs fib against
kcp-go in both roles through a relay that drops one datagram in 17, for every crypt above with and
without 10/3 FEC, and checks that each side recovered the other's lost packets from parity. It runs
in CI as the `kcp-interop` job.

```sh
cd kcp/interop && go test -race ./...
```

## Server

A `Server` keys sessions by peer address, as kcp-go's `Listener` does. A datagram from an address
without a session starts one only when its first segment is the start of a conversation: data with
sequence number 0. A peer that starts another conversation that way from the same address replaces
its session (closed with `ErrReplaced`). `ServerConfig.Accept` filters, `MaxSessions` bounds them,
and `OnSession` takes each new session on a fib worker instead of `Accept`.

KCP has no close: a session ends with `Close`, when its peer is silent for `Config.IdleTimeout`
(`ErrIdle`, one minute by default), or when a segment goes `Config.DeadLink` transmissions without
an acknowledgement (`ErrDeadLink`). An application that may be quiet longer sends its own
keepalives, as kcptun does through smux.

## Read and Write, or messages

Without `Config.OnMessage`, a session is a `net.Conn` with deadlines: `Read` takes messages out of
KCP only as the application reads, so a slow reader closes the receive window and the peer waits;
`Write` blocks while `Config.WriteQueue` segments (twice the send window) are queued or in flight.
Like kcp-go's, `Write` sends bytes in segments of their own, so the peer's reads see a stream.

With `Config.OnMessage`, each message comes to the callback whole as it completes, on a fib worker,
one at a time per session, with the session's mutex released. The callback may `Send` (one message,
never blocking); it must not `Write`, which could wait for acknowledgements only that worker reads.

## Decisions pending review

- **Event-driven flushing** rather than a fixed interval: acknowledgements are not delayed by up to
  `interval`, idle sessions cost no timer; more acknowledgement datagrams when bursts are small.
- **New sessions only on sequence number 0** (kcp-go creates one on any segment): a client whose
  session the server timed out cannot revive it with a mid-conversation segment, which would wait
  forever for the segments before it; it gets no answer and dead-links.
- **One session per address** with kcp-go's replacement rule, rather than one per (address,
  conversation) as the reference implementation's users sometimes multiplex.
- **Defaults**: windows 128/128 (kcp-go 32/32), idle timeout one minute (kcp-go none), dead link
  closes the session (kcp-go keeps it).
- **No FEC auto-tuning**: the shards must match the peer's. FEC shards from a peer when the session
  has none are still read, their parity ignored. kcp-go's out-of-band packets (type 0xf3) are
  dropped.
- **Close sends what is due and stops**: unacknowledged data is not waited for, as in kcp-go.
