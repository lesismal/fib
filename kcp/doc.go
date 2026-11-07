// Package kcp implements KCP, the ARQ protocol of
// github.com/skywind3000/kcp, on fib's UDP bindings, wire-compatible with
// the reference implementation and with github.com/xtaci/kcp-go (and so
// with kcptun), including kcp-go's encryption and forward error
// correction.
//
// KCP trades bandwidth for latency: it retransmits sooner than TCP, on
// selective and cumulative acknowledgements and, with Config.Resend, on
// the acknowledgements of later segments, and it can turn its congestion
// control off. It has no handshake and no close: a conversation is a
// 32-bit ID both sides know, and it ends when either stops answering.
//
// The package is in three layers:
//
//   - KCP is the protocol without any I/O (sans-IO): Input takes the
//     peer's packets, Send queues messages, Recv returns the peer's, and
//     Flush, when NextFlush says, sends through a function of the
//     caller's. Unlike the reference implementation it is flushed when
//     something is due rather than on a fixed interval, so an idle
//     conversation costs nothing.
//   - Crypt and the FEC shards (Config.DataShards, Config.ParityShards)
//     frame the packets as kcp-go does: its CFB block ciphers, XOR table,
//     AEADs and "none", and its Reed-Solomon code, with the parity
//     github.com/klauspost/reedsolomon computes, without depending on
//     either.
//   - Session is a conversation on a fib UDP connection, a net.Conn, or a
//     message-at-a-time session with Config.OnMessage. Server is the
//     fib.Handler of a UDP binding that makes a Session of each peer, and
//     a net.Listener of them; Dial starts one from a binding of its own.
//
// A Session takes each burst of datagrams fib reads in one go, answering
// it with one batch of datagrams (sendmmsg on Linux), and keeps a timer
// only while something is in flight or its idle timeout runs.
//
//	srv, err := kcp.Listen(engine, ":4000", kcp.ServerConfig{
//		Config: kcp.Config{NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true},
//	})
//	go http.Serve(srv, handler)
//
//	conn, err := kcp.Dial(engine, "example.com:4000", kcp.Config{
//		NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true,
//	})
package kcp
