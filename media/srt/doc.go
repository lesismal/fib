// Package srt implements SRT, the Secure Reliable Transport of Haivision
// (draft-sharabayko-srt-01), in live mode: MPEG-TS over UDP with the
// losses repaired by retransmission within a fixed latency, as encoders,
// ffmpeg and OBS speak it to ingest and from distribution servers.
//
// Conn is the protocol without any I/O (sans-IO): the caller feeds it
// datagrams and the time, asks NextTimeout when to call HandleTimeout,
// and gets datagrams back through Config.Output. It does:
//
//   - the version 5 handshake (section 4.3), as a caller (NewCaller:
//     induction, then conclusion with the listener's SYN cookie) and, with
//     Listener, as a listener, which keeps no state for a caller until its
//     conclusion proves with the cookie that it owns its address; the
//     HSREQ/HSRSP extension with TSBPD, too-late drop, periodic NAK and
//     the retransmitted-packet flag, and the latency both sides agree on
//     (the larger); the stream ID extension (SID), and rejection codes
//     both ways (RejectReason);
//   - acknowledgements (section 4.8): a full ACK every 10 ms while there
//     is something new to acknowledge, a light ACK every 64 packets,
//     ACKACK, and the round-trip time measured from them;
//   - loss reports (section 4.8.2): a NAK with the loss list, ranges
//     included, as soon as a gap appears, and again every
//     (RTT + 4 * RTTVar) / 2 for the losses still open;
//   - the sender buffer, retransmitting what a NAK names, bounded by
//     Config.SendBufferPackets and by the peer's flow window, and giving up
//     on packets older than the latency allows (a DROPREQ tells the
//     receiver);
//   - the receiver buffer with timestamp-based packet delivery (TSBPD,
//     section 4.5): each payload goes to OnData at the sender's timestamp
//     plus the latency, on a time base that follows the least delay seen
//     and a sender clock that drifts; packets still missing when a later
//     one is due are dropped as too late (section 4.6);
//   - keepalives, SHUTDOWN, and a peer idle timeout;
//   - encryption (section 6): AES-CTR with 128-, 192- or 256-bit stream
//     keys, wrapped (RFC 3394) with a key PBKDF2-HMAC-SHA1 derives from
//     the passphrase and the salt of the KMREQ the caller sends in its
//     conclusion; the listener answers with the KMRSP. A key refresh the
//     peer sends later (KMREQ as a control packet) is installed with its
//     even and odd keys, so a sender rekeying, as libsrt does every 2^24
//     packets, is followed.
//
// Server runs a Listener on a fib UDP binding, where every peer address
// is a fib connection, and makes a Session of every caller it accepts:
// one mutex and one timer per session drive the Conn, and the datagrams a
// call emits go out in one fib SendDatagrams batch. The stream ID names
// the stream, plainly ("app/stream") or in the access control syntax
// ("#!::r=app/stream,m=publish,u=user"); ServerConfig.OnConnect is where
// authentication goes. A publisher (m=publish) is demuxed (package mpegts)
// into the hub; a player (m=request, the default) subscribes to the hub
// and receives the stream muxed into MPEG-TS, 7 transport packets per SRT
// packet, dropped (hub.NewDropper) while the peer acknowledges too little.
// Dial, Publish and Play are the caller side on Engine.OpenUDPPeer: a raw
// session, a push of a hub stream to a listener (an SRT server, or ffmpeg
// with mode=listener), and a pull from one into a hub.
//
// Left out: rendezvous mode, file (message and stream) mode and the
// congestion control it needs, AES-GCM, packet filters (FEC), socket
// groups (bonding), the HSv4 handshake of SRT before 1.3, and starting a
// key refresh: a Conn keeps the key it began with for as long as it lives,
// which is about 2^31 packets before a sequence number repeats under one
// key, more than a day at 20 Mbit/s.
//
// Usage, a server:
//
//	h := hub.New(hub.Config{})
//	srv, err := srt.NewServer(srt.ServerConfig{Hub: h, Latency: 200 * time.Millisecond})
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Network: "udp", Addr: ":9000", Handler: srv})
//	go engine.Run()
//	// ffmpeg ... -f mpegts "srt://host:9000?streamid=#!::r=live/s,m=publish"
//	// ffplay "srt://host:9000?streamid=live/s"
//
// Usage, a client pushing a hub stream:
//
//	s, err := srt.Publish(engine, "host:9000", h, "live/s",
//		srt.ClientConfig{StreamID: "#!::r=live/s,m=publish"})
package srt
