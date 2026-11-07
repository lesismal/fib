// Package utp implements uTP, the Micro Transport Protocol of BEP 29, on
// fib's UDP bindings, wire-compatible with libutp, the reference
// implementation that Transmission, older libtorrents and
// anacrolix/torrent (through go-libutp) use.
//
// uTP is a reliable, ordered byte stream over UDP for BitTorrent peers:
// a SYN and its STATE answer start a connection, DATA packets carry 16-bit
// sequence numbers, STATE packets acknowledge them, cumulatively and with
// the selective acknowledgement extension, a FIN ends each direction and a
// RESET the connection. Its congestion control is LEDBAT: every packet
// carries a microsecond timestamp and the difference the peer measured on
// the last one, from which the sender estimates the queuing delay its own
// traffic causes and keeps it near a 100ms target, yielding to TCP and to
// other traffic on the same link.
//
// The package is in two layers:
//
//   - Mux is uTP without a socket (sans-I/O): the connections of one UDP
//     port, which takes the port's datagrams through HandleDatagram and
//     sends through a function of the caller's. A BitTorrent client
//     shares the one port between uTP, the DHT and UDP trackers by
//     sorting what it reads with IsUTP (a DHT message, bencoded, begins
//     with 'd'; a UDP tracker's with a zero byte). Its connections, Conn,
//     are net.Conns with deadlines; Mux is a net.Listener of those peers
//     start, and Dial starts one.
//   - Socket is a Mux on a fib UDP binding: a fib.Handler that hands each
//     burst of a peer's datagrams to the Mux and answers it with one batch
//     (sendmmsg on Linux), dials from the same port, and passes what is
//     not uTP to a function of the caller's. Listen adds one to a running
//     engine; Dial and DialContext start a connection from a binding of
//     their own.
//
// The protocol follows libutp's: connection IDs (the initiator receives
// on the ID its SYN carries and sends on the next), retransmission
// timeouts from the smoothed round trip with exponential backoff, fast
// retransmission on three duplicate acknowledgements or a selective
// acknowledgement with three packets past a hole, LEDBAT with libutp's
// delay histories, the advertised window as the reader's free buffer, so
// that a reader that does not read stops the sender, Nagle-like
// coalescing of small writes (Config.NoDelay turns it off), keepalives
// every 29 seconds, and FIN with eof_pkt semantics. Past libutp, a
// connection also finds a loss from acknowledgements that arrive well
// after the lost packet should have been acknowledged, and sends a probe
// when acknowledgements stop (RACK and the tail loss probe of RFC 8985):
// libutp sends no selective acknowledgement while it has data to send, so
// a transfer both ways otherwise finds each of its losses by a timeout.
//
//	engine, _ := fib.New(fib.DefaultConfig())
//	go engine.Run()
//
//	s, _ := utp.Listen(engine, "udp", ":6881", utp.Config{})
//	go func() {
//		for {
//			c, err := s.Accept()
//			if err != nil {
//				return
//			}
//			go serve(c)
//		}
//	}()
//
//	c, _ := s.DialContext(ctx, "203.0.113.7:6881") // from the same port
//	c.Write(handshake)
package utp
