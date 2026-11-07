// Package peer implements the BitTorrent peer wire protocol (BEP 3) with
// the Fast extension (BEP 6), the extension protocol (BEP 10) and its
// ut_metadata (BEP 9) and ut_pex (BEP 11) extensions, BEP 52's v2
// handshake upgrade and hash messages, and Message Stream Encryption
// (package mse), on fib's TCP connections and on any net.Conn, such as
// uTP's.
//
// The package is in two layers.
//
// The codec has no I/O. AppendHandshake and ParseHandshake are the
// handshake, with Reserved for the extension bits and NewPeerID for an
// Azureus-style peer ID. A Parser takes a stream of messages in pieces of
// any size and yields each Message, a piece's block aliasing the input
// when it arrived whole; AppendMessage encodes one. Bitfield is a set of
// pieces, AllowedFastSet BEP 6's algorithm. ExtendedHandshake,
// MetadataMessage with MetadataAssembler, and Pex with PexState are the
// extension messages and the bookkeeping they need.
//
// A Server is the protocol of a set of torrents. As the fib.Handler of a
// TCP binding it handshakes each incoming connection, MSE or plaintext as
// Config.Encryption allows, and hands it to the Torrent whose info hash
// the peer asked for (v1, or v2 truncated, a hybrid's v1 handshake
// upgraded to v2 when the peer asks); Dial and Open begin connections.
// Each is a Conn, whose Handler hears the handshake, each message and the
// close. A Conn tracks choking, interest, the requests outstanding
// (Request keeps up to the peer's reqq, and drops them on a choke without
// the Fast extension), sends keepalives every two minutes and closes on
// silence. What a handler sends from its callbacks goes out in one write
// when it returns; Congested and OnWritable pace uploads by what waits
// for the socket. Over a net.Conn, a goroutine reads and another writes,
// so that neither side ever waits on the other's reading.
//
//	s := peer.NewServer(peer.Config{Encryption: mse.Prefer})
//	s.AddTorrent(&peer.Torrent{InfoHash: ih, Handler: &peer.HandlerFuncs{
//		Handshake: func(c *peer.Conn) {
//			c.SendExtendedHandshake(&peer.ExtendedHandshake{MetadataSize: len(info)})
//			c.Send(&peer.Message{Type: peer.MsgHaveAll})
//		},
//		Message: func(c *peer.Conn, m *peer.Message) {
//			switch m.Type {
//			case peer.MsgInterested:
//				c.Send(&peer.Message{Type: peer.MsgUnchoke})
//			case peer.MsgRequest:
//				c.SendPiece(m.Index, m.Begin, block(m.Request()))
//			}
//		},
//	}})
//	engine, _ := fib.New(fib.DefaultConfig(), fib.Binding{Addr: ":6881", Handler: s})
//	go engine.Run()
//	c, err := s.Dial(engine, peer.DialOptions{Addr: "192.0.2.1:6881", InfoHash: ih})
package peer
