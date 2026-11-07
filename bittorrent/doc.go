// Package bittorrent is a BitTorrent client on fib: it downloads and
// seeds v1, v2 and hybrid torrents (BEP 3, BEP 52) from .torrent files and
// magnet links (BEP 9, BEP 53), over TCP and uTP (BEP 29), with Message
// Stream Encryption, the Fast extension (BEP 6), the extension protocol
// (BEP 10) and peer exchange (BEP 11), and finds peers through trackers
// (BEP 3, BEP 15, BEP 12), the DHT (BEP 5) and local service discovery
// (BEP 14).
//
// A Client is one fib engine and one port: TCP for the peer wire
// protocol (package peer's Server), and UDP shared by uTP (package
// utp's Socket), the DHT node (package dht) and the UDP tracker client
// (package tracker), which tell their datagrams apart by the first byte.
// A Torrent is a torrent in it:
//
//	c, _ := bittorrent.NewClient(bittorrent.Config{DataDir: "downloads"})
//	defer c.Close()
//	t, _ := c.AddMagnet("magnet:?xt=urn:btih:...", nil)
//	<-t.GotInfo()
//	<-t.Done()
//
// What a torrent does:
//
//   - Its storage is a Storage, files below Config.DataDir by default
//     (NewFileStorage, with paths made safe for the operating system, and
//     BEP 47 padding files never written), or memory (NewMemoryStorage).
//     What storage holds when a torrent is added is checked, so that an
//     interrupted download resumes.
//   - Its pieces are picked rarest first, ties broken at random, after a
//     few random ones, by priority (the files', BEP 53's so=, and those of
//     a Reader, which streams a file as it downloads), or in order. Blocks
//     of 16 KiB are requested as deep as each peer's reqq allows; once
//     every block is requested, endgame mode asks a second peer for those
//     outstanding and cancels the slower. A peer that sends nothing for a
//     minute is snubbed: its requests go to others.
//   - Each piece is checked, by SHA-1 (v1) or against its file's merkle
//     tree (v2), whose piece layers a magnet link's torrent asks v2 peers
//     for (BEP 52 hash requests); this side answers those from the layers
//     it has. A peer that sent a whole piece that failed is banned; when
//     several did, the blocks each sent are compared with the good piece
//     once it passes, and those that sent bad ones are banned.
//   - Uploads follow tit for tat: four upload slots for the peers it gets
//     the most from (that take the most, when seeding), and an optimistic
//     unchoke that moves every thirty seconds. Choked peers' requests are
//     rejected (BEP 6), and the pieces allowed fast are served while
//     choked. Uploads pace to the socket and to Config.UploadRate.
//   - Connections are dialed over uTP first, then TCP, with MSE first and
//     plaintext after when the peer refuses (Config.Encryption), within
//     the limits of connections, dials in flight and dials per second.
//   - Peers come from the trackers (HTTP and UDP, announced to under
//     both hashes of a hybrid torrent), the DHT (announced to every
//     fifteen minutes, and every minute while there are no peers), peer
//     exchange, local service discovery, the magnet link's x.pe and
//     AddPeers; a private torrent (BEP 27) uses its trackers alone. Web
//     seeds (BEP 19) serve whole pieces over HTTP range requests.
//
// Config.OnEvent hears what happens, and Torrent.Stats and Torrent.Peers
// tell the state.
package bittorrent
