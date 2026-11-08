# BitTorrent

[中文](bittorrent.zh-CN.md)

The [`bittorrent`](../bittorrent) packages are the BitTorrent protocols on fib: bencoding, .torrent
files and magnet links (v1, v2 and hybrid), trackers over HTTP and UDP in both roles, the peer wire
protocol with its extensions and Message Stream Encryption, uTP and the mainline DHT, and a client
that downloads and seeds with all of them. Each is wire compatible with the clients in use
(libtorrent, Transmission, anacrolix/torrent), which [`bittorrent/interop`](../bittorrent/interop)
checks against libtorrent-rasterbar, anacrolix/torrent, anacrolix/dht, libutp and opentracker. Like the rest of fib they depend on nothing but the standard library: SHA-1,
SHA-256, RC4 and ed25519 come from it, and the 768-bit Diffie-Hellman of MSE is `math/big`.

## Packages

| Package | What it is | On |
| --- | --- | --- |
| [`bencode`](../bittorrent/bencode) | Bencoding with `encoding/json`-style struct tags, `RawMessage`, an in-place and a streaming `Decoder` | — |
| [`metainfo`](../bittorrent/metainfo) | .torrent files v1, v2 and hybrid, info hashes, piece and file layout, piece verification (SHA-1 and SHA-256 merkle), a `Builder`, magnet links | — |
| [`tracker`](../bittorrent/tracker) | HTTP(S) and UDP tracker clients, BEP 12 tiers, and one `Server` for both protocols over a replaceable `Store` | fib/http, fib UDP |
| [`peer`](../bittorrent/peer) | The peer wire protocol: a codec without I/O, and a `Server` of torrents whose `Conn`s run on fib TCP or any `net.Conn` | fib TCP, `net.Conn` |
| [`peer/mse`](../bittorrent/peer/mse) | Message Stream Encryption (MSE/PE) without I/O, and over a `net.Conn` | — |
| [`utp`](../bittorrent/utp) | uTP (BEP 29) with LEDBAT: `Mux` without a socket, `Socket` on a fib UDP binding | fib UDP |
| [`dht`](../bittorrent/dht) | A mainline DHT `Node` without a socket, or on a binding of its own | fib UDP |
| [`bittorrent`](../bittorrent) | The client: `Client`, `Torrent`, `File`, a streaming `Reader`, file or memory `Storage` ([details](#client-package-bittorrent)) | all of the above, on an engine of its own |
| [`interop`](../bittorrent/interop) | A module of its own with the interop tests, never a dependency of fib | — |

## One port for TCP and UDP

A BitTorrent client listens on one port number: TCP for peers, and on UDP, uTP peers, the DHT and
its UDP tracker client's replies. The UDP parts own no socket, so they can share one, told apart by
a datagram's first byte:

| First byte | Protocol | Test |
| --- | --- | --- |
| `0x01`–`0x41` (type and version 1) | uTP | `utp.IsUTP` |
| `'d'` (a bencoded dictionary) | DHT | `dht.Node.HandleDatagram` claims it |
| `0x00` (the high byte of an action 0–3) | UDP tracker reply | `tracker.IsUDPReply`, `tracker.UDPClient.HandleDatagram` |

`utp.Socket` reads the port: it hands uTP to its `Mux` and everything else to the function
`OnOther` sets; the DHT and the tracker client send through `Socket.WriteTo`. The `Socket` is also
a `net.Listener`, so `peer.Server.Serve` takes the peers that arrive over uTP, while the same
`peer.Server` is the `fib.Handler` of the TCP binding on the same port number:

```go
peers := peer.NewServer(peer.Config{Encryption: mse.Prefer})
engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: ":6881", Handler: peers}) // TCP peers
go engine.Run()

sock, err := utp.Listen(engine, "udp", ":6881", utp.Config{})
go peers.Serve(sock) // uTP peers
node, err := dht.New(dht.Config{Send: func(to netip.AddrPort, b []byte) { _ = sock.WriteTo(b, to) }})
trk := tracker.NewUDPClient(tracker.UDPClientConfig{
	Send: func(to netip.AddrPort, b []byte) error { return sock.WriteTo(b, to) },
})
sock.OnOther(func(from netip.AddrPort, b []byte) { // on the fib worker, b valid until it returns
	if !node.HandleDatagram(from, b) {
		trk.HandleDatagram(from, b)
	}
})
```

## bencode

`Marshal` and `Unmarshal` map bencoding onto Go values as `encoding/json` does: struct tags
`bencode:"key,omitempty"` and `bencode:"-"`, keys written sorted, integers range-checked into every
int and uint kind, `bool` as 0 or 1, byte strings into `string`, `[]byte` or arrays such as an info
hash `[20]byte`. Decoding into `any` gives `int64`, `string`, `[]any` and `map[string]any`. A value
of the wrong type is skipped and reported after the rest is decoded.

`RawMessage` keeps a value's bytes exactly as they arrived, which an info hash needs: it is the
hash of the info dictionary as encoded in the file. `Lookup(torrent, "info")` finds those bytes
without decoding anything, `Split` and `Valid` walk values.

Syntax is always strict (no leading zeros, no `-0`, nothing after the value), and `Limits`
(`MaxDepth` 256, `MaxSize` 64 MiB) bound hostile input. Unsorted and repeated keys are accepted by
default, the last one winning, because real torrents have them; `Decoder.Strict` requires the
canonical form. `NewBytesDecoder` decodes in place: with `NoCopy`, `[]byte` and `RawMessage` alias
the input, and `InputOffset` says where the next value starts, which a ut_metadata message (a
dictionary followed by raw data) needs. `NewDecoder` streams from an `io.Reader`.

## metainfo

`Parse` and `Load` read v1 (BEP 3), v2 and hybrid (BEP 52) torrents, keeping the info dictionary's
bytes for `InfoHashV1` (SHA-1) and `InfoHashV2` (SHA-256). They reject what a client must not
trust: file paths that could escape the download directory, clashing paths, piece hashes that do
not match the length, hybrid torrents whose v1 and v2 file lists disagree, and wrong piece layers.

`Info.FileList` gives the files in order with their offsets, BEP 47 padding files included; in a
v2-only torrent each file starts on a piece boundary. `PieceSize`, `PieceSegments` and `Segments`
map pieces and byte ranges onto files. `VerifyPiece` checks a piece against the v1 SHA-1, the v2
merkle tree (through the file's piece layer, or its pieces root for a file of one piece), or both;
`MerkleRoot`, `PadHash`, `LayerRoot` and `VerifyProof` are the helpers BEP 52's hash requests need.

`Builder` hashes an `fs.FS` (`Build`) or a path (`BuildPath`) into a v1, v2 or hybrid torrent, with
trackers (BEP 12), web seeds (BEP 19), DHT nodes (BEP 5) and the private flag (BEP 27).
`ParseMagnet` and `Magnet.String` handle magnet links (BEP 9): `xt=urn:btih:` in hex or base32,
`urn:btmh:` for v2, `dn`, `tr`, `ws`, `x.pe`, `so` (BEP 53) and `xl`.

## tracker

The clients:

- `HTTPClient` announces and scrapes through a fib/http `Client`, which brings HTTPS, keep-alive
  and HTTP/2. It reads compact and dictionary peer lists (BEP 23), `peers6` (BEP 7), failure and
  warning messages, interval and min interval, the tracker id and `external ip` (BEP 24).
- `UDPClient` speaks BEP 15 without a socket: it sends through `UDPClientConfig.Send` and is handed
  replies through `HandleDatagram`, so it shares the client's UDP port. `ListenUDPClient` gives it
  bindings of its own instead. It keeps each connection ID for its minute and retransmits after
  15 × 2^n seconds (`Timeout`, `Retransmits`), and sends BEP 41 URL data.
- `Client` picks one of the two by URL scheme; `Tiers` announces through the tiers of an
  announce-list as BEP 12 says (shuffled once, a tracker that answers promoted, tracker IDs kept).

`Server` is a tracker on both protocols at once over one `Store`: `ServeHTTP` is a fib/http handler
(the last path segment starting with `announce` or `scrape`, a passkey path segment before it), and
`UDP()` is the `fib.Handler` of UDP bindings. Its UDP connection IDs are an HMAC-SHA256 of a secret,
the minute and the client's address, valid one to two minutes, so it keeps no state per client.
`MemoryStore`, the default `Store`, keeps 64 shards, bounds torrents, peers per torrent and peers
in all, expires peers after `PeerTTL`, and hands out random peers (a random start and a coprime
stride), only leechers to a seeder. For private trackers, `ServerConfig.AllowInfoHash` is an
allow-list and `Authorize` sees each request's `Auth`: remote address, URL (BEP 41 data over UDP)
and passkey (`?passkey=` or `/<key>/announce`).

```go
srv := tracker.NewServer(tracker.ServerConfig{})
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Addr: ":6969", Handler: fibhttp.NewHandler(srv)},
	fib.Binding{Network: "udp", Addr: ":6969", Handler: srv.UDP()})
```

## peer and mse

The codec has no I/O: `AppendHandshake` and `ParseHandshake` with `Reserved` for the extension bits
(`SetExtended`, `SetDHT`, `SetFast`, `SetV2`) and `NewPeerID` for an Azureus-style peer ID; a
`Parser` that takes the stream in pieces of any size and yields each `Message`, a piece's block
aliasing the input when it arrived whole; `AppendMessage`. Messages cover BEP 3 (and `port`,
BEP 5), the Fast extension (BEP 6: suggest, have all, have none, reject, allowed fast, with
`AllowedFastSet`), the extension protocol (BEP 10: `ExtendedHandshake`), ut_metadata (BEP 9:
`MetadataMessage`, and `MetadataAssembler`, which checks the SHA-1 or SHA-256 and returns the parsed
`metainfo.Info`), ut_pex (BEP 11: `Pex`, and `PexState` for the deltas to send and the rate of
those received) and the hash request, hashes and hash reject messages of BEP 52. `Bitfield` is a
set of pieces.

A `Server` is the protocol of a set of torrents (`AddTorrent`, `RemoveTorrent`). As the
`fib.Handler` of a TCP binding it handshakes each connection, MSE or plaintext as
`Config.Encryption` allows, and hands it to the `Torrent` whose info hash the peer asked for: v1, or
v2 truncated, with a hybrid's v1 handshake upgraded to v2 when the peer sets the v2 bit. `Dial`
connects out on fib, `Open` over a `net.Conn`, `ServeConn` and `Serve` take incoming ones over any
`net.Conn` or `net.Listener` (uTP's). Each is a `Conn` whose `Handler` hears the handshake
(`OnHandshake`), each message (`OnMessage`) and the close (`OnClose`), and optionally
`OnWritable` (`WritableHandler`).

A `Conn` tracks choking, interest and the requests outstanding: `Request` keeps up to the peer's
`reqq` (and `Config.MaxRequests`, 64) in flight, aware of choking and allowed-fast pieces, and
without the Fast extension a choke drops them. It sends keepalives every two minutes and closes
after three of silence. What a handler sends from its callbacks goes out in one write when it
returns; `Congested` and `OnWritable` pace uploads by what waits for the socket, and `HoldReads`
stops reading from the peer while the handler's own work backs up. Over a `net.Conn`,
one goroutine reads and another writes, so that neither side ever waits on the other's reading.

Package `mse` is MSE/PE: 768-bit Diffie-Hellman, RC4 with the first 1024 bytes dropped, both
roles. `Initiator` and `Receiver` are the handshakes without I/O; `KeyTable` finds the torrent
(SKEY) a receiver is asked for in O(1); `Client` and `Server` run them over a `net.Conn`. `Policy`
is `Allow` (default: plaintext preferred, encryption accepted), `Prefer` or `Require`; a receiver
also recognizes a plaintext handshake.

## utp

uTP (BEP 29) is a reliable byte stream over UDP with LEDBAT congestion control, wire compatible
with libutp, which Transmission, older libtorrents and anacrolix/torrent (through go-libutp) use.

- `Mux` is uTP without a socket: the connections of one UDP port, keyed by peer address and
  connection ID. `HandleDatagram(from, b)` takes a datagram and reports whether it was uTP; the
  acknowledgements and packets it causes wait for `Flush`, which the caller calls once per burst
  read, so a burst is answered with one acknowledgement per connection and one batch. `Dial`
  starts a connection; a `Mux` is a `net.Listener` of those peers start (`Accept`, `AcceptUTP`).
- `Socket` is a `Mux` on a fib UDP binding: its `OnDatagrams` hands each burst of a peer's
  datagrams to the `Mux` and answers it with one batch (`sendmmsg` on Linux), it dials from the
  same port (`DialContext`, `DialAddrPort`), and passes what is not uTP to `OnOther`. `Listen`
  adds one to a running engine, or `NewSocket` and `Attach` make one the handler of a binding of
  `fib.New`. `utp.Dial` and `utp.DialContext` dial from a binding of their own.
- `Conn` is a `net.Conn` with deadlines, plus `CloseWrite`, `RTT` and `Stats`.

The protocol follows libutp's: version 1 headers, SYN, DATA, STATE, FIN and RESET, its
connection-ID rule, 16-bit sequence numbers with wraparound, selective acknowledgements both ways,
fast retransmission on three duplicate acknowledgements or a SACK three packets past a hole,
retransmission timeouts from the smoothed RTT with backoff (`MinRTO` 500 ms), LEDBAT with libutp's
delay histories (`TargetDelay` 100 ms), the advertised window as the reader's free buffer, Nagle-like
coalescing of small writes (`NoDelay` turns it off), keepalives every 29 s, and FIN with eof_pkt.
Past libutp, a connection also detects loss by RACK and probes the tail (RFC 8985): libutp sends no
selective acknowledgement while it has data to send, so a transfer both ways would otherwise find
each loss only by a timeout. Packets are a fixed 1400 bytes (`PacketSize`); there is no path MTU
discovery and no clock-drift penalty.

## dht

A `Node` is a mainline DHT node (BEP 5): a routing table of k-buckets (K = 8) split around its own
ID, with good, questionable and bad nodes (15 minutes), a replacement cache per bucket, a ping
before a questionable node is evicted, and a refresh of quiet buckets. Only nodes that answered a
query of ours enter it, at most one per public IP. It answers ping, find_node, get_peers and
announce_peer (with implied_port), sample_infohashes (BEP 51) and BEP 44 get and put of immutable
and mutable (ed25519) items. Tokens are bound to the asker's IP and valid 5 to 10 minutes; announced
peers are kept 30 minutes (`PeerTTL`) and items two hours, bounded by `MaxTorrents`,
`MaxPeersPerTorrent` and `MaxItems`; queries are rate-limited per source IP (`QueryRate`).

Its lookups are Kademlia's iterative ones, `Alpha` (3) queries in flight: `FindNode`, `GetPeers`
(peers handed to a callback as they arrive), `Announce` (then announces to the K closest that gave a
token), `Get`, `GetMutable` and `Put`; `Ping` and `SampleInfohashes` ask one node. `Bootstrap`
starts from `Config.Routers` (`DefaultRouters` when nil) and nodes added by `AddNodes` or
`AddTorrentNodes`; `State` saves the ID and routing table across a restart.

BEP 42: the ID is derived from the external address when known (`Config.PublicIP`, or by vote of
the `ip` key of responses), and `Config.EnforceNodeID` keeps nodes whose IDs do not match their
addresses out of the table. BEP 43: `Config.ReadOnly` queries but answers nothing. BEP 32: a `Node`
is of one address family; `Config.IPv6` makes one that speaks `nodes6`, and a client on both runs
two.

`New` makes a `Node` that sends through `Config.Send` and is fed by `HandleDatagram`, which claims
datagrams that begin with `'d'`; `Listen` runs one on a binding of its own. Timers are
`time.AfterFunc`: one per query in flight, and one that maintains the node every few seconds.

```go
node, err := dht.Listen(engine, ":6881", dht.Config{})
err = node.Bootstrap(ctx)
n, err := node.Announce(ctx, infoHash, 0, true, func(peers []netip.AddrPort) {
	// connect to peers
})
```

## Client (package bittorrent)

A `Client` is one fib engine and one port: TCP for the peer wire protocol (a `peer.Server`), and
UDP shared by a `utp.Socket`, a `dht.Node` and a `tracker.UDPClient`, as in
[One port for TCP and UDP](#one-port-for-tcp-and-udp). Its `Config` zero value is a working client:
it stores in the working directory, listens on `:6881` (`DefaultListenAddr`; a zero port picks one
free for both TCP and UDP), and speaks TCP, uTP, MSE, the DHT, PEX, local service discovery and
trackers, each of which a `Disable*` field turns off.

```go
c, err := bittorrent.NewClient(bittorrent.Config{DataDir: "downloads"})
defer c.Close()
t, err := c.AddMagnet("magnet:?xt=urn:btih:...", nil) // or AddTorrent, AddTorrentFile
<-t.GotInfo()                                         // the info dictionary, from peers
r := t.Files()[0].NewReader()                         // io.ReadSeekCloser: streams as it downloads
defer r.Close()
_, err = io.Copy(w, r)
<-t.Done()
```

- **Torrents** come from .torrent files (v1, v2, hybrid) or magnet links (ut_metadata; `so=` selects
  files). `AddOptions` start one paused, in sequential order, with file priorities, extra peers and
  trackers, or on a storage of its own. A `Torrent` has `GotInfo`, `Done` and `Removed` channels,
  `State` and `Err`, `Start`, `Pause`, `Stop` and `Remove(deleteData)`, `SetSequential`,
  `SetFilePriority` (`PriorityNone`, `Low`, `Normal`, `High`), `AddPeers`, `Files`, `Stats`, `Peers`
  and `NewReader`. `Config.OnEvent` hears metadata, checked, piece verified, hash failed, completed,
  peer connected, disconnected and banned, seed limit, state changes and errors, in order.
- **Storage** is a `Storage` (`ReadAt` and `WriteAt` by file index, `Close`): files below
  `DataDir` (`NewFileStorage`, paths made safe for the operating system by `SanitizePath`, BEP 47
  padding never written) or memory (`NewMemoryStorage`). What storage holds when a torrent is added
  is hash-checked, so an interrupted download resumes.
- **Disk pool**: storage is never read or written, and no piece is hashed, on a fib worker (or on
  the goroutine reading a uTP connection). `Config.DiskWorkers` goroutines (GOMAXPROCS, at most
  `DefaultMaxDiskWorkers` = 8) take the jobs from one queue: a received block is copied into a
  pooled buffer and its write queued, and the piece is checked on the pool once its last block
  lands; an upload's reads run there, one job per peer at a time of up to 16 blocks, still paced by
  `Congested`, `OnWritable` and `UploadRate`; so do the answers to BEP 52 hash requests and the check
  of a fetched piece layer. The blocks waiting are bounded by `Config.DiskQueueBytes`
  (`DefaultDiskQueueBytes`, 64 MiB): past it the connections that sent them stop reading
  (`peer.Conn.HoldReads`, which is `fib.Connection.HoldReads` or a paused reading goroutine) until
  half has been written. `Stop`, `Remove` and `Close` close a storage only once the reads and writes
  under way are over; those still queued are dropped.
- **Picking**: rarest first with random ties, the first 4 pieces at random; file priorities,
  sequential mode and the windows of open `Reader`s (the nearest piece most urgent; readahead 4 MiB
  by default, `SetReadahead`) come first. 16 KiB blocks are pipelined to each peer's `reqq`; in
  endgame a block goes to a second peer and the slower request is cancelled. A peer that sends
  nothing for 60 s is snubbed and its requests go to others.
- **Verification**: SHA-1 for v1, the merkle tree for v2. A v2-only magnet's torrent fetches its
  piece layers with BEP 52 hash requests, and the client answers peers' hash requests from the
  layers it has (and computes them after a hybrid magnet download). A peer that sent a whole piece
  that failed is banned; when several sent it, their blocks are compared with the good piece once
  it passes and those that sent bad ones are banned.
- **Choking**: four upload slots (`UploadSlots`) for the peers it downloads most from (that take
  most, when seeding), plus an optimistic unchoke that moves every 30 s and favours new peers
  threefold. Fast extension: have all and have none, 10 pieces allowed fast, a choked peer's
  requests rejected. Uploads pace to the socket (`Congested`, `OnWritable`) and to `UploadRate`.
- **Connections** dial uTP first, then TCP; MSE first and plaintext after a refusal
  (`EncryptionPrefer`, the default), the reverse with `EncryptionAllow`, never plaintext with
  `EncryptionRequire`; bounded by `MaxConns` (200), `MaxConnsPerTorrent` (50), `MaxHalfOpen` (32)
  and `DialsPerTick` (8), with backoff. Of two connections to one peer, the one the lower peer ID
  began is kept.
- **Peers** come from trackers (tiered, started, completed and stopped, under both hashes of a
  hybrid), the DHT (announce every 15 minutes, every minute while there are none; `PORT` both
  ways), PEX (a message a minute per peer), local service discovery (BEP 14, IPv4), a magnet's
  `x.pe`, `AddPeers` and `Config.PeerSources`; a private torrent uses its trackers alone. Web seeds
  (BEP 19) serve whole pieces through range requests on the fib/http client.
- **Seeding** pauses a torrent at `SeedRatio` or `SeedTime`; `Ban` bans an address from every torrent.

## Interop

[`bittorrent/interop`](../bittorrent/interop) is a module of its own (`replace
github.com/lesismal/fib => ../..`), so that anacrolix/torrent v1.61.0, anacrolix/dht v2.23.0 and
go-libutp v1.5.1 are never fib's dependencies. go-libutp wraps libutp through cgo, so the module
needs cgo and a C++ compiler; it runs on Linux and macOS, not Windows. libtorrent-rasterbar 2.0.11
is driven through its Python binding (`python3-libtorrent`, `libtorrent_peer.py`), so its test
runs only with `FIB_LIBTORRENT=1`: `libtorrent.sh` runs it in a container, and the CI job installs
the package and sets the variable. It all runs in CI as the `bittorrent-interop` job, and the
network parsers of every package are fuzzed in the `fuzz-bittorrent` job.

| Package | What is checked |
| --- | --- |
| metainfo | fib's v1, v2 and hybrid torrents read the same in anacrolix (info hashes, files, pieces roots, `ValidatePieceLayers`); anacrolix's torrents parse the same in fib and their pieces verify; v1 info dictionaries are byte-identical where the file orders agree; anacrolix's test torrents, libtorrent's v2 and hybrid ones among them, read from its module directory (skipped when absent); magnet links round-trip |
| tracker | anacrolix's tracker client announces to and scrapes fib's HTTP and UDP servers on 127.0.0.1 and ::1, with a passkey through BEP 41; fib's clients announce to anacrolix's HTTP and UDP tracker servers; by hand, fib's clients announce to and scrape opentracker (Debian's package, in docker) over HTTP and UDP |
| peer, mse | over TCP, fib fetches the metadata and every piece, verified, from an anacrolix seeder: plaintext, MSE offering plaintext, and MSE with RC4 required; anacrolix, given only the info hash, downloads 3 MB from a fib seeder with default settings, RC4 forced, RC4 required on both sides and plaintext forced; fib's `mse` against anacrolix's `mse` package in both roles, the SKEY one of 100 |
| utp | fib against libutp (go-libutp) in both roles: 4 MB each way, checksummed; write, close and read to EOF; through a relay that drops 1 datagram in 20 and holds back 1 in 7, 1 MB each way |
| client, anacrolix | fib's client downloads from anacrolix's and anacrolix's from fib's, each over TCP and over uTP (go-libutp) separately, from a .torrent and from a btih magnet, with RC4 required, and hybrid torrents; fib's client finds an anacrolix seeder through a fib `tracker.Server`. anacrolix cannot add v2-only torrents, so libtorrent covers those |
| client, libtorrent | 22 of 22: each downloads from the other v1, hybrid and v2-only torrents, from .torrent files and magnets (btih; btmh for v2, whose piece layers come by BEP 52 hash requests both ways), over TCP and over uTP, with RC4 required |
| dht | 6 fib and 6 anacrolix nodes on loopback: they bootstrap from each other and hold each other in their tables, ping both ways, find_node converges, announce and get_peers both ways (explicit and implied port), anacrolix asks fib for sample_infohashes, BEP 44 immutable and mutable items both ways, and the two compute the same BEP 42 IDs for 200 addresses; by hand, fib bootstraps from the public DHT (13 s) and finds 240 peers of an Ubuntu image in 2.6 s |

```sh
cd bittorrent/interop
go test -race -count=1 -timeout 12m ./...                                    # everything (CI)
go test -race -count=1 -run 'Torrents|Magnet' .                              # metainfo
go test -race -count=1 -run Tracker .                                        # tracker
go test -race -count=1 -run TestPeer .                                       # peer, mse
CGO_ENABLED=1 go test -count=1 -timeout 300s -run TestUTP -v .               # utp, 35-65 s
go test -race -count=1 -run DHT .                                            # dht
go test -race -count=1 -run TestClient .                                     # client against anacrolix
./libtorrent.sh                                                              # client against libtorrent, needs docker
FIB_LIBTORRENT=1 CGO_ENABLED=1 go test -count=1 -run TestClientLibtorrent .  # the same with python3-libtorrent installed
./opentracker.sh                                                             # opentracker, needs docker
FIB_DHT_PUBLIC=1 FIB_DHT_INFOHASH=<40 hex> go test -run TestDHTPublic -v .   # public DHT, needs internet
```

### Upstream bugs found

- **libutp: the initiator's `fast_resend_seq_nr`.** `utp_connect` starts a connection at a random
  sequence number but leaves `fast_resend_seq_nr` at the 1 `utp_create_socket` set. For about half
  of the random start numbers that 1 then compares as ahead of every packet sent, and the
  initiator never retransmits on a selective acknowledgement, only on its retransmission timeout of
  a second or more. Through the lossy relay, libutp to libutp moved 4 MB in 45 s, against 12–16 s
  with fib at either end. Recorded in `bittorrent/interop/utp_test.go`.
- **anacrolix/dht v2.23.0: `peer_store.InMemory` never returns announced peers.** It keys peers by
  IP and decodes the key as a whole address, so what it returns has a mangled IP and port. The
  interop test gives the anacrolix servers a correct store. (anacrolix in turn rejects an immutable
  BEP 44 put without `seq`, so fib's carries `seq` 0.)
- **libutp: a lost SYN answer leaves a half-open connection for good.** libutp ignores a repeated
  SYN for a connection ID it already has, and that half-open connection never times out, so a
  dialer that only resends its SYN waits forever. fib's dialer resends once under the same ID and
  then resets and starts again under a new one; libutp may still hand its application the
  abandoned connection.
- **anacrolix/torrent v1.61.0: the connection writer can lose a wake-up.** A reason to send
  requests (an unchoke) that arrives between the writer filling its buffer and its waiting is
  dropped (`chansync.BroadcastCond`), and nothing is requested until its keepalive timer
  (`KeepAliveTimeout`, a minute) fires. A seeder with nothing more to say saw downloads stall a
  minute in 1–2% of runs; the interop tests set the keepalive to 2 s.

## BEP coverage

| BEP | Title | Status | Package |
| --- | --- | --- | --- |
| 3 | The BitTorrent Protocol Specification | full | `bencode`, `metainfo`, `tracker`, `peer`, `bittorrent` |
| 5 | DHT Protocol | full (torrent `nodes` key, `port` message and reserved bit included); the client does not yet persist its node state | `dht`, `metainfo`, `peer`, `bittorrent` |
| 6 | Fast Extension | full (checked against the BEP's test vectors) | `peer` |
| 7 | IPv6 Tracker Extension | full | `tracker`, `bittorrent` |
| 9 | Extension for Peers to Send Metadata Files | full (ut_metadata, magnet links) | `peer`, `metainfo` |
| 10 | Extension Protocol | full | `peer` |
| 11 | Peer Exchange (PEX) | full | `peer` |
| 12 | Multitracker Metadata Extension | full | `metainfo`, `tracker`, `bittorrent` |
| 14 | Local Service Discovery | partial: IPv4 only | `bittorrent` |
| 15 | UDP Tracker Protocol | full, client and server | `tracker`, `bittorrent` |
| 16 | Superseeding | not done | — |
| 19 | WebSeed (GetRight style) | full: `url-list` and `ws` read, whole pieces fetched by range requests | `metainfo`, `bittorrent` |
| 21 | Extension for Partial Seeds | full in the client (`upload_only` sent once nothing wanted is missing, and heeded from peers; `EventPaused` in the tracker client); the tracker server gives paused peers no special handling | `tracker`, `peer`, `bittorrent` |
| 23 | Tracker Returns Compact Peer Lists | full | `tracker` |
| 24 | Tracker Returns External IP | full | `tracker` |
| 27 | Private Torrents | full: the `private` flag, trackers alone in the client, and the server's allow-list and authorization hooks | `metainfo`, `tracker`, `bittorrent` |
| 29 | uTorrent transport protocol | full, without path MTU discovery and the clock-drift penalty | `utp`, `bittorrent` |
| 30 | Merkle Hash Torrent Extension | not done (superseded by BEP 52) | — |
| 31 | Failure Retry Extension | not done | — |
| 32 | IPv6 extension for DHT | partial: one `Node` per address family, `want` for the other family ignored; the client's IPv6 node is optional (`DHTIPv6`) | `dht`, `bittorrent` |
| 33 | DHT Scrape | not done | — |
| 35 | Torrent Signing | not done | — |
| 38 | Finding Local Data Via Torrent File Hints | not done | — |
| 40 | Canonical Peer Priority | not done | — |
| 41 | UDP Tracker Protocol Extensions | full | `tracker` |
| 42 | DHT Security Extension | full | `dht` |
| 43 | Read-only DHT Nodes | full | `dht` |
| 44 | Storing Arbitrary Data in the DHT | full, except `cas` on the putting side | `dht` |
| 47 | Padding Files and Extended File Attributes | full | `metainfo` |
| 48 | Tracker Protocol Extension: Scrape | full, without full scrape | `tracker` |
| 51 | DHT Infohash Indexing | full | `dht` |
| 52 | The BitTorrent Protocol Specification v2 | full: torrents, handshake upgrade, hash requests asked and answered from piece layers; proofs served with `proof_layers` > 0 are not interop-tested | `metainfo`, `peer`, `bittorrent` |
| 53 | Magnet URI Extension: Select Specific File Indices | full | `metainfo`, `bittorrent` |
| 55 | Holepunch Extension (ut_holepunch) | not done | — |
| — | Message Stream Encryption (MSE/PE) | full | `peer/mse`, `peer`, `bittorrent` |

## Decisions pending review

Decided while building, recorded here for review: the question, the choice and why, and the
alternative.

### bencode and metainfo

- **Builder's default version**: hybrid, which v1-only and v2 clients both read. Alternative: v1,
  smaller and what most tools make.
- **File order**: by path components, even for v1, so a hybrid's v1 list and v2 tree agree.
  anacrolix sorts joined path strings, so `z.bin` and `z/x` come out in a different order there.
- **Hybrid padding**: a `.pad/<n>` file with attribute `p` after every file but the last that
  does not end on a piece boundary, as libtorrent does; the `x` and `h` attributes are not
  recorded, so builds are reproducible.
- **Automatic piece length**: the smallest power of two from 16 KiB that gives under 2048 pieces,
  at most 16 MiB (`ChoosePieceLength`).
- **What Builder takes**: regular files and symlinks to files; symlinks to directories, other
  special files and empty directories are skipped; a torrent of nothing is an error.
- **Parse tolerance**: keys of the wrong type outside `info` are ignored; inside it they are left
  empty and then fail validation; whitespace after the dictionary is accepted.
- **Piece layers**: missing ones are accepted (a client fetches them from peers), wrong ones are an
  error. A hybrid's `VerifyPiece` falls back to v1 when a layer is missing; v2-only returns
  `ErrNoPieceLayer`.
- **Paths**: `..`, `.`, empty components, `/` and NUL are rejected; Windows reserved names,
  backslashes and case-only clashes are left to the storage.
- **v2 single-file torrents**: only when the file tree has one entry and its key equals `name`.
- **bencode syntax**: leading zeros and `-0` are always rejected, also without `Strict`.
- **Decoding into `any`**: integers as `int64`, byte strings as `string`.
- **Limits**: `MaxDepth` 256, `MaxSize` 64 MiB for a stream; `Load` reads at most 128 MiB
  (`MaxFileSize`).
- **Magnets**: an all-zero hash is rejected; `so` is kept as ranges.
- **Builder hashes on one goroutine.** Alternative: a hashing goroutine per CPU.

### tracker

- **fib/http `Client`, not `net/http`**, for HTTP trackers. Alternative: a `Doer` interface either
  could satisfy.
- **`NumWant`**: 0 means the tracker's default and a negative number none (anacrolix uses -1 for the
  default).
- **A negative `Left`** (unknown size) is sent as `MaxInt64`.
- **The server records only the source address**; the `ip`, `ipv4`, `ipv6` parameters and the UDP
  IP field count only with `TrustIPParam`, so the tracker cannot be used to reflect traffic at a
  third party.
- **The store keys peers by peer ID**, which merges a peer's IPv4 and IPv6 announces, at the price
  that whoever knows an ID can overwrite its address. Alternative: by ip:port, as opentracker does.
- **Defaults**: interval 30 min, numwant 50 capped at 200, UDP replies at most 1400 bytes,
  `PeerTTL` one hour; at most 1M torrents, 64K peers per torrent, 2M peers (about 300 MB at worst).
- **A seeder is given only leechers**; BEP 21's paused peers get no special handling.
- **No full scrape**; an HTTP scrape takes at most 64 hashes (`MaxScrape`), a UDP one 74.
- **A UDP request with a bad connection ID is dropped** without an error reply.
- **UDP scrapes carry no URL data**, so a passkey only guards announces over UDP.
- **A UDP announce goes to the first address resolved** (IPv4 first for `udp`), without falling
  back to the others.
- **UDP retransmission**: connect and announce share one counter and the transaction ID is reused;
  an expired connection ID means connecting again.
- **`Tiers.Timeout`** 30 s per tracker (`DefaultTrackerTimeout`).
- **An empty 8-byte UDP announce reply is an `*Error`**, as opentracker sends it.

### peer and mse

- **Encryption policy**: `Allow` (default, plaintext preferred), `Prefer`, `Require`. Alternative:
  libtorrent's separate in/out policies and encryption level.
- **No automatic plaintext retry** after an MSE dial is refused: that is the client's choice.
- **MSE**: the info hash of the BitTorrent handshake inside must equal the SKEY.
- **BEP 52 upgrade**: as receiver, a hybrid's v1 handshake is upgraded to v2 when the peer sets the
  v2 bit (anacrolix never does); as initiator the v2 bit is set only when dialing a hybrid by its
  v1 hash.
- **Allowed-fast set for IPv6**: the address masked to /48, all 16 bytes hashed.
- **`Conn.HoldReads`** pauses a `net.Conn` transport's reading goroutine before its next read,
  and is `fib.Connection.HoldReads` on a fib one; holds do not nest. Alternative: blocking inside
  the handler, which a fib worker must not do.
- **No write backpressure** on peer connections, so two peers uploading to each other cannot
  deadlock; uploads are paced by `fib.Connection.NotifyOutput` and `MaxQueuedBytes` (1 MiB)
  through `Congested` and `OnWritable`. Over a `net.Conn` the writer goroutine's queue is
  unbounded.
- **Limits and defaults**: messages at most 1 MiB + 64, metadata 16 MiB, advertised `reqq` 250, at
  most 64 requests outstanding, handshake timeout 20 s, idle 3 min, keepalive 2 min, extension IDs
  ut_metadata 1 and ut_pex 2.
- **PEX**: a message less than 45 s after the last heeded one is ignored, not a reason to
  disconnect; sends are truncated to 50 peers per family; all six keys are sent.
- **Protocol violations close the connection**: Fast messages without the Fast bit, extended ones
  without BEP 10, a bad extended handshake.
- **MSE padding**: PadC empty, PadA, PadB and PadD random 0–512 bytes; up to 512 accepted.
- **Peer ID prefix** `-FB0001-` ("FB" is not a registered client code).
- **Choke and cancel**: without the Fast extension a choke drops the requests outstanding and a
  cancel drops a request at once; with it, they wait for the reject or the piece.

### utp

- **`MinRTO` 500 ms**, as BEP 29 says (libutp: 1 s).
- **First timeout 1 s, four SYNs over about 15 s** (libutp: 3 s, three SYNs over about 21 s).
- **Slow start** doubles the window each RTT until the delay reaches 90% of the target (libutp
  grows about one packet per RTT).
- **A window under one packet** still lets one packet be in flight.
- **An acceptor may write before the initiator's first data**; such packets acknowledge one short
  of the SYN, so a libutp initiator whose SYN acknowledgement was lost does not hang.
- **A fib initiator sends a STATE** once connected.
- **A duplicate SYN is answered again.**
- **An acknowledgement that is stale but not ahead**: the data is kept, the acknowledgement ignored.
- **RACK, tail-loss probe and three-duplicate-ack retransmission** added beyond libutp.
- **Zero-window probes** start at max(RTO, 500 ms) and double up to 15 s (libutp: a fixed 15 s).
- **Timeouts**: idle 3 min, half-open 10 s, close linger 30 s then RESET; a RESET is sent on idle,
  on linger and on `Mux.Close`.
- **After both FINs** a connection lingers at most 3 s to acknowledge the peer's FIN again.
- **The name `Socket`**, not `Listener`, since it also dials and carries other protocols.
- **`HandleDatagram` defers sends to `Flush`**, which the caller must call after each burst.
- **Fixed 1400-byte packets and a 32-bit SACK mask.**
- **The receiver accepts `RecvBuffer` plus 8 packets** before dropping.
- **No clock-drift penalty, and an allocation per packet.**

### dht

- **One `Node` per address family**, as libtorrent does; `want` for the other family is ignored.
- **Routing table admission**: only nodes that answered our queries; a node that queries us is
  pinged first if its bucket has room; nodes from replies enter only after they answer.
- **One node per public IP** (private and loopback addresses exempt).
- **Node failure**: bad after 2 unanswered queries in a row, dropped after 5; not configurable.
- **Lookups** count only answering nodes towards K; a query unanswered after `QueryTimeout`/4
  stops counting against `Alpha`; lookups never resend, pings and single queries resend once;
  at most 64 candidates and 256 queries per lookup.
- **`Bootstrap`** returns after the lookup of its own ID (up to 3 times if BEP 42 changed the ID);
  refresh continues in the background, and the node bootstraps again when it has fewer than K
  nodes.
- **Defaults**: 20 queries/s per IP with a 5 s burst; 4096 torrents × 512 peers; 2048 items;
  get_peers answers at most 64 values (32 for IPv6); BEP 51: 20 samples, interval 6 h.
- **Unknown methods** with a `target` or `info_hash` are answered as find_node, others with error
  204.
- **A put with zero `k` and signature is immutable.**
- **Version `v` = `"FB\x00\x01"`** (an unregistered client code).
- **BEP 42 votes** need at least 3 distinct voter IPs, at most 64 kept; an ID set in `Config.ID` is
  never changed; `EnforceNodeID` is off by default.
- **Tokens**: 8 bytes of HMAC-SHA1 of the asker's IP.
- **Not done**: BEP 33, and `cas` when putting.

### Client (package bittorrent)

- **`ListenAddr` defaults to `:6881`**; with port 0 the client takes the port TCP got and tries
  again if UDP's is taken. Alternative: `:0` by default.
- **`Encryption`'s zero value is `EncryptionPrefer`** (MSE first, as libtorrent and anacrolix), and
  there is no mode that disables MSE altogether.
- **Local service discovery is IPv4 only**, on the standard library's `net.ListenMulticastUDP`,
  since fib has no multicast bindings; it turns itself off silently if joining the group fails.
- **Bans** are by IP (a loopback peer by its listen address) and last for the client's lifetime.
- **Blocks are written straight to storage** (through the disk pool) and read back to verify the
  piece; there are no pieces in memory, and smart-ban hashes come from that read-back.
- **One disk pool per client, one FIFO queue** for writes, piece checks, upload reads and hash
  answers: simple and fair enough, as an upload job yields after 16 blocks. Alternatives: a queue
  per torrent or per kind of job (writes before reads, as libtorrent's disk threads favour), or a
  goroutine per job, which would bound nothing.
- **`DiskWorkers` defaults to min(GOMAXPROCS, 8)**: hashing is CPU-bound, and more than 8
  concurrent writes rarely help a disk. Alternative: a fixed number, or separate hashing and I/O
  workers.
- **The byte bound counts received blocks only** (`DiskQueueBytes`, 64 MiB): they are the memory
  peers can grow. Upload reads and piece checks hold a buffer only while running, so at most
  DiskWorkers of them; their jobs are bounded by one upload job per peer and one check per piece
  in flight. Alternatives: counting every buffer, or a bound per torrent.
- **The bound is soft**: a block is never refused or dropped, since it arrives on a fib worker that
  must not wait; reads stop once it is reached, so it is exceeded by what reads already delivered
  (one read round per connection). Reads resume at half the bound, all held connections at once.
  Alternative: dropping blocks past it, which the peer would have to send again.
- **A block's write that finds its storage closed** (a stopped or removed torrent) is dropped
  without an `EventError`; the block becomes missing again. A real write error still emits
  `EventError`.
- **Closing a storage waits for the reads and writes under way** (a lock per torrent the jobs hold
  for their I/O), not for the queue: queued jobs find the storage gone and do nothing. `Stop`,
  `Remove` and `Client.Close` therefore return after at most one slow I/O per disk worker.
- **Not on the pool**: the initial check (a goroutine per torrent, as before), a web seed's write
  and check (on its fetch goroutine), `Reader` reads (the caller's goroutine), and the
  `StorageOpener`, which a magnet link's torrent calls on a fib worker when the info dictionary
  arrives: an opener must not block (the default one does no I/O). The SHA-1 of a received info
  dictionary also runs there; it is hashed once and at most a few MiB.
- **The wanted empty files are created at the end of the initial check**, rather than at
  completion, so they exist once `Done` is closed; files selected later are still created at
  completion, on the pool.
- **Padding bytes are never requested** (anacrolix refuses requests inside padding).
- **v2-only torrents serve the whole last 16 KiB block** of a file, zeros past its end, because
  libtorrent requests whole blocks.
- **BEP 52 hash requests** ask for up to 512 hashes with `proof_layers` 0, as anacrolix does, and
  the assembled layer is checked against the file's root. Answers carry uncles for
  max(0, `proof_layers` − log2(length)) levels, a reading of libtorrent's semantics that only the
  0-proof path has interop-tested; only the piece layer is served.
- **Hybrids are dialed with the v1 hash** (and the v2 bit) unless the peer came from a v2
  announce; hybrids announce both hashes to trackers, the DHT and LSD.
- **Seeding unchokes by upload rate** (libtorrent's fastest_upload) rather than round robin; a
  full seed drops seeds and upload-only peers; peer candidates are retried after 2 s << failures
  and dropped after 6 failures unless added by hand.
- **A `Reader` that raises pieces of an unselected file** reopens `Done()` until they arrive.
- **No fast-resume file**: every add hash-checks what storage holds. `Preallocate` truncates
  files to length (sparse) rather than `fallocate`.
- **Web seeds**: 4 pieces in flight per seed, a seed dropped after 5 failures, whole pieces only.
- **Events** are delivered in order on one dispatcher goroutine that never blocks the client;
  connect and disconnect carry a `PeerInfo` snapshot.
