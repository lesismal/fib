# BitTorrent

[中文](bittorrent.zh-CN.md)

The [`bittorrent`](../bittorrent) packages are the BitTorrent protocols on fib: bencoding, .torrent
files and magnet links (v1, v2 and hybrid), trackers over HTTP and UDP in both roles, the peer wire
protocol with its extensions and Message Stream Encryption, uTP and the mainline DHT. Each is wire
compatible with the clients in use (libtorrent, Transmission, anacrolix/torrent), which
[`bittorrent/interop`](../bittorrent/interop) checks against anacrolix/torrent, anacrolix/dht,
libutp and opentracker. Like the rest of fib they depend on nothing but the standard library: SHA-1,
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
| `bittorrent` | The client | see [Client](#client-package-bittorrent) |
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
returns; `Congested` and `OnWritable` pace uploads by what waits for the socket. Over a `net.Conn`,
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

<!-- CLIENT SECTION: to be filled -->

## Interop

[`bittorrent/interop`](../bittorrent/interop) is a module of its own (`replace
github.com/lesismal/fib => ../..`), so that anacrolix/torrent v1.61.0, anacrolix/dht v2.23.0 and
go-libutp v1.5.1 are never fib's dependencies. go-libutp wraps libutp through cgo, so the module
needs cgo and a C++ compiler; it runs on Linux and macOS, not Windows. It runs in CI as the
`bittorrent-interop` job, and the network parsers of every package are fuzzed in the
`fuzz-bittorrent` job.

| Package | What is checked |
| --- | --- |
| metainfo | fib's v1, v2 and hybrid torrents read the same in anacrolix (info hashes, files, pieces roots, `ValidatePieceLayers`); anacrolix's torrents parse the same in fib and their pieces verify; v1 info dictionaries are byte-identical where the file orders agree; anacrolix's test torrents, libtorrent's v2 and hybrid ones among them, read from its module directory (skipped when absent); magnet links round-trip |
| tracker | anacrolix's tracker client announces to and scrapes fib's HTTP and UDP servers on 127.0.0.1 and ::1, with a passkey through BEP 41; fib's clients announce to anacrolix's HTTP and UDP tracker servers; by hand, fib's clients announce to and scrape opentracker (Debian's package, in docker) over HTTP and UDP |
| peer, mse | over TCP, fib fetches the metadata and every piece, verified, from an anacrolix seeder: plaintext, MSE offering plaintext, and MSE with RC4 required; anacrolix, given only the info hash, downloads 3 MB from a fib seeder with default settings, RC4 forced, RC4 required on both sides and plaintext forced; fib's `mse` against anacrolix's `mse` package in both roles, the SKEY one of 100 |
| utp | fib against libutp (go-libutp) in both roles: 4 MB each way, checksummed; write, close and read to EOF; through a relay that drops 1 datagram in 20 and holds back 1 in 7, 1 MB each way |
| dht | 6 fib and 6 anacrolix nodes on loopback: they bootstrap from each other and hold each other in their tables, ping both ways, find_node converges, announce and get_peers both ways (explicit and implied port), anacrolix asks fib for sample_infohashes, BEP 44 immutable and mutable items both ways, and the two compute the same BEP 42 IDs for 200 addresses; by hand, fib bootstraps from the public DHT (13 s) and finds 240 peers of an Ubuntu image in 2.6 s |

```sh
cd bittorrent/interop
go test -race -count=1 -timeout 12m ./...                                    # everything (CI)
go test -race -count=1 -run 'Torrents|Magnet' .                              # metainfo
go test -race -count=1 -run Tracker .                                        # tracker
go test -race -count=1 -run TestPeer .                                       # peer, mse
CGO_ENABLED=1 go test -count=1 -timeout 300s -run TestUTP -v .               # utp, 35-65 s
go test -race -count=1 -run DHT .                                            # dht
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

## BEP coverage

| BEP | Title | Status | Package |
| --- | --- | --- | --- |
| 3 | The BitTorrent Protocol Specification | full | `bencode`, `metainfo`, `tracker`, `peer` |
| 5 | DHT Protocol | full (torrent `nodes` key, `port` message and reserved bit included) | `dht`, `metainfo`, `peer` |
| 6 | Fast Extension | full (checked against the BEP's test vectors) | `peer` |
| 7 | IPv6 Tracker Extension | full | `tracker` |
| 9 | Extension for Peers to Send Metadata Files | full (ut_metadata, magnet links) | `peer`, `metainfo` |
| 10 | Extension Protocol | full | `peer` |
| 11 | Peer Exchange (PEX) | full | `peer` |
| 12 | Multitracker Metadata Extension | full | `metainfo`, `tracker` |
| 15 | UDP Tracker Protocol | full, client and server | `tracker` |
| 19 | WebSeed (GetRight style) | partial: `url-list` in torrents and `ws` in magnets | `metainfo` |
| 21 | Extension for Partial Seeds | partial: `EventPaused`, `upload_only`; no special handling in the server | `tracker`, `peer` |
| 23 | Tracker Returns Compact Peer Lists | full | `tracker` |
| 24 | Tracker Returns External IP | full | `tracker` |
| 27 | Private Torrents | partial: the `private` flag, and the server's allow-list and authorization hooks | `metainfo`, `tracker` |
| 29 | uTorrent transport protocol | full, without path MTU discovery and the clock-drift penalty | `utp` |
| 31 | Failure Retry Extension | not done | — |
| 32 | IPv6 extension for DHT | partial: one `Node` per address family, `want` for the other family ignored | `dht` |
| 33 | DHT Scrape | not done | — |
| 35 | Torrent Signing | not done | — |
| 38 | Finding Local Data Via Torrent File Hints | not done | — |
| 41 | UDP Tracker Protocol Extensions | full | `tracker` |
| 42 | DHT Security Extension | full | `dht` |
| 43 | Read-only DHT Nodes | full | `dht` |
| 44 | Storing Arbitrary Data in the DHT | full, except `cas` on the putting side | `dht` |
| 47 | Padding Files and Extended File Attributes | full | `metainfo` |
| 48 | Tracker Protocol Extension: Scrape | full, without full scrape | `tracker` |
| 51 | DHT Infohash Indexing | full | `dht` |
| 52 | The BitTorrent Protocol Specification v2 | full in `metainfo`; partial in `peer`: handshake upgrade and the hash messages' codec, not answering hash requests from piece layers nor verifying the proofs (pending client) | `metainfo`, `peer` |
| 53 | Magnet URI Extension: Select Specific File Indices | partial: `so` parsed and formatted, applied by the client | `metainfo` |
| — | Message Stream Encryption (MSE/PE) | full | `peer/mse`, `peer` |

<!-- CLIENT SECTION: to be filled -->

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

<!-- CLIENT SECTION: to be filled -->
