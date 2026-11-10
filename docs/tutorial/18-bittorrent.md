# BitTorrent

[English](18-bittorrent.md) | [简体中文](18-bittorrent.zh-CN.md)

This chapter is the BitTorrent stack on fib: a client that downloads and seeds v1, v2 and hybrid
torrents from `.torrent` files and magnet links, and the packages it is built from, each usable on
its own. It shows the client lifecycle, streaming a file while it downloads, seeding, resume data
and the one UDP port that uTP, the DHT and UDP trackers share. The examples compile against this
checkout; at runtime they need a network and a real torrent, and nothing else — the whole stack is
the standard library plus fib.

## The model

A `Client` is one fib engine — its own, or one it shares through `Config.SharedEngine` — and one
port number. On TCP that port is a `peer.Server`, served as a `fib.Binding` handler like any other
protocol. On UDP it is a `utp.Socket`, and the same socket carries the DHT node and the UDP tracker
client: they tell datagrams apart by their first byte, so a peer reaches the client at the port the
trackers and the DHT see. Inside the client a `Torrent` holds the pieces, the peers and the
discovery. Its storage is read and written, and its pieces hashed, on the client's disk pool
(`Config.DiskWorkers`), never on a fib worker, while peers, requests, choking and announces run on
fib's workers and event loops. `GotInfo` and `Done` are channels, so a program waits for the two
moments it cares about instead of polling, and `Config.OnEvent` hears everything else. Every
protocol package — `bencode`, `metainfo`, `tracker`, `peer`, `utp`, `dht` — works without the
client, which is what makes the daemon and the library the same code.

## A first download

```go
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/lesismal/fib/bittorrent"
)

func main() {
	c, err := bittorrent.NewClient(bittorrent.Config{
		DataDir:    "downloads", // the torrents' files are written here
		ListenAddr: ":6881",     // TCP peers, and the one UDP port for uTP, the DHT and trackers
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	t, err := c.AddMagnet(os.Args[1], nil) // AddTorrentFile for a .torrent, AddTorrent for a metainfo
	if err != nil {
		log.Fatal(err)
	}
	<-t.GotInfo() // the info dictionary, fetched from peers (BEP 9)
	fmt.Printf("%q: %d files, %d bytes\n", t.Name(), len(t.Files()), t.Info().TotalLength())

	go progress(t)

	// Stream the first file while the rest downloads: a Read waits for its piece.
	files := t.Files()
	if len(files) == 0 {
		log.Fatal("torrent has no files")
	}
	r := files[0].NewReader()
	if _, err := io.Copy(os.Stdout, r); err != nil {
		log.Fatal(err)
	}
	if err := r.Close(); err != nil {
		log.Fatal(err)
	}

	<-t.Done() // every piece wanted is had; the client keeps seeding until Close
	fmt.Fprintf(os.Stderr, "\ndone: %.0f%%\n", t.Stats().Progress*100)
}

func progress(t *bittorrent.Torrent) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			s := t.Stats()
			fmt.Fprintf(os.Stderr, "\r%3.0f%% %7.0f KiB/s %d peers",
				s.Progress*100, s.DownloadRate/1024, s.Peers)
		case <-t.Done():
			return
		}
	}
}
```

- `AddMagnet` parses the link (v1 `btih`, v2 `btmh`, `so` file selection), then fetches the info
  dictionary from peers with `ut_metadata`. A magnet without a working tracker needs
  `AddOptions.Peers` or `Torrent.AddPeers` to find the first peers.
- `File.NewReader` is an `io.ReadSeekCloser` over one file; `Torrent.NewReader` reads the whole
  torrent's data in piece order. `SetReadahead` fetches further ahead, which is how a player streams
  a media file without stalling, and `SetContext` ends a reader with a context.
- `Done` closes when the torrent has every piece it wants; the client then seeds it. `Stats` reports
  progress, rates, peers and hash failures at any moment, and `Recheck` hashes the data again after
  the files changed behind the client's back.

## Adding torrents and options

A torrent comes from a file (`AddTorrentFile`), a parsed `*metainfo.MetaInfo` (`AddTorrent`) or a
magnet (`AddMagnet`). `AddOptions` says how it starts and what it does beyond the client's
defaults:

```go
func addWithOptions(c *bittorrent.Client) (*bittorrent.Torrent, error) {
	mi, err := metainfo.Load("example.torrent")
	if err != nil {
		return nil, err
	}
	return c.AddTorrent(mi, &bittorrent.AddOptions{
		Paused:         true,                                                   // add it, Start it later
		Sequential:     true,                                                   // pieces in order, for playback while downloading
		FilePriorities: []int{bittorrent.PriorityHigh, bittorrent.PriorityNone}, // by Info.FileList index
		Trackers:       []string{"udp://tracker.example:6969"},                 // one more tier, beside the torrent's
		Peers:          []string{"192.0.2.10:6881"},                            // beside what discovery finds
	})
}
```

`Torrent` methods change a running one: `Start`, `Pause`, `Stop`, `Remove(deleteData)`,
`SetSequential`, `SetFilePriority`, `SetLimits`, `AddPeers`. `PriorityNone` means "not wanted":
pieces made only of such files are not downloaded, which is how a selective download works.

## Progress and events

`Stats` is a snapshot for a UI or a log line; `OnEvent` is the ordered stream, delivered on a
goroutine of its own:

```go
func clientWithEvents() (*bittorrent.Client, error) {
	return bittorrent.NewClient(bittorrent.Config{
		OnEvent: func(e bittorrent.Event) {
			switch e.Type {
			case bittorrent.EventMetadata:
				log.Printf("%s: info dictionary in, %d files", e.Torrent.Name(), len(e.Torrent.Files()))
			case bittorrent.EventCompleted:
				log.Printf("%s: every piece wanted is had", e.Torrent.Name())
			case bittorrent.EventError:
				log.Printf("%s: %v", e.Torrent.Name(), e.Err)
			}
		},
	})
}
```

`Event` also carries the piece (verified, failed) and the peer (connected, disconnected, banned).
`EventQueue` bounds the events waiting for `OnEvent`; past it piece and peer events are dropped, the
oldest first, and an `EventDropped` follows with the count, which `DroppedEvents` totals. `State`
says where a torrent is: `StateMetadata`, `StateChecking`, `StateDownloading`, `StateSeeding`.
`Trackers`, `Sources`, `WebSeeds` and `Peers` say where the swarm is: each tracker URL with its last
error, consecutive failures and next announce; each peer source with its candidates; each web seed
with its failures and next try.

## Seeding

Seeding is a torrent added from `metainfo.Builder` with `AddOptions.Seed`: the client takes the files
it is given as every piece, with no second hashing pass. Each piece is checked on the disk pool the
first time a peer asks for a block of it, so a corrupted file is found on first serve:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/lesismal/fib/bittorrent"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

func main() {
	c, err := bittorrent.NewClient(bittorrent.Config{
		DataDir:    "seeds",
		ListenAddr: ":6881",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Hash the directory into a hybrid (v1 + v2) torrent. The trackers are
	// where leechers find this seeder.
	mi, err := (&metainfo.Builder{
		Version:  metainfo.Hybrid,
		Trackers: [][]string{{"udp://tracker.example:6969"}},
	}).BuildPath("seeds/data")
	if err != nil {
		log.Fatal(err)
	}

	t, err := c.AddTorrent(mi, &bittorrent.AddOptions{Seed: true})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("seeding %q as %s", t.Name(), mi.Info.InfoHashV1())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done() // serve until interrupted
}
```

`metainfo.Builder` also takes `WebSeeds` (BEP 19), `Nodes` (DHT nodes for a trackerless torrent),
`Private` (BEP 27, which turns the DHT, PEX and local discovery off) and a fixed `PieceLength`.
`Seed` with `Resume` is refused (`ErrSeedResume`): the resume data of a seeded torrent already lists
the pieces still unchecked. A torrent made elsewhere seeds without `Seed`; the usual check hashes
what storage holds once, and an interrupted or finished download resumes from there.

## Resume and storage

A full check hashes every piece. `ResumeData` records which pieces are had and how each file looked,
and `AddOptions.Resume` takes it back on the next start:

```go
func saveResume(t *bittorrent.Torrent, path string) error {
	data, err := t.ResumeData() // errs before the info dictionary and during a check
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644) // save after Stop, or when ResumeGen moved
}

func addResumed(c *bittorrent.Client, torrentPath, resumePath string) (*bittorrent.Torrent, error) {
	data, err := os.ReadFile(resumePath)
	if err != nil {
		return nil, err
	}
	return c.AddTorrentFile(torrentPath, &bittorrent.AddOptions{Resume: data})
}
```

`ResumeGen` changes whenever a piece is had; save when it differs from the last save, and once more
after `Stop` or `Close`. Pieces in files whose size or modification time changed are hash-checked
anyway, and the trust is libtorrent's, so content replaced with the same size and stamp is not seen.
Storage is a file tree below `Config.DataDir` (`NewFileStorage`, `Preallocate`, `MaxOpenFiles`), a
per-torrent override through `AddOptions.Storage`, or `NewMemoryStorage()` for a torrent that should
never touch the disk.

## The standalone packages

The client composes packages that work alone: `bencode` (marshal and unmarshal with struct tags,
`RawMessage`, `Lookup`), `metainfo` (torrents, magnets, piece layout, `Builder`), `tracker` (HTTP
and UDP clients, and one `Server` for both protocols over a `Store`), `peer` (the wire protocol as a
codec plus a `Server` of torrents), `utp`, `dht` and `peer/mse`. A tracker and the peers it serves
are two handlers on one engine:

```go
func swarmBindings(mi *metainfo.MetaInfo, h peer.Handler) []fib.Binding {
	peers := peer.NewServer(peer.Config{Encryption: mse.Prefer})
	pt := &peer.Torrent{InfoHash: mi.Info.InfoHashV1(), InfoHashV2: mi.Info.InfoHashV2(), Handler: h}
	if err := peers.AddTorrent(pt); err != nil {
		panic(err) // that info hash is already served
	}
	trk := tracker.NewServer(tracker.ServerConfig{})
	return []fib.Binding{
		{Addr: "127.0.0.1:6881", Handler: peers},                     // peers, over TCP
		{Addr: "127.0.0.1:6969", Handler: fibhttp.NewHandler(trk)},   // the HTTP tracker
		{Network: "udp", Addr: "127.0.0.1:6969", Handler: trk.UDP()}, // and the UDP one, same port
	}
}
```

The one UDP port is what makes a client reachable at the port that trackers and the DHT see.
`utp.Socket` reads the port and hands everything that is not uTP to `OnOther`, while the DHT node and
the UDP tracker client send through `Socket.WriteTo`:

```go
func shareOnePort(engine *fib.Engine, peers *peer.Server) (*utp.Socket, error) {
	sock, err := utp.Listen(engine, "udp", ":6881", utp.Config{})
	if err != nil {
		return nil, err
	}
	go func() { _ = peers.Serve(sock) }() // the peers that arrive over uTP
	node, err := dht.New(dht.Config{Send: func(to netip.AddrPort, b []byte) { _ = sock.WriteTo(b, to) }})
	if err != nil {
		return nil, err
	}
	trk := tracker.NewUDPClient(tracker.UDPClientConfig{
		Send: func(to netip.AddrPort, b []byte) error { return sock.WriteTo(b, to) },
	})
	sock.OnOther(func(from netip.AddrPort, b []byte) { // b is valid only during this call
		if !node.HandleDatagram(from, b) {
			trk.HandleDatagram(from, b) // a tracker reply, then
		}
	})
	return sock, nil
}
```

The DHT is a peer source of its own: `Node.Bootstrap` fills its routing table from the public
routers, and `GetPeers` hands over the peers of an info hash as they arrive:

```go
err := node.GetPeers(ctx, ih, func(addrs []netip.AddrPort) {
	log.Print(addrs) // dial them, or hand them to Torrent.AddPeers
})
```

Each of `utp.Mux`, `dht.Node` and the tracker client owns no socket, which is why they share one;
`utp.Listen`, `dht.Listen` and `tracker.ListenUDPClient` each add a binding of their own instead, for
a program that needs only one of them.

## Encryption, versions and limits

```go
func limitedClient() (*bittorrent.Client, error) {
	c, err := bittorrent.NewClient(bittorrent.Config{
		Encryption: bittorrent.EncryptionPrefer, // MSE first; plaintext only when the peer refuses
		MaxConns:   100,                         // peer connections, all torrents together
	})
	if err != nil {
		return nil, err
	}
	c.SetLimits(bittorrent.Limits{DownloadRate: 10 << 20}) // 10 MiB/s, changed while it runs
	return c, nil
}
```

`EncryptionAllow` dials plaintext first and `EncryptionRequire` refuses plaintext in both
directions. Rates, connection counts, upload slots and seed limits are the client's `Limits` and a
torrent's `TorrentLimits`; a `SeedRatio` or `SeedTime` pauses a torrent when reached. v1, v2 and
hybrid torrents are all first class: a v2 magnet fetches its piece layers with BEP 52 hash requests,
and a hybrid is announced under both info hashes. Wire compatibility with libtorrent, Transmission,
anacrolix/torrent, libutp and opentracker is checked by [`bittorrent/interop`](../bittorrent.md#interop).

## Pitfalls

- **A magnet link is metadata, not a torrent.** `GotInfo` may never close if no peer or web seed has
  the info dictionary; give the torrent peers or trackers, and bound the wait.
- **`Close` on the client ends everything.** It closes the torrents, the bindings and the storage; a
  program that keeps serving HTTP on the same engine passes `Config.SharedEngine`.
- **The info hash is the identity.** Two clients that see different info hashes are downloading
  different torrents even with identical files: a `source`, a different piece length or a re-encoded
  info dictionary changes the hash. Read torrents with `metainfo.Load`/`Parse`.
- **Storage trust is not verification.** Pieces taken from resume data or `Seed` are served without
  a check until first read; a peer that gets a bad one may ban this client.
- **Don't copy a file into the directory it downloads into.** `Files()[0].NewReader` reads from
  storage while the client writes it; stream to a different writer, not back into `DataDir`.
- **The disk pool is not the fib pool.** Raise `DiskWorkers`/`DiskQueueBytes` for a slow disk; doing
  storage I/O in `OnEvent` blocks the event stream, not a fib worker.

## See also

- [BitTorrent reference](../bittorrent.md) — every package, the one-port protocol table, client details and interop
- [Raw TCP, UDP and Unix sockets](19-raw-io.md) — the fib layer the client is built on
- [Engine and configuration](02-engine.md) — the pools, buffers and bindings the client inherits
- [The hack guide](../hacks.md) — UDP `DatagramsHandler`, `SendBatch` and the goroutine cheat sheet
- [The README](../../README.md#bittorrent) — the client and its packages at a glance
