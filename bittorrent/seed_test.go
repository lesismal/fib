package bittorrent

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// eventLog records the events of a type a client's OnEvent hears.
type eventLog struct {
	typ EventType
	mu  sync.Mutex
	evs []Event
}

func (l *eventLog) option(c *Config) {
	c.OnEvent = func(e Event) {
		if e.Type == l.typ {
			l.mu.Lock()
			l.evs = append(l.evs, e)
			l.mu.Unlock()
		}
	}
}

// wait waits for an event, and returns the first.
func (l *eventLog) wait(t *testing.T) Event {
	t.Helper()
	deadline := time.Now().Add(transferTimeout)
	for {
		l.mu.Lock()
		if len(l.evs) > 0 {
			e := l.evs[0]
			l.mu.Unlock()
			return e
		}
		l.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("no %v event", l.typ)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitFor polls cond until it holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(transferTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A torrent added as a seed reads nothing until a peer asks; a peer then
// downloads all of it, each piece checked once on the way out; resume
// data keeps the pieces not checked yet apart.
func TestSeedMode(t *testing.T) {
	for _, v := range []metainfo.Version{metainfo.V1, metainfo.V2, metainfo.Hybrid} {
		t.Run(versionNames[v], func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			files := testFiles(t, sdir, "content", 100_000, 300_000, 0, 65_536, 1234)
			mi := buildTorrent(t, sdir, "content", v, 32<<10)
			n := mi.Info.PieceCount()
			seeder := newTestClient(t, sdir)
			open, rc := countingOpener(NewFileStorage(sdir, FileStorageOptions{}), false)
			if _, err := seeder.AddTorrent(mi, &AddOptions{Storage: open, Seed: true, Resume: []byte("d")}); !errors.Is(err, ErrSeedResume) {
				t.Fatalf("Seed with Resume: %v", err)
			}
			st, err := seeder.AddTorrent(mi, &AddOptions{Storage: open, Seed: true})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, st)
			if s := st.Stats(); s.State != StateSeeding || s.PiecesHave != n || s.Trusted != n {
				t.Fatalf("added as a seed: %+v", s)
			}
			if r := rc.total(); r != 0 {
				t.Fatalf("%d reads at add", r)
			}

			// Resume data of the unchecked seed: re-added with it, the
			// pieces are still had, unchecked, and nothing is read.
			data, err := st.ResumeData()
			if err != nil {
				t.Fatal(err)
			}
			again := newTestClient(t, sdir)
			open2, rc2 := countingOpener(NewFileStorage(sdir, FileStorageOptions{}), false)
			at, err := again.AddTorrent(mi, &AddOptions{Storage: open2, Resume: data, Paused: true})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, at)
			if s := at.Stats(); s.PiecesHave != n || s.Trusted != n || rc2.total() != 0 {
				t.Fatalf("resumed seed: %+v, %d reads", s, rc2.total())
			}
			again.Close()

			gen := st.ResumeGen()
			leecher := newTestClient(t, ldir)
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, lt)
			checkFiles(t, ldir, files)
			if s := lt.Stats(); s.HashFails != 0 {
				t.Fatalf("leecher: %+v", s)
			}
			// Every piece was served, and so checked.
			waitFor(t, "every piece checked", func() bool { return st.Stats().Trusted == 0 })
			if s := st.Stats(); s.HashFails != 0 || s.PiecesHave != n || s.State != StateSeeding {
				t.Fatalf("seeder: %+v", s)
			}
			if st.ResumeGen() == gen {
				t.Fatal("ResumeGen did not move as pieces were checked")
			}
		})
	}
}

// A seed of a magnet link takes its storage as complete once the info
// dictionary has come.
func TestSeedModeMagnet(t *testing.T) {
	sdir, mdir := t.TempDir(), t.TempDir()
	testFiles(t, sdir, "content", 200_000, 70_000)
	testFiles(t, mdir, "content", 200_000, 70_000)
	mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 16<<10)
	seeder, _ := seed(t, sdir, mi)
	c := newTestClient(t, mdir)
	open, rc := countingOpener(NewFileStorage(mdir, FileStorageOptions{}), false)
	m := mi.Magnet()
	m.Peers = []string{addrOf(seeder)}
	tor, err := c.AddMagnet(m.String(), &AddOptions{Storage: open, Seed: true})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, tor)
	if s := tor.Stats(); s.Trusted != mi.Info.PieceCount() || s.Downloaded != 0 || rc.total() != 0 {
		t.Fatalf("magnet seed: %+v, %d reads", s, rc.total())
	}
}

// A file changed after the torrent was made is caught the first time its
// piece is to be served: the block is rejected, the piece no longer had,
// and the torrent downloads it from a good seed.
func TestSeedModeCorrupt(t *testing.T) {
	sdir, gdir, ldir := t.TempDir(), t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 100_000, 300_000)
	testFiles(t, gdir, "content", 100_000, 300_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	var rel string
	for r := range files {
		rel = r
		break
	}
	p := filepath.Join(sdir, rel)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[5] ^= 0xff
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	bad := int((mi.Info.FileList()[fileIndex(t, mi.Info, rel[len("content/"):])].Offset + 5) / mi.Info.PieceLength)

	fails := &eventLog{typ: EventHashFailed}
	seeder := newTestClient(t, sdir, fails.option)
	st, err := seeder.AddTorrent(mi, &AddOptions{Seed: true})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, st)
	leecher := newTestClient(t, ldir)
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	if e := fails.wait(t); e.Piece != bad {
		t.Fatalf("hash failure of piece %d, want %d", e.Piece, bad)
	}
	if st.HavePiece(bad) || st.State() != StateDownloading {
		t.Fatalf("bad piece still had: %+v", st.Stats())
	}
	// Everything else comes through; the bad piece never does.
	waitFor(t, "the good pieces", func() bool { return lt.Stats().PiecesHave == mi.Info.PieceCount()-1 })
	if lt.HavePiece(bad) || lt.Stats().HashFails != 0 {
		t.Fatalf("leecher got the bad piece: %+v", lt.Stats())
	}

	good, _ := seed(t, gdir, mi)
	st.AddPeers(addrOf(good))
	lt.AddPeers(addrOf(good))
	waitDone(t, st)
	waitDone(t, lt)
	checkFiles(t, sdir, files)
	checkFiles(t, ldir, files)
}

// A hybrid torrent from a magnet link of its v1 hash, downloaded, computes
// its piece layers: it answers BEP 52 hash requests, and a peer of its v2
// hash alone downloads from it.
func TestHybridMagnetLayers(t *testing.T) {
	sdir, bdir, cdir := t.TempDir(), t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 200_000, 70_000, 5_000)
	mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 16<<10)
	if len(mi.PieceLayers) == 0 {
		t.Fatal("no piece layers built")
	}
	seeder, _ := seed(t, sdir, mi)

	b := newTestClient(t, bdir)
	m := mi.Magnet()
	m.InfoHashV2 = metainfo.InfoHashV2{}
	m.Peers = []string{addrOf(seeder)}
	bt, err := b.AddMagnet(m.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, bt)
	waitFor(t, "the piece layers", func() bool {
		got := bt.MetaInfo().PieceLayers
		if len(got) != len(mi.PieceLayers) {
			return false
		}
		for root, layer := range mi.PieceLayers {
			if !bytes.Equal(got[root], layer) {
				return false
			}
		}
		return true
	})
	seeder.Close()

	// A peer of the v2 hash asks for each layer.
	ih2 := mi.Info.InfoHashV2()
	type answer struct {
		root [32]byte
		data []byte
		ok   bool
	}
	answers := make(chan answer, len(mi.PieceLayers))
	base := uint32(log2(mi.Info.PieceLength / metainfo.BlockSize))
	srv := peer.NewServer(peer.Config{})
	pt := &peer.Torrent{InfoHashV2: ih2, Handler: &peer.HandlerFuncs{
		Handshake: func(c *peer.Conn) {
			if !c.V2() {
				answers <- answer{}
				return
			}
			for root, layer := range mi.PieceLayers {
				_ = c.Send(&peer.Message{
					Type: peer.MsgHashRequest, PiecesRoot: root, BaseLayer: base,
					Index: 0, Length: uint32(nextPow2(len(layer) / 32)),
				})
			}
		},
		Message: func(c *peer.Conn, msg *peer.Message) {
			switch msg.Type {
			case peer.MsgHashes:
				answers <- answer{root: msg.PiecesRoot, data: bytes.Clone(msg.Data), ok: true}
			case peer.MsgHashReject:
				answers <- answer{root: msg.PiecesRoot}
			}
		},
	}}
	if err := srv.AddTorrent(pt); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", addrOf(b))
	if err != nil {
		t.Fatal(err)
	}
	pc, err := srv.Open(conn, peer.DialOptions{InfoHash: ih2.Truncate()})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	for range mi.PieceLayers {
		select {
		case a := <-answers:
			layer, known := mi.PieceLayers[a.root]
			if !a.ok || !known || len(a.data) < len(layer) || !bytes.Equal(a.data[:len(layer)], layer) {
				t.Fatalf("hash request of %x: answered %v, %d bytes", a.root[:4], a.ok, len(a.data))
			}
		case <-time.After(transferTimeout):
			t.Fatal("no answer to a hash request")
		}
	}

	c := newTestClient(t, cdir)
	m = mi.Magnet()
	m.InfoHash = metainfo.InfoHash{}
	m.Peers = []string{addrOf(b)}
	ct, err := c.AddMagnet(m.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, ct)
	checkFiles(t, cdir, files)
}
