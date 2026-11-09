package bittorrent

import (
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// pickTorrent is a torrent of n pieces with the availabilities avail,
// all of normal priority and wanted, enough had (none of these) that the
// picker goes rarest first.
func pickTorrent(avail ...int32) *Torrent {
	t := &Torrent{rng: newRNG(), partials: map[int]*partial{}, haveCount: randomFirstPieces}
	t.pieces = make([]pieceState, len(avail))
	for i, a := range avail {
		t.pieces[i] = pieceState{prio: PriorityNormal, avail: a, dataLen: 4 * blockSize}
	}
	t.missing = len(avail)
	t.rebuildIndexLocked()
	return t
}

// checkIndex checks that the index holds exactly the pieces wanted and
// not had, each where it says it is.
func checkIndex(t *testing.T, tor *Torrent) {
	t.Helper()
	ix := &tor.pick
	n := 0
	for i := range tor.pieces {
		ps := &tor.pieces[i]
		want := !ps.have && ps.prio > PriorityNone
		if ps.inIndex != want {
			t.Fatalf("piece %d in index %v, want %v", i, ps.inIndex, want)
		}
		if !ps.inIndex {
			continue
		}
		n++
		cl := ix.classes[ps.ixPrio]
		if cl == nil || int(ps.ixAvail) >= len(cl.buckets) || int(ps.ixPos) >= len(cl.buckets[ps.ixAvail]) ||
			cl.buckets[ps.ixAvail][ps.ixPos] != int32(i) {
			t.Fatalf("piece %d not where the index says", i)
		}
		if ps.ixPrio != ps.prio || ps.ixAvail != min(max(ps.avail, 0), maxAvailBuckets-1) {
			t.Fatalf("piece %d filed under prio %d avail %d, has %d %d", i, ps.ixPrio, ps.ixAvail, ps.prio, ps.avail)
		}
	}
	if n != ix.count {
		t.Fatalf("index counts %d pieces, holds %d", ix.count, n)
	}
	for k := 1; k < len(ix.prios); k++ {
		if ix.prios[k-1] <= ix.prios[k] {
			t.Fatalf("priorities not highest first: %v", ix.prios)
		}
	}
}

func anyPiece(int) bool { return true }

func TestPickRarestFirst(t *testing.T) {
	tor := pickTorrent(3, 1, 2, 1, 0)
	checkIndex(t, tor)
	seen := map[int]bool{}
	for range 200 {
		i, ok := tor.pickPiece(anyPiece, 1)
		if !ok || (i != 1 && i != 3) {
			t.Fatalf("picked %d %v, want 1 or 3", i, ok)
		}
		seen[i] = true
	}
	if !seen[1] || !seen[3] {
		t.Fatalf("ties not broken at random: %v", seen)
	}
	// Piece 4 no peer has: only a pick from availability 0 (a web
	// seed's) takes it.
	if i, _ := tor.pickPiece(anyPiece, 0); i != 4 {
		t.Fatalf("from availability 0 picked %d, want 4", i)
	}
	// Under way pieces are passed over, and can is obeyed.
	tor.pieces[1].partial = &partial{}
	if i, _ := tor.pickPiece(anyPiece, 1); i != 3 {
		t.Fatalf("picked %d, want 3", i)
	}
	if i, _ := tor.pickPiece(func(i int) bool { return i == 0 }, 1); i != 0 {
		t.Fatalf("picked %d, want 0", i)
	}
	if _, ok := tor.pickPiece(func(int) bool { return false }, 1); ok {
		t.Fatal("picked a piece can refuses")
	}
}

func TestPickIndexFollowsChanges(t *testing.T) {
	tor := pickTorrent(3, 1, 2, 1, 0)
	// Availability: piece 0 becomes the rarest.
	tor.addAvailLocked(0, -1)
	tor.addAvailLocked(0, -1)
	tor.addAvailLocked(0, -1)
	tor.addAvailLocked(0, 1)
	tor.addAvailLocked(1, 1)
	tor.addAvailLocked(3, 1)
	checkIndex(t, tor)
	if i, _ := tor.pickPiece(anyPiece, 1); i != 0 {
		t.Fatalf("picked %d, want 0", i)
	}
	// Had: out of the index.
	tor.pieces[0].have = true
	tor.missing--
	tor.indexRemoveLocked(0)
	checkIndex(t, tor)
	if i, _ := tor.pickPiece(anyPiece, 1); i < 1 || i > 3 {
		t.Fatalf("picked %d, want 1, 2 or 3 (two peers have each)", i)
	}
	// A change that did not go through the index (a piece no longer
	// had) is found by the count, and the index made again.
	tor.pieces[0].have = false
	tor.missing++
	if i, _ := tor.pickPiece(anyPiece, 1); i != 0 {
		t.Fatalf("picked %d after a change outside the index, want 0", i)
	}
	checkIndex(t, tor)
	// Priorities: the highest first, whatever the rarity; none is never
	// picked.
	tor.pieces[3].prio = PriorityHigh
	tor.pieces[0].prio = PriorityNone
	tor.missing--
	tor.rebuildIndexLocked()
	checkIndex(t, tor)
	if i, _ := tor.pickPiece(anyPiece, 1); i != 3 {
		t.Fatalf("picked %d, want 3 (high priority)", i)
	}
	tor.pieces[3].partial = &partial{}
	if i, _ := tor.pickPiece(anyPiece, 1); i != 1 && i != 2 {
		t.Fatalf("picked %d, want 1 or 2", i)
	}
}

func TestPickSequentialAndUrgent(t *testing.T) {
	for _, n := range []int{8, 1000} { // a class looked at whole, and one walked by index
		avail := make([]int32, n)
		for i := range avail {
			avail[i] = int32(1 + (n-i)%5)
		}
		tor := pickTorrent(avail...)
		tor.sequential = true
		for want := range 3 {
			i, ok := tor.pickPiece(anyPiece, 1)
			if !ok || i != want {
				t.Fatalf("n=%d: sequential picked %d %v, want %d", n, i, ok, want)
			}
			tor.pieces[i].partial = &partial{}
		}
		tor.pieces[0].have = true
		tor.missing--
		tor.indexRemoveLocked(0)
		if i, _ := tor.pickPiece(func(i int) bool { return i > 5 }, 1); i != 6 {
			t.Fatalf("n=%d: sequential picked %d, want 6", n, i)
		}
		// Urgent pieces, the lowest index first, before anything.
		tor.sequential = false
		tor.pieces[7].prio = priorityUrgent + 10
		tor.pieces[5].prio = priorityUrgent + 10
		tor.rebuildIndexLocked()
		if i, _ := tor.pickPiece(anyPiece, 1); i != 5 {
			t.Fatalf("n=%d: urgent picked %d, want 5", n, i)
		}
	}
}

// reserveTorrent is a torrent of n pieces of 4 blocks of a 4-piece
// layout every peer has, for nextRequest.
func reserveTorrent(t *testing.T, n int) *Torrent {
	t.Helper()
	plen := int64(4 * blockSize)
	info := &metainfo.Info{Name: "x", PieceLength: plen, Length: int64(n) * plen, Pieces: make([]byte, 20*n)}
	tor := pickTorrent(make([]int32, n)...)
	tor.info = info
	tor.c = &Client{cfg: Config{DiskQueueBytes: 1 << 20}}
	for i := range tor.pieces {
		tor.pieces[i].avail = 2
		tor.pieces[i].dataLen = plen
	}
	tor.rebuildIndexLocked()
	return tor
}

func reservePeer(n int) *peerConn {
	bf := peer.NewBitfield(n)
	bf.SetAll()
	return &peerConn{bits: bf, pending: map[peer.Request]time.Time{}}
}

func TestReservedPieces(t *testing.T) {
	tor := reserveTorrent(t, 6)
	a, b := reservePeer(6), reservePeer(6)
	request := func(pc *peerConn) peer.Request {
		t.Helper()
		r, ok := tor.nextRequest(pc, false)
		if !ok {
			t.Fatal("nothing to request")
		}
		pc.pending[r] = time.Time{}
		return r
	}
	// a's requests are one piece's blocks in order, then another's.
	first := request(a)
	for k := 1; k < 4; k++ {
		if r := request(a); r.Index != first.Index || r.Begin != uint32(k*blockSize) {
			t.Fatalf("request %d of a: %+v, want block %d of piece %d", k, r, k, first.Index)
		}
	}
	p := tor.partials[int(first.Index)]
	if p.owner != a || len(a.owned) != 1 {
		t.Fatalf("piece %d owned by %p (a %p), a owns %d", first.Index, p.owner, a, len(a.owned))
	}
	second := request(a)
	if second.Index == first.Index {
		t.Fatal("a asked for a full piece again")
	}
	// b never asks for a's pieces while others are to be had.
	for range 8 {
		if r := request(b); r.Index == first.Index || r.Index == second.Index {
			t.Fatalf("b asked for a's piece %d", r.Index)
		}
	}
	// a goes: its pieces' blocks not received are b's to take.
	for r := range a.pending {
		tor.releaseRequest(r)
	}
	tor.releaseOwnedLocked(a)
	if p.owner != nil || !p.free {
		t.Fatalf("piece %d still owned (%p) or not free (%v)", first.Index, p.owner, p.free)
	}
	r := request(b)
	if r.Index != first.Index && r.Index != second.Index {
		t.Fatalf("b asked for piece %d, not one a gave up", r.Index)
	}
	if q := tor.partials[int(r.Index)]; q.owner != b {
		t.Fatalf("piece %d taken by b not owned by it", r.Index)
	}
}

// TestPieceWrittenWhole checks that a piece whose blocks are buffered is
// checked from memory and written in one write, not read back (the check
// of the empty files when the torrent is added reads each once); and that
// with no room for buffers (DiskQueueBytes below a piece) each block is
// written as it comes and the piece read back to be checked.
func TestPieceWrittenWhole(t *testing.T) {
	for _, buffered := range []bool{true, false} {
		name := map[bool]string{true: "buffered", false: "unbuffered"}[buffered]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sdir, ldir := t.TempDir(), t.TempDir()
			const size, plen = 1_000_000, 64 << 10
			files := testFiles(t, sdir, "content", size)
			mi := buildTorrent(t, sdir, "content", metainfo.V1, plen)
			seeder, _ := seed(t, sdir, mi, func(c *Config) { c.DisableUTP = true })
			lw := &watch{opener: NewFileStorage(ldir, FileStorageOptions{})}
			leecher := newTestClient(t, ldir, func(c *Config) {
				c.DisableUTP = true
				c.Storage = lw.open
				if !buffered {
					c.DiskQueueBytes = plen / 2
				}
			})
			lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, lt)
			checkFiles(t, ldir, files)
			reads, writes := lw.check(t, "leecher")
			pieces := int64((size + plen - 1) / plen)
			blocks := int64((size + blockSize - 1) / blockSize)
			if buffered && (writes != pieces || reads > pieces) {
				t.Fatalf("%d writes and %d reads, want %d writes (one a piece) and no more reads than the first check's, %d", writes, reads, pieces, pieces)
			}
			if !buffered && (writes != blocks || reads < 2*pieces) {
				t.Fatalf("%d writes and %d reads, want %d writes (one a block) and %d reads at least", writes, reads, blocks, 2*pieces)
			}
			if n := leecher.pieceBufs.Load(); n != 0 {
				t.Fatalf("%d bytes of piece buffers held after the download", n)
			}
		})
	}
}

// TestPeersDialedAtOnce checks that the peers a torrent is added with,
// and those added later, are dialed at once, not at the next tick (up to
// a second later, which was most of a fast download's time).
func TestPeersDialedAtOnce(t *testing.T) {
	sdir := t.TempDir()
	testFiles(t, sdir, "content", 100_000)
	mi := buildTorrent(t, sdir, "content", metainfo.V1, 16<<10)
	seeder, _ := seed(t, sdir, mi, func(c *Config) { c.DisableUTP = true })
	other, _ := seed(t, sdir, mi, func(c *Config) { c.DisableUTP = true })
	leecher := newTestClient(t, t.TempDir(), func(c *Config) { c.DisableUTP = true })
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	tried := func(addr string) bool {
		lt.mu.Lock()
		defer lt.mu.Unlock()
		cand := lt.pool[netip.MustParseAddrPort(addr)]
		return cand != nil && (cand.dialing || cand.connected || cand.fails > 0)
	}
	if !tried(addrOf(seeder)) {
		t.Fatal("the peer added with the torrent was not dialed when it was added")
	}
	lt.AddPeers(addrOf(other))
	if !tried(addrOf(other)) {
		t.Fatal("a peer added later was not dialed when it was added")
	}
}
