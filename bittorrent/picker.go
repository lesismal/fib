package bittorrent

import (
	"math/rand/v2"
	"net/netip"
	"slices"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bittorrent/peer"
)

const blockSize = peer.BlockSize

// File priorities. A piece's priority is the highest of the files it
// holds bytes of, and a Reader raises the pieces ahead of it above them
// all.
const (
	PriorityNone   = 0 // not downloaded
	PriorityLow    = 1
	PriorityNormal = 4
	PriorityHigh   = 7
	// priorityUrgent is the least priority of a piece a Reader waits for
	// or will soon: those go first, the nearest first.
	priorityUrgent = 16
)

// randomFirstPieces is how many pieces a download fetches at random
// before it goes rarest first: a peer with nothing has nothing to trade,
// and the rarest piece is usually slow to get, so the first few are any
// that come quickly.
const randomFirstPieces = 4

// maxBlockDuplicates is how many peers one block is requested from at
// once in endgame mode.
const maxBlockDuplicates = 2

// pieceState is what the picker knows of a piece.
type pieceState struct {
	prio    int   // effective priority: of the files, or a Reader's
	filePri int   // the files' priority alone
	avail   int32 // peers that have it
	// dataLen is the piece's length without the padding (BEP 47) that
	// ends it: the bytes requested of peers. The rest is zeros, which
	// anacrolix and libtorrent do not serve.
	dataLen int64
	have    bool
	// trusted is a piece had because AddOptions.Seed took the storage as
	// complete, not yet checked: it is, before it is first served.
	trusted bool
	partial *partial
	// Where the piece is in the picker's index (pickIndex), when it is
	// in it: the priority and availability it was filed under, and its
	// position in that bucket.
	inIndex bool
	ixPrio  int
	ixAvail int32
	ixPos   int32
}

// blockState is a block of a piece being downloaded.
type blockState struct {
	reqs uint8 // peers it is requested from now
	done bool
	from netip.AddrPort // who sent it, by ban address
}

// partial is a piece being downloaded: its blocks, who sent them, and
// how far it is from verification.
type partial struct {
	index   int
	blocks  []blockState
	done    int
	writing int  // blocks being written to storage
	busy    bool // being verified
	// waitHashes says the piece's data is all there but its v2 hash is
	// not known yet: verification waits for the piece layer (BEP 52).
	waitHashes bool
	// owner is the peer that reserved the piece: its blocks are
	// requested of that peer alone (but in endgame mode), one after
	// another, so that a peer's requests are a run of one piece rather
	// than picked again for each block. nil for a piece any peer may
	// take: one a Reader waits for, or one its owner gave up.
	owner *peerConn
	// free says the piece is in Torrent.free: unowned, with blocks to
	// request.
	free bool
	// next is the first block that may be neither done nor requested:
	// those before it are, so the next request starts there.
	next int
	// buf holds the piece's blocks as they arrive, when the client's
	// piece buffers have room (Client.pieceBufs): the piece is then
	// checked from memory and written whole, at once, and copied is how
	// many blocks are in it. nil for a piece whose blocks are each
	// written as they come.
	buf    []byte
	copied atomic.Int32
}

func newPartial(index int, size int64) *partial {
	n := int((size + blockSize - 1) / blockSize)
	return &partial{index: index, blocks: make([]blockState, n)}
}

func (p *partial) complete() bool { return p.done == len(p.blocks) }

// requestable reports whether a block of p is neither done nor
// requested, and moves p.next up to the first such block.
func (p *partial) requestable() bool {
	for p.next < len(p.blocks) {
		if bs := &p.blocks[p.next]; !bs.done && bs.reqs == 0 {
			return true
		}
		p.next++
	}
	return false
}

// reopen notes that block b of p may be requested again.
func (p *partial) reopen(b int) {
	if b < p.next {
		p.next = b
	}
}

// blockRequest is block b of piece i as a request.
func (t *Torrent) blockRequest(i, b int) peer.Request {
	size := t.pieces[i].dataLen
	begin := int64(b) * blockSize
	return peer.Request{Index: uint32(i), Begin: uint32(begin), Length: uint32(min(blockSize, size-begin))}
}

// wanted reports piece i as one to download.
func (t *Torrent) wanted(i int) bool {
	ps := &t.pieces[i]
	return !ps.have && ps.prio > PriorityNone
}

// nextRequest picks the next block to request from pc, and marks it
// requested: from a piece pc reserved, so that pieces complete one after
// another rather than all at the end, or one no peer holds; else from a
// new piece, chosen by priority, then rarity (randomly for the first few,
// by index in sequential mode), which pc reserves; else, in endgame mode,
// a block another peer has outstanding. A new piece of a higher priority
// than those under way is looked for whenever the priorities change.
// With fastOnly, only the pieces pc allowed fast qualify. Called with t.mu
// held.
func (t *Torrent) nextRequest(pc *peerConn, fastOnly bool) (peer.Request, bool) {
	can := func(i int) bool {
		if !pc.has(i) || !t.wanted(i) {
			return false
		}
		return !fastOnly || pc.c.AllowedFast(uint32(i))
	}
	// A piece of pc's, or one any peer may take, of the highest
	// priority among those with a block to request.
	var best *partial
	consider := func(p *partial) {
		if p.waitHashes || p.busy || !p.requestable() || !can(p.index) {
			return
		}
		if best == nil || t.pieces[p.index].prio > t.pieces[best.index].prio {
			best = p
		}
	}
	kept := pc.owned[:0]
	for _, p := range pc.owned {
		if p.owner != pc || t.partials[p.index] != p {
			continue
		}
		kept = append(kept, p)
		consider(p)
	}
	clear(pc.owned[len(kept):])
	pc.owned = kept
	if len(t.free) > 0 {
		kept := t.free[:0]
		for _, p := range t.free {
			if p.owner != nil || t.partials[p.index] != p || p.complete() {
				p.free = false
				continue
			}
			kept = append(kept, p)
			consider(p)
		}
		clear(t.free[len(kept):])
		t.free = kept
	}
	// A new piece, when there is nothing else or the priorities changed
	// since pc last looked, and it beats every piece under way.
	if best == nil || pc.prioGen != t.prioGen {
		pc.prioGen = t.prioGen
		if i, ok := t.pickPiece(can, 1); ok && (best == nil || t.pieces[i].prio > t.pieces[best.index].prio) {
			p := newPartial(i, t.pieces[i].dataLen)
			t.pieces[i].partial = p
			t.partials[i] = p
			t.c.allocPieceBuf(t, p)
			best = p
		}
	}
	if best != nil {
		for b := best.next; b < len(best.blocks); b++ {
			bs := &best.blocks[b]
			if bs.done || bs.reqs > 0 {
				continue
			}
			r := t.blockRequest(best.index, b)
			if _, dup := pc.pending[r]; dup {
				continue
			}
			bs.reqs++
			if b == best.next {
				best.next++
			}
			if best.owner == nil && t.pieces[best.index].prio < priorityUrgent {
				best.owner = pc
				pc.owned = append(pc.owned, best)
			}
			return r, true
		}
	}
	// Endgame: every block this peer could send is requested from
	// another; ask this one too, and cancel the other when one arrives.
	var pick peer.Request
	found := false
	for _, p := range t.partials {
		if p.waitHashes || p.busy || !can(p.index) {
			continue
		}
		for b := range p.blocks {
			bs := &p.blocks[b]
			if bs.done || bs.reqs >= maxBlockDuplicates {
				continue
			}
			r := t.blockRequest(p.index, b)
			if _, dup := pc.pending[r]; dup {
				continue
			}
			if !found || t.pieces[p.index].prio > t.pieces[pick.Index].prio {
				pick, found = r, true
			}
			break
		}
	}
	if found {
		t.partials[int(pick.Index)].blocks[pick.Begin/blockSize].reqs++
		t.endgame = true
	}
	return pick, found
}

// releaseOwnedLocked gives up the pieces pc reserved: any peer may take
// their blocks not requested of it. Called when pc closes, chokes this
// side or is snubbed.
func (t *Torrent) releaseOwnedLocked(pc *peerConn) {
	for _, p := range pc.owned {
		if p.owner == pc {
			p.owner = nil
			t.markFreeLocked(p)
		}
	}
	clear(pc.owned)
	pc.owned = pc.owned[:0]
}

// markFreeLocked lists p, which no peer reserved, among the pieces any
// peer may take blocks of.
func (t *Torrent) markFreeLocked(p *partial) {
	if p.free || p.owner != nil || t.partials[p.index] != p {
		return
	}
	p.free = true
	t.free = append(t.free, p)
}

// pickPiece chooses a piece not yet under way that can(i) allows: the
// highest priority first; among those the lowest index for urgent pieces
// and in sequential mode, a random one for the first few pieces, and
// else the rarest, ties broken at random. Pieces fewer than minAvail
// peers have are passed over: a peer has none of those that no peer has.
// It looks at the index of the pieces by priority and availability
// rather than at every piece.
func (t *Torrent) pickPiece(can func(int) bool, minAvail int) (int, bool) {
	ix := &t.pick
	if ix.count != t.missing {
		// A change of what is had or wanted that did not go through
		// the index: it is made again.
		t.rebuildIndexLocked()
	}
	ok := func(i int32) bool { return t.pieces[i].partial == nil && can(int(i)) }
	random := t.haveCount < randomFirstPieces
	for _, prio := range ix.prios {
		cl := ix.classes[prio]
		if prio >= priorityUrgent || t.sequential {
			if i, found := t.lowestIndex(cl, prio, minAvail, ok); found {
				return i, true
			}
			continue
		}
		nb := len(cl.buckets)
		if nb <= minAvail {
			continue
		}
		if random {
			// Any bucket from a random one on, any piece in it.
			start := minAvail + t.rng.IntN(nb-minAvail)
			for k := range nb - minAvail {
				a := minAvail + (start-minAvail+k)%(nb-minAvail)
				if i, found := t.randomIn(cl.buckets[a], ok); found {
					return i, true
				}
			}
			continue
		}
		for a := minAvail; a < nb; a++ {
			if i, found := t.randomIn(cl.buckets[a], ok); found {
				return i, true
			}
		}
	}
	return -1, false
}

// randomIn is the first piece of b that ok allows, from a random place.
func (t *Torrent) randomIn(b []int32, ok func(int32) bool) (int, bool) {
	n := len(b)
	if n == 0 {
		return -1, false
	}
	start := 0
	if n > 1 {
		start = t.rng.IntN(n)
	}
	for k := range n {
		if i := b[(start+k)%n]; ok(i) {
			return int(i), true
		}
	}
	return -1, false
}

// lowestIndex is the lowest-indexed piece of class cl (priority prio)
// that ok allows and minAvail peers have: from its members when they are
// few, else in index order from the first piece missing, so that a
// sequential download need not look at the whole class each time.
func (t *Torrent) lowestIndex(cl *prioClass, prio, minAvail int, ok func(int32) bool) (int, bool) {
	if cl.n <= 256 {
		best := int32(-1)
		for a := minAvail; a < len(cl.buckets); a++ {
			for _, i := range cl.buckets[a] {
				if (best < 0 || i < best) && ok(i) {
					best = i
				}
			}
		}
		return int(best), best >= 0
	}
	ix := &t.pick
	for ix.firstMissing < len(t.pieces) && t.pieces[ix.firstMissing].have {
		ix.firstMissing++
	}
	for i := ix.firstMissing; i < len(t.pieces); i++ {
		ps := &t.pieces[i]
		if ps.inIndex && ps.ixPrio == prio && int(ps.ixAvail) >= minAvail && ok(int32(i)) {
			return i, true
		}
	}
	return -1, false
}

// pickIndex files the pieces to download (wanted and not had) by
// priority, then by how many peers have them, so that the picker finds
// the rarest piece of the highest priority without looking at every
// piece. Pieces under way stay in it and are passed over. Guarded by
// t.mu.
type pickIndex struct {
	classes map[int]*prioClass
	prios   []int // the priorities of classes, highest first
	count   int   // pieces in the index
	// firstMissing is no more than the first piece not had: where a
	// sequential pick from a large class starts looking.
	firstMissing int
}

// prioClass is the pieces of one priority, in buckets by availability:
// buckets[a] holds those a peers have (the last bucket also those more
// peers have than buckets go up to).
type prioClass struct {
	buckets [][]int32
	n       int
}

// maxAvailBuckets bounds the buckets of a class: past it, pieces are
// equally common for the picker.
const maxAvailBuckets = 1024

// rebuildIndexLocked files every piece to download again.
func (t *Torrent) rebuildIndexLocked() {
	ix := &t.pick
	ix.classes = map[int]*prioClass{}
	ix.prios = ix.prios[:0]
	ix.count = 0
	ix.firstMissing = 0
	for i := range t.pieces {
		t.pieces[i].inIndex = false
	}
	for i := range t.pieces {
		t.indexAddLocked(i)
	}
}

// indexAddLocked files piece i, if it is to be downloaded.
func (t *Torrent) indexAddLocked(i int) {
	ps := &t.pieces[i]
	if ps.inIndex || ps.have || ps.prio <= PriorityNone {
		return
	}
	ix := &t.pick
	if ix.classes == nil {
		ix.classes = map[int]*prioClass{}
	}
	cl := ix.classes[ps.prio]
	if cl == nil {
		cl = &prioClass{}
		ix.classes[ps.prio] = cl
		k, _ := slices.BinarySearchFunc(ix.prios, ps.prio, func(a, b int) int { return b - a })
		ix.prios = slices.Insert(ix.prios, k, ps.prio)
	}
	a := min(max(ps.avail, 0), maxAvailBuckets-1)
	for int(a) >= len(cl.buckets) {
		cl.buckets = append(cl.buckets, nil)
	}
	ps.inIndex, ps.ixPrio, ps.ixAvail, ps.ixPos = true, ps.prio, a, int32(len(cl.buckets[a]))
	cl.buckets[a] = append(cl.buckets[a], int32(i))
	cl.n++
	ix.count++
	if i < ix.firstMissing {
		ix.firstMissing = i
	}
}

// indexRemoveLocked takes piece i out of the index.
func (t *Torrent) indexRemoveLocked(i int) {
	ps := &t.pieces[i]
	if !ps.inIndex {
		return
	}
	ix := &t.pick
	cl := ix.classes[ps.ixPrio]
	b := cl.buckets[ps.ixAvail]
	last := b[len(b)-1]
	b[ps.ixPos] = last
	t.pieces[last].ixPos = ps.ixPos
	cl.buckets[ps.ixAvail] = b[:len(b)-1]
	ps.inIndex = false
	cl.n--
	ix.count--
	if cl.n == 0 {
		delete(ix.classes, ps.ixPrio)
		if k := slices.Index(ix.prios, ps.ixPrio); k >= 0 {
			ix.prios = slices.Delete(ix.prios, k, k+1)
		}
	}
}

// addAvailLocked counts one peer more (d 1) or less (d -1) that has
// piece i, and files it again by that.
func (t *Torrent) addAvailLocked(i int, d int32) {
	ps := &t.pieces[i]
	ps.avail += d
	if ps.inIndex && min(max(ps.avail, 0), maxAvailBuckets-1) != ps.ixAvail {
		t.indexRemoveLocked(i)
		t.indexAddLocked(i)
	}
}

// releaseRequest marks r no longer outstanding from one peer, so that
// the block can be requested again. Called with t.mu held.
func (t *Torrent) releaseRequest(r peer.Request) {
	p := t.partials[int(r.Index)]
	if p == nil {
		return
	}
	b := int(r.Begin / blockSize)
	if b < len(p.blocks) && p.blocks[b].reqs > 0 {
		p.blocks[b].reqs--
		if p.blocks[b].reqs == 0 && !p.blocks[b].done {
			p.reopen(b)
			if p.owner == nil {
				t.markFreeLocked(p)
			}
		}
	}
}

// dropPartial forgets piece i's progress: its data failed verification,
// or it was had otherwise. A piece buffer no check is using is given
// back.
func (t *Torrent) dropPartial(i int) {
	if p := t.partials[i]; p != nil && !p.busy {
		// Blocks may still be copied into it: the buffer is left to the
		// collector rather than pooled.
		t.c.freePieceBuf(t, p, false)
	}
	delete(t.partials, i)
	t.pieces[i].partial = nil
}

// dropBufferedLocked forgets the pieces under way whose blocks are in
// buffers, which a torrent whose storage closes would otherwise hold
// until it starts again; those being checked are dropped by their check,
// which finds no storage.
func (t *Torrent) dropBufferedLocked() {
	for i, p := range t.partials {
		if p.buf != nil && !p.busy {
			t.dropPartial(i)
		}
	}
}

// newRNG is the picker's source of randomness.
func newRNG() *rand.Rand {
	return rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), rand.Uint64()))
}
