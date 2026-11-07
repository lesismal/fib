package bittorrent

import (
	"math/rand/v2"
	"net/netip"
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
	partial *partial
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
}

func newPartial(index int, size int64) *partial {
	n := int((size + blockSize - 1) / blockSize)
	return &partial{index: index, blocks: make([]blockState, n)}
}

func (p *partial) complete() bool { return p.done == len(p.blocks) }

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
// requested: from a piece already under way, so that pieces complete
// one after another rather than all at the end; else from a new piece,
// chosen by priority, then rarity (randomly for the first few, by index in
// sequential mode); else, in endgame mode, a block another peer has
// outstanding. With fastOnly, only the pieces pc allowed fast qualify.
// Called with t.mu held.
func (t *Torrent) nextRequest(pc *peerConn, fastOnly bool) (peer.Request, bool) {
	can := func(i int) bool {
		if !pc.has(i) || !t.wanted(i) {
			return false
		}
		return !fastOnly || pc.c.AllowedFast(uint32(i))
	}
	// A piece under way, of the highest priority among those.
	var best *partial
	for _, p := range t.partials {
		if p.waitHashes || p.complete() || !can(p.index) {
			continue
		}
		if best != nil && t.pieces[p.index].prio <= t.pieces[best.index].prio {
			continue
		}
		for b := range p.blocks {
			if bs := &p.blocks[b]; !bs.done && bs.reqs == 0 {
				best = p
				break
			}
		}
	}
	// A new piece, when it beats every piece under way.
	if i, ok := t.pickPiece(can); ok && (best == nil || t.pieces[i].prio > t.pieces[best.index].prio) {
		p := newPartial(i, t.pieces[i].dataLen)
		t.pieces[i].partial = p
		t.partials[i] = p
		best = p
	}
	if best != nil {
		for b := range best.blocks {
			if bs := &best.blocks[b]; !bs.done && bs.reqs == 0 {
				r := t.blockRequest(best.index, b)
				if _, dup := pc.pending[r]; dup {
					continue
				}
				bs.reqs++
				return r, true
			}
		}
	}
	// Endgame: every block this peer could send is requested from
	// another; ask this one too, and cancel the other when one arrives.
	var pick peer.Request
	found := false
	for _, p := range t.partials {
		if p.waitHashes || !can(p.index) {
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

// pickPiece chooses a piece not yet under way that can(i) allows: the
// highest priority first; among those the lowest index for urgent pieces
// and in sequential mode, a random one for the first few pieces, and
// else the rarest, ties broken at random.
func (t *Torrent) pickPiece(can func(int) bool) (int, bool) {
	best, bestPrio, bestAvail, ties := -1, 0, int32(0), 0
	random := t.haveCount < randomFirstPieces
	for i := range t.pieces {
		ps := &t.pieces[i]
		if ps.partial != nil || ps.prio < bestPrio || !can(i) {
			continue
		}
		if ps.prio > bestPrio {
			best, bestPrio, bestAvail, ties = i, ps.prio, ps.avail, 1
			continue
		}
		// Equal priority.
		switch {
		case ps.prio >= priorityUrgent || t.sequential:
			// The lowest index wins, and best has it already.
		case random:
			ties++
			if t.rng.IntN(ties) == 0 {
				best = i
			}
		case ps.avail < bestAvail:
			best, bestAvail, ties = i, ps.avail, 1
		case ps.avail == bestAvail:
			ties++
			if t.rng.IntN(ties) == 0 {
				best = i
			}
		}
	}
	return best, best >= 0
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
	}
}

// dropPartial forgets piece i's progress: its data failed verification.
func (t *Torrent) dropPartial(i int) {
	delete(t.partials, i)
	t.pieces[i].partial = nil
}

// newRNG is the picker's source of randomness.
func newRNG() *rand.Rand {
	return rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), rand.Uint64()))
}
