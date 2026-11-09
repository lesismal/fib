package bittorrent

import (
	"errors"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// AddOptions.Seed: the storage taken as complete, each piece checked when
// it is first served.

// ErrSeedResume is what AddTorrent and AddMagnet return for AddOptions
// with both Seed and Resume set.
var ErrSeedResume = errors.New("bittorrent: AddOptions.Seed and Resume both set")

// trustAllLocked takes every piece as had without reading storage, for
// AddOptions.Seed, in place of the check of the info dictionary's arrival.
func (t *Torrent) trustAllLocked() {
	for i := range t.pieces {
		t.markTrustedLocked(i)
	}
	// The check this takes the place of is over: what downloads from now
	// on announces completed (see updateDoneLocked).
	t.checked = true
	t.updateDoneLocked()
	t.c.emit(Event{Type: EventChecked, Torrent: t})
	for _, pc := range t.peers {
		t.updateInterestLocked(pc)
	}
}

// markTrustedLocked takes piece i as had, not checked, and tells the
// peers; the caller then calls updateDoneLocked.
func (t *Torrent) markTrustedLocked(i int) {
	ps := &t.pieces[i]
	if ps.have {
		return
	}
	ps.have, ps.trusted = true, true
	t.have.Set(i)
	t.haveCount++
	t.trusted++
	if ps.prio > PriorityNone {
		t.missing--
	}
	if ps.partial != nil {
		t.dropPartial(i)
	}
	for _, pc := range t.peers {
		_ = pc.c.Send(&peer.Message{Type: peer.MsgHave, Index: uint32(i)})
	}
	close(t.pieceSig)
	t.pieceSig = make(chan struct{})
}

// checkTrusted checks piece i, if it is trusted, before a block of it is
// served, and reports whether it may be: had, and checked now or before.
// It runs on the disk pool. A check of the piece already under way, on
// another worker, is waited for rather than made twice.
func (t *Torrent) checkTrusted(i int) bool {
	t.mu.Lock()
	for {
		ps := &t.pieces[i]
		if !ps.trusted {
			ok := ps.have
			t.mu.Unlock()
			return ok
		}
		ch, busy := t.trustChecks[i]
		if !busy {
			break
		}
		t.mu.Unlock()
		<-ch
		t.mu.Lock()
	}
	ch := make(chan struct{})
	t.trustChecks[i] = ch
	layers := t.pieceLayersFor(i)
	t.mu.Unlock()

	err := errNoStorage
	st, info, done := t.useStorage()
	var data []byte
	if st != nil {
		data = make([]byte, info.PieceSize(i))
		err = readData(st, info, info.PieceOffset(i), data)
	}
	done()
	if err == nil {
		err = info.VerifyPiece(i, data, layers)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.trustChecks, i)
	close(ch)
	ps := &t.pieces[i]
	if !ps.trusted {
		return ps.have
	}
	switch {
	case err == nil:
		ps.trusted = false
		t.trusted--
		t.resumeGen++
		t.hashedData = true
		t.c.emit(Event{Type: EventPieceVerified, Torrent: t, Piece: i})
		if t.trusted == 0 {
			// Layers wait for every piece to be checked.
			t.computeLayersLocked(false)
		}
		return true
	case errors.Is(err, errNoStorage):
		// Stopped or removed: checked once it serves again.
		return false
	case errors.Is(err, metainfo.ErrNoPieceLayer):
		// A v2 torrent from a magnet link without the layer yet: peers
		// are asked for it, and the piece is not served until then.
		t.requestHashesLocked()
		return false
	}
	// A failed read is a piece missing as much as bad data is.
	ps.trusted = false
	t.trusted--
	ps.have = false
	t.have.Clear(i)
	t.haveCount--
	t.resumeGen++
	if ps.prio > PriorityNone {
		t.missing++
	}
	t.hashFails++
	t.c.emit(Event{Type: EventHashFailed, Torrent: t, Piece: i, Err: err})
	t.updateDoneLocked()
	return false
}
