package bittorrent

import (
	"errors"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Torrent.Recheck: the data in storage checked again while the torrent
// runs.

// ErrRecheckInterrupted is the Err of the EventChecked that ends a
// Recheck its storage's closing (Stop, Remove, Client.Close) cut short.
var ErrRecheckInterrupted = errors.New("bittorrent: recheck interrupted: storage closed")

// Recheck checks the data in the torrent's storage again, piece by piece,
// after its files were changed, replaced or restored behind the client's
// back (qBittorrent's "force recheck"). The torrent keeps its connections,
// priorities, limits and counters; it is in StateChecking until the check
// ends with an EventChecked, then downloading or seeding as its pieces
// say, or paused or stopped again if it was.
//
// Each piece had becomes one not checked yet, as with AddOptions.Seed, and
// the check hashes them in order on the disk pool, one at a time: one
// that fails, or cannot be read, is no longer had (an EventHashFailed,
// counted in Stats.HashFails) and is downloaded again once the check
// ends; a piece not had whose data passes becomes had, and the peers are
// told with a have message. Peers keep the pieces they were told of
// before, as the protocol has no way to take a have back: a request for a
// block of a piece the check has not reached yet checks that piece first,
// on the disk pool (as in seed mode), and one for a piece lost is
// rejected (or, without the Fast extension, not answered). No piece is
// requested while the check runs; blocks already asked for are still
// taken. Pieces AddOptions.Seed took as had are checked like the rest.
//
// A stopped torrent's storage is opened for the check and closed after
// it. ResumeData errs while the check runs; ResumeGen moves with each
// piece it changes. Stop or Remove during the check ends it (an
// EventChecked whose Err is ErrRecheckInterrupted), the pieces it had not
// reached staying unchecked, to be checked when first served, and listed
// so in resume data. A Recheck while a check runs, the first one included,
// checks again once that one ends, however many times it was called.
//
// It is an error before the info dictionary is known, and on a torrent
// removed or in StateError (Start it first).
func (t *Torrent) Recheck() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.state == StateRemoved:
		return errors.New("bittorrent: recheck of a removed torrent")
	case t.state == StateError:
		return errors.New("bittorrent: recheck of a torrent in StateError")
	case t.info == nil:
		return errors.New("bittorrent: no data to check before the info dictionary")
	}
	if t.checking {
		if !t.recheckAgain && !t.checked {
			// The first check: once it is over.
			go func() {
				select {
				case <-t.checkDone:
				case <-t.removed:
					return
				}
				t.mu.Lock()
				defer t.mu.Unlock()
				if t.recheckAgain && !t.checking {
					t.recheckAgain = false
					_ = t.startRecheckLocked()
				}
			}()
		}
		t.recheckAgain = true
		return nil
	}
	return t.startRecheckLocked()
}

// startRecheckLocked starts a Recheck: every piece had taken as not
// checked, and the check on a goroutine of its own that runs each piece's
// on the disk pool.
func (t *Torrent) startRecheckLocked() error {
	if t.state == StateRemoved || t.state == StateError {
		return nil
	}
	closeAfter := false
	if t.storage == nil {
		st, err := t.opener(t.info)
		if err != nil {
			return err
		}
		t.storage = st
		closeAfter = true
	}
	t.checking = true
	t.recheckPrev = t.state
	t.recheckClose = closeAfter
	t.state = StateChecking
	t.trustHadLocked()
	go t.recheck(t.info)
	return nil
}

// trustHadLocked takes every piece had as not checked yet, for a recheck
// to check: until it does, a block of it is served only once the piece
// passes (see checkTrusted).
func (t *Torrent) trustHadLocked() {
	for i := range t.pieces {
		ps := &t.pieces[i]
		if ps.have && !ps.trusted {
			ps.trusted = true
			t.trusted++
		}
	}
}

// endRecheck ends a recheck: the torrent goes back to what it was doing
// and requests what it lacks now; or, Recheck having been called
// meanwhile, it checks again from the first piece.
func (t *Torrent) endRecheck(interrupted bool) {
	t.mu.Lock()
	again := t.recheckAgain && !interrupted
	t.recheckAgain = false
	if again && t.state == StateChecking {
		t.trustHadLocked()
		go t.recheck(t.info)
		t.mu.Unlock()
		return
	}
	t.checking = false
	var closeSt Storage
	if t.state == StateChecking {
		switch {
		case t.paused:
			t.state = t.recheckPrev
			if t.state == StateStopped && t.recheckClose {
				closeSt = t.storage
				t.storage = nil
			}
		default:
			t.state = StateDownloading
			select {
			case <-t.done:
				t.state = StateSeeding
			default:
			}
		}
	}
	t.recheckClose = false
	t.updateDoneLocked()
	var err error
	if interrupted {
		err = ErrRecheckInterrupted
	}
	t.c.emit(Event{Type: EventChecked, Torrent: t, Err: err})
	for _, pc := range t.peers {
		t.updateInterestLocked(pc)
		t.fillRequestsLocked(pc)
	}
	if len(t.partials) > 0 {
		t.requestHashesLocked()
	}
	t.computeLayersLocked(false)
	if again {
		// Paused during the check: the torrent stays so.
		_ = t.startRecheckLocked()
	}
	t.mu.Unlock()
	if closeSt != nil {
		_ = t.closeStorage(closeSt, false)
	}
}

// recheck runs a Recheck: each piece's check queued on the disk pool in
// turn, then its end.
func (t *Torrent) recheck(info *metainfo.Info) {
	buf := make([]byte, info.PieceLength)
	done := make(chan bool, 1)
	interrupted := false
	for i := range info.PieceCount() {
		select {
		case <-t.removed:
			return
		default:
		}
		t.c.disk.run(func() { done <- t.recheckPiece(info, i, buf) })
		if !<-done {
			interrupted = true
			break
		}
	}
	if !interrupted {
		t.mu.Lock()
		empty := t.emptyFilesLocked()
		t.mu.Unlock()
		if len(empty) > 0 {
			t.createFiles(empty)
		}
	}
	t.endRecheck(interrupted)
}

// recheckPiece checks piece i again, on the disk pool, and reports false
// once the storage is closed, which ends the recheck.
func (t *Torrent) recheckPiece(info *metainfo.Info, i int, buf []byte) bool {
	t.mu.Lock()
	ps := t.pieces[i]
	t.mu.Unlock()
	if ps.trusted {
		t.checkTrusted(i)
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.storage != nil
	}
	if ps.have {
		// Had and checked since the recheck began: downloaded.
		return true
	}
	data := buf[:info.PieceSize(i)]
	st, _, done := t.useStorage()
	if st == nil {
		done()
		return false
	}
	err := readData(st, info, info.PieceOffset(i), data)
	done()
	if err != nil {
		return true
	}
	t.mu.Lock()
	layers := t.pieceLayersFor(i)
	t.mu.Unlock()
	err = info.VerifyPiece(i, data, layers)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pieces[i].have {
		return true
	}
	switch {
	case err == nil:
		t.hashedData = true
		t.markHaveLocked(i)
	case errors.Is(err, metainfo.ErrNoPieceLayer) && t.partials[i] == nil:
		// As in check: the data may be good, once the layer is known.
		p := newPartial(i, t.pieces[i].dataLen)
		for b := range p.blocks {
			p.blocks[b].done = true
		}
		p.done = len(p.blocks)
		p.waitHashes = true
		t.pieces[i].partial = p
		t.partials[i] = p
	}
	return true
}
