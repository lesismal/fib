package bittorrent

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net/netip"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// readData reads the torrent's bytes at off into p: the files' from
// storage, zeros for padding files and for the gaps between a v2
// torrent's files. A short read is an error.
func readData(st Storage, info *metainfo.Info, off int64, p []byte) error {
	clear(p)
	files := info.FileList()
	for _, seg := range info.Segments(off, int64(len(p))) {
		if files[seg.File].IsPad() {
			continue
		}
		start := files[seg.File].Offset + seg.Offset - off
		n, err := st.ReadAt(seg.File, p[start:start+seg.Length], seg.Offset)
		if int64(n) < seg.Length {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return nil
}

// writeData writes p, the torrent's bytes at off, to the files it covers;
// what falls in padding files is not written, as their bytes are zeros
// by definition.
func writeData(st Storage, info *metainfo.Info, off int64, p []byte) error {
	files := info.FileList()
	for _, seg := range info.Segments(off, int64(len(p))) {
		if files[seg.File].IsPad() {
			continue
		}
		start := files[seg.File].Offset + seg.Offset - off
		if _, err := st.WriteAt(seg.File, p[start:start+seg.Length], seg.Offset); err != nil {
			return fmt.Errorf("bittorrent: writing %s: %w", info.Name, err)
		}
	}
	return nil
}

// pieceLayersFor returns the layer a v2 check of piece i needs, the only
// one of t.layers it reads, so that the check runs without t.mu.
func (t *Torrent) pieceLayersFor(i int) metainfo.PieceLayers {
	if !t.info.HasV2() || len(t.layers) == 0 {
		return nil
	}
	off := t.info.PieceOffset(i)
	for _, f := range t.info.FileList() {
		if f.Length > 0 && !f.IsPad() && f.Offset <= off && off < f.Offset+f.Length {
			if l, ok := t.layers[f.PiecesRoot]; ok {
				return metainfo.PieceLayers{f.PiecesRoot: l}
			}
			return nil
		}
	}
	return nil
}

// verifyPiece checks piece i, whose blocks p holds are all in storage.
func (t *Torrent) verifyPiece(i int, p *partial) {
	t.mu.Lock()
	st, info := t.storage, t.info
	layers := t.pieceLayersFor(i)
	t.mu.Unlock()
	if st == nil {
		t.mu.Lock()
		p.busy = false
		t.mu.Unlock()
		return
	}
	data := make([]byte, info.PieceSize(i))
	err := readData(st, info, info.PieceOffset(i), data)
	if err == nil {
		err = info.VerifyPiece(i, data, layers)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	p.busy = false
	if t.partials[i] != p || t.pieces[i].have {
		return
	}
	switch {
	case err == nil:
		t.smartBanGoodLocked(i, data)
		t.markHaveLocked(i)
	case errors.Is(err, metainfo.ErrNoPieceLayer):
		// A v2 torrent from a magnet link: the piece waits for its file's
		// piece layer, which peers are asked for.
		p.waitHashes = true
		t.requestHashesLocked()
	default:
		t.hashFails++
		t.wasted += int64(len(data))
		t.smartBanBadLocked(i, p, data)
		t.dropPartial(i)
		t.c.emit(Event{Type: EventHashFailed, Torrent: t, Piece: i})
		for _, pc := range t.peers {
			t.fillRequestsLocked(pc)
		}
	}
}

// markHaveLocked records piece i had and checked, and tells the peers.
func (t *Torrent) markHaveLocked(i int) {
	ps := &t.pieces[i]
	if ps.have {
		return
	}
	ps.have = true
	t.have.Set(i)
	t.haveCount++
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
	t.c.emit(Event{Type: EventPieceVerified, Torrent: t, Piece: i})
	t.updateDoneLocked()
}

// blockKey is a block of a piece, by index.
type blockKey struct{ piece, block int }

// blockRecord is what a peer sent for a block of a piece that failed its
// check: its SHA-1, to compare with the block once the piece passes.
type blockRecord struct {
	from netip.AddrPort
	sum  [20]byte
}

// smartBanBadLocked handles a piece that failed its check: when one peer
// sent all of it, that peer sent bad data and is banned; otherwise what
// each sent is remembered, and once the piece passes, whoever sent a
// block other than the good one is banned (libtorrent's smart ban).
func (t *Torrent) smartBanBadLocked(i int, p *partial, data []byte) {
	var only netip.AddrPort
	one := true
	for b, bs := range p.blocks {
		if b == 0 {
			only = bs.from
		} else if bs.from != only {
			one = false
		}
	}
	if one && only.IsValid() {
		t.c.ban(only, fmt.Errorf("sent piece %d, which failed its check", i))
		return
	}
	for b, bs := range p.blocks {
		if !bs.from.IsValid() {
			continue
		}
		start := b * blockSize
		end := min(start+blockSize, len(data))
		k := blockKey{i, b}
		recs := t.banRecords[k]
		if len(recs) < 8 {
			t.banRecords[k] = append(recs, blockRecord{from: bs.from, sum: sha1.Sum(data[start:end])})
		}
	}
}

// smartBanGoodLocked compares a piece that passed with what peers sent of
// it when it failed before.
func (t *Torrent) smartBanGoodLocked(i int, data []byte) {
	nb := (len(data) + blockSize - 1) / blockSize
	for b := range nb {
		k := blockKey{i, b}
		recs, ok := t.banRecords[k]
		if !ok {
			continue
		}
		delete(t.banRecords, k)
		start := b * blockSize
		good := sha1.Sum(data[start:min(start+blockSize, len(data))])
		for _, r := range recs {
			if r.sum != good {
				t.c.ban(r.from, fmt.Errorf("sent a bad block of piece %d", i))
			}
		}
	}
}

// startCheckLocked checks what the storage already holds, on a goroutine
// of its own: the resume of an interrupted download, or a seed's data.
func (t *Torrent) startCheckLocked() {
	t.checking = true
	prev := t.state
	if prev == StateMetadata || prev == StateDownloading || prev == StateSeeding {
		t.state = StateChecking
	}
	st, info := t.storage, t.info
	go t.check(st, info)
}

func (t *Torrent) check(st Storage, info *metainfo.Info) {
	n := info.PieceCount()
	buf := make([]byte, info.PieceLength)
	for i := range n {
		select {
		case <-t.removed:
			return
		default:
		}
		data := buf[:info.PieceSize(i)]
		if err := readData(st, info, info.PieceOffset(i), data); err != nil {
			continue
		}
		t.mu.Lock()
		layers := t.pieceLayersFor(i)
		t.mu.Unlock()
		err := info.VerifyPiece(i, data, layers)
		t.mu.Lock()
		switch {
		case err == nil:
			t.markHaveLocked(i)
		case errors.Is(err, metainfo.ErrNoPieceLayer) && t.partials[i] == nil && !t.pieces[i].have:
			// The data may be good: check it once the layer is known.
			p := newPartial(i, info.PieceSize(i))
			for b := range p.blocks {
				p.blocks[b].done = true
			}
			p.done = len(p.blocks)
			p.waitHashes = true
			t.pieces[i].partial = p
			t.partials[i] = p
		}
		t.mu.Unlock()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checking = false
	if t.state == StateChecking {
		t.state = StateDownloading
	}
	t.updateDoneLocked()
	t.c.emit(Event{Type: EventChecked, Torrent: t})
	for _, pc := range t.peers {
		t.updateInterestLocked(pc)
		t.fillRequestsLocked(pc)
	}
	if len(t.partials) > 0 {
		t.requestHashesLocked()
	}
}
