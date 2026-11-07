package bittorrent

import (
	"errors"
	"net/netip"
	"time"

	"github.com/lesismal/fib/bittorrent/peer"
)

// onExtended handles an extension message (BEP 10): the extended
// handshake, ut_metadata (BEP 9) and ut_pex (BEP 11).
func (t *Torrent) onExtended(pc *peerConn, m *peer.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if pc.closed {
		return
	}
	if m.ExtendedID == 0 {
		t.onExtHandshakeLocked(pc)
		return
	}
	switch pc.c.ExtensionName(m.ExtendedID) {
	case peer.ExtMetadata:
		mm, err := peer.ParseMetadataMessage(m.Data)
		if err != nil {
			go pc.c.CloseWithError(err)
			return
		}
		t.onMetadataLocked(pc, mm)
	case peer.ExtPex:
		if t.isPrivate() || t.c.cfg.DisablePEX {
			return
		}
		p, err := peer.ParsePex(m.Data)
		if err != nil || !pc.pex.Received(time.Now(), p) {
			return
		}
		var v1, v1utp []netip.AddrPort
		for _, a := range p.Added {
			if a.Flags&peer.PexUTP != 0 {
				v1utp = append(v1utp, a.Addr)
			} else {
				v1 = append(v1, a.Addr)
			}
		}
		t.addPeersLocked(SourcePEX, false, append(v1utp, v1...))
	}
}

func (t *Torrent) onExtHandshakeLocked(pc *peerConn) {
	h := pc.c.RemoteExtensions()
	if h == nil {
		return
	}
	if h.Port > 0 && h.Port < 65536 && !pc.listen.IsValid() {
		pc.listen = netip.AddrPortFrom(pc.addr.Addr(), uint16(h.Port))
		if t.c.banned(pc.addr, pc.listen) {
			go pc.c.Close()
			return
		}
		if cand := t.pool[pc.listen]; cand != nil {
			cand.connected = true
			cand.conn = pc
		}
	}
	if h.UploadOnly != 0 && t.info != nil && t.missing == 0 && t.haveCount == len(t.pieces) {
		// Two seeds: nothing to trade.
		go pc.c.Close()
		return
	}
	if t.info == nil {
		t.startMetadataLocked(pc, h)
	}
}

// startMetadataLocked begins fetching the info dictionary from pc, once
// its extended handshake says how large it is.
func (t *Torrent) startMetadataLocked(pc *peerConn, h *peer.ExtendedHandshake) {
	if h.ID(peer.ExtMetadata) == 0 || h.MetadataSize <= 0 {
		pc.noMetadata = true
		return
	}
	if t.meta == nil {
		a, err := peer.NewMetadataAssembler(h.MetadataSize, 0)
		if err != nil {
			pc.noMetadata = true
			return
		}
		t.meta = a
	} else if t.meta.Size() != h.MetadataSize {
		pc.noMetadata = true
		return
	}
	t.requestMetadataLocked(pc)
}

// requestMetadataLocked asks pc for the pieces of the info dictionary no
// other peer was asked for lately.
func (t *Torrent) requestMetadataLocked(pc *peerConn) {
	if t.meta == nil || pc.noMetadata || t.info != nil {
		return
	}
	now := time.Now()
	sent := 0
	for _, i := range t.meta.Missing(nil) {
		if at, ok := t.metaReqs[i]; ok && now.Sub(at) < metadataRetry {
			continue
		}
		b := peer.AppendMetadataMessage(nil, &peer.MetadataMessage{Type: peer.MetadataRequest, Piece: i})
		if err := pc.c.SendExtended(peer.ExtMetadata, b); err != nil {
			pc.noMetadata = true
			return
		}
		t.metaReqs[i] = now
		if sent++; sent >= 4 {
			return
		}
	}
}

func (t *Torrent) onMetadataLocked(pc *peerConn, mm *peer.MetadataMessage) {
	switch mm.Type {
	case peer.MetadataRequest:
		reply := &peer.MetadataMessage{Type: peer.MetadataReject, Piece: mm.Piece}
		if t.info != nil {
			if piece, ok := peer.MetadataPiece(t.infoBytes, mm.Piece); ok {
				reply = &peer.MetadataMessage{Type: peer.MetadataData, Piece: mm.Piece, TotalSize: len(t.infoBytes), Data: piece}
			}
		}
		_ = pc.c.SendExtended(peer.ExtMetadata, peer.AppendMetadataMessage(nil, reply))
	case peer.MetadataReject:
		delete(t.metaReqs, mm.Piece)
		pc.noMetadata = true
	case peer.MetadataData:
		if t.meta == nil || t.info != nil {
			return
		}
		delete(t.metaReqs, mm.Piece)
		if err := t.meta.Add(mm); err != nil {
			pc.noMetadata = true
			return
		}
		if !t.meta.Complete() {
			t.requestMetadataLocked(pc)
			return
		}
		info, err := t.meta.Info(t.ih, t.ih2)
		if err != nil {
			if errors.Is(err, peer.ErrMetadataHash) {
				// Some peer sent a bad piece; which one is not known, so
				// all of them are fetched again.
				clear(t.metaReqs)
			} else {
				t.meta = nil
			}
			return
		}
		t.meta = nil
		clear(t.metaReqs)
		if err := t.setInfoLocked(info); err != nil {
			return
		}
		// What the torrent added under one hash of a hybrid now also
		// answers to the other.
		t.c.server.RemoveTorrent(t.pt)
		t.pt = &peer.Torrent{InfoHash: t.ih, InfoHashV2: t.ih2, Handler: t}
		_ = t.c.server.AddTorrent(t.pt)
		for _, o := range t.peers {
			t.updateInterestLocked(o)
			if o.c.Extended() {
				_ = o.c.SendExtendedHandshake(t.extHandshakeLocked())
			}
		}
	}
}

// sendPexLocked sends pc the peers connected since the last message, and
// those gone (BEP 11), at most once a minute.
func (t *Torrent) sendPexLocked(pc *peerConn, now time.Time) {
	if t.isPrivate() || t.c.cfg.DisablePEX {
		return
	}
	if h := pc.c.RemoteExtensions(); h == nil || h.ID(peer.ExtPex) == 0 {
		return
	}
	current := make([]peer.PexPeer, 0, len(t.peers))
	for _, o := range t.peers {
		if o == pc || !o.listen.IsValid() {
			continue
		}
		var f peer.PexFlags
		if o.c.Obfuscated() {
			f |= peer.PexEncryption
		}
		if o.seed {
			f |= peer.PexSeed
		}
		if o.utp {
			f |= peer.PexUTP
		}
		if o.outgoing {
			f |= peer.PexReachable
		}
		current = append(current, peer.PexPeer{Addr: o.listen, Flags: f})
	}
	msg, ok := pc.pex.Next(now, current)
	if !ok {
		return
	}
	b, err := msg.Marshal()
	if err == nil {
		_ = pc.c.SendExtended(peer.ExtPex, b)
	}
}
