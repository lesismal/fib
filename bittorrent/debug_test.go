package bittorrent

import "fmt"

// debugState describes the picker's and the peers' state, for tests.
func (t *Torrent) debugState() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := fmt.Sprintf("missing=%d endgame=%v checking=%v running=%v\n", t.missing, t.endgame, t.checking, t.running())
	for i, p := range t.partials {
		s += fmt.Sprintf(" partial %d done=%d/%d writing=%d busy=%v wait=%v prio=%d have=%v blocks=%v\n", i, p.done, len(p.blocks), p.writing, p.busy, p.waitHashes, t.pieces[i].prio, t.pieces[i].have, p.blocks)
	}
	for _, pc := range t.peers {
		n := 0
		if pc.bits != nil {
			n = pc.bits.Count()
		}
		s += fmt.Sprintf(" peer %v pending=%v choked=%v interested=%v snubbed=%v has=%d upq=%d\n", pc.addr, pc.pending, pc.c.PeerChoking(), pc.c.AmInterested(), pc.snubbed, n, len(pc.upq))
	}
	return s
}
