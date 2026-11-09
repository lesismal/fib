package bittorrent

import (
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
	"github.com/lesismal/fib/bittorrent/peer/mse"
	"github.com/lesismal/fib/bufferpool"
)

// Limits of a peer connection.
const (
	// allowedFastCount is how many pieces each peer is allowed fast
	// (BEP 6 suggests 10).
	allowedFastCount = 10
	// maxUploadQueue bounds the requests queued from one peer; past it
	// they are rejected.
	maxUploadQueue = 256
	// snubTimeout is how long a peer may keep requests outstanding
	// without sending a block before it is snubbed: its requests go to
	// others, and it gets one at a time.
	snubTimeout = 60 * time.Second
	// metadataRetry is how long a ut_metadata request waits before it is
	// sent to another peer.
	metadataRetry = 10 * time.Second
)

// peerConn is the torrent's side of a connection: what the peer has, what
// was requested of it and what it asked for. Guarded by the torrent's
// mutex.
type peerConn struct {
	t        *Torrent
	c        *peer.Conn
	addr     netip.AddrPort // the remote address
	listen   netip.AddrPort // where it accepts connections, when known
	utp      bool
	outgoing bool
	since    time.Time

	bits     *peer.Bitfield
	rawBits  []byte // a bitfield that came before the info dictionary
	haveAll  bool
	haveNone bool
	haves    []uint32
	seed     bool

	pending map[peer.Request]time.Time
	// owned are the pieces the peer reserved (partial.owner), and prioGen
	// the Torrent.prioGen it last picked a piece at.
	owned     []*partial
	prioGen   uint64
	lastBlock time.Time
	snubbed   bool
	upq       []peer.Request
	pumping   bool            // an upload job of its is queued or running
	hashing   int             // its hash requests being answered on the disk pool
	fastSet   map[uint32]bool // pieces this side allowed fast

	down, up          rate
	downTotal         int64
	upTotal           int64
	lastUnchoke       time.Time
	pex               peer.PexState
	noMetadata        bool
	closed            bool
	contributedBlocks int
}

func (pc *peerConn) has(i int) bool { return pc.bits != nil && pc.bits.Has(i) }

// applyInfoLocked turns what the peer said it has before the info
// dictionary was known into a Bitfield of n pieces.
func (pc *peerConn) applyInfoLocked(n int) {
	t := pc.t
	switch {
	case pc.haveAll:
		pc.bits = peer.NewBitfield(n)
		pc.bits.SetAll()
	case pc.rawBits != nil:
		bf, err := peer.ParseBitfield(pc.rawBits, n)
		if err != nil {
			go pc.c.CloseWithError(err)
			return
		}
		pc.bits = bf
	default:
		pc.bits = peer.NewBitfield(n)
	}
	for _, i := range pc.haves {
		pc.bits.Set(int(i))
	}
	pc.rawBits, pc.haves = nil, nil
	for i := range n {
		if pc.bits.Has(i) {
			t.addAvailLocked(i, 1)
		}
	}
	pc.seed = pc.bits.All()
}

// remoteAddrPort is a net.Addr's address and port.
func remoteAddrPort(a net.Addr) netip.AddrPort {
	switch a := a.(type) {
	case *net.TCPAddr:
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	case *net.UDPAddr:
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	case nil:
		return netip.AddrPort{}
	}
	ap, _ := netip.ParseAddrPort(a.String())
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

func isUTPAddr(a net.Addr) bool {
	_, ok := a.(*net.UDPAddr)
	return ok
}

// OnHandshake implements peer.Handler: the connection joins the torrent,
// unless a limit, a ban, or another connection to the same peer says no.
func (t *Torrent) OnHandshake(c *peer.Conn) {
	cl := t.c
	addr := remoteAddrPort(c.RemoteAddr())
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.dials[c]
	if rec != nil {
		delete(t.dials, c)
		t.dialDoneLocked(rec)
	}
	reject := func() {
		if rec != nil {
			// Not again for a while: the reason is likely to stand.
			rec.cand.nextTry = time.Now().Add(time.Minute)
		}
		go c.Close()
	}
	if !t.running() || cl.closing.Load() {
		reject()
		return
	}
	if c.PeerID() == cl.peerID {
		// Ourselves, through an address of ours a tracker handed out.
		if rec != nil {
			rec.cand.self = true
		}
		reject()
		return
	}
	listen := netip.AddrPort{}
	if rec != nil {
		listen = rec.cand.addr
	}
	if cl.banned(addr, listen) || cl.refuses(addr) {
		reject()
		return
	}
	var replaced *peerConn
	for _, o := range t.peers {
		if o.closed || o.c.PeerID() != c.PeerID() {
			continue
		}
		// Two connections to one peer, as when both dial at once: each
		// side keeps the one the side of the lower peer ID began, so that
		// they keep the same one rather than each closing the other's.
		initiator := func(x *peer.Conn) string {
			if x.Initiator() {
				return string(cl.peerID[:])
			}
			id := x.PeerID()
			return string(id[:])
		}
		if c.Initiator() == o.c.Initiator() || initiator(o.c) < initiator(c) {
			if rec != nil {
				if !o.listen.IsValid() {
					o.listen = rec.cand.addr
				}
				rec.cand.connected = true
				rec.cand.conn = o
			}
			go c.Close()
			return
		}
		replaced = o
		break
	}
	if replaced != nil {
		if !listen.IsValid() {
			listen = replaced.listen
		}
		if cand := t.pool[replaced.listen]; cand != nil && cand.conn == replaced {
			cand.conn = nil
			rec = &dialAttempt{cand: cand}
		}
		go replaced.c.Close()
	}
	if rec == nil && replaced == nil && (len(t.peers) >= t.maxConnsLocked() || int(cl.conns.Load()) >= cl.lim().MaxConns) {
		reject()
		return
	}
	pc := &peerConn{
		t: t, c: c, addr: addr, listen: listen, outgoing: c.Initiator(),
		utp: isUTPAddr(c.RemoteAddr()), since: time.Now(),
		pending: map[peer.Request]time.Time{}, lastBlock: time.Now(),
	}
	if rec != nil {
		rec.cand.connected = true
		rec.cand.conn = pc
	}
	t.peers[c] = pc
	c.SetAttachment(pc)
	cl.conns.Add(1)
	if t.info != nil {
		pc.bits = peer.NewBitfield(len(t.pieces))
	}
	c.Batch(func() {
		if c.Extended() {
			_ = c.SendExtendedHandshake(t.extHandshakeLocked())
		}
		t.sendHavesLocked(pc)
		if cl.dht4 != nil && c.RemoteReserved().DHT() {
			_ = c.Send(&peer.Message{Type: peer.MsgPort, Port: uint16(cl.port)})
		}
	})
	info := pc.infoLocked()
	t.c.emit(Event{Type: EventPeerConnected, Torrent: t, Peer: addr, PeerInfo: &info})
}

// extHandshakeLocked is this side's extended handshake (BEP 10).
func (t *Torrent) extHandshakeLocked() *peer.ExtendedHandshake {
	h := &peer.ExtendedHandshake{Port: t.c.port}
	if t.info != nil {
		h.MetadataSize = len(t.infoBytes)
		if t.missing == 0 {
			h.UploadOnly = 1
		}
	}
	if t.c.cfg.Encryption != EncryptionAllow {
		h.Encryption = 1
	}
	if t.isPrivate() || t.c.cfg.DisablePEX {
		// No ut_pex on a private torrent (BEP 27).
		m := map[string]int{}
		for k, v := range peer.DefaultExtensions {
			m[k] = int(v)
		}
		m[peer.ExtPex] = 0
		h.M = m
	}
	return h
}

func (t *Torrent) isPrivate() bool { return t.info != nil && t.info.Private }

// sendHavesLocked tells a new peer what this side has: have all or have
// none under the Fast extension, a bitfield otherwise; and the pieces it
// may request while choked.
func (t *Torrent) sendHavesLocked(pc *peerConn) {
	c := pc.c
	fast := c.Fast()
	switch {
	case t.info == nil || t.haveCount == 0:
		if fast {
			_ = c.Send(&peer.Message{Type: peer.MsgHaveNone})
		}
	case t.haveCount == len(t.pieces) && fast:
		_ = c.Send(&peer.Message{Type: peer.MsgHaveAll})
	default:
		_ = c.Send(&peer.Message{Type: peer.MsgBitfield, Data: t.have.Bytes()})
	}
	if fast && t.info != nil && t.haveCount > 0 {
		set := peer.AllowedFastSet(pc.addr.Addr(), [20]byte(t.ih), len(t.pieces), allowedFastCount)
		if t.ih.IsZero() {
			set = peer.AllowedFastSet(pc.addr.Addr(), [20]byte(t.ih2.Truncate()), len(t.pieces), allowedFastCount)
		}
		for _, i := range set {
			if t.pieces[i].have {
				if pc.fastSet == nil {
					pc.fastSet = map[uint32]bool{}
				}
				pc.fastSet[i] = true
				_ = c.Send(&peer.Message{Type: peer.MsgAllowedFast, Index: i})
			}
		}
	}
}

// OnClose implements peer.Handler.
func (t *Torrent) OnClose(c *peer.Conn, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if rec := t.dials[c]; rec != nil {
		delete(t.dials, c)
		t.dialFailedLocked(rec, c.LocalAddr() != nil, err)
		return
	}
	pc := t.peers[c]
	if pc == nil {
		if c.Initiator() {
			// The dialer has not recorded c yet: it will find this.
			t.early[c] = earlyClose{err, time.Now()}
		}
		return
	}
	pc.closed = true
	delete(t.peers, c)
	t.c.conns.Add(-1)
	for r := range pc.pending {
		t.releaseRequest(r)
	}
	pc.pending = nil
	t.releaseOwnedLocked(pc)
	if pc.bits != nil && t.info != nil {
		for i := range t.pieces {
			if pc.bits.Has(i) {
				t.addAvailLocked(i, -1)
			}
		}
	}
	if t.optimistic == pc {
		t.optimistic = nil
	}
	for k, r := range t.hashReqs {
		if r.pc == pc {
			delete(t.hashReqs, k)
		}
	}
	if cand := t.pool[pc.listen]; cand != nil && cand.conn == pc {
		cand.connected = false
		cand.conn = nil
		cand.seed = pc.seed
		// A connection that lasted is worth another soon; one that keeps
		// ending early waits longer each time.
		now := time.Now()
		if now.Sub(pc.since) > time.Minute {
			cand.fails = 0
		} else {
			cand.fails++
		}
		cand.nextTry = now.Add(2 * time.Second << min(cand.fails, 9))
	}
	info := pc.infoLocked()
	t.c.emit(Event{Type: EventPeerDisconnected, Torrent: t, Peer: pc.addr, Err: err, PeerInfo: &info})
}

// OnWritable implements peer.WritableHandler: the uploads held back by a
// congested socket go on.
func (t *Torrent) OnWritable(c *peer.Conn) {
	if pc, ok := c.Attachment().(*peerConn); ok {
		t.pump(pc)
	}
}

// OnMessage implements peer.Handler.
func (t *Torrent) OnMessage(c *peer.Conn, m *peer.Message) {
	pc, ok := c.Attachment().(*peerConn)
	if !ok || m.Keepalive {
		return
	}
	switch m.Type {
	case peer.MsgPiece:
		t.onPiece(pc, m)
		return
	case peer.MsgRequest:
		t.onRequest(pc, m.Request())
		return
	case peer.MsgExtended:
		t.onExtended(pc, m)
		return
	case peer.MsgPort:
		t.c.addDHTNode(netip.AddrPortFrom(pc.addr.Addr(), m.Port))
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if pc.closed {
		return
	}
	switch m.Type {
	case peer.MsgChoke:
		// Its pieces go to the peers that may send them.
		t.releaseOwnedLocked(pc)
		if !c.Fast() {
			// The peer dropped every request; with the Fast extension it
			// rejects each instead.
			for r := range pc.pending {
				t.releaseRequest(r)
			}
			clear(pc.pending)
		}
	case peer.MsgUnchoke, peer.MsgAllowedFast:
		t.fillRequestsLocked(pc)
	case peer.MsgInterested:
		t.maybeUnchokeLocked(pc)
	case peer.MsgNotInterested:
	case peer.MsgHave:
		t.onHaveLocked(pc, int(m.Index))
	case peer.MsgBitfield:
		t.onBitfieldLocked(pc, m.Data)
	case peer.MsgHaveAll:
		pc.haveAll = true
		pc.seed = true
		if t.info != nil {
			old := pc.bits
			pc.bits = peer.NewBitfield(len(t.pieces))
			pc.bits.SetAll()
			for i := range t.pieces {
				if old == nil || !old.Has(i) {
					t.addAvailLocked(i, 1)
				}
			}
			t.updateInterestLocked(pc)
			t.fillRequestsLocked(pc)
		}
		t.closeIfBothSeedsLocked(pc)
	case peer.MsgHaveNone:
		pc.haveNone = true
	case peer.MsgCancel:
		r := m.Request()
		for i, q := range pc.upq {
			if q == r {
				pc.upq = append(pc.upq[:i], pc.upq[i+1:]...)
				// BEP 6: a cancelled request is answered all the same,
				// with the piece or a reject, so the peer can count it
				// gone.
				_ = c.Reject(r)
				break
			}
		}
	case peer.MsgRejectRequest:
		r := m.Request()
		if _, ok := pc.pending[r]; ok {
			delete(pc.pending, r)
			t.releaseRequest(r)
		}
		if !c.PeerChoking() {
			// Rejected while unchoked: the peer will not serve it; try
			// another block later rather than at once, in a loop.
			return
		}
	case peer.MsgHashRequest:
		t.onHashRequestLocked(pc, m)
	case peer.MsgHashes:
		t.onHashesLocked(pc, m)
	case peer.MsgHashReject:
		t.onHashRejectLocked(pc, m)
	}
}

func (t *Torrent) onHaveLocked(pc *peerConn, i int) {
	if t.info == nil {
		pc.haves = append(pc.haves, uint32(i))
		return
	}
	if i < 0 || i >= len(t.pieces) {
		go pc.c.CloseWithError(peer.ErrProtocol)
		return
	}
	if pc.bits.Has(i) {
		return
	}
	pc.bits.Set(i)
	t.addAvailLocked(i, 1)
	if pc.bits.All() {
		pc.seed = true
		t.closeIfBothSeedsLocked(pc)
	}
	if t.wanted(i) {
		if !pc.c.AmInterested() {
			_ = pc.c.Send(&peer.Message{Type: peer.MsgInterested})
		}
		t.fillRequestsLocked(pc)
	}
}

func (t *Torrent) onBitfieldLocked(pc *peerConn, b []byte) {
	if t.info == nil {
		pc.rawBits = append([]byte(nil), b...)
		return
	}
	bf, err := peer.ParseBitfield(b, len(t.pieces))
	if err != nil {
		go pc.c.CloseWithError(err)
		return
	}
	for i := range t.pieces {
		if bf.Has(i) && !pc.bits.Has(i) {
			t.addAvailLocked(i, 1)
		}
	}
	pc.bits = bf
	pc.seed = bf.All()
	t.updateInterestLocked(pc)
	t.fillRequestsLocked(pc)
	t.closeIfBothSeedsLocked(pc)
}

// closeIfBothSeedsLocked drops a connection between two full seeds,
// which have nothing to trade.
func (t *Torrent) closeIfBothSeedsLocked(pc *peerConn) {
	if pc.seed && t.info != nil && t.haveCount == len(t.pieces) {
		go pc.c.Close()
	}
}

// updateInterestLocked tells the peer whether this side wants any of its
// pieces.
func (t *Torrent) updateInterestLocked(pc *peerConn) {
	want := false
	if t.info != nil && t.missing > 0 && pc.bits != nil {
		for i := range t.pieces {
			if pc.bits.Has(i) && t.wanted(i) {
				want = true
				break
			}
		}
	}
	if want != pc.c.AmInterested() {
		typ := peer.MsgNotInterested
		if want {
			typ = peer.MsgInterested
		}
		_ = pc.c.Send(&peer.Message{Type: typ})
	}
}

// fillRequestsLocked keeps pc's request pipeline full: as many blocks
// outstanding as the peer's reqq and this side's limit allow, one for a
// snubbed peer, only allowed-fast pieces while choked.
func (t *Torrent) fillRequestsLocked(pc *peerConn) {
	if t.info == nil || t.checking || t.storage == nil || !t.running() || pc.bits == nil || pc.closed || t.missing == 0 {
		return
	}
	c := pc.c
	choked := c.PeerChoking()
	if choked && !c.Fast() {
		return
	}
	limit := c.RequestLimit()
	if pc.snubbed {
		limit = 1
	}
	now := time.Now()
	for len(pc.pending) < limit && t.downAllow() {
		r, ok := t.nextRequest(pc, choked)
		if !ok {
			return
		}
		if err := c.Request(r); err != nil {
			t.releaseRequest(r)
			return
		}
		// The download rates are spent as blocks are asked for, so that
		// they bound what peers send rather than what this side reads.
		t.downTake(int(r.Length))
		if len(pc.pending) == 0 {
			// The wait for a block starts with the first request.
			pc.lastBlock = now
		}
		pc.pending[r] = now
	}
}

// refillLocked tops pc's request pipeline up once at most half of it is
// outstanding, rather than for each block that arrives: the requests go
// out together, and the picker runs once for the lot.
func (t *Torrent) refillLocked(pc *peerConn) {
	if len(pc.pending) > pc.c.RequestLimit()/2 {
		return
	}
	t.fillRequestsLocked(pc)
}

// onPiece takes a block: into its piece's buffer, or to storage, and a
// piece whose blocks are all there is checked.
func (t *Torrent) onPiece(pc *peerConn, m *peer.Message) {
	r := m.Request()
	n := len(m.Data)
	t.downloaded.Add(int64(n))
	t.mu.Lock()
	if pc.closed {
		t.mu.Unlock()
		return
	}
	pc.down.add(n)
	t.downRate.add(n)
	pc.downTotal += int64(n)
	if _, ok := pc.pending[r]; ok {
		delete(pc.pending, r)
		t.releaseRequest(r)
	}
	pc.lastBlock = time.Now()
	pc.snubbed = false
	i := int(r.Index)
	var p *partial
	var b int
	if t.info != nil && i < len(t.pieces) && r.Begin%blockSize == 0 {
		p = t.partials[i]
		b = int(r.Begin / blockSize)
		if p != nil && (b >= len(p.blocks) || t.blockRequest(i, b) != r || p.blocks[b].done || p.busy) {
			p = nil
		}
	}
	if p == nil || t.storage == nil {
		t.wasted += int64(n)
		t.refillLocked(pc)
		t.mu.Unlock()
		return
	}
	bs := &p.blocks[b]
	bs.done = true
	bs.from = banAddr(pc)
	p.done++
	pc.contributedBlocks++
	if t.endgame {
		// Whoever else was asked for the block need not send it.
		for _, o := range t.peers {
			if o == pc {
				continue
			}
			if _, ok := o.pending[r]; ok {
				delete(o.pending, r)
				t.releaseRequest(r)
				_ = o.c.Cancel(r)
			}
		}
	}
	buf := p.buf
	if buf == nil {
		p.writing++
	}
	off := t.info.PieceOffset(i) + int64(r.Begin)
	t.refillLocked(pc)
	t.mu.Unlock()

	if buf != nil {
		// The block's place in the buffer is its own: it is marked done
		// above, so no other copy of it lands, and the buffer is not
		// pooled while blocks may still be copied into it.
		copy(buf[r.Begin:], m.Data)
		if int(p.copied.Add(1)) == len(p.blocks) {
			t.pieceBuffered(pc, i, p)
		}
		return
	}
	// The block aliases the connection's read buffer: the write gets a
	// copy, and the reads from pc stop while too many wait.
	wb := bufferpool.Get(n)
	copy(wb, m.Data)
	t.c.disk.runBytes(int64(n), pc.c, func() { t.writeBlock(i, b, p, off, wb) })
}

// pieceBuffered queues the check of piece i, whose blocks are all in p's
// buffer, on the disk pool: once it passes, it is written whole.
func (t *Torrent) pieceBuffered(pc *peerConn, i int, p *partial) {
	t.mu.Lock()
	buf := p.buf
	ok := buf != nil && t.partials[i] == p && !p.busy
	if ok {
		p.busy = true
	}
	t.mu.Unlock()
	if ok {
		t.c.disk.runBytes(int64(len(buf)), pc.c, func() { t.checkBuffered(i, p, buf) })
	}
}

// checkBuffered, on the disk pool, checks piece i from buf, p's buffer,
// and writes it to storage when it passes, in one write per file it
// covers. Data that fails is never written. A v2 piece whose layer is not
// known yet is written and waits for it, to be checked from storage.
func (t *Torrent) checkBuffered(i int, p *partial, buf []byte) {
	t.mu.Lock()
	layers := t.pieceLayersFor(i)
	info := t.info
	t.mu.Unlock()
	err := info.VerifyPiece(i, buf, layers)
	noLayer := errors.Is(err, metainfo.ErrNoPieceLayer)
	var werr error
	if err == nil || noLayer {
		werr = errNoStorage
		st, info, done := t.useStorage()
		if st != nil {
			werr = writeData(st, info, info.PieceOffset(i), buf)
		}
		done()
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.partials[i] == p && !t.pieces[i].have
	switch {
	case !current:
	case werr != nil:
		if !errors.Is(werr, errNoStorage) {
			t.c.emit(Event{Type: EventError, Torrent: t, Err: werr})
		}
		t.dropPartial(i)
		for _, pc := range t.peers {
			t.fillRequestsLocked(pc)
		}
	case err == nil:
		t.smartBanGoodLocked(i, buf)
		t.hashedData = true
		t.markHaveLocked(i)
	case noLayer:
		// A v2 torrent from a magnet link: the piece, in storage now,
		// waits for its file's piece layer, which peers are asked for.
		t.c.freePieceBuf(t, p, true)
		p.busy = false
		p.waitHashes = true
		// The layer may have come while the check ran.
		t.kickWaitingLocked()
		t.requestHashesLocked()
		return
	default:
		t.hashFails++
		t.wasted += int64(len(buf))
		t.smartBanBadLocked(i, p, buf)
		t.dropPartial(i)
		t.c.emit(Event{Type: EventHashFailed, Torrent: t, Piece: i})
		for _, pc := range t.peers {
			t.fillRequestsLocked(pc)
		}
	}
	// Every block was copied in before the check: the buffer is the
	// pool's again.
	t.c.freePieceBuf(t, p, true)
	p.busy = false
}

// writeBlock, on the disk pool, writes block b of piece i, which p holds,
// and checks the piece if that was its last block to land: the way of a
// piece that has no buffer.
func (t *Torrent) writeBlock(i, b int, p *partial, off int64, buf []byte) {
	err := errNoStorage
	st, info, done := t.useStorage()
	if st != nil {
		err = writeData(st, info, off, buf)
	}
	done()
	bufferpool.Put(buf)

	t.mu.Lock()
	p.writing--
	if err != nil {
		p.blocks[b].done = false
		p.done--
		p.reopen(b)
		if p.owner == nil {
			t.markFreeLocked(p)
		}
		t.mu.Unlock()
		if st != nil {
			t.c.emit(Event{Type: EventError, Torrent: t, Err: err})
		}
		return
	}
	verify := p.complete() && p.writing == 0 && !p.busy && !p.waitHashes && t.partials[i] == p
	if verify {
		p.busy = true
	}
	t.mu.Unlock()
	if verify {
		t.verifyPiece(i, p)
	}
}

// servableLen is how far into piece i a request may reach: the piece's
// length, but in a v2 torrent the last piece of a file to the end of its
// last 16 KiB block, which libtorrent requests whole, the bytes past the
// file being the zeros that align the next file.
func (t *Torrent) servableLen(i int) int64 {
	n := t.info.PieceSize(i)
	if t.info.HasV2() && !t.info.HasV1() {
		n = min(t.info.PieceLength, (n+blockSize-1)/blockSize*blockSize)
	}
	return n
}

// onRequest queues a peer's request, or rejects it: a piece this side
// does not have, a block too large, a peer choked that was not allowed
// the piece fast, or too many queued.
func (t *Torrent) onRequest(pc *peerConn, r peer.Request) {
	t.mu.Lock()
	ok := t.info != nil && !pc.closed && int(r.Index) < len(t.pieces) && t.pieces[r.Index].have &&
		r.Length > 0 && r.Length <= peer.MaxBlockSize &&
		int64(r.Begin)+int64(r.Length) <= t.servableLen(int(r.Index)) &&
		len(pc.upq) < maxUploadQueue && t.storage != nil
	if ok && pc.c.AmChoking() && !(pc.c.Fast() && pc.fastSet[r.Index]) {
		ok = false
	}
	if ok {
		for _, q := range pc.upq {
			if q == r {
				t.mu.Unlock()
				return
			}
		}
		pc.upq = append(pc.upq, r)
	}
	t.mu.Unlock()
	if !ok {
		_ = pc.c.Reject(r)
		return
	}
	t.pump(pc)
}

// pump uploads the blocks pc asked for, while its socket keeps up and the
// upload rate allows; OnWritable and the client's ticks start it again.
// The reads run on the disk pool, one job of a peer's at a time.
func (t *Torrent) pump(pc *peerConn) {
	t.mu.Lock()
	start := !pc.pumping && t.canPumpLocked(pc)
	pc.pumping = pc.pumping || start
	t.mu.Unlock()
	if start {
		t.c.disk.run(func() { t.pumpBlocks(pc) })
	}
}

func (t *Torrent) canPumpLocked(pc *peerConn) bool {
	return !pc.closed && len(pc.upq) > 0 && t.storage != nil && !pc.c.Congested() && t.upAllow()
}

// pumpBlocks, on the disk pool, reads and sends up to uploadBatch of the
// blocks pc asked for, and queues itself again if more may go.
func (t *Torrent) pumpBlocks(pc *peerConn) {
	for k := 0; ; k++ {
		t.mu.Lock()
		if !t.canPumpLocked(pc) {
			pc.pumping = false
			t.mu.Unlock()
			return
		}
		if k == uploadBatch {
			t.mu.Unlock()
			t.c.disk.run(func() { t.pumpBlocks(pc) })
			return
		}
		r := pc.upq[0]
		pc.upq = pc.upq[1:]
		ps := t.pieces[r.Index]
		t.mu.Unlock()

		// A piece AddOptions.Seed took as had is checked before its first
		// block goes out; one that failed is not served.
		if !ps.have || (ps.trusted && !t.checkTrusted(int(r.Index))) {
			_ = pc.c.Reject(r)
			continue
		}
		buf := bufferpool.Get(int(r.Length))
		err := errNoStorage
		st, info, done := t.useStorage()
		if st != nil {
			err = readData(st, info, info.PieceOffset(int(r.Index))+int64(r.Begin), buf)
		}
		done()
		if err == nil {
			err = pc.c.SendPiece(r.Index, r.Begin, buf)
		} else {
			_ = pc.c.Reject(r)
		}
		bufferpool.Put(buf)
		n := int(r.Length)
		t.mu.Lock()
		if err == nil {
			t.upTake(n)
			t.uploaded.Add(int64(n))
			pc.up.add(n)
			t.upRate.add(n)
			pc.upTotal += int64(n)
		} else {
			pc.pumping = false
		}
		t.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// banAddr is the address a peer is banned by: its IP (port zero), but
// for loopback, where every peer of the machine shares the one address,
// its listening address, or else the connection's.
func banAddr(pc *peerConn) netip.AddrPort {
	return banKey(pc.addr, pc.listen)
}

func banKey(addr, listen netip.AddrPort) netip.AddrPort {
	if !addr.Addr().IsLoopback() {
		return netip.AddrPortFrom(addr.Addr(), 0)
	}
	if listen.IsValid() {
		return listen
	}
	return addr
}

// chokeLocked chokes pc: under the Fast extension each request queued,
// but those of pieces allowed fast, is rejected (BEP 6); without it they
// are dropped, as the peer drops them.
func (t *Torrent) chokeLocked(pc *peerConn) {
	_ = pc.c.Send(&peer.Message{Type: peer.MsgChoke})
	if pc.c.Fast() {
		kept := pc.upq[:0]
		for _, r := range pc.upq {
			if pc.fastSet[r.Index] {
				kept = append(kept, r)
				continue
			}
			_ = pc.c.Reject(r)
		}
		pc.upq = kept
	} else {
		pc.upq = nil
	}
}

func (t *Torrent) unchokeLocked(pc *peerConn, now time.Time) {
	_ = pc.c.Send(&peer.Message{Type: peer.MsgUnchoke})
	pc.lastUnchoke = now
}

// maybeUnchokeLocked unchokes a peer that just became interested when an
// upload slot is free, rather than at the next rechoke.
func (t *Torrent) maybeUnchokeLocked(pc *peerConn) {
	if !pc.c.AmChoking() || t.info == nil || t.haveCount == 0 {
		return
	}
	unchoked := 0
	for _, o := range t.peers {
		if !o.c.AmChoking() {
			unchoked++
		}
	}
	if unchoked <= t.uploadSlotsLocked() {
		t.unchokeLocked(pc, time.Now())
	}
}

// dialEncryption is the order a dial tries MSE in under a policy.
func dialEncryption(e Encryption) []mse.Policy {
	switch e {
	case EncryptionRequire:
		return []mse.Policy{mse.Require}
	case EncryptionAllow:
		return []mse.Policy{mse.Allow, mse.Prefer}
	default:
		return []mse.Policy{mse.Prefer, mse.Allow}
	}
}
