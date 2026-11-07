package bittorrent

import (
	"net"
	"net/netip"
	"time"

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

	pending   map[peer.Request]time.Time
	lastBlock time.Time
	snubbed   bool
	upq       []peer.Request
	pumping   bool
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
			t.pieces[i].avail++
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
	if cl.banned(addr, listen) {
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
	if rec == nil && replaced == nil && (len(t.peers) >= cl.cfg.MaxConnsPerTorrent || int(cl.conns.Load()) >= cl.cfg.MaxConns) {
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
	if pc.bits != nil && t.info != nil {
		for i := range t.pieces {
			if pc.bits.Has(i) {
				t.pieces[i].avail--
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
					t.pieces[i].avail++
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
	t.pieces[i].avail++
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
			t.pieces[i].avail++
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
	for len(pc.pending) < limit && t.c.down.allow() {
		r, ok := t.nextRequest(pc, choked)
		if !ok {
			return
		}
		if err := c.Request(r); err != nil {
			t.releaseRequest(r)
			return
		}
		if len(pc.pending) == 0 {
			// The wait for a block starts with the first request.
			pc.lastBlock = now
		}
		pc.pending[r] = now
	}
}

// onPiece takes a block: it goes to storage, and a piece whose blocks
// are all there is checked.
func (t *Torrent) onPiece(pc *peerConn, m *peer.Message) {
	r := m.Request()
	n := len(m.Data)
	t.downloaded.Add(int64(n))
	t.c.down.take(n)
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
		t.fillRequestsLocked(pc)
		t.mu.Unlock()
		return
	}
	bs := &p.blocks[b]
	bs.done = true
	bs.from = banAddr(pc)
	p.done++
	p.writing++
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
	st := t.storage
	info := t.info
	t.mu.Unlock()

	err := writeData(st, info, info.PieceOffset(i)+int64(r.Begin), m.Data)

	t.mu.Lock()
	p.writing--
	if err != nil {
		bs.done = false
		p.done--
		t.mu.Unlock()
		t.c.emit(Event{Type: EventError, Torrent: t, Err: err})
		return
	}
	verify := p.complete() && p.writing == 0 && !p.busy && !p.waitHashes && t.partials[i] == p
	if verify {
		p.busy = true
	}
	t.fillRequestsLocked(pc)
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
func (t *Torrent) pump(pc *peerConn) {
	for {
		t.mu.Lock()
		if pc.pumping || pc.closed || len(pc.upq) == 0 || t.storage == nil || pc.c.Congested() || !t.c.up.allow() {
			t.mu.Unlock()
			return
		}
		r := pc.upq[0]
		pc.upq = pc.upq[1:]
		pc.pumping = true
		st, info := t.storage, t.info
		t.mu.Unlock()

		buf := bufferpool.Get(int(r.Length))
		err := readData(st, info, info.PieceOffset(int(r.Index))+int64(r.Begin), buf)
		if err == nil {
			err = pc.c.SendPiece(r.Index, r.Begin, buf)
		} else {
			_ = pc.c.Reject(r)
		}
		bufferpool.Put(buf)
		n := int(r.Length)
		t.mu.Lock()
		pc.pumping = false
		if err == nil {
			t.c.up.take(n)
			t.uploaded.Add(int64(n))
			pc.up.add(n)
			t.upRate.add(n)
			pc.upTotal += int64(n)
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
	if unchoked < t.c.cfg.UploadSlots+1 {
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
