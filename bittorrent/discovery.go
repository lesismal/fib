package bittorrent

import (
	"context"
	"net/netip"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
	"github.com/lesismal/fib/bittorrent/peer/mse"
	"github.com/lesismal/fib/bittorrent/tracker"
)

// Source is where the address of a peer came from.
type Source uint8

// The sources of peers.
const (
	SourceTracker Source = 1 << iota
	SourceDHT
	SourcePEX
	SourceLSD
	SourceManual
	SourceIncoming
	SourceOther
)

// PeerSource finds the peers of torrents. The client's trackers, DHT
// and local service discovery are PeerSources, and Config.PeerSources
// adds others.
type PeerSource interface {
	// Find runs on a goroutine of its own while t runs, until ctx ends,
	// calling found with the addresses of the peers it finds.
	Find(ctx context.Context, t *Torrent, found func([]netip.AddrPort))
}

// candidate is a peer address the torrent may connect to.
type candidate struct {
	addr      netip.AddrPort
	sources   Source
	v2        bool // found under the truncated v2 hash: dial with it
	connected bool
	dialing   bool
	conn      *peerConn
	fails     int
	nextTry   time.Time
	seed      bool
	self      bool
	noUTP     bool
}

// dialAttempt is one try at connecting to a candidate: over uTP or TCP,
// under one of the MSE policies the client's Encryption tries in turn.
type dialAttempt struct {
	cand   *candidate
	utp    bool
	enc    []mse.Policy
	encIdx int
	hash   [20]byte
}

// maxCandidates bounds the addresses a torrent keeps.
const maxCandidates = 2000

func (t *Torrent) addPeers(src Source, v2 bool, addrs []netip.AddrPort) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addPeersLocked(src, v2, addrs)
}

func (t *Torrent) addPeersLocked(src Source, v2 bool, addrs []netip.AddrPort) {
	for _, a := range addrs {
		a = netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
		if !a.IsValid() || a.Port() == 0 || a.Addr().IsUnspecified() || a.Addr().IsMulticast() {
			continue
		}
		if t.c.isOwnAddr(a) {
			continue
		}
		if cand, ok := t.pool[a]; ok {
			cand.sources |= src
			if src == SourceManual {
				cand.nextTry = time.Time{}
			}
			continue
		}
		if len(t.pool) >= maxCandidates {
			continue
		}
		t.pool[a] = &candidate{addr: a, sources: src, v2: v2}
	}
}

// connectPeersLocked dials candidates while the limits allow: the
// torrent's and the client's connections, the dials in flight, and a few
// new dials per tick.
func (t *Torrent) connectPeersLocked(now time.Time) {
	if !t.running() {
		return
	}
	cl := t.c
	cfg := &cl.cfg
	budget := cfg.DialsPerTick
	for _, cand := range t.pool {
		if budget <= 0 || len(t.peers)+t.halfOpen >= cfg.MaxConnsPerTorrent {
			return
		}
		if int(cl.conns.Load()+cl.halfOpen.Load()) >= cfg.MaxConns || int(cl.halfOpen.Load()) >= cfg.MaxHalfOpen {
			return
		}
		if cand.connected || cand.dialing || cand.self || now.Before(cand.nextTry) {
			continue
		}
		if cand.seed && t.info != nil && t.missing == 0 {
			continue
		}
		if cl.banned(cand.addr, cand.addr) {
			continue
		}
		t.dialLocked(t.newAttempt(cand))
		budget--
	}
}

func (t *Torrent) newAttempt(cand *candidate) *dialAttempt {
	cl := t.c
	att := &dialAttempt{cand: cand, enc: dialEncryption(cl.cfg.Encryption), hash: t.ih}
	att.utp = cl.utp != nil && !cl.cfg.DisableUTP && (!cand.noUTP || cl.cfg.DisableTCP)
	if t.ih.IsZero() || (cand.v2 && !t.ih2.IsZero()) {
		att.hash = t.ih2.Truncate()
	}
	return att
}

func (t *Torrent) dialLocked(att *dialAttempt) {
	att.cand.dialing = true
	t.halfOpen++
	t.c.halfOpen.Add(1)
	go t.dial(att)
}

// dialDoneLocked ends a dial attempt's count as half-open.
func (t *Torrent) dialDoneLocked(att *dialAttempt) {
	if !att.cand.dialing {
		return
	}
	att.cand.dialing = false
	t.halfOpen--
	t.c.halfOpen.Add(-1)
}

func (t *Torrent) dial(att *dialAttempt) {
	cl := t.c
	pol := att.enc[att.encIdx]
	opts := peer.DialOptions{InfoHash: att.hash, Encryption: &pol}
	var c *peer.Conn
	var err error
	if att.utp {
		timeout := cl.cfg.UTPDialTimeout
		if cl.cfg.DisableTCP {
			timeout = 3 * timeout
		}
		ctx, cancel := context.WithTimeout(cl.ctx, timeout)
		uc, derr := cl.utp.DialAddrPort(ctx, att.cand.addr)
		cancel()
		if derr != nil {
			t.mu.Lock()
			t.dialFailedLocked(att, false, derr)
			t.mu.Unlock()
			return
		}
		if c, err = cl.server.Open(uc, opts); err != nil {
			_ = uc.Close()
		}
	} else {
		opts.Network = "tcp"
		opts.Addr = att.cand.addr.String()
		c, err = cl.server.Dial(cl.engine, opts)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err != nil {
		t.dialFailedLocked(att, false, err)
		return
	}
	if e, ok := t.early[c]; ok {
		delete(t.early, c)
		t.dialFailedLocked(att, att.utp || c.LocalAddr() != nil, e.err)
		return
	}
	if pc := t.peers[c]; pc != nil {
		// The handshake beat this goroutine to the lock.
		t.dialDoneLocked(att)
		att.cand.connected = true
		att.cand.conn = pc
		pc.listen = att.cand.addr
		return
	}
	t.dials[c] = att
}

// dialFailedLocked handles a connection that ended before its handshake:
// a uTP dial nobody answered is tried over TCP, a connection the peer
// closed during the handshake under the next MSE policy (with MSE after
// plaintext, or plaintext after MSE, as Config.Encryption says); and
// after all of that the candidate waits, longer each time it fails.
func (t *Torrent) dialFailedLocked(att *dialAttempt, connected bool, err error) {
	t.dialDoneLocked(att)
	cand := att.cand
	if !t.running() || t.c.closing.Load() {
		return
	}
	if att.utp && !connected && !t.c.cfg.DisableTCP {
		cand.noUTP = true
		next := *att
		next.utp = false
		t.dialLocked(&next)
		return
	}
	if connected && att.encIdx+1 < len(att.enc) {
		next := *att
		next.encIdx++
		t.dialLocked(&next)
		return
	}
	cand.fails++
	if cand.fails >= 6 && cand.sources&SourceManual == 0 {
		delete(t.pool, cand.addr)
		return
	}
	backoff := 15 * time.Second << min(cand.fails-1, 7)
	cand.nextTry = time.Now().Add(backoff)
}

// startDiscoveryLocked starts the peer sources: trackers, the DHT, local
// service discovery and the configured ones; a private torrent (BEP 27)
// has its trackers alone.
func (t *Torrent) startDiscoveryLocked(ctx context.Context) {
	cl := t.c
	var sources []PeerSource
	if !cl.cfg.DisableTrackers {
		sources = append(sources, trackerSource{})
	}
	if !t.isPrivate() {
		if cl.dht4 != nil || cl.dht6 != nil {
			sources = append(sources, dhtSource{})
		}
		if cl.lsd != nil {
			sources = append(sources, cl.lsd)
		}
		sources = append(sources, cl.cfg.PeerSources...)
	}
	for _, s := range sources {
		src := s
		kind := SourceOther
		switch src.(type) {
		case trackerSource:
			kind = SourceTracker
		case dhtSource:
			kind = SourceDHT
		case *lsd:
			kind = SourceLSD
		}
		cl.wg.Add(1)
		go func() {
			defer cl.wg.Done()
			src.Find(ctx, t, func(addrs []netip.AddrPort) { t.addPeers(kind, false, addrs) })
		}()
	}
}

// announceHashes are the hashes the torrent is announced under: v1, and
// the truncated v2 one, both for a hybrid (as libtorrent does).
func (t *Torrent) announceHashes() []metainfo.InfoHash {
	t.mu.Lock()
	defer t.mu.Unlock()
	var hs []metainfo.InfoHash
	if !t.ih.IsZero() {
		hs = append(hs, t.ih)
	}
	if !t.ih2.IsZero() {
		hs = append(hs, t.ih2.Truncate())
	}
	return hs
}

// left is the bytes the torrent misses, -1 before its info dictionary.
func (t *Torrent) left() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.info == nil {
		return -1
	}
	var n int64
	for i := range t.pieces {
		if !t.pieces[i].have {
			n += t.info.PieceSize(i)
		}
	}
	return n
}

// trackerSource announces to the torrent's trackers, tier by tier (BEP
// 12): started first, then at the interval each answer gives, completed
// when the download finishes, stopped when the torrent pauses.
type trackerSource struct{}

func (trackerSource) Find(ctx context.Context, t *Torrent, found func([]netip.AddrPort)) {
	t.mu.Lock()
	list := t.trackers
	t.mu.Unlock()
	if len(list) == 0 || t.c.trackers == nil {
		return
	}
	tiers := tracker.NewTiers(list)
	hashes := t.announceHashes()
	done := make(chan struct{}, len(hashes))
	for _, h := range hashes {
		v2 := !t.ih2.IsZero() && h == t.ih2.Truncate() && h != t.ih
		go func() {
			defer func() { done <- struct{}{} }()
			t.announceLoop(ctx, tiers, h, v2)
		}()
	}
	for range hashes {
		<-done
	}
}

func (t *Torrent) announceLoop(ctx context.Context, tiers *tracker.Tiers, h metainfo.InfoHash, v2 bool) {
	cl := t.c
	completed := t.completed
	select {
	case <-completed:
		completed = nil // done before it started: no completed event
	default:
	}
	event := tracker.EventStarted
	started := false
	backoff := 15 * time.Second
	announce := func(ctx context.Context, ev tracker.Event, numWant int) (*tracker.AnnounceResponse, error) {
		req := tracker.AnnounceRequest{
			InfoHash: h, PeerID: cl.peerID, Port: uint16(cl.port),
			Uploaded: t.uploaded.Load(), Downloaded: t.downloaded.Load(), Left: t.left(),
			Event: ev, NumWant: numWant, Key: cl.key,
		}
		resp, _, err := tiers.Announce(ctx, cl.trackers, req)
		return resp, err
	}
	for {
		actx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		resp, err := announce(actx, event, 0)
		cancel()
		var wait time.Duration
		if err == nil {
			started = true
			event = tracker.EventNone
			backoff = 15 * time.Second
			addrs := make([]netip.AddrPort, 0, len(resp.Peers))
			for _, p := range resp.Peers {
				addrs = append(addrs, p.Addr)
			}
			t.addPeers(SourceTracker, v2, addrs)
			wait = min(max(resp.Interval, resp.MinInterval, cl.cfg.MinAnnounceInterval), 2*time.Hour)
		} else {
			if ctx.Err() != nil {
				wait = 0
			} else {
				wait = backoff
				backoff = min(backoff*2, 30*time.Minute)
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			if started {
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = announce(sctx, tracker.EventStopped, -1)
				cancel()
			}
			return
		case <-timer.C:
		case <-completed:
			timer.Stop()
			completed = nil
			if started {
				event = tracker.EventCompleted
			}
		}
	}
}

// dhtSource looks the torrent up in the DHT and announces it there:
// every fifteen minutes, or every minute while it has no peers.
type dhtSource struct{}

func (dhtSource) Find(ctx context.Context, t *Torrent, found func([]netip.AddrPort)) {
	cl := t.c
	select {
	case <-cl.dhtReady:
	case <-ctx.Done():
		return
	}
	for {
		for _, h := range t.announceHashes() {
			v2 := !t.ih2.IsZero() && h == t.ih2.Truncate() && h != t.ih
			for _, n := range []*dhtNode{cl.dht4, cl.dht6} {
				if n == nil {
					continue
				}
				actx, cancel := context.WithTimeout(ctx, time.Minute)
				_, _ = n.Announce(actx, h, cl.port, false, func(addrs []netip.AddrPort) {
					t.addPeers(SourceDHT, v2, addrs)
				})
				cancel()
			}
		}
		t.mu.Lock()
		few := len(t.peers) == 0
		t.mu.Unlock()
		wait := 15 * time.Minute
		if few {
			wait = cl.cfg.DHTRetryInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
