package dht

import (
	"context"
	"crypto/sha1"
	"errors"
	"net/netip"
	"slices"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Lookup bounds: the candidates a lookup keeps, closest first, and the
// queries it may send in all, against a network that keeps handing it
// new nodes.
const (
	maxCandidates    = 8 * K
	maxLookupQueries = 256
)

type candState uint8

const (
	candFresh candState = iota
	candInflight
	candReplied
	candFailed
)

// cand is a node a lookup knows of.
type cand struct {
	NodeInfo
	state candState
	slow  bool
	token []byte
}

type lookupEvent struct {
	c    *cand
	m    *msg
	err  error
	slow bool
}

// lookup is an iterative lookup of BEP 5 as Kademlia has it: it queries
// the closest nodes it knows of to the target, Alpha at a time, learns
// closer ones from their answers, and has converged once it has answers
// from the K closest nodes it knows of that are there.
//
// It runs on the goroutine that calls run, so its callbacks may take
// their time; answers reach it through a channel. A query that has not
// been answered within a quarter of the query timeout stops counting against
// Alpha, so that a few silent nodes do not stall the lookup.
type lookup struct {
	n       *Node
	self    ID
	target  ID
	q       string
	mk      func() *args
	onReply func(c *cand, m *msg) (stop bool)

	cands   []*cand // nodes of known ID, closest first
	unknown []*cand // seeds of unknown ID, queried first
	seen    map[netip.AddrPort]bool

	alpha, maxInflight        int
	inflight, active, queries int
	stopped                   bool
	events                    chan lookupEvent
}

func (n *Node) newLookup(target ID, q string, mk func() *args) *lookup {
	n.mu.Lock()
	closest := n.table.closest(target, maxCandidates/2)
	var extra []netip.AddrPort
	if len(closest) < K {
		extra = append(extra, n.seeds...)
		for r := range n.routers {
			extra = append(extra, r)
		}
	}
	self := n.id
	n.mu.Unlock()
	l := &lookup{
		n: n, self: self, target: target, q: q, mk: mk,
		seen:        map[netip.AddrPort]bool{},
		alpha:       n.cfg.Alpha,
		maxInflight: 3 * n.cfg.Alpha,
	}
	// Each query posts at most two events, its "slow" mark and its end,
	// so this never fills.
	l.events = make(chan lookupEvent, 2*l.maxInflight)
	for _, ni := range closest {
		l.add(ni)
	}
	l.addSeeds(extra)
	return l
}

// add adds a node the lookup heard of.
func (l *lookup) add(ni NodeInfo) {
	if l.seen[ni.Addr] || ni.ID == l.self || ni.ID.IsZero() || ni.Addr.Addr().Is4() == l.n.cfg.IPv6 || l.n.refuses(ni.Addr) {
		return
	}
	l.seen[ni.Addr] = true
	l.insert(&cand{NodeInfo: ni})
}

func (l *lookup) insert(c *cand) {
	i, _ := slices.BinarySearchFunc(l.cands, c.ID, func(x *cand, id ID) int { return l.target.cmpDistance(x.ID, id) })
	if i >= maxCandidates {
		return
	}
	l.cands = slices.Insert(l.cands, i, c)
	if len(l.cands) > maxCandidates {
		l.cands = l.cands[:maxCandidates]
	}
}

// addSeeds adds addresses to start from whose IDs are not known.
func (l *lookup) addSeeds(addrs []netip.AddrPort) {
	for _, a := range addrs {
		a = unmap(a)
		if l.seen[a] || a.Addr().Is4() == l.n.cfg.IPv6 || l.n.refuses(a) {
			continue
		}
		l.seen[a] = true
		l.unknown = append(l.unknown, &cand{NodeInfo: NodeInfo{Addr: a}})
	}
}

// next returns the next node to query: a seed, else the closest node not
// queried yet, as long as fewer than K closer ones have answered. Nodes in
// flight do not count, as libtorrent's lookups have it: while the closest
// ones are slow to answer, or never will, the lookup goes on past them.
func (l *lookup) next() *cand {
	if len(l.unknown) > 0 {
		c := l.unknown[0]
		l.unknown = l.unknown[1:]
		return c
	}
	replied := 0
	for _, c := range l.cands {
		switch c.state {
		case candFresh:
			return c
		case candReplied:
			if replied++; replied >= K {
				return nil
			}
		}
	}
	return nil
}

// converged reports whether the K closest nodes that answered are known:
// none closer is left to query, and those closer still in flight are slow,
// most likely gone.
func (l *lookup) converged() bool {
	replied := 0
	for _, c := range l.cands {
		switch c.state {
		case candFresh:
			return false
		case candInflight:
			if !c.slow {
				return false
			}
		case candReplied:
			if replied++; replied == K {
				return true
			}
		}
	}
	return false
}

func (l *lookup) post(ev lookupEvent) {
	select {
	case l.events <- ev:
	default:
	}
}

func (l *lookup) send(c *cand) {
	c.state = candInflight
	l.inflight++
	l.active++
	l.queries++
	slowAfter := l.n.cfg.QueryTimeout / 4
	l.n.query(c.Addr, l.q, l.mk(), queryOpts{
		slowAfter: slowAfter,
		slow:      func() { l.post(lookupEvent{c: c, slow: true}) },
	}, func(m *msg, err error) { l.post(lookupEvent{c: c, m: m, err: err}) })
}

// run runs the lookup until it converges, runs out of nodes, a callback
// stops it, or ctx is done. It returns ErrNoNodes if it had no node to
// query at all.
func (l *lookup) run(ctx context.Context) error {
	for {
		for !l.stopped && l.active < l.alpha && l.inflight < l.maxInflight && l.queries < maxLookupQueries {
			c := l.next()
			if c == nil {
				break
			}
			l.send(c)
		}
		if l.inflight == 0 || l.stopped || l.converged() {
			break
		}
		select {
		case ev := <-l.events:
			l.handle(ev)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if l.queries == 0 {
		return ErrNoNodes
	}
	return nil
}

func (l *lookup) handle(ev lookupEvent) {
	c := ev.c
	if c.state != candInflight {
		return
	}
	if ev.slow {
		if !c.slow {
			c.slow = true
			l.active--
		}
		return
	}
	l.inflight--
	if !c.slow {
		l.active--
	}
	if ev.err != nil || ev.m == nil || ev.m.R == nil {
		c.state = candFailed
		return
	}
	r := ev.m.R
	id := ID(r.ID)
	if id == l.self {
		c.state = candFailed
		return
	}
	c.state = candReplied
	if c.ID != id {
		if !c.ID.IsZero() {
			if i := slices.Index(l.cands, c); i >= 0 {
				l.cands = slices.Delete(l.cands, i, i+1)
			}
		}
		c.ID = id
		l.insert(c)
	}
	for _, ni := range l.n.responseNodes(r) {
		l.add(ni)
	}
	if l.onReply != nil && l.onReply(c, ev.m) {
		l.stopped = true
	}
}

// closest returns up to K of the closest nodes that answered and pass
// keep, closest first.
func (l *lookup) closest(keep func(*cand) bool) []*cand {
	var out []*cand
	for _, c := range l.cands {
		if c.state == candReplied && (keep == nil || keep(c)) {
			if out = append(out, c); len(out) == K {
				break
			}
		}
	}
	return out
}

func nodeInfos(cs []*cand) []NodeInfo {
	out := make([]NodeInfo, len(cs))
	for i, c := range cs {
		out[i] = c.NodeInfo
	}
	return out
}

func hasToken(c *cand) bool { return len(c.token) > 0 }

func (n *Node) findNodeLookup(target ID) *lookup {
	return n.newLookup(target, "find_node", func() *args { return &args{Target: target[:]} })
}

// FindNode looks up the K nodes closest to target, closest first.
func (n *Node) FindNode(ctx context.Context, target ID) ([]NodeInfo, error) {
	l := n.findNodeLookup(target)
	err := l.run(ctx)
	return nodeInfos(l.closest(nil)), err
}

func (n *Node) getPeersLookup(ih metainfo.InfoHash, onPeers func([]netip.AddrPort)) *lookup {
	l := n.newLookup(ID(ih), "get_peers", func() *args { return &args{InfoHash: ih[:]} })
	seen := map[netip.AddrPort]bool{}
	// The peers announced to this node come first: it may well be one of
	// the closest to the torrent, which the lookup never asks.
	n.mu.Lock()
	own := n.peers.get(ID(ih), n.maxValues(), n.cfg.IPv6, time.Now())
	n.mu.Unlock()
	for _, a := range own {
		seen[a] = true
	}
	if n.cfg.Filter != nil {
		own = slices.DeleteFunc(own, n.refuses)
	}
	if len(own) > 0 && onPeers != nil {
		onPeers(own)
	}
	l.onReply = func(c *cand, m *msg) bool {
		c.token = m.R.Token
		var fresh []netip.AddrPort
		for _, v := range m.R.Values {
			a, ok := parseCompactAddr(v)
			if a = unmap(a); ok && a.Port() != 0 && !seen[a] && !n.refuses(a) {
				seen[a] = true
				fresh = append(fresh, a)
			}
		}
		if len(fresh) > 0 && onPeers != nil {
			onPeers(fresh)
		}
		return false
	}
	return l
}

// GetPeers looks up the peers of a torrent: onPeers, when not nil, gets
// each batch of peers not seen before as nodes return them, on the
// calling goroutine. It returns once the lookup converges.
func (n *Node) GetPeers(ctx context.Context, ih metainfo.InfoHash, onPeers func([]netip.AddrPort)) error {
	return n.getPeersLookup(ih, onPeers).run(ctx)
}

// Announce looks up the peers of a torrent as GetPeers does, then announces
// this peer to the K closest nodes that gave a token: at port, or, with
// impliedPort, at the port the DHT's datagrams come from (as a client
// whose uTP shares the DHT's socket wants). It returns how many nodes took
// the announcement. Peers expire from the nodes after 30 minutes; a
// client announces again sooner.
func (n *Node) Announce(ctx context.Context, ih metainfo.InfoHash, port int, impliedPort bool, onPeers func([]netip.AddrPort)) (int, error) {
	if !impliedPort && (port <= 0 || port > 65535) {
		return 0, errors.New("dht: announce of an invalid port")
	}
	l := n.getPeersLookup(ih, onPeers)
	if err := l.run(ctx); err != nil {
		return 0, err
	}
	targets := l.closest(hasToken)
	if len(targets) == 0 {
		return 0, ErrNoNodes
	}
	p := int64(port)
	return n.sendAll(ctx, targets, "announce_peer", func(c *cand) *args {
		a := &args{InfoHash: ih[:], Token: c.token, Port: &p}
		if impliedPort {
			a.ImpliedPort = 1
		}
		return a
	})
}

// sendAll sends a query to each of targets and returns how many answered
// without an error, and the first error if none did.
func (n *Node) sendAll(ctx context.Context, targets []*cand, q string, mk func(*cand) *args) (int, error) {
	ch := make(chan error, len(targets))
	for _, c := range targets {
		n.query(c.Addr, q, mk(c), queryOpts{retries: n.cfg.QueryRetries}, func(_ *msg, err error) { ch <- err })
	}
	ok := 0
	var first error
	for range targets {
		select {
		case err := <-ch:
			if err == nil {
				ok++
			} else if first == nil {
				first = err
			}
		case <-ctx.Done():
			return ok, ctx.Err()
		}
	}
	if ok == 0 {
		return 0, first
	}
	return ok, nil
}

func (n *Node) getLookup(target ID, onItem func(r *ret) bool) *lookup {
	l := n.newLookup(target, "get", func() *args { return &args{Target: target[:]} })
	l.onReply = func(c *cand, m *msg) bool {
		c.token = m.R.Token
		if len(m.R.V) == 0 || onItem == nil {
			return false
		}
		return onItem(m.R)
	}
	return l
}

// Get looks up the immutable item at target (BEP 44): the value whose
// SHA-1 it is. It stops at the first node that has it, and returns
// ErrNotFound if none does.
func (n *Node) Get(ctx context.Context, target ID) (*Item, error) {
	var found *Item
	l := n.getLookup(target, func(r *ret) bool {
		if ID(sha1.Sum(r.V)) != target {
			return false
		}
		it := &Item{V: r.V}
		if it.Verify() != nil {
			return false
		}
		found = it
		return true
	})
	if err := l.run(ctx); err != nil && found == nil {
		return nil, err
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

// GetMutable looks up the mutable item of key and salt (BEP 44) and
// returns the one of the highest sequence number with a valid signature
// that the closest nodes have, or ErrNotFound.
func (n *Node) GetMutable(ctx context.Context, key [32]byte, salt []byte) (*Item, error) {
	var best *Item
	l := n.getLookup(MutableTarget(key, salt), func(r *ret) bool {
		if len(r.K) != 32 || [32]byte(r.K) != key || len(r.Sig) != 64 || r.Seq == nil {
			return false
		}
		it := &Item{V: r.V, K: key, Salt: append([]byte(nil), salt...), Seq: *r.Seq, Sig: [64]byte(r.Sig)}
		if it.Verify() == nil && (best == nil || it.Seq > best.Seq) {
			best = it
		}
		return false
	})
	if err := l.run(ctx); err != nil && best == nil {
		return nil, err
	}
	if best == nil {
		return nil, ErrNotFound
	}
	return best, nil
}

// Put stores an item (BEP 44) on the K nodes closest to its target that
// gave a token, and returns how many took it. Items expire from nodes
// after two hours; a client puts them again sooner.
func (n *Node) Put(ctx context.Context, it *Item) (int, error) {
	if err := it.Verify(); err != nil {
		return 0, err
	}
	l := n.getLookup(it.Target(), nil)
	if err := l.run(ctx); err != nil {
		return 0, err
	}
	targets := l.closest(hasToken)
	if len(targets) == 0 {
		return 0, ErrNoNodes
	}
	return n.sendAll(ctx, targets, "put", func(c *cand) *args {
		// An immutable put carries "seq" 0 as well: BEP 44 leaves it out,
		// but anacrolix/dht refuses a put without it, and libtorrent
		// ignores it.
		seq := it.Seq
		a := &args{Token: c.token, V: it.V, Seq: &seq}
		if it.Mutable() {
			a.K, a.Sig, a.Salt = it.K[:], it.Sig[:], it.Salt
		}
		return a
	})
}
