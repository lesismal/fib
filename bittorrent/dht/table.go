package dht

import (
	"net/netip"
	"slices"
	"time"
)

// Routing table constants (BEP 5).
const (
	// K is the size of a bucket, and how many closest nodes a lookup
	// converges on.
	K = 8
	// GoodFor is how long a node stays good after it last answered us,
	// or, once it has answered at all, after it last queried us.
	GoodFor = 15 * time.Minute
	// badFailures is how many queries in a row a node may leave
	// unanswered before it is bad: replaced first, never handed out.
	badFailures = 2
	// dropFailures is how many in a row remove it from the table even
	// with nothing to replace it.
	dropFailures = 5
)

// entry is a node of the routing table.
type entry struct {
	NodeInfo
	added      time.Time
	lastReply  time.Time // it last answered a query of ours
	lastQuery  time.Time // it last queried us
	failures   int       // queries in a row it left unanswered
	pinging    bool      // a ping to decide whether to evict it is out
	replyCount int
}

func (e *entry) good(now time.Time) bool {
	if e.failures > 0 || e.replyCount == 0 {
		return false
	}
	return now.Sub(e.lastReply) < GoodFor || now.Sub(e.lastQuery) < GoodFor
}

func (e *entry) bad() bool { return e.failures >= badFailures }

// lastSeen is when the node last showed it was there.
func (e *entry) lastSeen() time.Time {
	if e.lastQuery.After(e.lastReply) {
		return e.lastQuery
	}
	return e.lastReply
}

// bucket is a k-bucket: the live nodes, least recently seen first, and the
// replacement cache of nodes that answered while it was full, most recent
// last.
type bucket struct {
	nodes        []*entry
	replacements []*entry
	changed      time.Time // last time a node was added, replaced, or answered
}

// table is the routing table of BEP 5 as libtorrent and most clients build
// it: buckets[i] holds the nodes whose IDs share exactly i leading bits
// with ours, and the last bucket all those that share more. Only the last
// bucket, the one our own ID falls in, splits when it is full, so the
// table knows many nodes near itself and a few far away.
//
// Only nodes that answered a query of ours enter it: a query, or a node
// some other node told us of, could come from or name any address.
//
// The table is not safe for concurrent use; the Node's mutex guards it.
type table struct {
	self    ID
	buckets []*bucket
	byAddr  map[netip.AddrPort]*entry
	// ips counts the nodes at each address, for onePerIP.
	ips map[netip.Addr]int
	// onePerIP admits a single node per IP address outside local
	// networks, as libtorrent's restrict_routing_ips does, so that one
	// host cannot fill the table with nodes of IDs it picked.
	onePerIP bool
	// accept, when set, vets a node before it enters (BEP 42).
	accept func(NodeInfo) bool
}

func newTable(self ID) *table {
	return &table{
		self:     self,
		buckets:  []*bucket{{}},
		byAddr:   map[netip.AddrPort]*entry{},
		ips:      map[netip.Addr]int{},
		onePerIP: true,
	}
}

// bucketIndex returns the bucket an ID falls in.
func (t *table) bucketIndex(id ID) int {
	i := commonPrefixLen(t.self, id)
	if i >= len(t.buckets) {
		i = len(t.buckets) - 1
	}
	return i
}

// len returns how many nodes the table holds, not counting replacements.
func (t *table) len() int { return len(t.byAddr) }

// get returns the table's node at addr, nil if none.
func (t *table) get(addr netip.AddrPort) *entry { return t.byAddr[addr] }

// replied records that the node answered a query of ours, adding it to the
// table if there is room. When its bucket is full of nodes that cannot be
// dropped yet, it goes to the replacement cache and the returned entry,
// if any, is a questionable node to ping: if that ping fails the node is
// bad, and the newest replacement takes its place.
func (t *table) replied(n NodeInfo, now time.Time) (ping *entry) {
	if n.ID == t.self || n.ID.IsZero() {
		return nil
	}
	if e := t.byAddr[n.Addr]; e != nil {
		if e.ID == n.ID {
			e.lastReply = now
			e.failures = 0
			e.pinging = false
			e.replyCount++
			b := t.buckets[t.bucketIndex(e.ID)]
			b.changed = now
			b.touch(e)
			return nil
		}
		// The address now answers with another ID: the node restarted
		// with a new one, or someone else has the address. Either way
		// the old entry is stale.
		t.remove(e)
	}
	if t.accept != nil && !t.accept(n) {
		return nil
	}
	if t.onePerIP && !isLocal(n.Addr.Addr()) && t.ips[n.Addr.Addr()] > 0 {
		return nil
	}
	e := &entry{NodeInfo: n, added: now, lastReply: now, replyCount: 1}
	for {
		i := t.bucketIndex(n.ID)
		b := t.buckets[i]
		if b.has(n.ID) {
			// Same ID at another address: keep the one we know.
			return nil
		}
		if len(b.nodes) < K {
			t.insert(b, e, now)
			return nil
		}
		if i == len(t.buckets)-1 && len(t.buckets) < 160 {
			t.split()
			continue
		}
		// Full and not splittable: a bad node makes way at once.
		for _, old := range b.nodes {
			if old.bad() {
				t.remove(old)
				t.insert(b, e, now)
				return nil
			}
		}
		b.addReplacement(e)
		if !b.full(now) {
			// Ping the least recently seen questionable node
			// before evicting it.
			for _, old := range b.nodes {
				if !old.good(now) && !old.pinging {
					old.pinging = true
					return old
				}
			}
		}
		return nil
	}
}

// queried records that a node of the table queried us.
func (t *table) queried(n NodeInfo, now time.Time) bool {
	e := t.byAddr[n.Addr]
	if e == nil || e.ID != n.ID {
		return false
	}
	e.lastQuery = now
	return true
}

// wants reports whether a node that is not in the table would get in were
// it to answer: whether pinging a stranger that queried us is worth it.
func (t *table) wants(id ID, now time.Time) bool {
	if id == t.self {
		return false
	}
	i := t.bucketIndex(id)
	b := t.buckets[i]
	if b.has(id) {
		return false
	}
	return len(b.nodes) < K || i == len(t.buckets)-1 && len(t.buckets) < 160 || !b.full(now)
}

// failed records that the node at addr left a query unanswered. A node
// that turns bad is replaced by the newest replacement, and one that keeps
// failing is dropped.
func (t *table) failed(addr netip.AddrPort, now time.Time) {
	e := t.byAddr[addr]
	if e == nil {
		return
	}
	e.failures++
	e.pinging = false
	if !e.bad() {
		return
	}
	b := t.buckets[t.bucketIndex(e.ID)]
	if r := b.popReplacement(t); r != nil {
		t.remove(e)
		t.insert(b, r, now)
		return
	}
	if e.failures >= dropFailures {
		t.remove(e)
	}
}

func (t *table) insert(b *bucket, e *entry, now time.Time) {
	b.nodes = append(b.nodes, e)
	b.changed = now
	t.byAddr[e.Addr] = e
	t.ips[e.Addr.Addr()]++
}

func (t *table) remove(e *entry) {
	b := t.buckets[t.bucketIndex(e.ID)]
	if i := slices.Index(b.nodes, e); i >= 0 {
		b.nodes = slices.Delete(b.nodes, i, i+1)
	}
	if t.byAddr[e.Addr] == e {
		delete(t.byAddr, e.Addr)
		if t.ips[e.Addr.Addr()]--; t.ips[e.Addr.Addr()] <= 0 {
			delete(t.ips, e.Addr.Addr())
		}
	}
}

// split splits the last bucket in two: the nodes and replacements that
// share one more bit with our ID move to a new last bucket.
func (t *table) split() {
	last := t.buckets[len(t.buckets)-1]
	nb := &bucket{changed: last.changed}
	t.buckets = append(t.buckets, nb)
	n := len(t.buckets) - 2
	keep := last.nodes[:0:0]
	for _, e := range last.nodes {
		if commonPrefixLen(t.self, e.ID) > n {
			nb.nodes = append(nb.nodes, e)
		} else {
			keep = append(keep, e)
		}
	}
	last.nodes = keep
	keepR := last.replacements[:0:0]
	for _, e := range last.replacements {
		if commonPrefixLen(t.self, e.ID) > n {
			nb.replacements = append(nb.replacements, e)
		} else {
			keepR = append(keepR, e)
		}
	}
	last.replacements = keepR
	// Replacements fill the room the split made.
	for _, b := range []*bucket{last, nb} {
		for len(b.nodes) < K {
			r := b.popReplacement(t)
			if r == nil {
				break
			}
			t.insert(b, r, b.changed)
		}
	}
}

// closest returns up to n nodes closest to target that are not bad,
// closest first.
func (t *table) closest(target ID, n int) []NodeInfo {
	all := make([]NodeInfo, 0, len(t.byAddr))
	for _, e := range t.byAddr {
		if !e.bad() {
			all = append(all, e.NodeInfo)
		}
	}
	sortNodes(target, all)
	if len(all) > n {
		all = all[:n]
	}
	return all
}

// nodes returns every node that is not bad.
func (t *table) nodes() []NodeInfo {
	all := make([]NodeInfo, 0, len(t.byAddr))
	for _, b := range t.buckets {
		for _, e := range b.nodes {
			if !e.bad() {
				all = append(all, e.NodeInfo)
			}
		}
	}
	return all
}

// stale returns a random target in each bucket that has not changed for
// interval: what a refresh looks up.
func (t *table) stale(now time.Time, interval time.Duration) []ID {
	var targets []ID
	for i, b := range t.buckets {
		if now.Sub(b.changed) >= interval {
			targets = append(targets, t.randomTarget(i))
			b.changed = now // refreshed, whatever the lookup finds
		}
	}
	return targets
}

// randomTarget returns a random ID in bucket i's range.
func (t *table) randomTarget(i int) ID {
	return randomIDWithPrefix(t.self, i, i < len(t.buckets)-1)
}

// questionable returns, for each bucket, its least recently seen node that
// is neither good nor already being pinged, to ping now.
func (t *table) questionable(now time.Time) []*entry {
	var out []*entry
	for _, b := range t.buckets {
		for _, e := range b.nodes {
			if !e.good(now) && !e.pinging {
				e.pinging = true
				out = append(out, e)
				break
			}
		}
	}
	return out
}

// pinged clears the pinging mark of the node at addr after a ping that
// did not time out.
func (t *table) pinged(addr netip.AddrPort) {
	if e := t.byAddr[addr]; e != nil {
		e.pinging = false
	}
}

func (b *bucket) has(id ID) bool {
	for _, e := range b.nodes {
		if e.ID == id {
			return true
		}
	}
	return false
}

// full reports whether every node of the bucket is good.
func (b *bucket) full(now time.Time) bool {
	for _, e := range b.nodes {
		if !e.good(now) {
			return false
		}
	}
	return len(b.nodes) >= K
}

// touch moves e to the end of the nodes, the most recently seen.
func (b *bucket) touch(e *entry) {
	if i := slices.Index(b.nodes, e); i >= 0 && i != len(b.nodes)-1 {
		copy(b.nodes[i:], b.nodes[i+1:])
		b.nodes[len(b.nodes)-1] = e
	}
}

func (b *bucket) addReplacement(e *entry) {
	for i, r := range b.replacements {
		if r.ID == e.ID || r.Addr == e.Addr {
			b.replacements = slices.Delete(b.replacements, i, i+1)
			break
		}
	}
	if len(b.replacements) >= K {
		b.replacements = slices.Delete(b.replacements, 0, 1)
	}
	b.replacements = append(b.replacements, e)
}

// popReplacement takes the newest replacement that can enter the table.
func (b *bucket) popReplacement(t *table) *entry {
	for len(b.replacements) > 0 {
		r := b.replacements[len(b.replacements)-1]
		b.replacements = b.replacements[:len(b.replacements)-1]
		if t.byAddr[r.Addr] != nil || b.has(r.ID) {
			continue
		}
		if t.onePerIP && !isLocal(r.Addr.Addr()) && t.ips[r.Addr.Addr()] > 0 {
			continue
		}
		r.failures = 0
		return r
	}
	return nil
}

// isLocal reports whether a is loopback, private or link-local: where
// BEP 42 does not apply and many nodes may share a host.
func isLocal(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsUnspecified()
}
