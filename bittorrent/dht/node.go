package dht

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// DefaultRouters are the bootstrap routers a Node uses when Config.Routers
// is nil.
var DefaultRouters = []string{
	"router.bittorrent.com:6881",
	"dht.transmissionbt.com:6881",
	"router.utorrent.com:6881",
	"dht.libtorrent.org:25401",
}

// DefaultVersion is the "v" a Node sends: two letters naming the client and
// two bytes of version, as BEP 20's peer ID prefixes do.
const DefaultVersion = "FB\x00\x01"

// Defaults of Config's zero fields.
const (
	DefaultQueryTimeout       = 2 * time.Second
	DefaultQueryRetries       = 1
	DefaultQueryRate          = 20
	DefaultMaxTorrents        = 4096
	DefaultMaxPeersPerTorrent = 512
	DefaultMaxItems           = 2048
	DefaultAlpha              = 3
)

// Errors of queries and lookups.
var (
	ErrTimeout  = errors.New("dht: query timed out")
	ErrClosed   = net.ErrClosed
	ErrNoNodes  = errors.New("dht: no nodes to query")
	ErrNotFound = errors.New("dht: item not found")
)

// Config configures a Node.
type Config struct {
	// ID is the node's ID. When zero, it is random, or derived from
	// PublicIP as BEP 42 has it; the node then changes it to a secure one
	// when the "ip" of responses shows its external address does not
	// match. A node that restarts passes the ID it had (State.ID).
	ID ID
	// IPv6 makes the node one of the IPv6 DHT (BEP 32): it talks to IPv6
	// nodes only and sends "nodes6". A node is of one address family;
	// a client on both runs two.
	IPv6 bool
	// Send sends a datagram. New requires it; Listen sets it. It must not
	// block nor call back into the Node. It may keep b, which the node
	// never changes.
	Send func(to netip.AddrPort, b []byte)
	// Routers are the "host:port" bootstrap routers, resolved at each
	// bootstrap. Nil is DefaultRouters; an empty slice is none.
	Routers []string
	// PublicIP is the address other nodes see this one at, if known: the
	// node starts with an ID secure for it (BEP 42).
	PublicIP netip.Addr
	// EnforceNodeID keeps nodes whose IDs do not match their IPs (BEP 42)
	// out of the routing table and ignores their queries. Nodes at local
	// addresses are exempt.
	EnforceNodeID bool
	// ReadOnly makes the node read-only (BEP 43): it answers no queries
	// and says so in its own, so that others leave it out of their
	// routing tables.
	ReadOnly bool
	// QueryTimeout is how long a query waits for its answer before it is
	// resent or fails. DefaultQueryTimeout when zero.
	QueryTimeout time.Duration
	// QueryRetries is how many times a ping, or a query of Ping and the
	// other single-query methods, is resent before it fails;
	// DefaultQueryRetries when zero, none when negative. Lookups never
	// resend: they route around a node that is silent.
	QueryRetries int
	// QueryRate bounds the queries a source IP gets answered, a second,
	// in bursts of up to five seconds' worth. DefaultQueryRate when zero,
	// unlimited when negative.
	QueryRate float64
	// MaxTorrents and MaxPeersPerTorrent bound the peers announced to the
	// node; MaxItems the BEP 44 items put to it. The defaults when zero.
	MaxTorrents, MaxPeersPerTorrent, MaxItems int
	// Alpha is how many queries a lookup has in flight at once.
	// DefaultAlpha when zero.
	Alpha int
	// OnAnnounce, when set, is told of each peer announced to the node,
	// on the goroutine that called HandleDatagram. It must not block.
	OnAnnounce func(infoHash metainfo.InfoHash, peer netip.AddrPort)
	// Version is the "v" the node sends; DefaultVersion when empty.
	Version string
}

// Node is a node of the mainline DHT (BEP 5): it keeps a routing table,
// answers queries, and looks up nodes, peers and items.
//
// A Node owns no socket. HandleDatagram takes the datagrams that arrive
// for it, and Config.Send sends its own, so one UDP socket can carry the
// DHT, uTP and a UDP tracker client; Listen makes a Node with a fib UDP
// binding of its own. A Node is safe for concurrent use. Its lookups
// (FindNode, GetPeers, Announce, Get, Put) block until they converge, and
// must not run on the goroutine that feeds HandleDatagram.
type Node struct {
	cfg  Config
	send func(netip.AddrPort, []byte)

	mu       sync.Mutex
	id       ID
	fixedID  bool
	table    *table
	txns     map[txnKey]*txn
	nextTID  uint16
	tokens   *tokens
	peers    *peerStore
	items    *itemStore
	limiter  rateLimiter
	voter    ipVoter
	external netip.Addr
	seeds    []netip.AddrPort
	hosts    []string
	routers  map[netip.AddrPort]bool
	timer    *time.Timer
	closed   bool

	bootstrapping bool
	lastBootstrap time.Time
	refreshing    bool
	lastExpire    time.Time

	sent, timeouts, answered int64

	onClose func()
	addr    net.Addr // of Listen's binding
}

type txnKey struct {
	addr netip.AddrPort
	t    string
}

// txn is a query waiting for its answer.
type txn struct {
	key     txnKey
	b       []byte
	retries int
	timeout time.Duration
	timer   *time.Timer
	slow    *time.Timer
	done    func(*msg, error)
}

// maintainEvery is how often the node rotates tokens, expires peers and
// items, pings questionable nodes and refreshes stale buckets.
const maintainEvery = 5 * time.Second

// New returns a Node that sends with cfg.Send.
func New(cfg Config) (*Node, error) {
	if cfg.Send == nil {
		return nil, errors.New("dht: Config.Send is required")
	}
	if cfg.PublicIP.IsValid() && cfg.PublicIP.Unmap().Is4() == cfg.IPv6 {
		return nil, errors.New("dht: Config.PublicIP is not of the node's address family")
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = DefaultQueryTimeout
	}
	if cfg.QueryRetries == 0 {
		cfg.QueryRetries = DefaultQueryRetries
	} else if cfg.QueryRetries < 0 {
		cfg.QueryRetries = 0
	}
	if cfg.QueryRate == 0 {
		cfg.QueryRate = DefaultQueryRate
	}
	if cfg.MaxTorrents <= 0 {
		cfg.MaxTorrents = DefaultMaxTorrents
	}
	if cfg.MaxPeersPerTorrent <= 0 {
		cfg.MaxPeersPerTorrent = DefaultMaxPeersPerTorrent
	}
	if cfg.MaxItems <= 0 {
		cfg.MaxItems = DefaultMaxItems
	}
	if cfg.Alpha <= 0 {
		cfg.Alpha = DefaultAlpha
	}
	if cfg.Version == "" {
		cfg.Version = DefaultVersion
	}
	if cfg.Routers == nil {
		cfg.Routers = DefaultRouters
	}
	now := time.Now()
	n := &Node{
		cfg:     cfg,
		send:    cfg.Send,
		id:      cfg.ID,
		fixedID: !cfg.ID.IsZero(),
		txns:    map[txnKey]*txn{},
		tokens:  newTokens(now),
		peers:   newPeerStore(cfg.MaxTorrents, cfg.MaxPeersPerTorrent),
		items:   &itemStore{items: map[ID]*storedItem{}, max: cfg.MaxItems},
		limiter: rateLimiter{rate: cfg.QueryRate, burst: max(1, 5*cfg.QueryRate), ips: map[netip.Addr]*ipBucket{}},
		hosts:   append([]string(nil), cfg.Routers...),
		routers: map[netip.AddrPort]bool{},
	}
	var b [2]byte
	randomBytes(b[:])
	n.nextTID = uint16(b[0])<<8 | uint16(b[1])
	if n.id.IsZero() {
		if cfg.PublicIP.IsValid() {
			n.id = SecureID(cfg.PublicIP)
			n.external = cfg.PublicIP.Unmap()
		} else {
			n.id = RandomID()
		}
	}
	n.table = n.newTable(n.id)
	n.timer = time.AfterFunc(maintainEvery, n.maintain)
	return n, nil
}

func (n *Node) newTable(id ID) *table {
	t := newTable(id)
	if n.cfg.EnforceNodeID {
		t.accept = func(ni NodeInfo) bool { return IDSecure(ni.ID, ni.Addr.Addr()) }
	}
	return t
}

// ID returns the node's ID.
func (n *Node) ID() ID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id
}

// ExternalIP returns the address other nodes see this one at, as their
// responses tell (BEP 42), or Config.PublicIP; invalid until known.
func (n *Node) ExternalIP() netip.Addr {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.external
}

// Nodes returns the nodes of the routing table that are not bad: what to
// save for a restart (see State).
func (n *Node) Nodes() []NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.table.nodes()
}

// AddNodes adds nodes to bootstrap from, such as those saved before a
// restart. They enter the routing table once they answer. Their IDs may
// be zero.
func (n *Node) AddNodes(nodes ...NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ni := range nodes {
		a := unmap(ni.Addr)
		if !a.IsValid() || a.Port() == 0 || a.Addr().Is4() == n.cfg.IPv6 || len(n.seeds) >= 1024 {
			continue
		}
		dup := false
		for _, s := range n.seeds {
			if s == a {
				dup = true
				break
			}
		}
		if !dup {
			n.seeds = append(n.seeds, a)
		}
	}
}

// AddTorrentNodes adds the "nodes" of a torrent file to bootstrap from:
// addresses at once, host names when the next bootstrap resolves them.
func (n *Node) AddTorrentNodes(nodes []metainfo.Node) {
	for _, tn := range nodes {
		if tn.Port <= 0 || tn.Port > 65535 {
			continue
		}
		if a, err := netip.ParseAddr(tn.Host); err == nil {
			n.AddNodes(NodeInfo{Addr: netip.AddrPortFrom(a, uint16(tn.Port))})
			continue
		}
		n.mu.Lock()
		if len(n.hosts) < 64 {
			n.hosts = append(n.hosts, net.JoinHostPort(tn.Host, strconv.Itoa(tn.Port)))
		}
		n.mu.Unlock()
	}
}

// Stats is a snapshot of a Node.
type Stats struct {
	Nodes, GoodNodes int // in the routing table
	Buckets          int
	Torrents, Peers  int // announced to the node
	Items            int // BEP 44 items stored
	Transactions     int // queries waiting for answers
	// Queries sent, those that timed out, and queries answered, since
	// the node started.
	QueriesSent, QueryTimeouts, QueriesAnswered int64
}

// Stats returns a snapshot of the node.
func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	s := Stats{Nodes: n.table.len(), Buckets: len(n.table.buckets), Torrents: len(n.peers.torrents),
		Items: len(n.items.items), Transactions: len(n.txns),
		QueriesSent: n.sent, QueryTimeouts: n.timeouts, QueriesAnswered: n.answered}
	for _, e := range n.table.byAddr {
		if e.good(now) {
			s.GoodNodes++
		}
	}
	for _, p := range n.peers.torrents {
		s.Peers += len(p)
	}
	return s
}

// Close stops the node: queries waiting for answers fail with ErrClosed,
// and a node of Listen removes its binding.
func (n *Node) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.timer.Stop()
	txns := n.txns
	n.txns = map[txnKey]*txn{}
	onClose := n.onClose
	n.mu.Unlock()
	for _, t := range txns {
		t.stop()
		if t.done != nil {
			t.done(nil, ErrClosed)
		}
	}
	if onClose != nil {
		onClose()
	}
	return nil
}

// HandleDatagram takes a datagram that arrived from addr and reports
// whether it was the DHT's: a bencoded dictionary, which begins with 'd',
// as neither uTP packets nor UDP tracker messages do. Datagrams that
// begin with 'd' but are not valid KRPC messages are dropped and still
// reported as the DHT's. b is not kept after HandleDatagram returns.
func (n *Node) HandleDatagram(from netip.AddrPort, b []byte) bool {
	if len(b) == 0 || b[0] != 'd' {
		return false
	}
	from = unmap(from)
	if !from.IsValid() || from.Port() == 0 || from.Addr().Is4() == n.cfg.IPv6 {
		return true
	}
	m, err := parseMsg(b)
	if err != nil {
		return true
	}
	switch m.Y {
	case "q":
		n.handleQuery(from, m)
	default:
		n.handleResponse(from, m)
	}
	return true
}

// queryOpts are the options of one query.
type queryOpts struct {
	retries   int
	slowAfter time.Duration
	slow      func()
}

// query sends a query to addr; done, when not nil, gets its response or
// error, once, on whichever goroutine gets it.
func (n *Node) query(addr netip.AddrPort, q string, a *args, o queryOpts, done func(*msg, error)) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		if done != nil {
			done(nil, ErrClosed)
		}
		return
	}
	a.ID = n.id[:]
	m := &msg{Y: "q", Q: q, A: a, V: n.cfg.Version}
	if n.cfg.ReadOnly {
		m.RO = 1
	}
	var key txnKey
	for {
		n.nextTID++
		key = txnKey{addr: addr, t: string([]byte{byte(n.nextTID >> 8), byte(n.nextTID)})}
		if n.txns[key] == nil {
			break
		}
	}
	m.T = key.t
	t := &txn{key: key, b: m.encode(), retries: o.retries, timeout: n.cfg.QueryTimeout, done: done}
	n.txns[key] = t
	n.sent++
	t.timer = time.AfterFunc(t.timeout, func() { n.expire(t) })
	if o.slow != nil && o.slowAfter > 0 {
		t.slow = time.AfterFunc(o.slowAfter, o.slow)
	}
	b := t.b
	n.mu.Unlock()
	n.send(addr, b)
}

// expire resends a query that got no answer, or fails it.
func (n *Node) expire(t *txn) {
	n.mu.Lock()
	if n.txns[t.key] != t {
		n.mu.Unlock()
		return
	}
	if t.retries > 0 {
		t.retries--
		t.timer.Reset(t.timeout)
		n.mu.Unlock()
		n.send(t.key.addr, t.b)
		return
	}
	delete(n.txns, t.key)
	n.timeouts++
	n.table.failed(t.key.addr, time.Now())
	n.mu.Unlock()
	if t.slow != nil {
		t.slow.Stop()
	}
	if t.done != nil {
		t.done(nil, ErrTimeout)
	}
}

func (t *txn) stop() {
	t.timer.Stop()
	if t.slow != nil {
		t.slow.Stop()
	}
}

// handleResponse matches a response or error to its query.
func (n *Node) handleResponse(from netip.AddrPort, m *msg) {
	n.mu.Lock()
	t := n.txns[txnKey{addr: from, t: m.T}]
	if t == nil {
		n.mu.Unlock()
		return
	}
	delete(n.txns, t.key)
	t.stop()
	now := time.Now()
	var ping *entry
	var rebootstrap bool
	var err error
	if m.Y == "r" {
		id := ID(m.R.ID)
		if !n.routers[from] {
			ping = n.table.replied(NodeInfo{ID: id, Addr: from}, now)
		}
		if ip, ok := parseCompactAddr(m.IP); ok {
			rebootstrap = n.vote(from.Addr(), ip.Addr(), now)
		}
	} else {
		// An error is an answer too: the node is there.
		if e := n.table.get(from); e != nil {
			e.failures = 0
			e.pinging = false
		}
		err = m.E
	}
	n.mu.Unlock()
	if ping != nil {
		n.ping(ping.Addr)
	}
	if rebootstrap {
		go func() { _ = n.Bootstrap(context.Background()) }()
	}
	if t.done != nil {
		t.done(m, err)
	}
}

// vote counts a response's view of our address, and when the votes settle
// on an address our ID is not secure for, takes a secure one (BEP 42),
// rebuilding the routing table around it. It reports whether the ID
// changed, for the caller to bootstrap again.
func (n *Node) vote(voter, ip netip.Addr, now time.Time) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.Is4() == n.cfg.IPv6 {
		return false
	}
	n.voter.add(voter, ip, now)
	w, ok := n.voter.winner()
	if !ok || w == n.external {
		return false
	}
	n.external = w
	if n.fixedID || isLocal(w) || IDSecure(n.id, w) {
		return false
	}
	n.id = SecureID(w)
	old := n.table
	n.table = n.newTable(n.id)
	for _, e := range old.byAddr {
		n.table.replied(e.NodeInfo, e.lastReply)
		if ne := n.table.get(e.Addr); ne != nil {
			ne.added, ne.lastQuery, ne.replyCount = e.added, e.lastQuery, e.replyCount
		}
	}
	return true
}

// ping pings a node of the table to see whether it is still there; the
// answer, or its absence, updates the table.
func (n *Node) ping(addr netip.AddrPort) {
	n.query(addr, "ping", &args{}, queryOpts{retries: n.cfg.QueryRetries}, func(m *msg, err error) {
		if err == nil {
			n.mu.Lock()
			n.table.pinged(addr)
			n.mu.Unlock()
		}
	})
}

// maxValues is how many peers a get_peers response carries: what keeps it
// within one unfragmented datagram along with the nodes.
func (n *Node) maxValues() int {
	if n.cfg.IPv6 {
		return 32
	}
	return 64
}

// Sample bounds of BEP 51: a response's samples fit in a datagram, and
// indexers are asked to come back after the interval.
const (
	maxSamples     = 20
	sampleInterval = 6 * 60 * 60 // seconds, libtorrent's default
)

// handleQuery answers a query.
func (n *Node) handleQuery(from netip.AddrPort, m *msg) {
	if n.cfg.ReadOnly {
		return
	}
	now := time.Now()
	n.mu.Lock()
	if n.closed || !n.limiter.allow(from.Addr(), now) {
		n.mu.Unlock()
		return
	}
	reply := &msg{T: m.T, Y: "r", IP: appendCompactAddr(nil, from), V: n.cfg.Version}
	r := &ret{ID: append([]byte(nil), n.id[:]...)}
	var kerr *Error
	var announced netip.AddrPort
	var announcedHash ID
	pingIt := false
	a := m.A
	id, ok := m.sender()
	switch {
	case !ok:
		kerr = &Error{Code: ErrCodeProtocol, Msg: "invalid id"}
	case n.cfg.EnforceNodeID && !IDSecure(id, from.Addr()):
		// Not a node we talk to; BEP 42 lets us drop it silently.
		n.mu.Unlock()
		return
	case id == n.id:
		// Ourselves, or someone with our ID: answering only confuses.
		n.mu.Unlock()
		return
	default:
		if m.RO == 0 && !n.table.queried(NodeInfo{ID: id, Addr: from}, now) && !n.routers[from] && n.table.wants(id, now) {
			pingIt = true
		}
		target := func(b []byte) (ID, bool) {
			if len(b) != 20 {
				kerr = &Error{Code: ErrCodeProtocol, Msg: "invalid target or info_hash"}
				return ID{}, false
			}
			return ID(b), true
		}
		switch m.Q {
		case "ping":
		case "find_node":
			if t, ok := target(a.Target); ok {
				n.addNodes(r, t, a, from)
			}
		case "get_peers":
			if ih, ok := target(a.InfoHash); ok {
				for _, p := range n.peers.get(ih, n.maxValues(), n.cfg.IPv6, now) {
					r.Values = append(r.Values, appendCompactAddr(nil, p))
				}
				r.Token = n.tokens.make(from.Addr())
				n.addNodes(r, ih, a, from)
			}
		case "announce_peer":
			ih, ok := target(a.InfoHash)
			if !ok {
				break
			}
			if !n.tokens.valid(a.Token, from.Addr()) {
				kerr = &Error{Code: ErrCodeProtocol, Msg: "bad token"}
				break
			}
			port := int64(from.Port())
			if a.ImpliedPort == 0 {
				if a.Port == nil || *a.Port <= 0 || *a.Port > 65535 {
					kerr = &Error{Code: ErrCodeProtocol, Msg: "invalid port"}
					break
				}
				port = *a.Port
			}
			announced = netip.AddrPortFrom(from.Addr(), uint16(port))
			announcedHash = ih
			n.peers.add(ih, announced, now)
		case "sample_infohashes":
			if t, ok := target(a.Target); ok {
				samples, num := n.peers.sample(maxSamples)
				b := make([]byte, 0, 20*len(samples))
				for _, h := range samples {
					b = append(b, h[:]...)
				}
				interval, total := int64(sampleInterval), int64(num)
				r.Interval, r.Num, r.Samples = &interval, &total, &b
				n.addNodes(r, t, a, from)
			}
		case "get":
			t, ok := target(a.Target)
			if !ok {
				break
			}
			r.Token = n.tokens.make(from.Addr())
			n.addNodes(r, t, a, from)
			if it := n.items.get(t, now); it != nil {
				if it.Mutable() {
					seq := it.Seq
					r.Seq = &seq
					if a.Seq != nil && it.Seq <= *a.Seq {
						break // the asker has it already
					}
					r.K = append([]byte(nil), it.K[:]...)
					r.Sig = append([]byte(nil), it.Sig[:]...)
				}
				r.V = it.V
			}
		case "put":
			kerr = n.put(a, from, now)
		default:
			// Forward compatibility, as libtorrent has it: an unknown
			// query with a target is answered as find_node.
			switch {
			case len(a.Target) == 20:
				n.addNodes(r, ID(a.Target), a, from)
			case len(a.InfoHash) == 20:
				n.addNodes(r, ID(a.InfoHash), a, from)
			default:
				kerr = &Error{Code: ErrCodeMethodUnknown, Msg: "method unknown"}
			}
		}
	}
	if kerr != nil {
		reply.Y, reply.E = "e", kerr
	} else {
		reply.R = r
	}
	b := reply.encode()
	n.answered++
	onAnnounce := n.cfg.OnAnnounce
	n.mu.Unlock()
	n.send(from, b)
	if pingIt {
		n.ping(from)
	}
	if announced.IsValid() && onAnnounce != nil {
		onAnnounce(metainfo.InfoHash(announcedHash), announced)
	}
}

// put stores the item of a put query (BEP 44).
func (n *Node) put(a *args, from netip.AddrPort, now time.Time) *Error {
	if !n.tokens.valid(a.Token, from.Addr()) {
		return &Error{Code: ErrCodeProtocol, Msg: "bad token"}
	}
	if len(a.V) > MaxItemValue {
		return ErrItemTooBig
	}
	it := &Item{V: a.V}
	// A key and signature of zeros are absent: some clients send the
	// zero values of fixed-size fields with an immutable put.
	if !allZero(a.K) || !allZero(a.Sig) {
		if len(a.K) != 32 || len(a.Sig) != 64 || a.Seq == nil {
			return &Error{Code: ErrCodeProtocol, Msg: "invalid k, sig or seq"}
		}
		it.K, it.Sig, it.Salt, it.Seq = [32]byte(a.K), [64]byte(a.Sig), a.Salt, *a.Seq
	}
	if err := it.Verify(); err != nil {
		var e *Error
		if errors.As(err, &e) {
			return e
		}
		return &Error{Code: ErrCodeProtocol, Msg: err.Error()}
	}
	if err := n.items.put(it, a.Cas, now); err != nil {
		var e *Error
		if errors.As(err, &e) {
			return e
		}
		return &Error{Code: ErrCodeServer, Msg: err.Error()}
	}
	return nil
}

// addNodes puts the K closest nodes to target into a response, if the
// query wants those of the node's family (BEP 32).
func (n *Node) addNodes(r *ret, target ID, a *args, from netip.AddrPort) {
	if !a.wants(n.cfg.IPv6, from.Addr().Is6()) {
		return
	}
	closest := n.table.closest(target, K)
	if n.cfg.IPv6 {
		r.Nodes6 = appendCompactNodes(nil, closest, true)
	} else {
		r.Nodes = appendCompactNodes(nil, closest, false)
	}
}

// maintain runs every maintainEvery: it rotates the token secret, expires
// peers and items, pings questionable nodes, refreshes buckets that have
// been quiet for GoodFor and bootstraps a table that has too few nodes.
func (n *Node) maintain() {
	now := time.Now()
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.tokens.rotate(now)
	if now.Sub(n.lastExpire) >= time.Minute {
		n.peers.expire(now)
		n.items.expire(now)
		n.lastExpire = now
	}
	pings := n.table.questionable(now)
	var refresh []ID
	if !n.refreshing && n.table.len() > 0 {
		refresh = n.table.stale(now, GoodFor)
		n.refreshing = len(refresh) > 0
	}
	bootstrap := n.table.len() < K && !n.bootstrapping && now.Sub(n.lastBootstrap) >= time.Minute &&
		(len(n.hosts) > 0 || len(n.seeds) > 0)
	n.timer.Reset(maintainEvery)
	n.mu.Unlock()
	for _, e := range pings {
		n.ping(e.Addr)
	}
	if len(refresh) > 0 {
		go n.refresh(refresh)
	}
	if bootstrap {
		go func() { _ = n.Bootstrap(context.Background()) }()
	}
}

// findAll runs find_node lookups of targets, a few at a time.
func (n *Node) findAll(ctx context.Context, targets []ID) {
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, t := range targets {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = n.FindNode(ctx, t)
			<-sem
		}()
	}
	wg.Wait()
}

// Bootstrap fills the routing table: it looks up its own ID from the
// routers, the nodes AddNodes and AddTorrentNodes added and those already
// in the table, and returns once that converges; a lookup of a random ID
// in each bucket that is not full then goes on in the background. The
// node also bootstraps by itself whenever its table has fewer than K
// nodes.
func (n *Node) Bootstrap(ctx context.Context) error {
	n.mu.Lock()
	if n.bootstrapping || n.closed {
		n.mu.Unlock()
		return nil
	}
	n.bootstrapping = true
	hosts := append([]string(nil), n.hosts...)
	seeds := append([]netip.AddrPort(nil), n.seeds...)
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.bootstrapping = false
		n.lastBootstrap = time.Now()
		n.mu.Unlock()
	}()
	routers := n.resolve(ctx, hosts)
	n.mu.Lock()
	for _, r := range routers {
		n.routers[r] = true
	}
	n.mu.Unlock()
	seeds = append(seeds, routers...)
	// The answers may show our external address and give us a new ID
	// (BEP 42): the lookup of our own ID is then done again, for the new
	// one, as the table is now arranged around it.
	for range 3 {
		id := n.ID()
		l := n.findNodeLookup(id)
		l.addSeeds(seeds)
		if err := l.run(ctx); err != nil && !errors.Is(err, ErrNoNodes) {
			return err
		}
		if n.ID() == id {
			break
		}
	}
	n.mu.Lock()
	var targets []ID
	t := n.table
	for i, b := range t.buckets {
		if len(b.nodes) < K && i < len(t.buckets)-1 {
			targets = append(targets, t.randomTarget(i))
		}
	}
	size := t.len()
	refresh := len(targets) > 0 && !n.refreshing && size > 0
	n.refreshing = n.refreshing || refresh
	n.mu.Unlock()
	if size == 0 {
		return ErrNoNodes
	}
	if refresh {
		go n.refresh(targets)
	}
	return nil
}

// refresh looks up a random target in each of the buckets that need it,
// in the background; one refresh runs at a time.
func (n *Node) refresh(targets []ID) {
	n.findAll(context.Background(), targets)
	n.mu.Lock()
	n.refreshing = false
	n.mu.Unlock()
}

// resolve resolves "host:port" strings to addresses of the node's family,
// the names all at once.
func (n *Node) resolve(ctx context.Context, hosts []string) []netip.AddrPort {
	network := "ip4"
	if n.cfg.IPv6 {
		network = "ip6"
	}
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out []netip.AddrPort
	)
	for _, hp := range hosts {
		host, ps, err := net.SplitHostPort(hp)
		if err != nil {
			continue
		}
		port, err := strconv.ParseUint(ps, 10, 16)
		if err != nil || port == 0 {
			continue
		}
		if a, err := netip.ParseAddr(host); err == nil {
			if a.Unmap().Is4() != n.cfg.IPv6 {
				mu.Lock()
				out = append(out, netip.AddrPortFrom(a.Unmap(), uint16(port)))
				mu.Unlock()
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			addrs, err := net.DefaultResolver.LookupNetIP(rctx, network, host)
			cancel()
			if err != nil {
				return
			}
			mu.Lock()
			for _, a := range addrs {
				out = append(out, netip.AddrPortFrom(a.Unmap(), uint16(port)))
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// call sends one query and waits for its answer.
func (n *Node) call(ctx context.Context, addr netip.AddrPort, q string, a *args) (*msg, error) {
	addr = unmap(addr)
	if addr.Addr().Is4() == n.cfg.IPv6 {
		return nil, errors.New("dht: address of the wrong family for the node")
	}
	ch := make(chan result, 1)
	n.query(addr, q, a, queryOpts{retries: n.cfg.QueryRetries}, func(m *msg, err error) { ch <- result{m, err} })
	select {
	case r := <-ch:
		return r.m, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type result struct {
	m   *msg
	err error
}

// Ping pings the node at addr and returns its ID.
func (n *Node) Ping(ctx context.Context, addr netip.AddrPort) (ID, error) {
	m, err := n.call(ctx, addr, "ping", &args{})
	if err != nil {
		return ID{}, err
	}
	return ID(m.R.ID), nil
}

// Samples is a node's answer to sample_infohashes (BEP 51).
type Samples struct {
	// Interval is how long the node asks to be left alone before the
	// next sample.
	Interval time.Duration
	// Num is how many info hashes the node has.
	Num int
	// InfoHashes are a random sample of them.
	InfoHashes []metainfo.InfoHash
	// Nodes are the nodes the node knows closest to the target.
	Nodes []NodeInfo
}

// SampleInfohashes asks the node at addr for a sample of the info hashes
// it stores peers of (BEP 51), and for its nodes closest to target, as a
// DHT indexer walks the keyspace.
func (n *Node) SampleInfohashes(ctx context.Context, addr netip.AddrPort, target ID) (*Samples, error) {
	m, err := n.call(ctx, addr, "sample_infohashes", &args{Target: target[:]})
	if err != nil {
		return nil, err
	}
	r := m.R
	if r.Samples == nil {
		return nil, errors.New("dht: node does not support sample_infohashes")
	}
	if len(*r.Samples)%20 != 0 {
		return nil, errMalformed
	}
	s := &Samples{}
	if r.Interval != nil {
		s.Interval = time.Duration(*r.Interval) * time.Second
	}
	if r.Num != nil {
		s.Num = int(*r.Num)
	}
	for b := *r.Samples; len(b) > 0; b = b[20:] {
		s.InfoHashes = append(s.InfoHashes, metainfo.InfoHash(b[:20]))
	}
	s.Nodes = n.responseNodes(r)
	return s, nil
}

// responseNodes returns the nodes of a response of the node's family.
func (n *Node) responseNodes(r *ret) []NodeInfo {
	var nodes []NodeInfo
	if n.cfg.IPv6 {
		nodes, _ = parseCompactNodes(r.Nodes6, true)
	} else {
		nodes, _ = parseCompactNodes(r.Nodes, false)
	}
	return nodes
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
