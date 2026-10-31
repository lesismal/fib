package turn

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

var (
	ErrNotTURN    = errors.New("turn: neither STUN nor ChannelData")
	ErrNoRelay    = errors.New("turn: no relayed address available")
	ErrDropped    = errors.New("turn: datagram dropped")
	errNoAllocate = errors.New("turn: ServerConfig.AllocateRelay is nil")
)

// Transport is how a client reaches the server: UDP, or a stream, TCP or
// TLS over TCP (RFC 8656 section 3.1). Peers are reached over UDP either
// way.
type Transport uint8

const (
	TransportUDP Transport = iota
	TransportTCP
)

func (t Transport) String() string {
	if t == TransportTCP {
		return "tcp"
	}
	return "udp"
}

// FiveTuple names an allocation by the client's address, the server's
// address the client reaches, and the transport between them: RFC 8656's
// 5-tuple. A server listening on several addresses keeps the allocations
// made through each apart, and over TCP each connection is a 5-tuple of
// its own.
type FiveTuple struct {
	Client, Server netip.AddrPort
	Transport      Transport
}

// Relay is one relayed transport address the host opened for an
// allocation: a UDP socket of its own, bound to the address clients are
// told. What it receives the host passes to Server.HandleRelayPacket with
// Addr; the server sends to peers through SendTo.
type Relay interface {
	// Addr is the relayed transport address, XOR-RELAYED-ADDRESS. Behind a
	// 1:1 NAT it is the public address, not the one the socket is bound
	// to; it is the key the host passes HandleRelayPacket either way.
	Addr() netip.AddrPort
	// SendTo sends b to peer from the relayed address. b is valid only
	// during the call. It may be called from several goroutines at once,
	// and must not wait: a peer the host cannot send to at once is one it
	// drops the datagram for, or queues it.
	SendTo(peer netip.AddrPort, b []byte)
	// Close releases the relayed address, once its allocation ends.
	Close()
}

// ServerConfig configures a Server. Realm, Auth, Send and AllocateRelay are
// required.
type ServerConfig struct {
	// Realm is the realm of the long-term credentials, sent in every
	// challenge; clients derive their key from it.
	Realm string
	// Auth looks up a user's key. StaticAuth and EphemeralAuth are two.
	Auth AuthFunc
	// Software, if set, is sent as SOFTWARE in responses.
	Software string

	// Send sends b to t.Client from the server address t.Server. b is
	// valid only during the call. It may be called from several goroutines
	// at once, and must not wait.
	Send func(t FiveTuple, b []byte)
	// AllocateRelay opens a relayed transport address for a new
	// allocation of user's, of IPv6 if ipv6 is set and IPv4 otherwise, as
	// REQUESTED-ADDRESS-FAMILY asks, and calls done with it once it is
	// open, exactly once: at once, from AllocateRelay itself, or later
	// from any goroutine. The Allocate request is answered when done
	// runs; its retransmissions meanwhile are dropped, and the allocation
	// counts against the quotas from the request on. An error refuses the
	// allocation with 508 (Insufficient Capacity), as when a port range is
	// exhausted. AllocateRelay must not wait: a host whose sockets take a
	// while to open opens them elsewhere, on a goroutine of its own.
	AllocateRelay func(t FiveTuple, user string, ipv6 bool, done func(Relay, error))

	// Limits are the lifetimes and quotas, which SetLimits changes while
	// the server runs.
	Limits
	// NonceSecret keys the nonces' HMAC. Random if empty; servers behind
	// one load balancer that may each see a client's requests share one.
	NonceSecret []byte

	// OnRelay, if set, is told of every datagram before it is relayed: n
	// bytes of user's, to a peer if toPeer and from one otherwise. It
	// returning false drops the datagram, which is where a host enforces
	// bandwidth limits and counts traffic. It may be called from several
	// goroutines at once.
	OnRelay func(user string, toPeer bool, n int) bool
	// OnEvent, if set, is told of what happens to allocations and
	// requests, for metrics and logs: see Event. It runs on the goroutine
	// that handled the packet, or that called HandleTimeout or Close, with
	// no lock held, and must not wait.
	OnEvent func(Event)

	// PeerAllowed decides which peers clients may install permissions and
	// channels for, and so relay to. A permission is for an IP address
	// alone (RFC 8656 section 9), so CreatePermission asks with port 0,
	// which an address any of whose ports is allowed allows; ChannelBind
	// asks with the channel's peer, and every datagram relayed to a peer
	// or from one is asked about again with its port, which is how a
	// server allows its own address on its relayed ports alone (see
	// Server.HoldsRelay) while its other ports stay closed to the relay.
	// It may be called from several goroutines at once.
	//
	// If nil, an address in AllowedPeers is allowed, then one in
	// DeniedPeers or that DeniedByDefault reports is refused, and any other
	// is allowed, whatever the port. The default keeps the server from
	// relaying into the network it runs in, its loopback and private
	// ranges, which would make it a proxy into them for anyone with
	// credentials (an SSRF hole): a deployment whose peers are on a
	// private network lists its prefixes in AllowedPeers.
	PeerAllowed  func(peer netip.AddrPort) bool
	AllowedPeers []netip.Prefix
	DeniedPeers  []netip.Prefix

	// SweepInterval is how often HandleTimeout looks for expired
	// allocations, permissions and channels to free; expired ones are
	// never used meanwhile. Default 1s.
	SweepInterval time.Duration
	// Rand is where nonce secrets and indication transaction IDs come
	// from. Default crypto/rand.
	Rand io.Reader
}

// Limits are a server's lifetimes and quotas: ServerConfig's, and what
// Server.SetLimits changes them to while it runs.
type Limits struct {
	// DefaultLifetime is the lifetime of an allocation whose client asks
	// for none or for less, 600s by default; MaxLifetime caps what a client
	// may ask for, 3600s by default.
	DefaultLifetime, MaxLifetime time.Duration
	// NonceLifetime is how long a nonce is good for before a request with
	// it is answered 438 (Stale Nonce) and a fresh one. Default 10 minutes.
	// Nonces are not stored: each is its expiry and an HMAC of it and the
	// client's address under NonceSecret.
	NonceLifetime time.Duration

	// MaxAllocations caps the allocations the server holds in all, which
	// it refuses beyond with 508 (Insufficient Capacity); and
	// MaxAllocationsPerUser those of one username, refused with 486
	// (Allocation Quota Reached). Zero means no limit.
	MaxAllocations, MaxAllocationsPerUser int
	// MaxPermissions caps the permissions one allocation holds, and
	// MaxChannels its channel bindings, so that a client with credentials
	// cannot have the server keep memory without bound: a CreatePermission
	// or ChannelBind that would go past either is refused with 508
	// (Insufficient Capacity), as RFC 8656 section 9 allows, once the
	// allocation's expired ones are forgotten. A permission a channel
	// installs counts. Zero means no limit beyond the 4096 channel numbers.
	MaxPermissions, MaxChannels int
}

// withDefaults is l with its zero lifetimes set to their defaults.
func (l Limits) withDefaults() Limits {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&l.DefaultLifetime, DefaultLifetime)
	def(&l.MaxLifetime, time.Hour)
	if l.MaxLifetime < l.DefaultLifetime {
		l.MaxLifetime = l.DefaultLifetime
	}
	def(&l.NonceLifetime, 10*time.Minute)
	return l
}

func (c *ServerConfig) setDefaults() {
	c.Limits = c.Limits.withDefaults()
	if c.SweepInterval <= 0 {
		c.SweepInterval = time.Second
	}
	if c.Rand == nil {
		c.Rand = rand.Reader
	}
}

// channelQuarantine is how long a channel number and peer stay tied to each
// other after their binding expires, so that neither is rebound to another
// while the peer's last datagrams may still arrive (RFC 8656 section 12).
const channelQuarantine = 5 * time.Minute

type chanBinding struct {
	number  uint16
	peer    netip.AddrPort
	expires time.Time
}

// allocation is one client's. Its fields up to mu are set when it is made
// and never change; mu guards the rest.
type allocation struct {
	tuple     FiveTuple
	user      string
	key       []byte
	relay     Relay
	relayAddr netip.AddrPort
	// txID and lifetime are those of the Allocate request that made the
	// allocation, to answer its retransmissions alike.
	txID     [stun.TransactionIDSize]byte
	lifetime time.Duration
	// toPeer and toClient count what was relayed each way.
	toPeer, toClient trafficCounter

	mu sync.Mutex
	// freed is set once the allocation has left the server's tables, so
	// that a request or datagram that found it just before does nothing.
	freed      bool
	expires    time.Time
	perms      map[netip.Addr]time.Time
	chans      map[uint16]*chanBinding
	chanByPeer map[netip.AddrPort]*chanBinding
}

// pendingAlloc is an Allocate request waiting for its relayed address.
type pendingAlloc struct {
	tuple    FiveTuple
	user     string
	key      []byte
	txID     [stun.TransactionIDSize]byte
	lifetime time.Duration
	// now is the request's time, which the lifetime runs from.
	now time.Time
	// done is set by the first call of the host's done.
	done atomic.Bool
	// canceled, guarded by Server.mu, is set when the client's connection
	// closes or the server does before the relay is ready.
	canceled bool
}

// shardCount is the number of shards the allocations are spread over, so
// that datagrams of different allocations look theirs up without contending
// for one lock.
const shardCount = 64

// shard holds the allocations whose client address, or relayed address,
// hashes to it. The padding keeps two shards' locks off one cache line.
type shard struct {
	mu      sync.RWMutex
	allocs  map[FiveTuple]*allocation
	byRelay map[netip.AddrPort]*allocation
	_       [64]byte
}

// Server is the core of a TURN server (RFC 8656) relaying UDP for clients
// that reach it over UDP, with the long-term credentials of RFC 8489. It
// does no I/O: the host passes it every datagram the listening socket
// receives through HandlePacket and every datagram a relayed address
// receives through HandleRelayPacket, sends what it emits through
// ServerConfig.Send and Relay.SendTo, opens relayed addresses when
// ServerConfig.AllocateRelay asks, and passes the time through NextTimeout
// and HandleTimeout. Package turnserver runs it on a fib engine.
//
// Its methods may be called from any number of goroutines at once.
// Datagrams of different allocations are relayed in parallel: each
// allocation has a lock of its own, and the tables that find one are
// spread over shards. The quotas and the allocations in the making are
// behind one lock, which only Allocate, Refresh to zero, the sweep and
// Close take. No lock is held while a callback runs.
type Server struct {
	cfg      ServerConfig
	nonceKey []byte
	// indID is the random prefix of Data indications' transaction IDs and
	// indSeq the counter in their last eight bytes.
	indID  [stun.TransactionIDSize]byte
	indSeq atomic.Uint64

	// limits and auth are what SetLimits and SetAuth last set; draining
	// is set by Drain.
	limits   atomic.Pointer[Limits]
	auth     atomic.Pointer[AuthFunc]
	draining atomic.Bool

	shards [shardCount]shard

	// What freed allocations relayed, and the datagrams, requests and
	// credentials refused, for Stats.
	goneToPeer, goneToClient     trafficCounter
	dropped, refused, authFailed atomic.Uint64

	// mu guards the fields below it, and orders every change to the
	// shards' tables: lookups take only a shard's lock.
	mu        sync.Mutex
	count     int
	perUser   map[string]int
	pending   map[FiveTuple]*pendingAlloc
	nextSweep time.Time
	closed    bool
}

// scratch holds what handling one packet decodes and builds, so that
// packets handled at once on several goroutines each have their own.
type scratch struct {
	in, out stun.Message
	buf     []byte
}

var scratchPool = sync.Pool{New: func() any { return new(scratch) }}

func getScratch() *scratch  { return scratchPool.Get().(*scratch) }
func putScratch(x *scratch) { scratchPool.Put(x) }

// NewServer returns a server configured by config.
func NewServer(config ServerConfig) (*Server, error) {
	switch {
	case config.Realm == "":
		return nil, errors.New("turn: ServerConfig.Realm is empty")
	case config.Auth == nil:
		return nil, errors.New("turn: ServerConfig.Auth is nil")
	case config.Send == nil:
		return nil, errors.New("turn: ServerConfig.Send is nil")
	case config.AllocateRelay == nil:
		return nil, errNoAllocate
	}
	config.setDefaults()
	s := &Server{
		cfg:     config,
		perUser: make(map[string]int),
		pending: make(map[FiveTuple]*pendingAlloc),
	}
	for i := range s.shards {
		s.shards[i].allocs = make(map[FiveTuple]*allocation)
		s.shards[i].byRelay = make(map[netip.AddrPort]*allocation)
	}
	s.limits.Store(&config.Limits)
	s.auth.Store(&config.Auth)
	s.nonceKey = config.NonceSecret
	if len(s.nonceKey) == 0 {
		s.nonceKey = make([]byte, 32)
		if _, err := io.ReadFull(config.Rand, s.nonceKey); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(config.Rand, s.indID[:]); err != nil {
		return nil, err
	}
	return s, nil
}

// hashAddr mixes an address and port into a shard index.
func hashAddr(a netip.AddrPort) uint64 {
	b := a.Addr().As16()
	h := binary.LittleEndian.Uint64(b[:8]) ^ binary.LittleEndian.Uint64(b[8:])*0x9E3779B97F4A7C15
	h ^= uint64(a.Port()) * 0xBF58476D1CE4E5B9
	h ^= h >> 29
	return h
}

func (s *Server) tupleShard(t FiveTuple) *shard {
	return &s.shards[hashAddr(t.Client)%shardCount]
}

func (s *Server) relayShard(a netip.AddrPort) *shard {
	return &s.shards[hashAddr(a)%shardCount]
}

// lookup returns t's allocation, expired or not.
func (s *Server) lookup(t FiveTuple) *allocation {
	sh := s.tupleShard(t)
	sh.mu.RLock()
	a := sh.allocs[t]
	sh.mu.RUnlock()
	return a
}

func (s *Server) lookupRelay(addr netip.AddrPort) *allocation {
	sh := s.relayShard(addr)
	sh.mu.RLock()
	a := sh.byRelay[addr]
	sh.mu.RUnlock()
	return a
}

// Allocations is the number of allocations the server holds, including
// expired ones the next sweep will free.
func (s *Server) Allocations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// RelayFor returns the relayed address of the allocation t made, if any.
func (s *Server) RelayFor(t FiveTuple) (netip.AddrPort, bool) {
	a := s.lookup(normTuple(t))
	if a == nil {
		return netip.AddrPort{}, false
	}
	return a.relayAddr, true
}

func normTuple(t FiveTuple) FiveTuple {
	return FiveTuple{unmap(t.Client), unmap(t.Server), t.Transport}
}

// DeniedByDefault reports whether ip is in a range a TURN server should not
// relay to unless told to: unspecified, loopback, private (RFC 1918 and
// unique local), link-local, multicast and broadcast addresses, shared
// address space (RFC 6598), the "this network", IETF protocol assignment,
// benchmarking and reserved IPv4 ranges, and the IPv6 prefixes that embed
// an IPv4 address a translator would reach (NAT64, 6to4), deprecated
// site-local addresses and discard-only addresses. These are where a relay
// would reach the server's own network rather than the Internet.
func DeniedByDefault(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return true
	}
	for _, p := range deniedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

var deniedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("100::/64"),
}

func (s *Server) peerAllowed(peer netip.AddrPort) bool {
	if s.cfg.PeerAllowed != nil {
		return s.cfg.PeerAllowed(peer)
	}
	ip := peer.Addr()
	for _, p := range s.cfg.AllowedPeers {
		if p.Contains(ip) {
			return true
		}
	}
	for _, p := range s.cfg.DeniedPeers {
		if p.Contains(ip) {
			return false
		}
	}
	return !DeniedByDefault(ip)
}

// HandlePacket processes a datagram b the listening socket received, from
// t.Client to t.Server: a STUN request or indication, or ChannelData. b is
// not retained. The error, if any, says why nothing was done with it; the
// host may log it or not.
func (s *Server) HandlePacket(t FiveTuple, b []byte, now time.Time) error {
	err := s.handlePacket(normTuple(t), b, now)
	if err != nil {
		s.dropped.Add(1)
	}
	return err
}

func (s *Server) handlePacket(t FiveTuple, b []byte, now time.Time) error {
	if IsChannelData(b) {
		return s.handleChannelData(t, b, now)
	}
	if !stun.IsMessage(b) {
		return ErrNotTURN
	}
	x := getScratch()
	defer putScratch(x)
	m := &x.in
	if err := m.Decode(b); err != nil {
		return err
	}
	if m.Contains(stun.AttrFingerprint) {
		if err := m.CheckFingerprint(); err != nil {
			return err
		}
	}
	switch m.Type.Class {
	case stun.ClassRequest:
		s.handleRequest(&exchange{s: s, t: t, m: m, x: x, now: now})
		return nil
	case stun.ClassIndication:
		if m.Type.Method == stun.MethodSend {
			return s.handleSend(t, m, now)
		}
	}
	return ErrDropped
}

// lockLive locks a and reports whether it is still the server's and has not
// expired, freeing it if it has; a is left locked only when it reports
// true.
func (s *Server) lockLive(a *allocation, now time.Time) bool {
	a.mu.Lock()
	if a.freed {
		a.mu.Unlock()
		return false
	}
	if !now.Before(a.expires) {
		a.mu.Unlock()
		s.free(a, FreedExpired)
		return false
	}
	return true
}

// live returns t's allocation if it has one that has not expired, freeing
// one that has.
func (s *Server) live(t FiveTuple, now time.Time) *allocation {
	a := s.lookup(t)
	if a == nil || !s.lockLive(a, now) {
		return nil
	}
	a.mu.Unlock()
	return a
}

// free takes a out of the server's tables and releases it, unless another
// goroutine got there first.
func (s *Server) free(a *allocation, why FreeReason) {
	s.mu.Lock()
	ok := s.unlinkLocked(a)
	s.mu.Unlock()
	if ok {
		s.release(a, why)
	}
}

// unlinkLocked takes a out of the tables, reporting whether it was there.
// Callers hold s.mu.
func (s *Server) unlinkLocked(a *allocation) bool {
	sh := s.tupleShard(a.tuple)
	sh.mu.Lock()
	ok := sh.allocs[a.tuple] == a
	if ok {
		delete(sh.allocs, a.tuple)
	}
	sh.mu.Unlock()
	if !ok {
		return false
	}
	rs := s.relayShard(a.relayAddr)
	rs.mu.Lock()
	if rs.byRelay[a.relayAddr] == a {
		delete(rs.byRelay, a.relayAddr)
	}
	rs.mu.Unlock()
	s.count--
	if s.perUser[a.user]--; s.perUser[a.user] <= 0 {
		delete(s.perUser, a.user)
	}
	return true
}

// release marks a freed, closes its relayed address and says so. Callers
// hold no lock.
func (s *Server) release(a *allocation, why FreeReason) {
	a.mu.Lock()
	a.freed = true
	a.mu.Unlock()
	a.relay.Close()
	toPeer, toClient := a.toPeer.load(), a.toClient.load()
	s.goneToPeer.addTraffic(toPeer)
	s.goneToClient.addTraffic(toClient)
	s.emit(Event{Kind: EventFreed, Tuple: a.tuple, User: a.user, Relay: a.relayAddr, Reason: why,
		ToPeer: toPeer, ToClient: toClient})
}

// SetLimits changes the lifetimes and quotas while the server runs, its
// zero lifetimes set to their defaults. They apply from the next request
// on: an allocation, permission or channel held past a lowered quota is
// kept until it ends, and a lifetime already granted runs its course.
func (s *Server) SetLimits(l Limits) {
	l = l.withDefaults()
	s.limits.Store(&l)
}

// Limits returns the lifetimes and quotas in force.
func (s *Server) Limits() Limits { return *s.limits.Load() }

// SetAuth changes how credentials are checked, from the next request on,
// those on allocations already made included; nil is ignored. A user the
// new AuthFunc refuses can no longer refresh, and its allocations end
// with their lifetimes.
func (s *Server) SetAuth(auth AuthFunc) {
	if auth != nil {
		s.auth.Store(&auth)
	}
}

// Drain has the server refuse new allocations with 508 (Insufficient
// Capacity) while those it holds go on relaying, and refreshing, until
// they end or Close: the server being replaced lets its clients finish, or
// move to another as their refreshes fail. It cannot be undone.
func (s *Server) Drain() { s.draining.Store(true) }

// Draining reports whether Drain was called.
func (s *Server) Draining() bool { return s.draining.Load() }

// ClientClosed frees the allocation of t, a client whose stream connection
// closed: over TCP and TLS an allocation lasts as long as its connection
// (RFC 8656 section 3.1). An allocation still being made for it is
// refused once its relayed address is ready, and the address closed.
func (s *Server) ClientClosed(t FiveTuple) {
	t = normTuple(t)
	s.mu.Lock()
	if p := s.pending[t]; p != nil {
		p.canceled = true
	}
	s.mu.Unlock()
	if a := s.lookup(t); a != nil {
		s.free(a, FreedClosed)
	}
}

// Close frees every allocation, closing its relayed address. An allocation
// whose relayed address is still being opened is refused once it is, and
// the address closed. Nothing is relayed or allocated after.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	var gone []*allocation
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, a := range sh.allocs {
			gone = append(gone, a)
		}
		sh.mu.RUnlock()
	}
	for _, a := range gone {
		s.unlinkLocked(a)
	}
	for _, p := range s.pending {
		p.canceled = true
	}
	s.mu.Unlock()
	for _, a := range gone {
		s.release(a, FreedClosed)
	}
}

// knownAttr reports whether a request's attribute of type t is one the
// server understands. DONT-FRAGMENT, EVEN-PORT, RESERVATION-TOKEN and
// PASSWORD-ALGORITHM are not: their requests are answered 420, which RFC
// 8656 section 7.2 has a server that cannot set the DF bit do for the
// first.
func knownAttr(t stun.AttrType) bool {
	switch t {
	case stun.AttrUsername, stun.AttrRealm, stun.AttrNonce, stun.AttrMessageIntegrity,
		stun.AttrMessageIntegritySHA256, stun.AttrLifetime, stun.AttrRequestedTransport,
		stun.AttrRequestedAddressFamily, stun.AttrXORPeerAddress, stun.AttrChannelNumber:
		return true
	}
	return !t.Required()
}

// exchange is one request being answered: what it came with, and the
// credentials it was found to have.
type exchange struct {
	s    *Server
	t    FiveTuple
	m    *stun.Message
	x    *scratch
	now  time.Time
	user string
	key  []byte
}

// fail answers the request with an error, and, for a request whose
// credentials passed, tells OnEvent it was refused.
func (e *exchange) fail(code int, reason string) {
	e.s.errorHeader(e.x, e.m, code, reason)
	e.s.finish(e.x, e.t, e.key)
	if e.key != nil {
		e.s.refusedEvent(e.t, e.user, e.m.Type.Method, code)
	}
}

func (s *Server) refusedEvent(t FiveTuple, user string, method stun.Method, code int) {
	s.refused.Add(1)
	s.emit(Event{Kind: EventRefused, Tuple: t, User: user, Method: method, Code: code})
}

// success starts a success response, which send sends.
func (e *exchange) success() *stun.Message {
	o := &e.x.out
	o.Build(stun.MessageType{Method: e.m.Type.Method, Class: stun.ClassSuccessResponse}, e.m.TransactionID)
	return o
}

func (e *exchange) send() { e.s.finish(e.x, e.t, e.key) }

func (s *Server) handleRequest(e *exchange) {
	m := e.m
	switch m.Type.Method {
	case stun.MethodBinding:
		// A TURN server is a STUN server too, which ICE agents ask for
		// their server-reflexive address without credentials.
		o := &e.x.out
		o.Build(stun.BindingSuccess, m.TransactionID)
		o.AddXORAddress(stun.AttrXORMappedAddress, e.t.Client)
		e.send()
		return
	case stun.MethodAllocate, stun.MethodRefresh, stun.MethodCreatePermission, stun.MethodChannelBind:
	default:
		e.fail(stun.CodeBadRequest, "Bad Request")
		return
	}
	if !s.authenticate(e) {
		return
	}
	var unknown []stun.AttrType
	for i := range m.Len() {
		if typ, _ := m.Attr(i); !knownAttr(typ) {
			unknown = append(unknown, typ)
		}
	}
	if len(unknown) > 0 {
		o := s.errorHeader(e.x, m, stun.CodeUnknownAttribute, "Unknown Attribute")
		o.AddUnknownAttributes(unknown)
		e.send()
		s.refusedEvent(e.t, e.user, m.Type.Method, stun.CodeUnknownAttribute)
		return
	}
	a := s.live(e.t, e.now)
	if a != nil && a.user != e.user {
		// Requests on an allocation must use its credentials (RFC 8656
		// section 5).
		e.fail(stun.CodeWrongCredentials, "Wrong Credentials")
		return
	}
	switch m.Type.Method {
	case stun.MethodAllocate:
		s.allocate(e, a)
	case stun.MethodRefresh:
		s.refresh(e, a)
	case stun.MethodCreatePermission:
		s.createPermission(e, a)
	case stun.MethodChannelBind:
		s.channelBind(e, a)
	}
}

// authenticate checks a request's long-term credentials (RFC 8489 section
// 9.2.4), answering it itself when they fail: 401 with the realm and a
// nonce for a request without credentials or with wrong ones, 400 for
// incomplete ones, and 438 with a fresh nonce for a stale one. It sets the
// exchange's user and key when they pass.
func (s *Server) authenticate(e *exchange) bool {
	m := e.m
	if !m.Contains(stun.AttrMessageIntegrity) {
		s.challenge(e, stun.CodeUnauthorized, "Unauthorized")
		return false
	}
	u, okUser := m.Get(stun.AttrUsername)
	realm, okRealm := m.Get(stun.AttrRealm)
	nonce, okNonce := m.Get(stun.AttrNonce)
	if !okUser || !okRealm || !okNonce {
		e.fail(stun.CodeBadRequest, "Bad Request")
		s.authFailedEvent(e, string(u), stun.CodeBadRequest)
		return false
	}
	if !s.validNonce(nonce, e.t.Client, e.now) {
		s.challenge(e, stun.CodeStaleNonce, "Stale Nonce")
		return false
	}
	if string(realm) != s.cfg.Realm {
		s.challenge(e, stun.CodeUnauthorized, "Unauthorized")
		s.authFailedEvent(e, string(u), stun.CodeUnauthorized)
		return false
	}
	key, ok := (*s.auth.Load())(string(u), string(realm))
	if !ok || m.CheckIntegrity(key) != nil {
		s.challenge(e, stun.CodeUnauthorized, "Unauthorized")
		s.authFailedEvent(e, string(u), stun.CodeUnauthorized)
		return false
	}
	e.user, e.key = string(u), key
	return true
}

func (s *Server) authFailedEvent(e *exchange, user string, code int) {
	s.authFailed.Add(1)
	s.emit(Event{Kind: EventAuthFailed, Tuple: e.t, User: user, Method: e.m.Type.Method, Code: code})
}

// nonceSize is the length of a nonce: 16 hex digits of its expiry in Unix
// seconds and 24 of the HMAC.
const nonceSize = 16 + 24

func (s *Server) makeNonce(client netip.AddrPort, expiry int64) []byte {
	var b [nonceSize]byte
	hex.Encode(b[:16], binary.BigEndian.AppendUint64(nil, uint64(expiry)))
	mac := hmac.New(sha256.New, s.nonceKey)
	mac.Write(b[:16])
	addr, _ := client.MarshalBinary()
	mac.Write(addr)
	hex.Encode(b[16:], mac.Sum(nil)[:12])
	return b[:]
}

func (s *Server) validNonce(nonce []byte, client netip.AddrPort, now time.Time) bool {
	if len(nonce) != nonceSize {
		return false
	}
	expiry, err := strconv.ParseUint(string(nonce[:16]), 16, 64)
	if err != nil || int64(expiry) <= now.Unix() {
		return false
	}
	return hmac.Equal(nonce, s.makeNonce(client, int64(expiry)))
}

func (s *Server) challenge(e *exchange, code int, reason string) {
	o := s.errorHeader(e.x, e.m, code, reason)
	o.AddString(stun.AttrRealm, s.cfg.Realm)
	o.Add(stun.AttrNonce, s.makeNonce(e.t.Client, e.now.Add(s.limits.Load().NonceLifetime).Unix()))
	s.finish(e.x, e.t, nil)
}

func (s *Server) errorHeader(x *scratch, m *stun.Message, code int, reason string) *stun.Message {
	o := &x.out
	o.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassErrorResponse}, m.TransactionID)
	o.AddErrorCode(code, reason)
	return o
}

// finish adds SOFTWARE, MESSAGE-INTEGRITY with key if any, and FINGERPRINT
// to the response being built in x, and sends it to t.
func (s *Server) finish(x *scratch, t FiveTuple, key []byte) {
	o := &x.out
	if s.cfg.Software != "" {
		o.AddString(stun.AttrSoftware, s.cfg.Software)
	}
	if key != nil {
		o.AddIntegrity(key)
	}
	o.AddFingerprint()
	s.cfg.Send(t, o.Raw)
}

// lifetime is the lifetime a request asks for, bounded by the server's
// default and maximum (RFC 8656 section 7.2), or zero if it asks for zero.
func (s *Server) lifetime(m *stun.Message) time.Duration {
	lim := s.limits.Load()
	l, err := m.Lifetime()
	if err != nil {
		return lim.DefaultLifetime
	}
	if l == 0 {
		return 0
	}
	return max(min(l, lim.MaxLifetime), lim.DefaultLifetime)
}

func (s *Server) allocate(e *exchange, a *allocation) {
	m := e.m
	if a != nil {
		if a.txID == m.TransactionID {
			s.allocated(e.x, e.t, a, e.key)
			return
		}
		e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
		return
	}
	proto, err := m.RequestedTransport()
	if err != nil {
		e.fail(stun.CodeBadRequest, "Bad Request")
		return
	}
	if proto != stun.ProtoUDP {
		e.fail(stun.CodeUnsupportedTransportProtocol, "Unsupported Transport Protocol")
		return
	}
	ipv6 := false
	if v, ok := m.Get(stun.AttrRequestedAddressFamily); ok {
		if len(v) != 4 || v[0] != 1 && v[0] != 2 {
			e.fail(stun.CodeAddressFamilyNotSupported, "Address Family not Supported")
			return
		}
		ipv6 = v[0] == 2
	}
	lifetime := s.lifetime(m)
	if lifetime == 0 {
		lifetime = s.limits.Load().DefaultLifetime
	}
	p := &pendingAlloc{tuple: e.t, user: e.user, key: e.key, txID: m.TransactionID, lifetime: lifetime, now: e.now}
	code, reason, gone := s.reserve(p)
	s.releaseAll(gone)
	switch code {
	case 0:
	case -1:
		// A retransmission of the request in the making, or a server
		// that has closed: nothing to say.
		return
	default:
		e.fail(code, reason)
		return
	}
	s.cfg.AllocateRelay(e.t, e.user, ipv6, func(r Relay, err error) { s.relayReady(p, r, err) })
}

// reserve records p as an allocation in the making, counted against the
// quotas, or says what to answer instead: an error code, or -1 for
// nothing at all. It returns the allocations the quotas' sweep freed, for
// the caller to release.
func (s *Server) reserve(p *pendingAlloc) (code int, reason string, gone []*allocation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return -1, "", nil
	}
	if s.lookup(p.tuple) != nil {
		// Another goroutine made one since the caller looked.
		return stun.CodeAllocationMismatch, "Allocation Mismatch", nil
	}
	if o := s.pending[p.tuple]; o != nil {
		if o.txID == p.txID {
			return -1, "", nil
		}
		return stun.CodeAllocationMismatch, "Allocation Mismatch", nil
	}
	if s.draining.Load() {
		return stun.CodeInsufficientCapacity, "Insufficient Capacity", nil
	}
	lim := s.limits.Load()
	if q := lim.MaxAllocationsPerUser; q > 0 && s.perUser[p.user] >= q {
		if gone = s.sweepLocked(p.now); s.perUser[p.user] >= q {
			return stun.CodeAllocationQuotaReached, "Allocation Quota Reached", gone
		}
	}
	if q := lim.MaxAllocations; q > 0 && s.count+len(s.pending) >= q {
		gone = append(gone, s.sweepLocked(p.now)...)
		if s.count+len(s.pending) >= q {
			return stun.CodeInsufficientCapacity, "Insufficient Capacity", gone
		}
	}
	s.pending[p.tuple] = p
	s.perUser[p.user]++
	return 0, "", gone
}

// relayReady is the done of an AllocateRelay: it makes the allocation and
// answers the request, or refuses it.
func (s *Server) relayReady(p *pendingAlloc, r Relay, err error) {
	if !p.done.CompareAndSwap(false, true) {
		// done called twice: the first call settled the request.
		if r != nil {
			r.Close()
		}
		return
	}
	a, refused := s.admit(p, r, err)
	if a == nil {
		if r != nil {
			r.Close()
		}
		if refused {
			x := getScratch()
			o := &x.out
			o.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassErrorResponse}, p.txID)
			o.AddErrorCode(stun.CodeInsufficientCapacity, "Insufficient Capacity")
			s.finish(x, p.tuple, p.key)
			putScratch(x)
			s.refusedEvent(p.tuple, p.user, stun.MethodAllocate, stun.CodeInsufficientCapacity)
		}
		return
	}
	x := getScratch()
	s.allocated(x, p.tuple, a, p.key)
	putScratch(x)
	s.emit(Event{Kind: EventAllocated, Tuple: a.tuple, User: a.user, Relay: a.relayAddr, Lifetime: a.lifetime})
}

// admit makes p's allocation with relay r, unless err, a duplicate
// relayed address, the client's going or the server's closing stand in
// the way; refused says the request is to be answered with 508.
func (s *Server) admit(p *pendingAlloc, r Relay, err error) (a *allocation, refused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending[p.tuple] == p {
		delete(s.pending, p.tuple)
	}
	gone := p.canceled || s.closed
	var relayAddr netip.AddrPort
	if !gone && err == nil && r != nil {
		relayAddr = unmap(r.Addr())
		if s.lookupRelay(relayAddr) != nil {
			// The host handed out an address still in use; the newer
			// allocation cannot share it.
			r = nil
		}
	}
	if gone || err != nil || r == nil {
		if s.perUser[p.user]--; s.perUser[p.user] <= 0 {
			delete(s.perUser, p.user)
		}
		return nil, !gone
	}
	a = &allocation{
		tuple:      p.tuple,
		user:       p.user,
		key:        p.key,
		relay:      r,
		relayAddr:  relayAddr,
		expires:    p.now.Add(p.lifetime),
		txID:       p.txID,
		lifetime:   p.lifetime,
		perms:      make(map[netip.Addr]time.Time),
		chans:      make(map[uint16]*chanBinding),
		chanByPeer: make(map[netip.AddrPort]*chanBinding),
	}
	sh := s.tupleShard(a.tuple)
	sh.mu.Lock()
	sh.allocs[a.tuple] = a
	sh.mu.Unlock()
	rs := s.relayShard(relayAddr)
	rs.mu.Lock()
	rs.byRelay[relayAddr] = a
	rs.mu.Unlock()
	s.count++
	if s.nextSweep.IsZero() || s.nextSweep.After(p.now.Add(s.cfg.SweepInterval)) {
		s.nextSweep = p.now.Add(s.cfg.SweepInterval)
	}
	return a, false
}

func (s *Server) allocated(x *scratch, t FiveTuple, a *allocation, key []byte) {
	o := &x.out
	o.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassSuccessResponse}, a.txID)
	o.AddXORAddress(stun.AttrXORRelayedAddress, a.relayAddr)
	o.AddLifetime(a.lifetime)
	o.AddXORAddress(stun.AttrXORMappedAddress, t.Client)
	s.finish(x, t, key)
}

func (s *Server) refresh(e *exchange, a *allocation) {
	if a == nil {
		e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
		return
	}
	lifetime := s.lifetime(e.m)
	if lifetime == 0 {
		s.free(a, FreedDeleted)
	} else {
		if !s.lockLive(a, e.now) {
			e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
			return
		}
		a.expires = e.now.Add(lifetime)
		a.mu.Unlock()
		s.emit(Event{Kind: EventRefreshed, Tuple: a.tuple, User: a.user, Relay: a.relayAddr, Lifetime: lifetime})
	}
	o := e.success()
	o.AddLifetime(lifetime)
	e.send()
}

// peers appends the XOR-PEER-ADDRESS attributes of m to dst; a
// CreatePermission request may carry several.
func peers(dst []netip.AddrPort, m *stun.Message) ([]netip.AddrPort, error) {
	for i := range m.Len() {
		typ, v := m.Attr(i)
		if typ != stun.AttrXORPeerAddress {
			continue
		}
		var one stun.Message
		one.Build(m.Type, m.TransactionID)
		one.Add(typ, v)
		a, err := one.XORAddress(typ)
		if err != nil {
			return dst, err
		}
		dst = append(dst, unmap(a))
	}
	return dst, nil
}

// checkPeer answers the request with the error peer calls for, if any: 443
// for a peer of another family than the relayed address, 403 for one the
// server does not relay to.
func (s *Server) checkPeer(e *exchange, a *allocation, peer netip.AddrPort) bool {
	if peer.Addr().Is4() != a.relayAddr.Addr().Is4() {
		e.fail(stun.CodePeerAddressFamilyMismatch, "Peer Address Family Mismatch")
		return false
	}
	if !s.peerAllowed(peer) {
		e.fail(stun.CodeForbidden, "Forbidden")
		return false
	}
	return true
}

func (s *Server) createPermission(e *exchange, a *allocation) {
	if a == nil {
		e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
		return
	}
	var buf [4]netip.AddrPort
	ps, err := peers(buf[:0], e.m)
	if err != nil || len(ps) == 0 {
		e.fail(stun.CodeBadRequest, "Bad Request")
		return
	}
	// Either every permission is installed or, if one peer is refused,
	// none is (RFC 8656 section 9.2).
	for _, p := range ps {
		if !s.checkPeer(e, a, netip.AddrPortFrom(p.Addr(), 0)) {
			return
		}
	}
	if !s.lockLive(a, e.now) {
		e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
		return
	}
	if q := s.limits.Load().MaxPermissions; q > 0 {
		fresh := 0
		for i, p := range ps {
			if _, ok := a.perms[p.Addr()]; !ok && !slices.ContainsFunc(ps[:i], func(o netip.AddrPort) bool { return o.Addr() == p.Addr() }) {
				fresh++
			}
		}
		if fresh > 0 && len(a.perms)+fresh > q {
			n := len(a.perms)
			forgetPermissions(a, e.now)
			fresh += len(a.perms) - n
			if len(a.perms)+fresh > q {
				a.mu.Unlock()
				e.fail(stun.CodeInsufficientCapacity, "Insufficient Capacity")
				return
			}
		}
	}
	for _, p := range ps {
		a.perms[p.Addr()] = e.now.Add(PermissionLifetime)
	}
	a.mu.Unlock()
	e.success()
	e.send()
}

// forgetPermissions forgets a's expired permissions. Callers hold a.mu.
func forgetPermissions(a *allocation, now time.Time) {
	for ip, exp := range a.perms {
		if !now.Before(exp) {
			delete(a.perms, ip)
		}
	}
}

func (s *Server) channelBind(e *exchange, a *allocation) {
	if a == nil {
		e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
		return
	}
	n, err1 := e.m.ChannelNumber()
	peer, err2 := e.m.XORAddress(stun.AttrXORPeerAddress)
	if err1 != nil || err2 != nil || n < MinChannel || n > MaxChannel {
		e.fail(stun.CodeBadRequest, "Bad Request")
		return
	}
	peer = unmap(peer)
	if !s.checkPeer(e, a, peer) {
		return
	}
	if !s.lockLive(a, e.now) {
		e.fail(stun.CodeAllocationMismatch, "Allocation Mismatch")
		return
	}
	code, reason := s.bindLocked(a, n, peer, e.now)
	a.mu.Unlock()
	if code != 0 {
		e.fail(code, reason)
		return
	}
	e.success()
	e.send()
}

// bindLocked binds channel n to peer on a, or refreshes the binding, and
// returns 0, or the error to refuse it with. Callers hold a.mu.
func (s *Server) bindLocked(a *allocation, n uint16, peer netip.AddrPort, now time.Time) (int, string) {
	// A number stays bound to its peer, and a peer to its number, until
	// the quarantine after the binding expires (RFC 8656 section 11.2).
	b := a.chans[n]
	if b != nil && b.peer != peer && !dropDeadChannel(a, b, now) {
		return stun.CodeBadRequest, "Bad Request"
	}
	if o := a.chanByPeer[peer]; o != nil && o.number != n && !dropDeadChannel(a, o, now) {
		return stun.CodeBadRequest, "Bad Request"
	}
	b = a.chans[n]
	lim := s.limits.Load()
	if q := lim.MaxChannels; b == nil && q > 0 && len(a.chans) >= q {
		for _, o := range a.chans {
			dropDeadChannel(a, o, now)
		}
		if len(a.chans) >= q {
			return stun.CodeInsufficientCapacity, "Insufficient Capacity"
		}
	}
	if _, ok := a.perms[peer.Addr()]; !ok {
		if q := lim.MaxPermissions; q > 0 && len(a.perms) >= q {
			if forgetPermissions(a, now); len(a.perms) >= q {
				return stun.CodeInsufficientCapacity, "Insufficient Capacity"
			}
		}
	}
	if b == nil {
		b = &chanBinding{number: n, peer: peer}
		a.chans[n] = b
		a.chanByPeer[peer] = b
	}
	b.expires = now.Add(ChannelLifetime)
	a.perms[peer.Addr()] = now.Add(PermissionLifetime)
	return 0, ""
}

// dropDeadChannel forgets b if its quarantine is over, and reports whether
// it did. Callers hold a.mu.
func dropDeadChannel(a *allocation, b *chanBinding, now time.Time) bool {
	if now.Before(b.expires.Add(channelQuarantine)) {
		return false
	}
	if a.chans[b.number] == b {
		delete(a.chans, b.number)
	}
	if a.chanByPeer[b.peer] == b {
		delete(a.chanByPeer, b.peer)
	}
	return true
}

// permitted reports whether a has a permission for ip. Callers hold a.mu.
func (a *allocation) permitted(ip netip.Addr, now time.Time) bool {
	exp, ok := a.perms[ip]
	return ok && now.Before(exp)
}

func (s *Server) handleSend(t FiveTuple, m *stun.Message, now time.Time) error {
	a := s.lookup(t)
	if a == nil {
		return ErrDropped
	}
	peer, err := m.XORAddress(stun.AttrXORPeerAddress)
	data, ok := m.Get(stun.AttrData)
	if err != nil || !ok || m.Contains(stun.AttrDontFragment) {
		return ErrDropped
	}
	peer = unmap(peer)
	if !s.lockLive(a, now) {
		return ErrDropped
	}
	ok = a.permitted(peer.Addr(), now)
	a.mu.Unlock()
	if !ok {
		return ErrDropped
	}
	return s.toPeer(a, peer, data)
}

func (s *Server) handleChannelData(t FiveTuple, b []byte, now time.Time) error {
	n, data, err := ParseChannelData(b)
	if err != nil {
		return err
	}
	a := s.lookup(t)
	if a == nil || !s.lockLive(a, now) {
		return ErrDropped
	}
	var peer netip.AddrPort
	c := a.chans[n]
	ok := c != nil && now.Before(c.expires) && a.permitted(c.peer.Addr(), now)
	if ok {
		peer = c.peer
	}
	a.mu.Unlock()
	if !ok {
		return ErrDropped
	}
	return s.toPeer(a, peer, data)
}

func (s *Server) toPeer(a *allocation, peer netip.AddrPort, data []byte) error {
	if s.cfg.PeerAllowed != nil && !s.cfg.PeerAllowed(peer) {
		return ErrDropped
	}
	if s.cfg.OnRelay != nil && !s.cfg.OnRelay(a.user, true, len(data)) {
		return ErrDropped
	}
	a.toPeer.add(len(data))
	a.relay.SendTo(peer, data)
	return nil
}

// HandleRelayPacket processes a datagram b the relayed address relay
// received from the peer at from: it goes to the allocation's client, in
// ChannelData if a channel is bound to the peer and in a Data indication
// otherwise, if the peer has a permission. b is not retained.
func (s *Server) HandleRelayPacket(relay, from netip.AddrPort, b []byte, now time.Time) error {
	err := s.handleRelayPacket(relay, from, b, now)
	if err != nil {
		s.dropped.Add(1)
	}
	return err
}

func (s *Server) handleRelayPacket(relay, from netip.AddrPort, b []byte, now time.Time) error {
	a := s.lookupRelay(unmap(relay))
	if a == nil || !s.lockLive(a, now) {
		return ErrDropped
	}
	from = unmap(from)
	ok := a.permitted(from.Addr(), now)
	var ch uint16
	if c := a.chanByPeer[from]; c != nil && now.Before(c.expires) {
		ch = c.number
	}
	a.mu.Unlock()
	if !ok || s.cfg.PeerAllowed != nil && !s.cfg.PeerAllowed(from) {
		return ErrDropped
	}
	if s.cfg.OnRelay != nil && !s.cfg.OnRelay(a.user, false, len(b)) {
		return ErrDropped
	}
	a.toClient.add(len(b))
	x := getScratch()
	defer putScratch(x)
	if ch != 0 {
		x.buf = AppendChannelData(x.buf[:0], ch, b)
		if a.tuple.Transport != TransportUDP {
			x.buf = padChannelData(x.buf)
		}
		s.cfg.Send(a.tuple, x.buf)
		return nil
	}
	o := &x.out
	o.Build(stun.MessageType{Method: stun.MethodData, Class: stun.ClassIndication}, s.nextIndicationID())
	o.AddXORAddress(stun.AttrXORPeerAddress, from)
	o.Add(stun.AttrData, b)
	o.AddFingerprint()
	s.cfg.Send(a.tuple, o.Raw)
	return nil
}

// nextIndicationID is a transaction ID for a Data indication: a random
// prefix and a counter, unique without a read of random bytes per
// datagram. Indications are not answered, so nothing hinges on guessing it.
func (s *Server) nextIndicationID() [stun.TransactionIDSize]byte {
	id := s.indID
	n := binary.BigEndian.Uint64(id[4:]) + s.indSeq.Add(1)
	binary.BigEndian.PutUint64(id[4:], n)
	return id
}

// NextTimeout returns when HandleTimeout must next be called: the next
// sweep, while the server holds allocations.
func (s *Server) NextTimeout() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return time.Time{}, false
	}
	return s.nextSweep, true
}

// HandleTimeout frees the allocations whose lifetime is over, closing their
// relayed addresses, and forgets expired permissions and channels.
func (s *Server) HandleTimeout(now time.Time) {
	s.mu.Lock()
	if now.Before(s.nextSweep) {
		s.mu.Unlock()
		return
	}
	gone := s.sweepLocked(now)
	s.mu.Unlock()
	s.releaseAll(gone)
}

func (s *Server) releaseAll(gone []*allocation) {
	for _, a := range gone {
		s.release(a, FreedExpired)
	}
}

// sweepLocked takes the expired allocations out of the tables and returns
// them, for the caller to release once it has let go of s.mu, and forgets
// the live ones' expired permissions and channels. Callers hold s.mu.
func (s *Server) sweepLocked(now time.Time) []*allocation {
	s.nextSweep = now.Add(s.cfg.SweepInterval)
	var gone []*allocation
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, a := range sh.allocs {
			a.mu.Lock()
			if !now.Before(a.expires) {
				gone = append(gone, a)
			} else {
				forgetPermissions(a, now)
				for _, b := range a.chans {
					dropDeadChannel(a, b, now)
				}
			}
			a.mu.Unlock()
		}
		sh.mu.RUnlock()
	}
	for _, a := range gone {
		s.unlinkLocked(a)
	}
	return gone
}
