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
	"strconv"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

var (
	ErrNotTURN    = errors.New("turn: neither STUN nor ChannelData")
	ErrNoRelay    = errors.New("turn: no relayed address available")
	ErrDropped    = errors.New("turn: datagram dropped")
	errNoAllocate = errors.New("turn: ServerConfig.AllocateRelay is nil")
)

// FiveTuple names an allocation by the client's address and the server's
// address the client sends to, over UDP: RFC 8656's 5-tuple, the transport
// protocol left out since this package speaks only UDP. A server listening
// on several addresses keeps the allocations made through each apart.
type FiveTuple struct {
	Client, Server netip.AddrPort
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
	// during the call.
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
	// valid only during the call.
	Send func(t FiveTuple, b []byte)
	// AllocateRelay opens a relayed transport address for a new
	// allocation of user's, of IPv6 if ipv6 is set and IPv4 otherwise, as
	// REQUESTED-ADDRESS-FAMILY asks. An error refuses the allocation with
	// 508 (Insufficient Capacity), as when a port range is exhausted.
	AllocateRelay func(t FiveTuple, user string, ipv6 bool) (Relay, error)

	// DefaultLifetime is the lifetime of an allocation whose client asks
	// for none or for less, 600s by default; MaxLifetime caps what a client
	// may ask for, 3600s by default.
	DefaultLifetime, MaxLifetime time.Duration
	// NonceLifetime is how long a nonce is good for before a request with
	// it is answered 438 (Stale Nonce) and a fresh one. Default 10 minutes.
	// Nonces are not stored: each is its expiry and an HMAC of it and the
	// client's address under NonceSecret.
	NonceLifetime time.Duration
	// NonceSecret keys the nonces' HMAC. Random if empty; servers behind
	// one load balancer that may each see a client's requests share one.
	NonceSecret []byte

	// MaxAllocations caps the allocations the server holds in all, which
	// it refuses beyond with 508 (Insufficient Capacity); and
	// MaxAllocationsPerUser those of one username, refused with 486
	// (Allocation Quota Reached). Zero means no limit.
	MaxAllocations, MaxAllocationsPerUser int
	// OnRelay, if set, is told of every datagram before it is relayed: n
	// bytes of user's, to a peer if toPeer and from one otherwise. It
	// returning false drops the datagram, which is where a host enforces
	// bandwidth limits and counts traffic.
	OnRelay func(user string, toPeer bool, n int) bool

	// PeerAllowed decides which peer addresses clients may install
	// permissions and channels for, and so relay to. If nil, an address
	// in AllowedPeers is allowed, then one in DeniedPeers or that
	// DeniedByDefault reports is refused, and any other is allowed. The
	// default keeps the server from relaying into the network it runs in,
	// its loopback and private ranges, which would make it a proxy into
	// them for anyone with credentials (an SSRF hole): a deployment whose
	// peers are on a private network lists its prefixes in AllowedPeers.
	PeerAllowed  func(netip.Addr) bool
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

func (c *ServerConfig) setDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.DefaultLifetime, DefaultLifetime)
	def(&c.MaxLifetime, time.Hour)
	if c.MaxLifetime < c.DefaultLifetime {
		c.MaxLifetime = c.DefaultLifetime
	}
	def(&c.NonceLifetime, 10*time.Minute)
	def(&c.SweepInterval, time.Second)
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

type allocation struct {
	tuple     FiveTuple
	user      string
	key       []byte
	relay     Relay
	relayAddr netip.AddrPort
	expires   time.Time
	// txID and lifetime are those of the Allocate request that made the
	// allocation, to answer its retransmissions alike.
	txID     [stun.TransactionIDSize]byte
	lifetime time.Duration

	perms      map[netip.Addr]time.Time
	chans      map[uint16]*chanBinding
	chanByPeer map[netip.AddrPort]*chanBinding
}

// Server is the core of a TURN server (RFC 8656) relaying UDP for clients
// that reach it over UDP, with the long-term credentials of RFC 8489. It
// does no I/O: the host passes it every datagram the listening socket
// receives through HandlePacket and every datagram a relayed address
// receives through HandleRelayPacket, sends what it emits through
// ServerConfig.Send and Relay.SendTo, opens relayed addresses when
// ServerConfig.AllocateRelay asks, and passes the time through NextTimeout
// and HandleTimeout. Its methods must be called from one goroutine at a
// time. Package turnserver runs it on a fib engine.
type Server struct {
	cfg      ServerConfig
	nonceKey []byte

	allocs  map[FiveTuple]*allocation
	byRelay map[netip.AddrPort]*allocation
	perUser map[string]int

	nextSweep time.Time
	indID     [stun.TransactionIDSize]byte
	in, out   stun.Message
	buf       []byte
}

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
		allocs:  make(map[FiveTuple]*allocation),
		byRelay: make(map[netip.AddrPort]*allocation),
		perUser: make(map[string]int),
	}
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

// Allocations is the number of allocations the server holds, including
// expired ones the next sweep will free.
func (s *Server) Allocations() int { return len(s.allocs) }

// RelayFor returns the relayed address of the allocation t made, if any.
func (s *Server) RelayFor(t FiveTuple) (netip.AddrPort, bool) {
	a := s.allocs[normTuple(t)]
	if a == nil {
		return netip.AddrPort{}, false
	}
	return a.relayAddr, true
}

func normTuple(t FiveTuple) FiveTuple { return FiveTuple{unmap(t.Client), unmap(t.Server)} }

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

func (s *Server) peerAllowed(ip netip.Addr) bool {
	ip = ip.Unmap()
	if s.cfg.PeerAllowed != nil {
		return s.cfg.PeerAllowed(ip)
	}
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
	t = normTuple(t)
	if IsChannelData(b) {
		return s.handleChannelData(t, b, now)
	}
	if !stun.IsMessage(b) {
		return ErrNotTURN
	}
	m := &s.in
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
		s.handleRequest(t, m, now)
		return nil
	case stun.ClassIndication:
		if m.Type.Method == stun.MethodSend {
			return s.handleSend(t, m, now)
		}
	}
	return ErrDropped
}

// live returns t's allocation if it has one that has not expired, freeing
// one that has.
func (s *Server) live(t FiveTuple, now time.Time) *allocation {
	a := s.allocs[t]
	if a != nil && !now.Before(a.expires) {
		s.remove(a)
		return nil
	}
	return a
}

func (s *Server) remove(a *allocation) {
	if s.allocs[a.tuple] != a {
		return
	}
	delete(s.allocs, a.tuple)
	delete(s.byRelay, a.relayAddr)
	if s.perUser[a.user]--; s.perUser[a.user] <= 0 {
		delete(s.perUser, a.user)
	}
	a.relay.Close()
}

// Close frees every allocation, closing its relayed address.
func (s *Server) Close() {
	for _, a := range s.allocs {
		s.remove(a)
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

func (s *Server) handleRequest(t FiveTuple, m *stun.Message, now time.Time) {
	switch m.Type.Method {
	case stun.MethodBinding:
		// A TURN server is a STUN server too, which ICE agents ask for
		// their server-reflexive address without credentials.
		o := &s.out
		o.Build(stun.BindingSuccess, m.TransactionID)
		o.AddXORAddress(stun.AttrXORMappedAddress, t.Client)
		s.finish(t, nil)
		return
	case stun.MethodAllocate, stun.MethodRefresh, stun.MethodCreatePermission, stun.MethodChannelBind:
	default:
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", nil)
		return
	}
	user, key, ok := s.authenticate(t, m, now)
	if !ok {
		return
	}
	var unknown []stun.AttrType
	for i := range m.Len() {
		if typ, _ := m.Attr(i); !knownAttr(typ) {
			unknown = append(unknown, typ)
		}
	}
	if len(unknown) > 0 {
		o := s.errorHeader(m, stun.CodeUnknownAttribute, "Unknown Attribute")
		o.AddUnknownAttributes(unknown)
		s.finish(t, key)
		return
	}
	a := s.live(t, now)
	if a != nil && a.user != user {
		// Requests on an allocation must use its credentials (RFC 8656
		// section 5).
		s.respondError(t, m, stun.CodeWrongCredentials, "Wrong Credentials", key)
		return
	}
	switch m.Type.Method {
	case stun.MethodAllocate:
		s.allocate(t, m, a, user, key, now)
	case stun.MethodRefresh:
		s.refresh(t, m, a, key, now)
	case stun.MethodCreatePermission:
		s.createPermission(t, m, a, key, now)
	case stun.MethodChannelBind:
		s.channelBind(t, m, a, key, now)
	}
}

// authenticate checks a request's long-term credentials (RFC 8489 section
// 9.2.4), answering it itself when they fail: 401 with the realm and a
// nonce for a request without credentials or with wrong ones, 400 for
// incomplete ones, and 438 with a fresh nonce for a stale one.
func (s *Server) authenticate(t FiveTuple, m *stun.Message, now time.Time) (user string, key []byte, ok bool) {
	if !m.Contains(stun.AttrMessageIntegrity) {
		s.challenge(t, m, stun.CodeUnauthorized, "Unauthorized", now)
		return "", nil, false
	}
	u, okUser := m.Get(stun.AttrUsername)
	realm, okRealm := m.Get(stun.AttrRealm)
	nonce, okNonce := m.Get(stun.AttrNonce)
	if !okUser || !okRealm || !okNonce {
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", nil)
		return "", nil, false
	}
	if !s.validNonce(nonce, t.Client, now) {
		s.challenge(t, m, stun.CodeStaleNonce, "Stale Nonce", now)
		return "", nil, false
	}
	if string(realm) != s.cfg.Realm {
		s.challenge(t, m, stun.CodeUnauthorized, "Unauthorized", now)
		return "", nil, false
	}
	key, ok = s.cfg.Auth(string(u), string(realm))
	if !ok || m.CheckIntegrity(key) != nil {
		s.challenge(t, m, stun.CodeUnauthorized, "Unauthorized", now)
		return "", nil, false
	}
	return string(u), key, true
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

func (s *Server) challenge(t FiveTuple, m *stun.Message, code int, reason string, now time.Time) {
	o := s.errorHeader(m, code, reason)
	o.AddString(stun.AttrRealm, s.cfg.Realm)
	o.Add(stun.AttrNonce, s.makeNonce(t.Client, now.Add(s.cfg.NonceLifetime).Unix()))
	s.finish(t, nil)
}

func (s *Server) errorHeader(m *stun.Message, code int, reason string) *stun.Message {
	o := &s.out
	o.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassErrorResponse}, m.TransactionID)
	o.AddErrorCode(code, reason)
	return o
}

func (s *Server) respondError(t FiveTuple, m *stun.Message, code int, reason string, key []byte) {
	s.errorHeader(m, code, reason)
	s.finish(t, key)
}

func (s *Server) successHeader(m *stun.Message) *stun.Message {
	o := &s.out
	o.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassSuccessResponse}, m.TransactionID)
	return o
}

// finish adds SOFTWARE, MESSAGE-INTEGRITY with key if any, and FINGERPRINT
// to the response being built, and sends it.
func (s *Server) finish(t FiveTuple, key []byte) {
	o := &s.out
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
	l, err := m.Lifetime()
	if err != nil {
		return s.cfg.DefaultLifetime
	}
	if l == 0 {
		return 0
	}
	return max(min(l, s.cfg.MaxLifetime), s.cfg.DefaultLifetime)
}

func (s *Server) allocate(t FiveTuple, m *stun.Message, a *allocation, user string, key []byte, now time.Time) {
	if a != nil {
		if a.txID == m.TransactionID {
			s.allocated(t, m, a, key)
			return
		}
		s.respondError(t, m, stun.CodeAllocationMismatch, "Allocation Mismatch", key)
		return
	}
	proto, err := m.RequestedTransport()
	if err != nil {
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", key)
		return
	}
	if proto != stun.ProtoUDP {
		s.respondError(t, m, stun.CodeUnsupportedTransportProtocol, "Unsupported Transport Protocol", key)
		return
	}
	ipv6 := false
	if v, ok := m.Get(stun.AttrRequestedAddressFamily); ok {
		if len(v) != 4 || v[0] != 1 && v[0] != 2 {
			s.respondError(t, m, stun.CodeAddressFamilyNotSupported, "Address Family not Supported", key)
			return
		}
		ipv6 = v[0] == 2
	}
	if q := s.cfg.MaxAllocationsPerUser; q > 0 && s.perUser[user] >= q {
		if s.sweep(now); s.perUser[user] >= q {
			s.respondError(t, m, stun.CodeAllocationQuotaReached, "Allocation Quota Reached", key)
			return
		}
	}
	if q := s.cfg.MaxAllocations; q > 0 && len(s.allocs) >= q {
		if s.sweep(now); len(s.allocs) >= q {
			s.respondError(t, m, stun.CodeInsufficientCapacity, "Insufficient Capacity", key)
			return
		}
	}
	relay, err := s.cfg.AllocateRelay(t, user, ipv6)
	if err != nil || relay == nil {
		s.respondError(t, m, stun.CodeInsufficientCapacity, "Insufficient Capacity", key)
		return
	}
	lifetime := s.lifetime(m)
	if lifetime == 0 {
		lifetime = s.cfg.DefaultLifetime
	}
	a = &allocation{
		tuple:      t,
		user:       user,
		key:        key,
		relay:      relay,
		relayAddr:  unmap(relay.Addr()),
		expires:    now.Add(lifetime),
		txID:       m.TransactionID,
		lifetime:   lifetime,
		perms:      make(map[netip.Addr]time.Time),
		chans:      make(map[uint16]*chanBinding),
		chanByPeer: make(map[netip.AddrPort]*chanBinding),
	}
	if old := s.byRelay[a.relayAddr]; old != nil {
		// The host handed out an address still in use; the newer
		// allocation cannot share it.
		relay.Close()
		s.respondError(t, m, stun.CodeInsufficientCapacity, "Insufficient Capacity", key)
		return
	}
	s.allocs[t] = a
	s.byRelay[a.relayAddr] = a
	s.perUser[user]++
	if s.nextSweep.IsZero() || s.nextSweep.After(now.Add(s.cfg.SweepInterval)) {
		s.nextSweep = now.Add(s.cfg.SweepInterval)
	}
	s.allocated(t, m, a, key)
}

func (s *Server) allocated(t FiveTuple, m *stun.Message, a *allocation, key []byte) {
	o := s.successHeader(m)
	o.AddXORAddress(stun.AttrXORRelayedAddress, a.relayAddr)
	o.AddLifetime(a.lifetime)
	o.AddXORAddress(stun.AttrXORMappedAddress, t.Client)
	s.finish(t, key)
}

func (s *Server) refresh(t FiveTuple, m *stun.Message, a *allocation, key []byte, now time.Time) {
	if a == nil {
		s.respondError(t, m, stun.CodeAllocationMismatch, "Allocation Mismatch", key)
		return
	}
	lifetime := s.lifetime(m)
	if lifetime == 0 {
		s.remove(a)
	} else {
		a.expires = now.Add(lifetime)
	}
	o := s.successHeader(m)
	o.AddLifetime(lifetime)
	s.finish(t, key)
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

// checkPeer answers m with the error peer calls for, if any: 443 for a
// peer of another family than the relayed address, 403 for one the server
// does not relay to.
func (s *Server) checkPeer(t FiveTuple, m *stun.Message, a *allocation, peer netip.AddrPort, key []byte) bool {
	if peer.Addr().Is4() != a.relayAddr.Addr().Is4() {
		s.respondError(t, m, stun.CodePeerAddressFamilyMismatch, "Peer Address Family Mismatch", key)
		return false
	}
	if !s.peerAllowed(peer.Addr()) {
		s.respondError(t, m, stun.CodeForbidden, "Forbidden", key)
		return false
	}
	return true
}

func (s *Server) createPermission(t FiveTuple, m *stun.Message, a *allocation, key []byte, now time.Time) {
	if a == nil {
		s.respondError(t, m, stun.CodeAllocationMismatch, "Allocation Mismatch", key)
		return
	}
	var buf [4]netip.AddrPort
	ps, err := peers(buf[:0], m)
	if err != nil || len(ps) == 0 {
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", key)
		return
	}
	// Either every permission is installed or, if one peer is refused,
	// none is (RFC 8656 section 9.2).
	for _, p := range ps {
		if !s.checkPeer(t, m, a, p, key) {
			return
		}
	}
	for _, p := range ps {
		a.perms[p.Addr()] = now.Add(PermissionLifetime)
	}
	s.successHeader(m)
	s.finish(t, key)
}

func (s *Server) channelBind(t FiveTuple, m *stun.Message, a *allocation, key []byte, now time.Time) {
	if a == nil {
		s.respondError(t, m, stun.CodeAllocationMismatch, "Allocation Mismatch", key)
		return
	}
	n, err1 := m.ChannelNumber()
	peer, err2 := m.XORAddress(stun.AttrXORPeerAddress)
	if err1 != nil || err2 != nil || n < MinChannel || n > MaxChannel {
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", key)
		return
	}
	peer = unmap(peer)
	if !s.checkPeer(t, m, a, peer, key) {
		return
	}
	// A number stays bound to its peer, and a peer to its number, until
	// the quarantine after the binding expires (RFC 8656 section 11.2).
	b := a.chans[n]
	if b != nil && b.peer != peer && !s.dropDeadChannel(a, b, now) {
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", key)
		return
	}
	if o := a.chanByPeer[peer]; o != nil && o.number != n && !s.dropDeadChannel(a, o, now) {
		s.respondError(t, m, stun.CodeBadRequest, "Bad Request", key)
		return
	}
	if b == nil || b.peer != peer {
		b = &chanBinding{number: n, peer: peer}
		a.chans[n] = b
		a.chanByPeer[peer] = b
	}
	b.expires = now.Add(ChannelLifetime)
	a.perms[peer.Addr()] = now.Add(PermissionLifetime)
	s.successHeader(m)
	s.finish(t, key)
}

// dropDeadChannel forgets b if its quarantine is over, and reports whether
// it did.
func (s *Server) dropDeadChannel(a *allocation, b *chanBinding, now time.Time) bool {
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

func (a *allocation) permitted(ip netip.Addr, now time.Time) bool {
	exp, ok := a.perms[ip]
	return ok && now.Before(exp)
}

func (s *Server) handleSend(t FiveTuple, m *stun.Message, now time.Time) error {
	a := s.live(t, now)
	if a == nil {
		return ErrDropped
	}
	peer, err := m.XORAddress(stun.AttrXORPeerAddress)
	data, ok := m.Get(stun.AttrData)
	if err != nil || !ok || m.Contains(stun.AttrDontFragment) {
		return ErrDropped
	}
	peer = unmap(peer)
	if !a.permitted(peer.Addr(), now) {
		return ErrDropped
	}
	return s.toPeer(a, peer, data)
}

func (s *Server) handleChannelData(t FiveTuple, b []byte, now time.Time) error {
	n, data, err := ParseChannelData(b)
	if err != nil {
		return err
	}
	a := s.live(t, now)
	if a == nil {
		return ErrDropped
	}
	c := a.chans[n]
	if c == nil || !now.Before(c.expires) || !a.permitted(c.peer.Addr(), now) {
		return ErrDropped
	}
	return s.toPeer(a, c.peer, data)
}

func (s *Server) toPeer(a *allocation, peer netip.AddrPort, data []byte) error {
	if s.cfg.OnRelay != nil && !s.cfg.OnRelay(a.user, true, len(data)) {
		return ErrDropped
	}
	a.relay.SendTo(peer, data)
	return nil
}

// HandleRelayPacket processes a datagram b the relayed address relay
// received from the peer at from: it goes to the allocation's client, in
// ChannelData if a channel is bound to the peer and in a Data indication
// otherwise, if the peer has a permission. b is not retained.
func (s *Server) HandleRelayPacket(relay, from netip.AddrPort, b []byte, now time.Time) error {
	a := s.byRelay[unmap(relay)]
	if a == nil {
		return ErrDropped
	}
	if !now.Before(a.expires) {
		s.remove(a)
		return ErrDropped
	}
	from = unmap(from)
	if !a.permitted(from.Addr(), now) {
		return ErrDropped
	}
	if s.cfg.OnRelay != nil && !s.cfg.OnRelay(a.user, false, len(b)) {
		return ErrDropped
	}
	if c := a.chanByPeer[from]; c != nil && now.Before(c.expires) {
		s.buf = AppendChannelData(s.buf[:0], c.number, b)
		s.cfg.Send(a.tuple, s.buf)
		return nil
	}
	o := &s.out
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
	n := binary.BigEndian.Uint64(s.indID[4:]) + 1
	binary.BigEndian.PutUint64(s.indID[4:], n)
	return s.indID
}

// NextTimeout returns when HandleTimeout must next be called: the next
// sweep, while the server holds allocations.
func (s *Server) NextTimeout() (time.Time, bool) {
	if len(s.allocs) == 0 {
		return time.Time{}, false
	}
	return s.nextSweep, true
}

// HandleTimeout frees the allocations whose lifetime is over, closing their
// relayed addresses, and forgets expired permissions and channels.
func (s *Server) HandleTimeout(now time.Time) {
	if now.Before(s.nextSweep) {
		return
	}
	s.sweep(now)
}

func (s *Server) sweep(now time.Time) {
	s.nextSweep = now.Add(s.cfg.SweepInterval)
	for _, a := range s.allocs {
		if !now.Before(a.expires) {
			s.remove(a)
			continue
		}
		for ip, exp := range a.perms {
			if !now.Before(exp) {
				delete(a.perms, ip)
			}
		}
		for _, b := range a.chans {
			s.dropDeadChannel(a, b, now)
		}
	}
}
