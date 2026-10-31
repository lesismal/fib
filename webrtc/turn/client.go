package turn

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

var (
	ErrClosed       = errors.New("turn: client closed")
	ErrNotAllocated = errors.New("turn: no allocation")
	ErrAllocating   = errors.New("turn: allocation already requested")
	ErrUnauthorized = errors.New("turn: the server rejected the credentials")
	ErrNoChannels   = errors.New("turn: every channel number is in use")
	ErrBadResponse  = errors.New("turn: malformed response")
)

// Error is an error response from a TURN server to one of the client's
// requests.
type Error struct {
	Method stun.Method
	Code   int
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("turn: %v: %d %s", e.Method, e.Code, e.Reason)
}

// Lifetimes of RFC 8656: a permission lasts 300 seconds and a channel
// binding 600, from when it was last installed or refreshed. The client
// refreshes both a minute before they would expire.
const (
	PermissionLifetime = 300 * time.Second
	ChannelLifetime    = 600 * time.Second
	DefaultLifetime    = 600 * time.Second

	permissionRefresh = PermissionLifetime - time.Minute
	channelRefresh    = 4 * time.Minute
	// maxStaleRetries bounds how often one request is sent again with a
	// fresh nonce, so that a server that keeps calling every nonce stale
	// cannot keep the client sending forever.
	maxStaleRetries = 3
)

// ClientConfig configures a Client.
type ClientConfig struct {
	// Server is the TURN server's address, which the client sends to and
	// takes packets from. A 300 (Try Alternate) response to the Allocate
	// request moves it to the ALTERNATE-SERVER the response names.
	Server netip.AddrPort
	// Username and Password are the long-term credentials, RFC 8489
	// section 9.2; EphemeralCredentials makes the time-limited kind most
	// WebRTC services hand out. The realm and nonce come from the server's
	// first 401 response.
	Username, Password string
	// Lifetime is the allocation lifetime the client asks for, which the
	// server may shorten or lengthen. Zero leaves it to the server, 600s
	// by default. The client refreshes the allocation a minute before it
	// would expire, or halfway through a lifetime shorter than two minutes.
	Lifetime time.Duration
	// BindChannels has SendTo bind a channel to each peer it sends to, so
	// that once bound, data goes with the 4-byte ChannelData header rather
	// than the 36 bytes and more of a Send indication.
	BindChannels bool
	// Software, if set, is sent as SOFTWARE in requests.
	Software string
	// Transaction sets how requests are retransmitted; the zero value is
	// RFC 8489's.
	Transaction stun.TransactionConfig
	// Stream says the client reaches the server over TCP or TLS: its
	// ChannelData messages are padded to a multiple of four bytes, and
	// its requests are sent once (Transaction.Reliable). The host frames
	// what the stream brings with FrameLen and passes each message to
	// HandlePacket.
	Stream bool

	// Send sends b to the server at to. b is valid only during the call.
	// It is required.
	Send func(to netip.AddrPort, b []byte)
	// OnAllocated reports the allocation's relayed address, which peers
	// send to, and the client's address as the server sees it, the
	// mapped address that becomes a relay candidate's related address.
	OnAllocated func(relayed, mapped netip.AddrPort)
	// OnFailed reports that the allocation could not be made, or was lost
	// when a refresh failed. The client then sends nothing more.
	OnFailed func(err error)
	// Rand is where transaction IDs come from. Default crypto/rand.
	Rand io.Reader
}

type clientState uint8

const (
	clientIdle clientState = iota
	clientAllocating
	clientAllocated
	clientClosing
	clientClosed
	clientFailed
)

type requestKind uint8

const (
	reqAllocate requestKind = iota
	reqRefresh
	reqPermission
	reqChannel
)

func (k requestKind) method() stun.Method {
	switch k {
	case reqAllocate:
		return stun.MethodAllocate
	case reqRefresh:
		return stun.MethodRefresh
	case reqPermission:
		return stun.MethodCreatePermission
	}
	return stun.MethodChannelBind
}

// request is one of the client's requests in flight, with what it needs to
// be built again with a new nonce or for another server.
type request struct {
	kind     requestKind
	tx       stun.Transaction
	peer     netip.AddrPort
	channel  uint16
	lifetime time.Duration
	authed   bool
	stale    int
	// unclocked is set on a request SendTo sent, which had no clock to
	// time it by: its retransmissions are timed from the next time the
	// client is given.
	unclocked bool
}

type permission struct {
	refreshAt time.Time
	pending   bool
	err       error
}

type channel struct {
	number    uint16
	peer      netip.AddrPort
	refreshAt time.Time
	pending   bool
	bound     bool
	failed    bool
}

// Client is the client side of one TURN allocation over UDP (RFC 8656): it
// allocates a relayed address on the server, keeps it, its permissions and
// its channels refreshed, wraps what is sent to peers in Send indications or
// ChannelData messages, and unwraps what the server relays back. It does no
// I/O: what it sends goes out through ClientConfig.Send, the caller passes
// it every packet from the server's address through HandlePacket, and time
// passes through NextTimeout and HandleTimeout. Its methods must be called
// from one goroutine at a time.
type Client struct {
	cfg        ClientConfig
	server     netip.AddrPort
	state      clientState
	redirected bool

	realm, nonce string
	key          []byte

	relayed, mapped netip.AddrPort
	refreshAt       time.Time
	refreshing      bool

	txs       map[[stun.TransactionIDSize]byte]*request
	perms     map[netip.Addr]*permission
	chans     map[netip.AddrPort]*channel
	chanByNum map[uint16]*channel
	nextChan  uint16

	// now is the time the client was last given. SendTo, which has no
	// clock to pass along as ice.Agent's Send has none, may start requests
	// at a time no one gave it: they are sent at once and marked unclocked,
	// and timed from the next time the client is given.
	now       time.Time
	inSendTo  bool
	unclocked bool
	in        stun.Message
	out       stun.Message
	buf       []byte
}

// NewClient returns a client configured by config. Allocate starts it.
func NewClient(config ClientConfig) (*Client, error) {
	if config.Send == nil {
		return nil, errors.New("turn: ClientConfig.Send is nil")
	}
	if !config.Server.IsValid() {
		return nil, errors.New("turn: ClientConfig.Server is not an address")
	}
	if config.Rand == nil {
		config.Rand = rand.Reader
	}
	if config.Stream {
		config.Transaction.Reliable = true
	}
	return &Client{
		cfg:       config,
		server:    unmap(config.Server),
		txs:       make(map[[stun.TransactionIDSize]byte]*request),
		perms:     make(map[netip.Addr]*permission),
		chans:     make(map[netip.AddrPort]*channel),
		chanByNum: make(map[uint16]*channel),
		nextChan:  MinChannel,
	}, nil
}

func unmap(a netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

// Server is the address of the server the client speaks to, which a 300
// response may have changed from ClientConfig.Server.
func (c *Client) Server() netip.AddrPort { return c.server }

// Relayed returns the allocation's relayed address and the client's mapped
// address, once allocated.
func (c *Client) Relayed() (relayed, mapped netip.AddrPort, ok bool) {
	return c.relayed, c.mapped, c.state == clientAllocated
}

// Allocate asks the server for an allocation. OnAllocated or OnFailed
// reports how it went.
func (c *Client) Allocate(now time.Time) error {
	switch c.state {
	case clientIdle:
	case clientClosing, clientClosed:
		return ErrClosed
	default:
		return ErrAllocating
	}
	c.setClock(now)
	c.state = clientAllocating
	c.send(&request{kind: reqAllocate, lifetime: c.cfg.Lifetime})
	return nil
}

// CreatePermission has the server let peer's IP address send to the
// relayed address, and keeps the permission refreshed until Close. SendTo
// creates the permissions it needs itself.
func (c *Client) CreatePermission(peer netip.Addr, now time.Time) error {
	if c.state != clientAllocated {
		return c.notAllocated()
	}
	c.setClock(now)
	peer = peer.Unmap()
	p := c.perms[peer]
	if p == nil {
		p = &permission{}
		c.perms[peer] = p
	} else if p.pending {
		return nil
	}
	p.err = nil
	p.pending = true
	c.send(&request{kind: reqPermission, peer: netip.AddrPortFrom(peer, 0)})
	return nil
}

// BindChannel binds a channel to peer, after which data to and from it goes
// in ChannelData messages, and keeps it refreshed until Close. Binding a
// channel installs the permission for peer's IP address too.
func (c *Client) BindChannel(peer netip.AddrPort, now time.Time) error {
	if c.state != clientAllocated {
		return c.notAllocated()
	}
	c.setClock(now)
	return c.bindChannel(unmap(peer))
}

func (c *Client) bindChannel(peer netip.AddrPort) error {
	ch := c.chans[peer]
	if ch == nil {
		n, ok := c.freeChannel()
		if !ok {
			return ErrNoChannels
		}
		ch = &channel{number: n, peer: peer}
		c.chans[peer] = ch
		c.chanByNum[n] = ch
	} else if ch.pending {
		return nil
	}
	ch.failed = false
	ch.pending = true
	c.send(&request{kind: reqChannel, peer: peer, channel: ch.number})
	return nil
}

func (c *Client) freeChannel() (uint16, bool) {
	for range MaxChannel - MinChannel + 1 {
		n := c.nextChan
		if c.nextChan++; c.nextChan > MaxChannel {
			c.nextChan = MinChannel
		}
		if c.chanByNum[n] == nil {
			return n, true
		}
	}
	return 0, false
}

func (c *Client) notAllocated() error {
	if c.state == clientClosing || c.state == clientClosed {
		return ErrClosed
	}
	return ErrNotAllocated
}

// SendTo sends data to peer through the relay: in a ChannelData message
// once a channel to peer is bound, and in a Send indication before. The
// first send to a peer's IP address creates the permission it needs, and,
// with ClientConfig.BindChannels, binds a channel; the requests go out
// ahead of the data, so a server that takes them in order relays it. It
// reports the error a permission for peer was refused with, if it was.
func (c *Client) SendTo(peer netip.AddrPort, data []byte) error {
	if c.state != clientAllocated {
		return c.notAllocated()
	}
	peer = unmap(peer)
	if ch := c.chans[peer]; ch != nil && ch.bound {
		c.buf = AppendChannelData(c.buf[:0], ch.number, data)
		if c.cfg.Stream {
			c.buf = padChannelData(c.buf)
		}
		c.cfg.Send(c.server, c.buf)
		return nil
	}
	p := c.perms[peer.Addr()]
	if p != nil && p.err != nil {
		return p.err
	}
	c.inSendTo = true
	if p == nil {
		_ = c.CreatePermission(peer.Addr(), c.now)
	}
	if ch := c.chans[peer]; c.cfg.BindChannels && (ch == nil || !ch.pending && !ch.failed) {
		_ = c.bindChannel(peer)
	}
	c.inSendTo = false
	o := &c.out
	o.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, c.newID())
	o.AddXORAddress(stun.AttrXORPeerAddress, peer)
	o.Add(stun.AttrData, data)
	c.cfg.Send(c.server, o.Raw)
	return nil
}

// IsServerPacket reports whether b, from the address from, is the server's:
// a STUN message or a ChannelData message from its address. It is how a
// caller sharing one socket among ICE, DTLS and TURN tells which packets go
// to HandlePacket.
func (c *Client) IsServerPacket(from netip.AddrPort, b []byte) bool {
	return unmap(from) == c.server && (stun.IsMessage(b) || IsChannelData(b))
}

// HandlePacket processes a packet b the caller received from the address
// from. ok reports whether it was the server's (see IsServerPacket); if it
// is not, the caller demultiplexes it as it otherwise would. When the
// server relayed a peer's datagram, in a Data indication or ChannelData,
// HandlePacket returns the peer's address and the datagram, which aliases
// b. Responses to the client's requests return no data.
func (c *Client) HandlePacket(from netip.AddrPort, b []byte, now time.Time) (peer netip.AddrPort, data []byte, ok bool) {
	if !c.IsServerPacket(from, b) {
		return netip.AddrPort{}, nil, false
	}
	c.setClock(now)
	if IsChannelData(b) {
		n, d, err := ParseChannelData(b)
		if err != nil {
			return netip.AddrPort{}, nil, true
		}
		ch := c.chanByNum[n]
		if ch == nil || c.state != clientAllocated {
			return netip.AddrPort{}, nil, true
		}
		return ch.peer, d, true
	}
	m := &c.in
	if m.Decode(b) != nil {
		return netip.AddrPort{}, nil, true
	}
	if m.Contains(stun.AttrFingerprint) && m.CheckFingerprint() != nil {
		return netip.AddrPort{}, nil, true
	}
	switch m.Type.Class {
	case stun.ClassIndication:
		if m.Type.Method != stun.MethodData || c.state != clientAllocated {
			break
		}
		p, err := m.XORAddress(stun.AttrXORPeerAddress)
		d, has := m.Get(stun.AttrData)
		if err != nil || !has {
			break
		}
		return unmap(p), d, true
	case stun.ClassSuccessResponse, stun.ClassErrorResponse:
		c.handleResponse(m, now)
	}
	return netip.AddrPort{}, nil, true
}

func (c *Client) handleResponse(m *stun.Message, now time.Time) {
	r := c.txs[m.TransactionID]
	if r == nil || m.Type.Method != r.kind.method() {
		return
	}
	// A success response to an authenticated request must carry a valid
	// MESSAGE-INTEGRITY, or it may be forged; it is then ignored and the
	// transaction goes on (RFC 8489 section 9.2.5).
	if m.Type.Class == stun.ClassSuccessResponse && r.authed && m.CheckIntegrity(c.key) != nil {
		return
	}
	delete(c.txs, m.TransactionID)
	r.tx.Finish()
	if m.Type.Class == stun.ClassSuccessResponse {
		c.succeeded(r, m, now)
		return
	}
	code, reason, err := m.ErrorCode()
	if err != nil {
		c.failed(r, ErrBadResponse)
		return
	}
	switch code {
	case stun.CodeUnauthorized:
		// The first request goes without credentials, to learn the realm
		// and nonce from this challenge. A 401 to one sent with them means
		// they are wrong.
		realm, okRealm := m.Get(stun.AttrRealm)
		nonce, okNonce := m.Get(stun.AttrNonce)
		if !r.authed && okRealm && okNonce {
			c.setRealm(string(realm))
			c.nonce = string(nonce)
			c.send(r)
			return
		}
		c.failed(r, fmt.Errorf("%w: %w", ErrUnauthorized, &Error{r.kind.method(), code, string(reason)}))
		return
	case stun.CodeStaleNonce:
		nonce, okNonce := m.Get(stun.AttrNonce)
		if okNonce && r.stale < maxStaleRetries {
			if realm, ok := m.Get(stun.AttrRealm); ok && string(realm) != c.realm {
				c.setRealm(string(realm))
			}
			c.nonce = string(nonce)
			r.stale++
			c.send(r)
			return
		}
	case stun.CodeTryAlternate:
		if alt, err := m.Address(stun.AttrAlternateServer); err == nil && r.kind == reqAllocate && !c.redirected {
			// The allocation is made with the other server, which has a
			// realm and nonce of its own.
			c.redirected = true
			c.server = unmap(alt)
			c.realm, c.nonce, c.key = "", "", nil
			r.authed, r.stale = false, 0
			c.send(r)
			return
		}
	case stun.CodeAllocationMismatch:
		if r.kind == reqRefresh && r.lifetime == 0 {
			// A deallocation whose first response was lost: the
			// retransmission finds the allocation already gone.
			c.succeeded(r, m, now)
			return
		}
	}
	c.failed(r, &Error{r.kind.method(), code, string(reason)})
}

func (c *Client) setRealm(realm string) {
	c.realm = realm
	c.key = stun.LongTermKey(c.cfg.Username, realm, c.cfg.Password)
}

func (c *Client) refreshDelay(lifetime time.Duration) time.Duration {
	if lifetime >= 2*time.Minute {
		return lifetime - time.Minute
	}
	return lifetime / 2
}

func (c *Client) succeeded(r *request, m *stun.Message, now time.Time) {
	switch r.kind {
	case reqAllocate:
		relayed, err1 := m.XORAddress(stun.AttrXORRelayedAddress)
		mapped, err2 := m.XORAddress(stun.AttrXORMappedAddress)
		lifetime, err3 := m.Lifetime()
		if err := errors.Join(err1, err2, err3); err != nil {
			c.failed(r, fmt.Errorf("%w: %w", ErrBadResponse, err))
			return
		}
		c.relayed, c.mapped = unmap(relayed), unmap(mapped)
		c.state = clientAllocated
		c.refreshAt = now.Add(c.refreshDelay(lifetime))
		if c.cfg.OnAllocated != nil {
			c.cfg.OnAllocated(c.relayed, c.mapped)
		}
	case reqRefresh:
		c.refreshing = false
		if r.lifetime == 0 {
			c.state = clientClosed
			return
		}
		lifetime, err := m.Lifetime()
		if err != nil {
			lifetime = DefaultLifetime
		}
		c.refreshAt = now.Add(c.refreshDelay(lifetime))
	case reqPermission:
		if p := c.perms[r.peer.Addr()]; p != nil {
			p.pending = false
			p.refreshAt = now.Add(permissionRefresh)
		}
	case reqChannel:
		if ch := c.chans[r.peer]; ch != nil && ch.number == r.channel {
			ch.pending = false
			ch.bound = true
			ch.refreshAt = now.Add(channelRefresh)
		}
	}
}

func (c *Client) failed(r *request, err error) {
	switch r.kind {
	case reqAllocate:
		c.fail(err)
	case reqRefresh:
		c.refreshing = false
		if r.lifetime == 0 {
			c.state = clientClosed
			return
		}
		c.fail(err)
	case reqPermission:
		// A refusal is kept, for SendTo to report. A request that went
		// unanswered is sent again: the permission is kept until Close,
		// and a refresh lost to the network must not let it lapse while
		// no data goes to the peer to create it anew.
		if errors.Is(err, stun.ErrTimeout) {
			r.stale = 0
			c.send(r)
			return
		}
		if p := c.perms[r.peer.Addr()]; p != nil {
			p.pending = false
			p.err = err
		}
	case reqChannel:
		// Data to the peer goes on in Send indications. A refused channel
		// is not bound again unless BindChannel asks; one whose request
		// went unanswered is, by the next SendTo, with the same number,
		// which the server may still hold bound to the peer.
		if ch := c.chans[r.peer]; ch != nil && ch.number == r.channel {
			ch.pending = false
			ch.bound = false
			ch.failed = !errors.Is(err, stun.ErrTimeout)
		}
	}
}

func (c *Client) fail(err error) {
	c.state = clientFailed
	clear(c.txs)
	if c.cfg.OnFailed != nil {
		c.cfg.OnFailed(err)
	}
}

func (c *Client) newID() (id [stun.TransactionIDSize]byte) {
	if _, err := io.ReadFull(c.cfg.Rand, id[:]); err != nil {
		panic("turn: reading random bytes: " + err.Error())
	}
	return id
}

// send builds r afresh, with a new transaction ID and the credentials as
// they now stand, and sends it.
func (c *Client) send(r *request) {
	o := &c.out
	o.Build(stun.MessageType{Method: r.kind.method(), Class: stun.ClassRequest}, c.newID())
	switch r.kind {
	case reqAllocate:
		o.AddRequestedTransport(stun.ProtoUDP)
		if r.lifetime > 0 {
			o.AddLifetime(r.lifetime)
		}
	case reqRefresh:
		o.AddLifetime(r.lifetime)
	case reqPermission:
		o.AddXORAddress(stun.AttrXORPeerAddress, r.peer)
	case reqChannel:
		o.AddChannelNumber(r.channel)
		o.AddXORAddress(stun.AttrXORPeerAddress, r.peer)
	}
	if c.cfg.Software != "" {
		o.AddString(stun.AttrSoftware, c.cfg.Software)
	}
	r.authed = c.key != nil
	if r.authed {
		o.AddString(stun.AttrUsername, c.cfg.Username)
		o.AddString(stun.AttrRealm, c.realm)
		o.AddString(stun.AttrNonce, c.nonce)
		o.AddIntegrity(c.key)
	}
	o.AddFingerprint()
	r.tx.Start(o.Raw, c.now, c.cfg.Transaction)
	r.unclocked = c.inSendTo
	c.unclocked = c.unclocked || r.unclocked
	c.txs[r.tx.ID] = r
	c.cfg.Send(c.server, o.Raw)
}

// setClock takes now as the time, timing the unclocked requests from it.
func (c *Client) setClock(now time.Time) {
	c.now = now
	if !c.unclocked {
		return
	}
	c.unclocked = false
	for _, r := range c.txs {
		if r.unclocked {
			r.unclocked = false
			r.tx.Start(r.tx.Request, now, c.cfg.Transaction)
		}
	}
}

// NextTimeout returns when HandleTimeout must next be called, if ever. It
// changes with every call into the client, so the caller asks again after
// each one.
func (c *Client) NextTimeout() (time.Time, bool) {
	var t time.Time
	at := func(d time.Time) {
		if t.IsZero() || d.Before(t) {
			t = d
		}
	}
	if c.unclocked {
		at(c.now)
	}
	for _, r := range c.txs {
		if d, ok := r.tx.NextTimeout(); ok {
			at(d)
		}
	}
	if c.state == clientAllocated {
		if !c.refreshing {
			at(c.refreshAt)
		}
		for _, p := range c.perms {
			if !p.pending && p.err == nil {
				at(p.refreshAt)
			}
		}
		for _, ch := range c.chans {
			if ch.bound && !ch.pending {
				at(ch.refreshAt)
			}
		}
	}
	return t, !t.IsZero()
}

// HandleTimeout does what is due at now: retransmissions, and the refreshes
// of the allocation, its permissions and its channels.
func (c *Client) HandleTimeout(now time.Time) {
	c.setClock(now)
	for id, r := range c.txs {
		b, err := r.tx.HandleTimeout(now)
		if b != nil {
			c.cfg.Send(c.server, b)
		}
		if err != nil {
			delete(c.txs, id)
			c.failed(r, fmt.Errorf("turn: %v: %w", r.kind.method(), err))
		}
	}
	if c.state != clientAllocated {
		return
	}
	if !c.refreshing && !now.Before(c.refreshAt) {
		c.refreshing = true
		lifetime := c.cfg.Lifetime
		if lifetime <= 0 {
			lifetime = DefaultLifetime
		}
		c.send(&request{kind: reqRefresh, lifetime: lifetime})
	}
	for ip, p := range c.perms {
		if !p.pending && p.err == nil && !now.Before(p.refreshAt) {
			p.pending = true
			c.send(&request{kind: reqPermission, peer: netip.AddrPortFrom(ip, 0)})
		}
	}
	for _, ch := range c.chans {
		if ch.bound && !ch.pending && !now.Before(ch.refreshAt) {
			ch.pending = true
			c.send(&request{kind: reqChannel, peer: ch.peer, channel: ch.number})
		}
	}
}

// Close deallocates: it sends a Refresh with a zero lifetime, retransmitted
// until answered or given up, and forgets the permissions and channels.
// The client is done once NextTimeout reports nothing more to do.
func (c *Client) Close(now time.Time) {
	c.setClock(now)
	switch c.state {
	case clientAllocated:
		clear(c.txs)
		clear(c.perms)
		clear(c.chans)
		clear(c.chanByNum)
		c.state = clientClosing
		c.send(&request{kind: reqRefresh, lifetime: 0})
	case clientClosing, clientClosed:
	default:
		clear(c.txs)
		c.state = clientClosed
	}
}

// Closed reports whether the client is done: closed and deallocated, or
// failed.
func (c *Client) Closed() bool { return c.state == clientClosed || c.state == clientFailed }
