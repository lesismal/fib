package ice

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

var (
	ErrClosed       = errors.New("ice: agent closed")
	ErrNoSocket     = errors.New("ice: no local candidate on that LocalID")
	ErrCredentials  = errors.New("ice: ufrag or password is malformed")
	ErrNotBinding   = errors.New("ice: not a STUN Binding message")
	ErrLiteRelaying = errors.New("ice: a lite agent has only host candidates")
)

// State is the agent's connectivity as RTCIceConnectionState reports it.
type State uint8

const (
	// StateNew is an agent that knows no remote credentials yet.
	StateNew State = iota
	// StateChecking is an agent checking pairs, with none selected.
	StateChecking
	// StateConnected is an agent with a selected pair it has consent to
	// send on.
	StateConnected
	// StateDisconnected is an agent whose selected pair has gone without a
	// consent response for Config.DisconnectedTimeout; it may come back.
	StateDisconnected
	// StateFailed is an agent that found no working pair, or lost consent
	// on its selected one. Restart starts it over.
	StateFailed
	// StateClosed is an agent after Close.
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateChecking:
		return "checking"
	case StateConnected:
		return "connected"
	case StateDisconnected:
		return "disconnected"
	case StateFailed:
		return "failed"
	case StateClosed:
		return "closed"
	}
	return fmt.Sprintf("State(%d)", uint8(s))
}

// Socket is one of the caller's UDP sockets and the host addresses it is
// reachable at: its bound address, or, for a socket bound to a wildcard
// address, each interface address (HostAddrs) with its port.
type Socket struct {
	ID    LocalID
	Addrs []netip.AddrPort
}

// Pair is a local and a remote candidate the agent checks and may select.
// Data for the peer goes out of Local.LocalID to Remote.AddrPort().
type Pair struct {
	Local, Remote Candidate
}

// Config configures an Agent. The callbacks run synchronously inside the
// Agent's methods, so they must not call back into it; they are where the
// caller learns what to send and what changed.
type Config struct {
	// Controlling is the role the agent starts in: the offerer's, in
	// WebRTC, unless the peer is lite. A role conflict may switch it.
	Controlling bool
	// Lite makes this a lite agent (RFC 8445 section 2.5): it has only host
	// candidates, sends no checks, answers the peer's, and selects the pair
	// the controlling peer nominates. A lite agent is always controlled.
	// It sends no consent checks either, so it never reports
	// StateDisconnected or StateFailed: its caller times out a peer that
	// went quiet.
	Lite bool
	// Ufrag and Pwd are the local credentials; random ones are made if
	// empty.
	Ufrag, Pwd string
	// STUNServers are asked for server-reflexive candidates from each
	// socket Gather is given, those of the socket's address family. A
	// lite agent asks none of them.
	STUNServers []netip.AddrPort

	// Send sends b from the transport local to the address to. b is valid
	// only during the call. It is required.
	Send func(local LocalID, to netip.AddrPort, b []byte)
	// OnCandidate reports a local candidate to signal to the peer.
	OnCandidate func(c Candidate)
	// OnGatheringDone reports that Gather has found every candidate it
	// will: its STUN servers answered or gave up.
	OnGatheringDone func()
	// OnStateChange reports each change of State.
	OnStateChange func(s State)
	// OnSelectedPair reports the pair data now goes on.
	OnSelectedPair func(p Pair)

	// Ta paces checks: one every Ta (RFC 8445 section 14.2). Default 50ms.
	Ta time.Duration
	// CheckRTO, CheckMaxRTO and CheckMaxSends set how a check is
	// retransmitted. Defaults 500ms, 1600ms and 7 sends, so a pair that
	// does not answer fails after some 10s.
	CheckRTO, CheckMaxRTO time.Duration
	CheckMaxSends         int
	// NominationDelay is how long a controlling agent waits, after its
	// first pair succeeds, for a better pair before nominating the best it
	// has. It nominates sooner once no better pair is left to check.
	// Default 500ms.
	NominationDelay time.Duration
	// ConsentInterval is the mean time between consent checks on the
	// selected pair, RFC 7675's Tr; each is jittered by up to ±20%.
	// Default 5s.
	ConsentInterval time.Duration
	// DisconnectedTimeout is how long the selected pair may go without a
	// consent response before the agent reports StateDisconnected.
	// Default 8s.
	DisconnectedTimeout time.Duration
	// ConsentTimeout is how long it may go before consent is lost and the
	// agent fails, RFC 7675's 30s.
	ConsentTimeout time.Duration
	// FailedTimeout is how long the agent checks without selecting a pair
	// before it fails. Default 30s.
	FailedTimeout time.Duration
	// MaxPairs bounds the checklist (RFC 8445 section 6.1.2.5). Default 100.
	MaxPairs int
	// Software, if set, is sent as SOFTWARE in requests.
	Software string
	// Rand is where tie-breakers, transaction IDs and credentials come
	// from. Default crypto/rand.
	Rand io.Reader
}

func (c *Config) setDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.Ta, 50*time.Millisecond)
	def(&c.CheckRTO, 500*time.Millisecond)
	def(&c.CheckMaxRTO, 1600*time.Millisecond)
	def(&c.NominationDelay, 500*time.Millisecond)
	def(&c.ConsentInterval, 5*time.Second)
	def(&c.DisconnectedTimeout, 8*time.Second)
	def(&c.ConsentTimeout, 30*time.Second)
	def(&c.FailedTimeout, 30*time.Second)
	if c.CheckMaxSends <= 0 {
		c.CheckMaxSends = 7
	}
	if c.MaxPairs <= 0 {
		c.MaxPairs = 100
	}
	if c.Rand == nil {
		c.Rand = rand.Reader
	}
}

type pairState uint8

const (
	pairFrozen pairState = iota
	pairWaiting
	pairInProgress
	pairSucceeded
	pairFailed
)

type pairKey struct {
	local  LocalID
	remote netip.AddrPort
}

// pair is a candidate pair. Pairs are keyed by the local candidate's base
// and the remote address, the identity RFC 8445 section 6.1.2.4 prunes
// pairs to: two local candidates on one socket (a server-reflexive one and
// its host base, or two host addresses of a wildcard socket) send from the
// same place, and a reply to either may arrive on any connection of that
// socket, so the agent never tells them apart.
type pair struct {
	key        pairKey
	local      *Candidate
	remote     *Candidate
	foundation string
	priority   uint64
	state      pairState
	// nominate is set while a check with USE-CANDIDATE is due or in flight.
	nominate bool
	// nominateOnSuccess is set on a controlled agent when the peer
	// nominated the pair before this agent's own check of it succeeded.
	nominateOnSuccess bool
	queued            bool
}

type txKind uint8

const (
	txCheck txKind = iota
	txConsent
	txGather
)

type pending struct {
	kind  txKind
	pair  *pair
	nomin bool
	ctrl  bool
	key   string // the password the response is checked with
	local LocalID
	to    netip.AddrPort
	base  netip.AddrPort // a gather request's host address
	prio  uint32         // the PRIORITY a check carried
	tx    stun.Transaction
	// cancelled is set on a check superseded by a triggered check of its
	// pair (RFC 8445 section 7.3.1.4): it is no longer retransmitted, and
	// its timeout fails nothing, but a response to it still counts.
	cancelled bool
}

// Agent is an ICE agent (RFC 8445), full or, with Config.Lite, lite, for one
// data stream with one component, as WebRTC uses with BUNDLE and rtcp-mux,
// with trickle (RFC 8838) and consent freshness (RFC 7675). It does no I/O: packets come in
// through HandlePacket, go out through Config.Send, and time passes through
// HandleTimeout. Its methods must be called from one goroutine at a time.
type Agent struct {
	cfg         Config
	ufrag, pwd  string
	rufrag      string
	rpwd        string
	controlling bool
	tieBreaker  uint64
	remoteLite  bool
	state       State

	locals      []*Candidate
	remotes     []*Candidate
	remoteEnded bool
	prflxCount  int

	pairs     []*pair
	pairIndex map[pairKey]*pair
	triggered []*pair
	txs       map[[stun.TransactionIDSize]byte]*pending

	lastCheck     time.Time
	firstValid    time.Time
	checkingSince time.Time
	nominating    *pair

	selected      *pair
	consentPaused bool
	lastConsent   time.Time
	nextConsent   time.Time

	gathering   bool
	gatherFired bool

	in, out stun.Message
}

// NewAgent returns an agent configured by config.
func NewAgent(config Config) (*Agent, error) {
	if config.Send == nil {
		return nil, errors.New("ice: Config.Send is nil")
	}
	config.setDefaults()
	a := &Agent{
		cfg:         config,
		controlling: config.Controlling && !config.Lite,
		pairIndex:   make(map[pairKey]*pair),
		txs:         make(map[[stun.TransactionIDSize]byte]*pending),
	}
	if err := a.setLocalCredentials(config.Ufrag, config.Pwd); err != nil {
		return nil, err
	}
	a.tieBreaker = a.randUint64()
	return a, nil
}

func (a *Agent) randUint64() uint64 {
	var b [8]byte
	if _, err := io.ReadFull(a.cfg.Rand, b[:]); err != nil {
		panic("ice: reading random bytes: " + err.Error())
	}
	return binary.BigEndian.Uint64(b[:])
}

func (a *Agent) randomString(n int) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	b := make([]byte, n)
	if _, err := io.ReadFull(a.cfg.Rand, b); err != nil {
		panic("ice: reading random bytes: " + err.Error())
	}
	for i := range b {
		b[i] = alphabet[b[i]&63]
	}
	return string(b)
}

func (a *Agent) newTransactionID() (id [stun.TransactionIDSize]byte) {
	if _, err := io.ReadFull(a.cfg.Rand, id[:]); err != nil {
		panic("ice: reading random bytes: " + err.Error())
	}
	return id
}

func (a *Agent) setLocalCredentials(ufrag, pwd string) error {
	if ufrag == "" {
		ufrag = a.randomString(16)
	}
	if pwd == "" {
		pwd = a.randomString(32)
	}
	// RFC 8839 section 5.4: a ufrag of 4 to 256 ice-chars, a password of
	// 22 to 256.
	if len(ufrag) < 4 || len(ufrag) > 256 || !isICEChars(ufrag) ||
		len(pwd) < 22 || len(pwd) > 256 || !isICEChars(pwd) {
		return ErrCredentials
	}
	a.ufrag, a.pwd = ufrag, pwd
	return nil
}

// LocalCredentials returns the ufrag and password to signal to the peer.
func (a *Agent) LocalCredentials() (ufrag, pwd string) { return a.ufrag, a.pwd }

// LocalCandidates returns the local candidates gathered or added so far.
func (a *Agent) LocalCandidates() []Candidate {
	out := make([]Candidate, len(a.locals))
	for i, c := range a.locals {
		out[i] = *c
	}
	return out
}

// Controlling reports whether the agent is in the controlling role.
func (a *Agent) Controlling() bool { return a.controlling }

// State returns the agent's state.
func (a *Agent) State() State { return a.state }

// SelectedPair returns the pair data goes on, if one is selected.
func (a *Agent) SelectedPair() (Pair, bool) {
	if a.selected == nil {
		return Pair{}, false
	}
	return Pair{*a.selected.local, *a.selected.remote}, true
}

func (a *Agent) setState(s State) {
	if a.state == s {
		return
	}
	a.state = s
	if a.cfg.OnStateChange != nil {
		a.cfg.OnStateChange(s)
	}
}

// Gather adds a host candidate for each address of each socket and asks
// each STUN server, from each socket of its address family, for a
// server-reflexive one. OnCandidate reports every candidate, and
// OnGatheringDone the end, which is immediate without STUN servers.
func (a *Agent) Gather(sockets []Socket, now time.Time) error {
	if a.state == StateClosed {
		return ErrClosed
	}
	a.gathering = true
	a.gatherFired = false
	for _, s := range sockets {
		var v4, v6 netip.AddrPort
		for _, addr := range s.Addrs {
			addr = netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port())
			if err := a.AddLocalCandidate(Candidate{Type: Host, Address: addr.Addr(), Port: addr.Port(), LocalID: s.ID}, now); err != nil {
				return err
			}
			if addr.Addr().Is4() && !v4.IsValid() {
				v4 = addr
			} else if addr.Addr().Is6() && !v6.IsValid() {
				v6 = addr
			}
		}
		if a.cfg.Lite {
			continue
		}
		for _, server := range a.cfg.STUNServers {
			base := v4
			if server.Addr().Unmap().Is6() {
				base = v6
			}
			if !base.IsValid() {
				continue
			}
			a.out.Build(stun.BindingRequest, a.newTransactionID())
			if a.cfg.Software != "" {
				a.out.AddString(stun.AttrSoftware, a.cfg.Software)
			}
			a.out.AddFingerprint()
			p := &pending{kind: txGather, local: s.ID, to: server, base: base}
			p.tx.Start(a.out.Raw, now, stun.TransactionConfig{RTO: a.cfg.CheckRTO, MaxRTO: a.cfg.CheckMaxRTO, MaxSends: a.cfg.CheckMaxSends, LastWait: 2})
			a.txs[p.tx.ID] = p
			a.cfg.Send(s.ID, server, a.out.Raw)
		}
	}
	a.checkGatheringDone()
	return nil
}

func (a *Agent) checkGatheringDone() {
	if !a.gathering || a.gatherFired {
		return
	}
	for _, p := range a.txs {
		if p.kind == txGather {
			return
		}
	}
	a.gatherFired = true
	a.gathering = false
	if a.cfg.OnGatheringDone != nil {
		a.cfg.OnGatheringDone()
	}
}

// AddLocalCandidate adds a local candidate and reports it to OnCandidate.
// Gather adds host and server-reflexive candidates; this is for the others,
// a relay candidate above all: its LocalID names the TURN allocation, which
// the caller's Send relays through (wrapping what the agent sends in a Send
// indication or ChannelData), and whose received packets it passes to
// HandlePacket with that LocalID and the peer's address. A zero Foundation,
// Priority or Component is filled in.
func (a *Agent) AddLocalCandidate(c Candidate, now time.Time) error {
	if a.state == StateClosed {
		return ErrClosed
	}
	if a.cfg.Lite && c.Type != Host {
		return ErrLiteRelaying
	}
	if c.Hostname != "" || !c.Address.IsValid() {
		return ErrUnresolved
	}
	c.Address = c.Address.Unmap()
	if c.Component == 0 {
		c.Component = 1
	}
	if c.Transport == "" {
		c.Transport = "udp"
	}
	if c.Transport != "udp" {
		return ErrUnsupportedTransport
	}
	for _, l := range a.locals {
		if l.LocalID == c.LocalID && l.AddrPort() == c.AddrPort() && l.Type == c.Type {
			return nil
		}
	}
	if c.Foundation == "" {
		base := c.Address
		if c.Type == ServerReflexive {
			base = c.RelatedAddress.Addr()
		}
		var server netip.AddrPort
		if c.Type != Host {
			server = c.RelatedAddress
		}
		c.Foundation = foundation(c.Type, base, server)
	}
	if c.Priority == 0 {
		// Each candidate ranks below those added before it, so the order
		// the caller lists its sockets and addresses in is its preference.
		pref := 65535 - 16*len(a.locals)
		if pref < 0 {
			pref = 0
		}
		c.Priority = Priority(c.Type, uint16(pref), c.Component)
	}
	lc := &c
	a.locals = append(a.locals, lc)
	if c.Type == Host || c.Type == Relay {
		for _, r := range a.remotes {
			a.addPair(lc, r)
		}
		a.sortPairs()
	}
	if a.cfg.OnCandidate != nil {
		a.cfg.OnCandidate(c)
	}
	return nil
}

// SetRemoteCredentials sets the peer's ufrag and password. Checks start once
// they are known; until then the agent answers the peer's checks and keeps
// the candidates they reveal. New credentials after earlier ones mean the
// peer restarted ICE: the checks start over, while the selected pair keeps
// carrying data until another is selected.
func (a *Agent) SetRemoteCredentials(ufrag, pwd string, now time.Time) error {
	if a.state == StateClosed {
		return ErrClosed
	}
	if ufrag == "" || pwd == "" || !isICEChars(ufrag) {
		return ErrCredentials
	}
	if a.rufrag == ufrag && a.rpwd == pwd {
		return nil
	}
	if a.rufrag != "" {
		a.resetChecks()
	}
	a.rufrag, a.rpwd = ufrag, pwd
	a.startChecking(now)
	return nil
}

// SetRemoteLite records that the peer is a lite agent, which sends no
// checks: this agent then takes the controlling role, as RFC 8445 section
// 6.1.1 requires of a full agent facing a lite one.
func (a *Agent) SetRemoteLite(lite bool) {
	a.remoteLite = lite
	if lite && !a.cfg.Lite && !a.controlling {
		a.switchRole(true)
	}
}

func (a *Agent) startChecking(now time.Time) {
	if a.selected == nil || a.consentPaused {
		a.checkingSince = now
		if a.state != StateConnected && a.state != StateDisconnected {
			a.setState(StateChecking)
		}
	}
}

// AddRemoteCandidate adds a candidate the peer signaled, at any time:
// before or after checks start, as trickle ICE has it. A candidate of
// another ICE generation, by its ufrag extension, is ignored. A candidate
// whose address is an mDNS hostname is refused with ErrUnresolved; its
// caller resolves the name and adds it again with Address set.
func (a *Agent) AddRemoteCandidate(c Candidate, now time.Time) error {
	if a.state == StateClosed {
		return ErrClosed
	}
	if c.Component != 1 && c.Component != 0 {
		return ErrComponent
	}
	c.Component = 1
	if c.Transport != "" && !strings.EqualFold(c.Transport, "udp") {
		return ErrUnsupportedTransport
	}
	if c.Hostname != "" || !c.Address.IsValid() {
		return ErrUnresolved
	}
	if u, ok := c.Extension("ufrag"); ok && a.rufrag != "" && u != a.rufrag {
		return nil
	}
	c.Address = c.Address.Unmap()
	c.LocalID = 0
	addr := c.AddrPort()
	for _, r := range a.remotes {
		if r.AddrPort() != addr {
			continue
		}
		if r.Type == PeerReflexive && c.Type != PeerReflexive {
			// The check that revealed the address came before its
			// signaling: the candidate takes the signaled identity, and
			// its pairs keep their state (RFC 8445 section 7.3.1.3).
			*r = c
			for _, p := range a.pairs {
				if p.remote == r {
					p.foundation = p.local.Foundation + ":" + r.Foundation
				}
			}
			a.recomputePriorities()
		}
		return nil
	}
	rc := &c
	a.remotes = append(a.remotes, rc)
	before := len(a.pairIndex)
	for _, l := range a.locals {
		if l.Type == Host || l.Type == Relay {
			a.addPair(l, rc)
		}
	}
	a.sortPairs()
	if a.state == StateFailed && a.selected == nil && a.rpwd != "" && len(a.pairIndex) > before {
		a.startChecking(now)
	}
	return nil
}

// EndOfRemoteCandidates records that the peer signaled end-of-candidates:
// once its pairs have all failed, the agent fails without waiting for
// FailedTimeout.
func (a *Agent) EndOfRemoteCandidates() { a.remoteEnded = true }

// Restart starts ICE over with new local credentials, random if empty
// (RFC 8445 section 9): the peer's credentials and candidates are
// forgotten, and the caller signals the new credentials with the local
// candidates, which are kept. The selected pair keeps carrying data until
// the restarted checks select one.
func (a *Agent) Restart(ufrag, pwd string, now time.Time) error {
	if a.state == StateClosed {
		return ErrClosed
	}
	old, oldPwd := a.ufrag, a.pwd
	if err := a.setLocalCredentials(ufrag, pwd); err != nil {
		return err
	}
	if a.ufrag == old || a.pwd == oldPwd {
		a.ufrag, a.pwd = old, oldPwd
		return ErrCredentials
	}
	a.resetChecks()
	a.rufrag, a.rpwd = "", ""
	a.remoteLite = false
	a.checkingSince = now
	if a.selected == nil {
		a.setState(StateChecking)
	}
	return nil
}

// resetChecks forgets the remote candidates and the checklist, keeping the
// selected pair (and its candidates) for data until a new one is selected.
func (a *Agent) resetChecks() {
	for id, p := range a.txs {
		if p.kind != txGather {
			delete(a.txs, id)
		}
	}
	if a.selected != nil {
		sel := *a.selected
		sel.local, sel.remote = new(Candidate), new(Candidate)
		*sel.local, *sel.remote = *a.selected.local, *a.selected.remote
		a.selected = &sel
		a.consentPaused = true
	}
	a.remotes = nil
	a.pairs = nil
	clear(a.pairIndex)
	a.triggered = nil
	a.remoteEnded = false
	a.firstValid = time.Time{}
	a.nominating = nil
}

// Close stops the agent: it sends nothing more and its timers stop.
func (a *Agent) Close() {
	if a.state == StateClosed {
		return
	}
	clear(a.txs)
	a.pairs = nil
	clear(a.pairIndex)
	a.triggered = nil
	a.selected = nil
	a.setState(StateClosed)
}

func (a *Agent) pairPriority(l, r *Candidate) uint64 {
	g, d := uint64(l.Priority), uint64(r.Priority)
	if !a.controlling {
		g, d = d, g
	}
	p := min(g, d)<<32 + 2*max(g, d)
	if g > d {
		p++
	}
	return p
}

func (a *Agent) addPair(l, r *Candidate) {
	if l.Address.Is4() != r.Address.Is4() || l.Component != r.Component {
		return
	}
	key := pairKey{l.LocalID, r.AddrPort()}
	if p, ok := a.pairIndex[key]; ok {
		// A second local candidate on the same base: keep the one of
		// higher priority, which is what pruning keeps.
		if l.Priority > p.local.Priority {
			p.local = l
			p.foundation = l.Foundation + ":" + r.Foundation
			p.priority = a.pairPriority(l, r)
		}
		return
	}
	p := &pair{key: key, local: l, remote: r, foundation: l.Foundation + ":" + r.Foundation}
	p.priority = a.pairPriority(l, r)
	// RFC 8838 section 11: a new pair is Waiting unless a pair of its
	// foundation is already being checked, in which case it waits for that
	// one, Frozen; one whose foundation already succeeded is Waiting.
	p.state = pairWaiting
	for _, q := range a.pairs {
		if q.foundation == p.foundation && (q.state == pairWaiting || q.state == pairInProgress) {
			p.state = pairFrozen
			break
		}
	}
	a.pairs = append(a.pairs, p)
	a.pairIndex[key] = p
	if len(a.pairs) > a.cfg.MaxPairs {
		a.sortPairs()
		a.pruneTo(a.cfg.MaxPairs)
	}
}

// pruneTo drops the lowest-priority pairs not in use beyond n.
func (a *Agent) pruneTo(n int) {
	for i := len(a.pairs) - 1; i >= 0 && len(a.pairs) > n; i-- {
		p := a.pairs[i]
		if p.state == pairInProgress || p == a.selected || p == a.nominating {
			continue
		}
		delete(a.pairIndex, p.key)
		a.pairs = slices.Delete(a.pairs, i, i+1)
		a.triggered = slices.DeleteFunc(a.triggered, func(q *pair) bool { return q == p })
	}
}

func (a *Agent) sortPairs() {
	slices.SortStableFunc(a.pairs, func(x, y *pair) int {
		switch {
		case x.priority > y.priority:
			return -1
		case x.priority < y.priority:
			return 1
		}
		return 0
	})
}

func (a *Agent) recomputePriorities() {
	for _, p := range a.pairs {
		p.priority = a.pairPriority(p.local, p.remote)
	}
	a.sortPairs()
}

func (a *Agent) switchRole(controlling bool) {
	if a.controlling == controlling {
		return
	}
	a.controlling = controlling
	a.recomputePriorities()
	if !controlling {
		a.nominating = nil
		for _, p := range a.pairs {
			p.nominate = false
		}
	}
}

// localFor returns the local candidate a packet from remote arriving on id
// pairs with: the base of the socket, of remote's family.
func (a *Agent) localFor(id LocalID, remote netip.AddrPort) *Candidate {
	var best *Candidate
	for _, l := range a.locals {
		if l.LocalID != id || l.Address.Is4() != remote.Addr().Is4() || (l.Type != Host && l.Type != Relay) {
			continue
		}
		if best == nil || l.Priority > best.Priority {
			best = l
		}
	}
	return best
}

func (a *Agent) enqueueTriggered(p *pair) {
	if p.queued {
		return
	}
	p.queued = true
	if p.nominate {
		a.triggered = slices.Insert(a.triggered, 0, p)
	} else {
		a.triggered = append(a.triggered, p)
	}
}

func (a *Agent) selectPair(p *pair, now time.Time) {
	a.consentPaused = false
	a.lastConsent = now
	a.nextConsent = now.Add(a.consentDelay())
	a.nominating = nil
	if a.selected == nil || a.selected.key != p.key || a.selected.local != p.local || a.selected.remote != p.remote {
		a.selected = p
		if a.cfg.OnSelectedPair != nil {
			a.cfg.OnSelectedPair(Pair{*p.local, *p.remote})
		}
	}
	a.setState(StateConnected)
}

func (a *Agent) consentDelay() time.Duration {
	// RFC 7675 section 5.1: uniformly between 0.8 and 1.2 times Tr.
	j := int64(a.randUint64()>>1) % 401
	return a.cfg.ConsentInterval * time.Duration(800+j) / 1000
}
