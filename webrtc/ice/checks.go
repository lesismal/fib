package ice

import (
	"errors"
	"net/netip"
	"strconv"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// HandlePacket processes a STUN packet b that arrived on the transport local
// from the address from: a check from the peer, which it answers, or the
// response to one of its own requests. b is not retained. The caller
// demultiplexes first, passing only what stun.IsMessage accepts; any other
// STUN method is reported with ErrNotBinding.
func (a *Agent) HandlePacket(local LocalID, from netip.AddrPort, b []byte, now time.Time) error {
	if a.state == StateClosed {
		return ErrClosed
	}
	from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
	m := &a.in
	if err := m.Decode(b); err != nil {
		return err
	}
	if m.Type.Method != stun.MethodBinding {
		return ErrNotBinding
	}
	if m.Contains(stun.AttrFingerprint) {
		if err := m.CheckFingerprint(); err != nil {
			return err
		}
	}
	switch m.Type.Class {
	case stun.ClassRequest:
		return a.handleRequest(local, from, m, now)
	case stun.ClassSuccessResponse, stun.ClassErrorResponse:
		return a.handleResponse(local, from, m, now)
	}
	// A Binding indication is a keepalive, which needs no answer.
	return nil
}

// knownRequestAttr reports whether a Binding request's comprehension-required
// attribute is one this agent understands.
func knownRequestAttr(t stun.AttrType) bool {
	switch t {
	case stun.AttrUsername, stun.AttrMessageIntegrity, stun.AttrMessageIntegritySHA256,
		stun.AttrPriority, stun.AttrUseCandidate, stun.AttrXORMappedAddress, stun.AttrMappedAddress:
		return true
	}
	return !t.Required()
}

// ErrRejected is what HandlePacket reports for a check it answered with an
// error: one without credentials, with the wrong ones, or with an attribute
// it does not know.
var ErrRejected = errors.New("ice: request rejected")

func (a *Agent) handleRequest(local LocalID, from netip.AddrPort, m *stun.Message, now time.Time) error {
	user, okUser := m.Get(stun.AttrUsername)
	if !okUser || !m.Contains(stun.AttrMessageIntegrity) {
		a.sendError(local, from, m, stun.CodeBadRequest, "Bad Request")
		return ErrRejected
	}
	// USERNAME is "<this agent's ufrag>:<the peer's>" (RFC 8445 7.2.2).
	if len(user) <= len(a.ufrag) || string(user[:len(a.ufrag)]) != a.ufrag || user[len(a.ufrag)] != ':' {
		a.sendError(local, from, m, stun.CodeUnauthorized, "Unauthorized")
		return ErrRejected
	}
	if m.CheckIntegrity([]byte(a.pwd)) != nil {
		a.sendError(local, from, m, stun.CodeUnauthorized, "Unauthorized")
		return ErrRejected
	}
	var unknown []stun.AttrType
	for i := range m.Len() {
		if t, _ := m.Attr(i); !knownRequestAttr(t) {
			unknown = append(unknown, t)
		}
	}
	if len(unknown) > 0 {
		a.sendUnknown(local, from, m, unknown)
		return ErrRejected
	}
	prio, err := m.Uint32(stun.AttrPriority)
	if err != nil {
		a.sendError(local, from, m, stun.CodeBadRequest, "Bad Request")
		return ErrRejected
	}
	useCandidate := m.Contains(stun.AttrUseCandidate)

	// Role conflicts, RFC 8445 section 7.3.1.1.
	if theirs, err := m.Uint64(stun.AttrICEControlling); err == nil && a.controlling {
		if a.tieBreaker >= theirs {
			a.sendError(local, from, m, stun.CodeRoleConflict, "Role Conflict")
			return nil
		}
		a.switchRole(false)
	} else if theirs, err := m.Uint64(stun.AttrICEControlled); err == nil && !a.controlling {
		if a.tieBreaker >= theirs {
			a.switchRole(true)
		} else {
			a.sendError(local, from, m, stun.CodeRoleConflict, "Role Conflict")
			return nil
		}
	}

	a.sendSuccess(local, from, m)

	r := a.remoteAt(from)
	if r == nil {
		// A peer-reflexive candidate (RFC 8445 section 7.3.1.3): an
		// address of the peer's no signaling named, such as a NAT's
		// mapping toward this agent.
		a.prflxCount++
		r = &Candidate{
			Foundation: "prflx" + strconv.Itoa(a.prflxCount),
			Component:  1,
			Transport:  "udp",
			Priority:   prio,
			Address:    from.Addr(),
			Port:       from.Port(),
			Type:       PeerReflexive,
		}
		a.remotes = append(a.remotes, r)
	}
	l := a.localFor(local, from)
	if l == nil {
		return nil
	}
	key := pairKey{local, from}
	p := a.pairIndex[key]
	if p == nil {
		a.addPair(l, r)
		a.sortPairs()
		if p = a.pairIndex[key]; p == nil {
			return nil
		}
	}

	if a.cfg.Lite {
		// A lite agent checks nothing: the pair the controlling peer
		// nominates is valid by its check alone (RFC 8445 section 7.3.1.5).
		p.state = pairSucceeded
		if useCandidate {
			a.selectPair(p, now)
		}
		return nil
	}

	if useCandidate && !a.controlling {
		if p.state == pairSucceeded {
			a.selectPair(p, now)
			return nil
		}
		p.nominateOnSuccess = true
	}
	// A triggered check (RFC 8445 section 7.3.1.4). A pair already being
	// checked has its transaction cancelled and is checked again at once:
	// the peer's check says the path works now, while the request in
	// flight may be waiting out a long RTO after losses.
	if p.state == pairSucceeded {
		return nil
	}
	if a.selected != nil && !a.consentPaused && p != a.selected && !p.nominateOnSuccess {
		// Checks ended with the selection; the peer's late checks are
		// answered and no more.
		return nil
	}
	if p.state == pairInProgress {
		a.cancelChecks(p)
	}
	p.state = pairWaiting
	a.enqueueTriggered(p)
	return nil
}

// cancelChecks cancels p's checks in flight: they are not sent again and
// their timeout fails nothing, but a response that still arrives counts.
func (a *Agent) cancelChecks(p *pair) {
	for _, pend := range a.txs {
		if pend.kind == txCheck && pend.pair == p {
			pend.cancelled = true
		}
	}
}

func (a *Agent) remoteAt(addr netip.AddrPort) *Candidate {
	for _, r := range a.remotes {
		if r.AddrPort() == addr {
			return r
		}
	}
	return nil
}

func (a *Agent) sendSuccess(local LocalID, to netip.AddrPort, req *stun.Message) {
	o := &a.out
	o.Build(stun.BindingSuccess, req.TransactionID)
	o.AddXORAddress(stun.AttrXORMappedAddress, to)
	o.AddIntegrity([]byte(a.pwd))
	o.AddFingerprint()
	a.cfg.Send(local, to, o.Raw)
}

func (a *Agent) sendError(local LocalID, to netip.AddrPort, req *stun.Message, code int, reason string) {
	o := &a.out
	o.Build(stun.BindingError, req.TransactionID)
	o.AddErrorCode(code, reason)
	if code != stun.CodeBadRequest && code != stun.CodeUnauthorized {
		o.AddIntegrity([]byte(a.pwd))
	}
	o.AddFingerprint()
	a.cfg.Send(local, to, o.Raw)
}

func (a *Agent) sendUnknown(local LocalID, to netip.AddrPort, req *stun.Message, types []stun.AttrType) {
	o := &a.out
	o.Build(stun.BindingError, req.TransactionID)
	o.AddErrorCode(stun.CodeUnknownAttribute, "Unknown Attribute")
	o.AddUnknownAttributes(types)
	o.AddIntegrity([]byte(a.pwd))
	o.AddFingerprint()
	a.cfg.Send(local, to, o.Raw)
}

func (a *Agent) handleResponse(local LocalID, from netip.AddrPort, m *stun.Message, now time.Time) error {
	pend := a.txs[m.TransactionID]
	if pend == nil {
		return nil
	}
	if pend.kind == txGather {
		delete(a.txs, m.TransactionID)
		if m.Type.Class == stun.ClassSuccessResponse {
			mapped, err := m.XORAddress(stun.AttrXORMappedAddress)
			if err != nil {
				mapped, err = m.Address(stun.AttrMappedAddress)
			}
			if err == nil && mapped != pend.base {
				_ = a.AddLocalCandidate(Candidate{
					Type:           ServerReflexive,
					Address:        mapped.Addr(),
					Port:           mapped.Port(),
					RelatedAddress: pend.base,
					LocalID:        pend.local,
					Foundation:     foundation(ServerReflexive, pend.base.Addr(), pend.to),
				}, now)
			}
		}
		a.checkGatheringDone()
		return nil
	}
	// A response that fails its integrity check is ignored, and its
	// transaction goes on: it may be an attacker's (RFC 8489 section 10).
	if m.CheckIntegrity([]byte(pend.key)) != nil {
		return nil
	}
	delete(a.txs, m.TransactionID)
	p := pend.pair
	if pend.kind == txConsent {
		if m.Type.Class == stun.ClassSuccessResponse && p == a.selected && from == p.key.remote && local == p.key.local {
			a.lastConsent = now
			if a.state == StateDisconnected {
				a.setState(StateConnected)
			}
		}
		return nil
	}
	if a.pairIndex[p.key] != p {
		return nil // the pair was pruned or ICE restarted
	}
	if m.Type.Class == stun.ClassErrorResponse {
		code, _, _ := m.ErrorCode()
		if code == stun.CodeRoleConflict {
			// RFC 8445 section 7.2.5.1: switch to the role opposite the
			// one the request claimed, and check again.
			a.switchRole(!pend.ctrl)
			p.state = pairWaiting
			if !a.controlling {
				p.nominate = false
			}
			a.enqueueTriggered(p)
			return nil
		}
		a.failPair(p)
		return nil
	}
	// The check succeeds only if it was symmetric: the response came from
	// where the request went, to where it came from (RFC 8445 7.2.5.2.1).
	if from != p.key.remote || local != p.key.local {
		a.failPair(p)
		return nil
	}
	a.learnMapped(p, m, pend.prio)
	p.state = pairSucceeded
	if a.firstValid.IsZero() {
		a.firstValid = now
	}
	// Pairs that share its foundation may go ahead (RFC 8445 7.2.5.3.3).
	for _, q := range a.pairs {
		if q.state == pairFrozen && q.foundation == p.foundation {
			q.state = pairWaiting
		}
	}
	switch {
	case pend.nomin && a.controlling:
		p.nominate = false
		a.selectPair(p, now)
	case !a.controlling && p.nominateOnSuccess:
		a.selectPair(p, now)
	}
	return nil
}

// learnMapped makes the local candidate of the valid pair a successful
// check of p produced the one at the address the peer saw the check come
// from, XOR-MAPPED-ADDRESS (RFC 8445 section 7.2.5.3.1): a local candidate
// at that address on p's transport, a server-reflexive one say, or else a
// new peer-reflexive candidate with p's local candidate as its base and the
// priority the check carried, such as a NAT's mapping no STUN server
// revealed.
//
// The valid pair would have the same key as p, its transport and remote
// address, and the agent sends from that transport either way, so rather
// than adding a second pair that only duplicates p, p itself becomes the
// valid pair: its local candidate is replaced, which is what SelectedPair
// and OnSelectedPair then report. Peer-reflexive local candidates are not
// signaled, nor paired with other remote candidates (RFC 8445 section
// 7.2.5.3.1); LocalCandidates lists them.
func (a *Agent) learnMapped(p *pair, m *stun.Message, prio uint32) {
	mapped, err := m.XORAddress(stun.AttrXORMappedAddress)
	if err != nil {
		return
	}
	mapped = netip.AddrPortFrom(mapped.Addr().Unmap(), mapped.Port())
	if p.local.AddrPort() == mapped {
		return
	}
	var l *Candidate
	for _, c := range a.locals {
		if c.LocalID == p.key.local && c.AddrPort() == mapped {
			l = c
			break
		}
	}
	if l == nil {
		base := p.local
		if base.Type == PeerReflexive || base.Type == ServerReflexive {
			if b := a.localFor(p.key.local, p.key.remote); b != nil {
				base = b
			}
		}
		l = &Candidate{
			Foundation:     foundation(PeerReflexive, base.Address, netip.AddrPort{}),
			Component:      base.Component,
			Transport:      "udp",
			Priority:       prio,
			Address:        mapped.Addr(),
			Port:           mapped.Port(),
			Type:           PeerReflexive,
			RelatedAddress: base.AddrPort(),
			LocalID:        p.key.local,
		}
		a.locals = append(a.locals, l)
	}
	p.local = l
	p.priority = a.pairPriority(l, p.remote)
	a.sortPairs()
}

func (a *Agent) failPair(p *pair) {
	p.state = pairFailed
	if p.nominate {
		p.nominate = false
	}
	if a.nominating == p {
		a.nominating = nil
	}
}

// canCheck reports whether the agent may send checks: it knows the peer's
// password, and is a full agent.
func (a *Agent) canCheck() bool {
	return a.rpwd != "" && !a.cfg.Lite && a.state != StateClosed
}

// nextCheck picks the pair to check next, or nil: the triggered check queue
// first, then, while no pair is selected, the highest-priority Waiting
// pair, unfreezing a Frozen one whose foundation nothing is checking.
func (a *Agent) nextCheck() *pair {
	for len(a.triggered) > 0 {
		p := a.triggered[0]
		a.triggered = a.triggered[1:]
		p.queued = false
		if a.pairIndex[p.key] == p && (p.state == pairWaiting || p.nominate) {
			return p
		}
	}
	return a.ordinaryCheck()
}

// ordinaryCheck is the pair an ordinary check goes to next (RFC 8445
// section 6.1.4.2), if any.
func (a *Agent) ordinaryCheck() *pair {
	if a.selected != nil && !a.consentPaused {
		return nil
	}
	for _, p := range a.pairs {
		if p.state == pairWaiting {
			return p
		}
	}
	for _, p := range a.pairs {
		if p.state != pairFrozen {
			continue
		}
		busy := false
		for _, q := range a.pairs {
			if q.foundation == p.foundation && (q.state == pairWaiting || q.state == pairInProgress) {
				busy = true
				break
			}
		}
		if !busy {
			return p
		}
	}
	return nil
}

func (a *Agent) hasCheckWork() bool {
	return len(a.triggered) > 0 || a.ordinaryCheck() != nil
}

// sendRequest sends a Binding request for p: a check, with USE-CANDIDATE if
// p is being nominated, or a consent check.
func (a *Agent) sendRequest(p *pair, kind txKind, now time.Time) {
	o := &a.out
	o.Build(stun.BindingRequest, a.newTransactionID())
	o.AddString(stun.AttrUsername, a.rufrag+":"+a.ufrag)
	// PRIORITY is what a peer-reflexive candidate learned from this check
	// would have: its type's preference with the local candidate's local
	// preference (RFC 8445 section 7.1.1).
	prio := Priority(PeerReflexive, uint16(p.local.Priority>>8), p.local.Component)
	o.AddUint32(stun.AttrPriority, prio)
	if a.controlling {
		o.AddUint64(stun.AttrICEControlling, a.tieBreaker)
	} else {
		o.AddUint64(stun.AttrICEControlled, a.tieBreaker)
	}
	nomin := kind == txCheck && p.nominate && a.controlling
	if nomin {
		o.AddFlag(stun.AttrUseCandidate)
	}
	if a.cfg.Software != "" {
		o.AddString(stun.AttrSoftware, a.cfg.Software)
	}
	o.AddIntegrity([]byte(a.rpwd))
	o.AddFingerprint()
	pend := &pending{kind: kind, pair: p, nomin: nomin, ctrl: a.controlling, key: a.rpwd, local: p.key.local, to: p.key.remote, prio: prio}
	cfg := stun.TransactionConfig{RTO: a.cfg.CheckRTO, MaxRTO: a.cfg.CheckMaxRTO, MaxSends: a.cfg.CheckMaxSends, LastWait: 2}
	if kind == txConsent {
		// A consent check gives up before the next one is due.
		cfg.MaxSends = 4
	}
	pend.tx.Start(o.Raw, now, cfg)
	a.txs[pend.tx.ID] = pend
	if kind == txCheck {
		p.state = pairInProgress
	}
	a.cfg.Send(p.key.local, p.key.remote, o.Raw)
}

// NextTimeout returns when HandleTimeout must next be called, if ever. It
// changes with every call into the agent, so the caller asks again after
// each one; a time in the past means now.
func (a *Agent) NextTimeout() (time.Time, bool) {
	var t time.Time
	at := func(d time.Time) {
		if t.IsZero() || d.Before(t) {
			t = d
		}
	}
	if a.state == StateClosed {
		return t, false
	}
	for _, p := range a.txs {
		if d, ok := p.tx.NextTimeout(); ok {
			at(d)
		}
	}
	if a.canCheck() {
		if a.hasCheckWork() {
			at(a.lastCheck.Add(a.cfg.Ta))
		}
		if a.controlling && (a.selected == nil || a.consentPaused) && a.nominating == nil && a.hasValid() {
			at(a.firstValid.Add(a.cfg.NominationDelay))
		}
		if (a.selected == nil || a.consentPaused) && a.state != StateFailed {
			at(a.checkingSince.Add(a.cfg.FailedTimeout))
		}
	}
	if a.selected != nil && !a.consentPaused && !a.cfg.Lite {
		at(a.nextConsent)
		if a.state == StateConnected {
			at(a.lastConsent.Add(a.cfg.DisconnectedTimeout))
		}
		at(a.lastConsent.Add(a.cfg.ConsentTimeout))
	}
	return t, !t.IsZero()
}

// HandleTimeout does what is due at now: retransmissions, the next paced
// check, nomination, consent checks, and the timeouts of state.
func (a *Agent) HandleTimeout(now time.Time) {
	if a.state == StateClosed {
		return
	}
	for id, p := range a.txs {
		b, err := p.tx.HandleTimeout(now)
		if b != nil && !p.cancelled {
			a.cfg.Send(p.local, p.to, b)
		}
		if err == nil {
			continue
		}
		delete(a.txs, id)
		switch p.kind {
		case txGather:
			a.checkGatheringDone()
		case txCheck:
			if !p.cancelled && a.pairIndex[p.pair.key] == p.pair && p.pair.state == pairInProgress {
				a.failPair(p.pair)
			}
		}
	}
	if a.canCheck() {
		a.maybeNominate(now)
		if !now.Before(a.lastCheck.Add(a.cfg.Ta)) {
			if p := a.nextCheck(); p != nil {
				a.lastCheck = now
				a.sendRequest(p, txCheck, now)
			}
		}
	}
	a.checkConsent(now)
	a.checkFailed(now)
}

func (a *Agent) hasValid() bool {
	for _, p := range a.pairs {
		if p.state == pairSucceeded {
			return true
		}
	}
	return false
}

// maybeNominate has a controlling agent nominate the best valid pair once no
// pending pair could beat it, or NominationDelay after the first succeeded.
func (a *Agent) maybeNominate(now time.Time) {
	if !a.controlling || (a.selected != nil && !a.consentPaused) || a.nominating != nil {
		return
	}
	var best *pair
	for _, p := range a.pairs {
		if p.state == pairSucceeded {
			best = p
			break
		}
	}
	if best == nil {
		return
	}
	if now.Before(a.firstValid.Add(a.cfg.NominationDelay)) {
		for _, p := range a.pairs {
			if p == best {
				break
			}
			if p.state != pairFailed {
				return // a better pair may yet succeed
			}
		}
	}
	a.nominating = best
	best.nominate = true
	a.enqueueTriggered(best)
}

func (a *Agent) checkConsent(now time.Time) {
	if a.selected == nil || a.consentPaused || a.cfg.Lite {
		return
	}
	if since := now.Sub(a.lastConsent); since >= a.cfg.ConsentTimeout {
		// Consent is lost (RFC 7675 section 5.1): nothing more may be
		// sent on the pair.
		a.selected = nil
		a.setState(StateFailed)
		return
	} else if since >= a.cfg.DisconnectedTimeout {
		a.setState(StateDisconnected)
	}
	if !now.Before(a.nextConsent) && a.rpwd != "" {
		a.nextConsent = now.Add(a.consentDelay())
		a.sendRequest(a.selected, txConsent, now)
	}
}

func (a *Agent) checkFailed(now time.Time) {
	if !a.canCheck() || (a.selected != nil && !a.consentPaused) || a.state == StateFailed {
		return
	}
	failed := !now.Before(a.checkingSince.Add(a.cfg.FailedTimeout))
	if !failed && a.remoteEnded && !a.gathering && len(a.pairs) > 0 {
		failed = true
		for _, p := range a.pairs {
			if p.state != pairFailed {
				failed = false
				break
			}
		}
	}
	if failed {
		a.selected = nil
		a.consentPaused = false
		a.setState(StateFailed)
	}
}
