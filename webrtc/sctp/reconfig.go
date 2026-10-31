package sctp

import (
	"encoding/binary"
	"slices"
	"time"
)

// reconfig is the state of stream resets (RFC 6525): this side's one
// outstanding Outgoing SSN Reset Request, the streams waiting for the
// next, and the sequence numbers of the peer's requests.
type reconfig struct {
	reqSeq       uint32
	reqActive    bool
	reqLastTSN   uint32
	reqStreams   []uint16
	resetQueue   []uint16
	reconfAt     time.Time
	reconfResend bool

	peerReqSeq uint32
	lastResult uint32
	responses  []uint64
}

// ResetStream resets stream id's outgoing direction once what is queued on
// it has been sent, which is how a data channel closes (RFC 8831 section
// 6.7). Sending on it fails from now until the reset completes.
func (a *Association) ResetStream(id uint16) error {
	if a.closed {
		return ErrClosed
	}
	st := a.stream(id)
	if st.resetting {
		return nil
	}
	st.resetting = true
	a.resetQueue = append(a.resetQueue, id)
	if a.depth == 0 {
		a.enter(a.cfg.Now())
		a.exit()
	}
	return nil
}

// flushReconfig sends responses owed to the peer and this side's request,
// new or again.
func (a *Association) flushReconfig() {
	for _, r := range a.responses {
		b := a.reserve(chunkReconfig, 0, 12)
		binary.BigEndian.PutUint16(b, paramReconfigResponse)
		binary.BigEndian.PutUint16(b[2:], 12)
		binary.BigEndian.PutUint32(b[4:], uint32(r>>32))
		binary.BigEndian.PutUint32(b[8:], uint32(r))
	}
	a.responses = a.responses[:0]
	if a.state != StateEstablished {
		return
	}
	if !a.reqActive && len(a.resetQueue) > 0 {
		// A stream joins the request once all it had queued has a TSN, so
		// the peer resets it after the last of it.
		a.reqStreams = a.reqStreams[:0]
		n := 0
		for _, id := range a.resetQueue {
			if a.stream(id).queued == 0 {
				a.reqStreams = append(a.reqStreams, id)
			} else {
				a.resetQueue[n] = id
				n++
			}
		}
		a.resetQueue = a.resetQueue[:n]
		if len(a.reqStreams) == 0 {
			return
		}
		if !a.peerReconfig {
			// The peer cannot reset streams; finish locally.
			a.outgoingResetDone()
			return
		}
		a.reqActive = true
		a.reqLastTSN = a.nextTSN - 1
		a.reconfResend = true
	}
	if a.reqActive && a.reconfResend {
		a.reconfResend = false
		n := 16 + 2*len(a.reqStreams)
		b := a.reserve(chunkReconfig, 0, n)
		binary.BigEndian.PutUint16(b, paramOutgoingReset)
		binary.BigEndian.PutUint16(b[2:], uint16(n))
		binary.BigEndian.PutUint32(b[4:], a.reqSeq)
		binary.BigEndian.PutUint32(b[8:], a.peerReqSeq-1)
		binary.BigEndian.PutUint32(b[12:], a.reqLastTSN)
		for i, id := range a.reqStreams {
			binary.BigEndian.PutUint16(b[16+2*i:], id)
		}
		a.reconfAt = a.now.Add(a.rto)
	}
}

// outgoingResetDone completes this side's request: the streams start again
// at SSN 0 and their channels learn they are half closed.
func (a *Association) outgoingResetDone() {
	streams := a.reqStreams
	a.reqActive = false
	a.reqSeq++
	a.reconfAt = time.Time{}
	for _, id := range streams {
		st := a.stream(id)
		st.ssn = 0
		st.resetting = false
		if st.ch != nil {
			st.ch.outgoingReset()
		}
	}
	a.reqStreams = streams[:0]
}

func (a *Association) handleReconfig(v []byte) error {
	if a.state < StateEstablished {
		return nil
	}
	for p := v; len(p) > 0 && !a.closed; {
		typ, pv, rest, err := nextParam(p)
		if err != nil {
			return err
		}
		p = rest
		switch typ {
		case paramOutgoingReset:
			if len(pv) < 12 || len(pv)%2 != 0 {
				return ErrMalformed
			}
			a.handleResetRequest(pv)
		case paramReconfigResponse:
			if len(pv) < 8 {
				return ErrMalformed
			}
			seq := binary.BigEndian.Uint32(pv)
			result := binary.BigEndian.Uint32(pv[4:])
			if !a.reqActive || seq != a.reqSeq {
				continue
			}
			switch result {
			case reconfigInProgress:
				// Asked again when the timer runs out.
			default:
				// Done, or refused: either way the streams count as reset
				// on this side, as nothing more will be sent on them.
				a.outgoingResetDone()
			}
		case paramIncomingReset, paramSSNTSNReset, paramAddOutgoingStreams, paramAddIncomingStreams:
			if len(pv) < 4 {
				return ErrMalformed
			}
			seq := binary.BigEndian.Uint32(pv)
			a.respond(seq, reconfigDenied)
		}
	}
	return nil
}

// handleResetRequest serves the peer's Outgoing SSN Reset Request (RFC 6525
// section 5.2.2): once this side has every TSN up to the peer's last, the
// streams' incoming SSNs start over and their channels are told.
func (a *Association) handleResetRequest(pv []byte) {
	seq := binary.BigEndian.Uint32(pv)
	lastTSN := binary.BigEndian.Uint32(pv[8:])
	switch seq {
	case a.peerReqSeq - 1:
		a.queueResponse(seq, a.lastResult)
		return
	case a.peerReqSeq:
	default:
		a.queueResponse(seq, reconfigErrorBadSeq)
		return
	}
	if tsnGT(lastTSN, a.peerCum) {
		// Data before the reset is still on its way; the peer asks again.
		a.queueResponse(seq, reconfigInProgress)
		return
	}
	a.peerReqSeq++
	a.lastResult = reconfigSuccessPerformed
	a.queueResponse(seq, reconfigSuccessPerformed)
	ids := pv[12:]
	if len(ids) == 0 {
		// No streams listed means all of them.
		for id, st := range a.streams {
			if st != nil {
				a.incomingReset(uint16(id), st)
			}
		}
		return
	}
	for ; len(ids) >= 2; ids = ids[2:] {
		id := binary.BigEndian.Uint16(ids)
		if id < a.inStreams {
			a.incomingReset(id, a.stream(id))
		}
	}
}

func (a *Association) incomingReset(id uint16, st *stream) {
	a.spill(id, st)
	st.nextSSN = 0
	st.headN, st.headBytes = 0, 0
	for _, c := range st.ordered {
		a.held -= len(c.data)
		a.pool.put(c.data)
	}
	clear(st.ordered)
	st.ordered = st.ordered[:0]
	if st.ch != nil {
		st.ch.incomingReset()
	}
}

// respond answers a request this side does not perform, keeping the
// sequence numbers in step.
func (a *Association) respond(seq, result uint32) {
	switch seq {
	case a.peerReqSeq:
		a.peerReqSeq++
		a.lastResult = result
	case a.peerReqSeq - 1:
		result = a.lastResult
	default:
		result = reconfigErrorBadSeq
	}
	a.queueResponse(seq, result)
}

func (a *Association) queueResponse(seq, result uint32) {
	r := uint64(seq)<<32 | uint64(result)
	if !slices.Contains(a.responses, r) {
		a.responses = append(a.responses, r)
	}
}
