package quic

import (
	"bytes"
	"crypto/rand"
)

// The connection IDs this side issues to its peer (RFC 9000 section 5.1).
//
// The one of the handshake, sequence number 0, is the connection's scid.
// Once the handshake is confirmed, a connection issues more, up to
// Config.ActiveConnectionIDs and the peer's active_connection_id_limit, in
// NEW_CONNECTION_ID frames, and issues another each time the peer retires
// one with RETIRE_CONNECTION_ID. The peer may send to any of them, as a
// client does to a new one when its path changes, so that the change cannot
// be linked to the path before by an observer (RFC 9000 section 9.5); a
// server finds the connection by them (see Owns and ConnectionIDHandler).
//
// A server still sends disable_active_migration: it follows a client whose
// address changes under it, as a NAT rebinding changes it (see
// HandlePathDatagram), but answers a PATH_CHALLENGE on the path the
// connection sends on, which a client probing a path of its choosing would
// not hear.

// DefaultActiveConnectionIDs is Config.ActiveConnectionIDs's default.
const DefaultActiveConnectionIDs = 4

// maxActiveConnectionIDs bounds Config.ActiveConnectionIDs: a peer has no use
// for more, and a server keeps a route for each.
const maxActiveConnectionIDs = 8

// ConnectionIDHandler is a Handler that is told of the connection IDs the
// connection issues to its peer beyond the one of the handshake, which
// LocalCID is, and of each one the peer retires, which the connection then
// no longer owns: a server that finds a connection by the connection ID of a
// packet from an address it does not know keeps them. OnConnectionID runs
// like the handler's other calls, one at a time and with no lock held, with
// active set for one being issued, before the peer is sent it, and unset for
// one retired. The connection's own end retires none of them: a server
// forgets them all with the connection, by LocalCIDs.
type ConnectionIDHandler interface {
	OnConnectionID(c *Conn, cid []byte, active bool)
}

// localCID is a connection ID this side has issued, or is about to.
type localCID struct {
	seq   uint64
	cid   []byte
	token []byte
	// pending says its NEW_CONNECTION_ID is waiting to be sent, once the
	// handler has been told of it.
	pending bool
}

// LocalCIDs lists the connection IDs the connection owns: those it has
// issued to its peer and the peer has not retired, the one of the handshake
// among them unless the peer has retired it, and those about to be issued.
// It is safe to call from any goroutine.
func (c *Conn) LocalCIDs() [][]byte {
	if p := c.owned.Load(); p != nil {
		return *p
	}
	return [][]byte{c.scid}
}

// publishCIDsLocked makes what Owns and LocalCIDs see the connection IDs of
// localCIDs. Callers hold c.mu.
func (c *Conn) publishCIDsLocked() {
	cids := make([][]byte, len(c.localCIDs))
	for i, l := range c.localCIDs {
		cids[i] = l.cid
	}
	c.owned.Store(&cids)
}

// issueCIDsLocked queues the issue of as many connection IDs as the peer
// may have, beyond those it has: each goes to the handler first (see run),
// and to the peer after. It does nothing before the handshake is confirmed,
// since NEW_CONNECTION_ID goes in 1-RTT packets that are then sure to be
// read (RFC 9000 section 5.1.1). Callers hold c.mu.
func (c *Conn) issueCIDsLocked() {
	if !c.handshakeConfirmed || c.closed || c.peerParams == nil {
		return
	}
	limit := min(uint64(c.config.ActiveConnectionIDs), c.peerParams.activeCIDLimit)
	for uint64(len(c.localCIDs)+c.cidsIssuing) < limit {
		c.cidsIssuing++
		c.events = append(c.events, event{kind: evIssueCID})
	}
}

// issueCID makes a connection ID, tells the handler of it, and sends it to
// the peer. It runs on the goroutine that makes the handler's calls, with no
// lock held, so that a server routes the connection ID before the peer can
// use it.
func (c *Conn) issueCID() {
	cid := randomCID()
	c.mu.Lock()
	c.cidsIssuing--
	if c.closed {
		c.mu.Unlock()
		return
	}
	var token []byte
	if c.config.ResetKey != nil {
		token = c.config.ResetKey.token(cid)
	} else {
		// Nothing recognizes a reset from a client, but the frame carries a
		// token all the same.
		token = make([]byte, statelessResetTokenLen)
		_, _ = rand.Read(token)
	}
	c.localCIDs = append(c.localCIDs, localCID{seq: c.nextCIDSeq, cid: cid, token: token})
	c.nextCIDSeq++
	c.publishCIDsLocked()
	c.mu.Unlock()
	if h, ok := c.handler.(ConnectionIDHandler); ok {
		h.OnConnectionID(c, cid, true)
	}
	c.mu.Lock()
	for i := range c.localCIDs {
		if bytes.Equal(c.localCIDs[i].cid, cid) {
			c.localCIDs[i].pending = true
			c.cidsPending = true
			c.wantFlush = true
		}
	}
	c.mu.Unlock()
}

// handleRetireCID takes the peer's RETIRE_CONNECTION_ID for seq: the
// connection ID goes, and another is issued in its place (RFC 9000 section
// 19.16). dcid is the connection ID of the packet that carried the frame,
// which may not be the one retired. Callers hold c.mu.
func (c *Conn) handleRetireCID(seq uint64, dcid []byte) error {
	if seq >= c.nextCIDSeq {
		return transportErr(errProtocolViolation, "RETIRE_CONNECTION_ID for a connection ID not issued")
	}
	// Retiring costs this side a connection ID made and routed in its
	// place, so a peer that retires them in a loop is flooding it.
	if err := c.glitch(); err != nil {
		return err
	}
	for i, l := range c.localCIDs {
		if l.seq != seq {
			continue
		}
		if bytes.Equal(l.cid, dcid) {
			return transportErr(errProtocolViolation, "RETIRE_CONNECTION_ID for the connection ID it was sent to")
		}
		c.localCIDs = append(c.localCIDs[:i], c.localCIDs[i+1:]...)
		c.publishCIDsLocked()
		c.events = append(c.events, event{kind: evRetireCID, data: l.cid})
		c.issueCIDsLocked()
		return nil
	}
	// Retired already.
	return nil
}

// appendNewCIDs adds the NEW_CONNECTION_ID frames waiting to be sent, as
// many as fit in max bytes of payload.
func (c *Conn) appendNewCIDs(b []byte, max int, sp *sentPacket) []byte {
	if !c.cidsPending {
		return b
	}
	left := false
	for i := range c.localCIDs {
		l := &c.localCIDs[i]
		if !l.pending {
			continue
		}
		n := 1 + VarintLen(l.seq) + 1 + 1 + len(l.cid) + len(l.token)
		if len(b)+n > max {
			left = true
			continue
		}
		b = append(b, frameNewConnectionID)
		b = AppendVarint(b, l.seq)
		b = AppendVarint(b, 0)
		b = append(b, byte(len(l.cid)))
		b = append(b, l.cid...)
		b = append(b, l.token...)
		l.pending = false
		sp.add(sentFrame{kind: sfNewCID, off: l.seq})
	}
	c.cidsPending = left
	return b
}

// newCIDLost sends again the NEW_CONNECTION_ID for seq that was lost, if the
// peer has not retired its connection ID since.
func (c *Conn) newCIDLost(seq uint64) {
	for i := range c.localCIDs {
		if c.localCIDs[i].seq == seq {
			c.localCIDs[i].pending = true
			c.cidsPending = true
		}
	}
}
