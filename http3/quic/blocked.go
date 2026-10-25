package quic

// A sender that has data to send and is held back by its peer's flow control
// says so, once for each limit it reaches (RFC 9000 section 4.1): with
// DATA_BLOCKED when the connection's limit holds it, STREAM_DATA_BLOCKED when
// a stream's does, and STREAMS_BLOCKED when it wants a stream the peer does
// not allow it yet. The peer may raise the limit sooner for it, and it tells
// the peer's operator why the transfer stalled. A frame that is lost goes
// again only if the limit it reports still holds: once the peer has raised
// it, the frame is moot.

// blockedReport is the BLOCKED frame of one limit: the limit at it reported
// last, if reported, and whether a frame for it waits to be sent.
type blockedReport struct {
	at       uint64
	reported bool
	pending  bool
}

// block has a frame reporting the limit at sent, unless one has been for it.
func (b *blockedReport) block(at uint64) {
	if b.reported && b.at == at {
		return
	}
	b.at, b.reported, b.pending = at, true, true
}

// lost has the frame that reported at sent again, if it is still the limit
// last reported and limit, what the peer allows now, has not moved past it.
func (b *blockedReport) lost(at, limit uint64) {
	if b.reported && b.at == at && limit == at {
		b.pending = true
	}
}

// armed reports whether the frame waiting to be sent still reports the limit
// in force: limit, what the peer allows now, is the one it reported. A frame
// for a limit the peer has since raised is dropped, since a sender is asked
// not to say it is blocked once it can send (RFC 9000 section 4.1).
func (b *blockedReport) armed(limit uint64) bool {
	if !b.pending {
		return false
	}
	if b.at != limit {
		b.pending = false
		return false
	}
	return true
}

// appendBlocked adds the DATA_BLOCKED and STREAMS_BLOCKED frames waiting to be
// sent, as many as fit in max bytes of payload.
func (c *Conn) appendBlocked(b []byte, max int, sp *sentPacket) []byte {
	if r := &c.dataBlocked; r.armed(c.peerMaxData) && len(b)+1+VarintLen(r.at) <= max {
		b = append(b, frameDataBlocked)
		b = AppendVarint(b, r.at)
		r.pending = false
		sp.add(sentFrame{kind: sfDataBlocked, off: r.at})
	}
	for i := range c.streamsBlocked {
		r := &c.streamsBlocked[i]
		limit := c.peerMaxBidi
		typ, kind := byte(frameStreamsBlockedBidi), uint8(sfStreamsBlockedBidi)
		if i == 1 {
			limit, typ, kind = c.peerMaxUni, frameStreamsBlockedUni, sfStreamsBlockedUni
		}
		if r.armed(limit) && len(b)+1+VarintLen(r.at) <= max {
			b = append(b, typ)
			b = AppendVarint(b, r.at)
			r.pending = false
			sp.add(sentFrame{kind: kind, off: r.at})
		}
	}
	return b
}

// blockedPending reports whether a DATA_BLOCKED or STREAMS_BLOCKED frame
// waits to be sent.
func (c *Conn) blockedPending() bool {
	return c.dataBlocked.pending || c.streamsBlocked[0].pending || c.streamsBlocked[1].pending
}

// noteBlocked records why a stream with data to send has no credit for it:
// the stream's limit, the connection's, or both.
func (s *Stream) noteBlocked() {
	c := s.conn
	if s.send.next >= s.sendMax {
		s.blocked.block(s.sendMax)
	}
	if c.sentData >= c.peerMaxData {
		c.dataBlocked.block(c.peerMaxData)
	}
}
