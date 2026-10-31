package srtp

import (
	"crypto/subtle"
	"encoding/binary"
	"fmt"

	"github.com/lesismal/fib/webrtc/rtp"
)

// estimateROC guesses the rollover counter a packet with the sequence
// number was sent under, the one that puts it nearest the highest sequence
// number s.lastSeq seen so far (RFC 3711 Appendix A).
func (s *rtpState) estimateROC(seq uint16) uint32 {
	if !s.started {
		return s.roc
	}
	if s.lastSeq < 0x8000 {
		if seq > s.lastSeq && seq-s.lastSeq > 0x8000 && s.roc > 0 {
			return s.roc - 1
		}
		return s.roc
	}
	if s.lastSeq-0x8000 > seq {
		return s.roc + 1
	}
	return s.roc
}

// update moves the highest sequence number and rollover counter on for a
// packet that authenticated, or that this side sends.
func (s *rtpState) update(roc uint32, seq uint16) {
	switch {
	case !s.started:
		s.started = true
		s.roc, s.lastSeq = roc, seq
	case roc == s.roc+1:
		s.roc, s.lastSeq = roc, seq
	case roc == s.roc && seq > s.lastSeq:
		s.lastSeq = seq
	}
}

// ProtectRTP appends the SRTP packet protecting the RTP packet pkt to dst
// and returns the result. dst may be pkt[:0] to protect it in place, given
// room for the authentication tag, Profile().RTPOverhead() bytes; it must
// not overlap pkt otherwise.
func (c *Context) ProtectRTP(dst, pkt []byte) ([]byte, error) {
	hl, err := rtp.HeaderLen(pkt)
	if err != nil {
		return dst, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	ssrc := binary.BigEndian.Uint32(pkt[8:])
	seq := binary.BigEndian.Uint16(pkt[2:])
	s := c.rtpOut[ssrc]
	if s == nil {
		s = c.newRTPState()
		c.rtpOut[ssrc] = s
	}
	roc := s.estimateROC(seq)
	s.update(roc, seq)

	n := len(pkt)
	whole, out := sliceForAppend(dst, n+c.rtpTagLen)
	copy(out, pkt)
	if c.rtp.gcm != nil {
		iv := c.gcmIV(&c.rtp.salt, ssrc, roc, seq)
		c.rtp.gcm.Seal(out[hl:hl], iv, out[hl:n], out[:hl])
		return whole, nil
	}
	c.ctrIV(&c.rtp.salt, ssrc, uint64(roc)<<16|uint64(seq))
	c.xorCTR(c.rtp.block, out[hl:n], out[hl:n])
	binary.BigEndian.PutUint32(c.trailer[:4], roc)
	tag := c.hmacTag(&c.rtp, out[:n], c.trailer[:4])
	copy(out[n:], tag[:c.rtpTagLen])
	return whole, nil
}

// UnprotectRTP authenticates the SRTP packet pkt, appends the RTP packet
// it protects to dst and returns the result. dst may be pkt[:0] to
// unprotect it in place; it must not overlap pkt otherwise. A packet that
// fails authentication, or that the replay window has seen or left behind,
// returns an error and changes no state; with an AEAD profile the bytes
// past dst's length, pkt's own when in place, may have been overwritten.
func (c *Context) UnprotectRTP(dst, pkt []byte) ([]byte, error) {
	hl, err := rtp.HeaderLen(pkt)
	if err != nil {
		return dst, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pkt) < hl+c.rtpTagLen {
		return dst, ErrShortPacket
	}
	ssrc := binary.BigEndian.Uint32(pkt[8:])
	seq := binary.BigEndian.Uint16(pkt[2:])
	s := c.rtpIn[ssrc]
	var roc uint32
	if s != nil {
		roc = s.estimateROC(seq)
		if s.started && !s.replay.check(uint64(roc)<<16|uint64(seq)) {
			return dst, ErrReplayed
		}
	}

	n := len(pkt) - c.rtpTagLen
	var whole []byte
	if c.rtp.gcm != nil {
		var out []byte
		whole, out = sliceForAppend(dst, len(pkt))
		copy(out, pkt)
		iv := c.gcmIV(&c.rtp.salt, ssrc, roc, seq)
		if _, err := c.rtp.gcm.Open(out[hl:hl], iv, out[hl:], out[:hl]); err != nil {
			return dst, ErrAuthFailed
		}
		whole = whole[:len(dst)+n]
	} else {
		binary.BigEndian.PutUint32(c.trailer[:4], roc)
		tag := c.hmacTag(&c.rtp, pkt[:n], c.trailer[:4])
		if subtle.ConstantTimeCompare(tag[:c.rtpTagLen], pkt[n:]) != 1 {
			return dst, ErrAuthFailed
		}
		var out []byte
		whole, out = sliceForAppend(dst, n)
		copy(out, pkt[:n])
		c.ctrIV(&c.rtp.salt, ssrc, uint64(roc)<<16|uint64(seq))
		c.xorCTR(c.rtp.block, out[hl:], out[hl:])
	}

	if s == nil {
		s = c.newRTPState()
		c.rtpIn[ssrc] = s
	}
	s.update(roc, seq)
	s.replay.accept(uint64(roc)<<16 | uint64(seq))
	return whole, nil
}
