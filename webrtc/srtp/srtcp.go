package srtp

import (
	"crypto/subtle"
	"encoding/binary"
)

// srtcpHeaderSize is the part of an RTCP packet SRTCP leaves in the clear:
// the first packet's header and sender SSRC.
const srtcpHeaderSize = 8

// srtcpEBit marks an SRTCP packet as encrypted, in the word that also
// carries its 31-bit index.
const srtcpEBit = 1 << 31

// maxSRTCPIndex is the highest SRTCP index; a sender that reaches it must
// be rekeyed.
const maxSRTCPIndex = 1<<31 - 1

func checkRTCP(pkt []byte) error {
	if len(pkt) < srtcpHeaderSize {
		return ErrShortPacket
	}
	if pkt[0]>>6 != 2 {
		return ErrMalformed
	}
	return nil
}

// ProtectRTCP appends the SRTCP packet protecting the compound RTCP
// packet pkt to dst, encrypted, with the next SRTCP index of its sender
// SSRC, and returns the result. dst may be pkt[:0] to protect it in place,
// given room for Profile().RTCPOverhead() bytes more; it must not overlap
// pkt otherwise.
func (c *Context) ProtectRTCP(dst, pkt []byte) ([]byte, error) {
	if err := checkRTCP(pkt); err != nil {
		return dst, err
	}
	ssrc := binary.BigEndian.Uint32(pkt[4:])
	index, seen := c.rtcpOut[ssrc]
	if seen {
		if index == maxSRTCPIndex {
			return dst, ErrIndexExhaust
		}
		index++
	}
	c.rtcpOut[ssrc] = index
	word := srtcpEBit | index

	n := len(pkt)
	whole, out := sliceForAppend(dst, n+4+c.rtcpTagLen)
	copy(out, pkt)
	if c.rtcp.gcm != nil {
		// RFC 7714 section 9: the trailer word goes after the tag, and is
		// authenticated along with the header.
		aad := c.trailer[:12]
		copy(aad, out[:srtcpHeaderSize])
		binary.BigEndian.PutUint32(aad[8:], word)
		iv := c.gcmIV(&c.rtcp.salt, ssrc, 0, 0)
		binary.BigEndian.PutUint32(iv[8:], binary.BigEndian.Uint32(iv[8:])^index)
		c.rtcp.gcm.Seal(out[srtcpHeaderSize:srtcpHeaderSize], iv, out[srtcpHeaderSize:n], aad)
		binary.BigEndian.PutUint32(out[n+16:], word)
		return whole, nil
	}
	c.ctrIV(&c.rtcp.salt, ssrc, uint64(index))
	c.xorCTR(c.rtcp.block, out[srtcpHeaderSize:n], out[srtcpHeaderSize:n])
	binary.BigEndian.PutUint32(out[n:], word)
	tag := c.hmacTag(&c.rtcp, out[:n+4], nil)
	copy(out[n+4:], tag[:c.rtcpTagLen])
	return whole, nil
}

// UnprotectRTCP authenticates the SRTCP packet pkt, appends the compound
// RTCP packet it protects to dst and returns the result. It takes packets
// sent unencrypted, with the E bit clear, as well as encrypted ones. dst
// may be pkt[:0] to unprotect it in place; it must not overlap pkt
// otherwise. A packet that fails authentication, or that the replay window
// has seen or left behind, returns an error and changes no state.
func (c *Context) UnprotectRTCP(dst, pkt []byte) ([]byte, error) {
	if err := checkRTCP(pkt); err != nil {
		return dst, err
	}
	if len(pkt) < srtcpHeaderSize+4+c.rtcpTagLen {
		return dst, ErrShortPacket
	}
	ssrc := binary.BigEndian.Uint32(pkt[4:])
	var word uint32
	if c.rtcp.gcm != nil {
		word = binary.BigEndian.Uint32(pkt[len(pkt)-4:])
	} else {
		word = binary.BigEndian.Uint32(pkt[len(pkt)-c.rtcpTagLen-4:])
	}
	index := word &^ srtcpEBit
	encrypted := word&srtcpEBit != 0
	w := c.rtcpIn[ssrc]
	if w != nil && !w.check(uint64(index)) {
		return dst, ErrReplayed
	}

	var whole []byte
	if c.rtcp.gcm != nil {
		n := len(pkt) - 4 - 16
		iv := c.gcmIV(&c.rtcp.salt, ssrc, 0, 0)
		binary.BigEndian.PutUint32(iv[8:], binary.BigEndian.Uint32(iv[8:])^index)
		if encrypted {
			aad := c.trailer[:12]
			copy(aad, pkt[:srtcpHeaderSize])
			binary.BigEndian.PutUint32(aad[8:], word)
			var out []byte
			whole, out = sliceForAppend(dst, n+16)
			copy(out, pkt[:n+16])
			if _, err := c.rtcp.gcm.Open(out[srtcpHeaderSize:srtcpHeaderSize], iv, out[srtcpHeaderSize:], aad); err != nil {
				return dst, ErrAuthFailed
			}
			whole = whole[:len(dst)+n]
		} else {
			// Unencrypted, the whole packet and the trailer word are the
			// associated data, which must be contiguous.
			c.aad = append(append(c.aad[:0], pkt[:n]...), pkt[len(pkt)-4:]...)
			var tag [16]byte
			copy(tag[:], pkt[n:n+16])
			if _, err := c.rtcp.gcm.Open(tag[:0], iv, tag[:], c.aad); err != nil {
				return dst, ErrAuthFailed
			}
			var out []byte
			whole, out = sliceForAppend(dst, n)
			copy(out, pkt[:n])
		}
	} else {
		n := len(pkt) - c.rtcpTagLen - 4
		tag := c.hmacTag(&c.rtcp, pkt[:n+4], nil)
		if subtle.ConstantTimeCompare(tag[:c.rtcpTagLen], pkt[n+4:]) != 1 {
			return dst, ErrAuthFailed
		}
		var out []byte
		whole, out = sliceForAppend(dst, n)
		copy(out, pkt[:n])
		if encrypted {
			c.ctrIV(&c.rtcp.salt, ssrc, uint64(index))
			c.xorCTR(c.rtcp.block, out[srtcpHeaderSize:], out[srtcpHeaderSize:])
		}
	}

	if w == nil {
		w = &replayWindow{}
		w.init(c.rtcpReplayWindow)
		c.rtcpIn[ssrc] = w
	}
	w.accept(uint64(index))
	return whole, nil
}
