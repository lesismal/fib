package srt

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"time"
)

// cookieBucket is how long a SYN cookie stays valid: the bucket it was
// made in and the next.
const cookieBucket = time.Minute

// Request is a caller's conclusion handshake, for a listener to accept or
// reject.
type Request struct {
	Remote       netip.AddrPort
	PeerSocketID uint32
	// StreamID is the caller's stream ID, empty when it sent none.
	StreamID string
	// Encrypted says the caller sent keying material: it has a
	// passphrase.
	Encrypted bool
	// Latency is the larger of the latencies the caller asked for.
	Latency time.Duration
}

// ListenerConfig configures a Listener.
type ListenerConfig struct {
	// Accept decides on a caller's conclusion handshake. It returns the
	// Config of the new Conn (its callbacks included; StreamID is ignored)
	// or an error that rejects the caller: a RejectReason is sent as it
	// is, any other error as RejectPeer. The Config's Passphrase must
	// match the caller's: a caller without one, or with another, is
	// rejected (RejectUnsecure, RejectBadSecret), as is a caller with one
	// when the Config has none.
	Accept func(req *Request) (Config, error)
	// Rand is the source of cookies and socket IDs; crypto/rand when nil.
	Rand io.Reader
}

// Listener answers the handshakes of callers without keeping any state
// for them before their conclusion, which a SYN cookie proves comes from
// the address that asked for it, and makes a Conn of each caller it
// accepts. It is safe for concurrent use.
type Listener struct {
	cfg    ListenerConfig
	secret [32]byte
}

// NewListener returns a Listener.
func NewListener(cfg ListenerConfig) (*Listener, error) {
	if cfg.Accept == nil {
		return nil, errors.New("srt: ListenerConfig.Accept is nil")
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Reader
	}
	l := &Listener{cfg: cfg}
	if _, err := io.ReadFull(cfg.Rand, l.secret[:]); err != nil {
		return nil, err
	}
	return l, nil
}

// cookie returns the SYN cookie of addr in the bucket of t.
func (l *Listener) cookie(addr netip.AddrPort, t time.Time) uint32 {
	h := sha256.New()
	h.Write(l.secret[:])
	b, _ := addr.MarshalBinary()
	h.Write(b)
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(t.Unix()/int64(cookieBucket/time.Second))))
	sum := h.Sum(nil)
	if c := binary.BigEndian.Uint32(sum); c != 0 {
		return c
	}
	return 1
}

// IsHandshake reports whether b is a handshake packet, and of which
// caller socket; a server routing datagrams by address sends a caller that
// starts again with a new socket to the Listener rather than to the old
// Conn.
func IsHandshake(b []byte) (callerSocketID uint32, induction, ok bool) {
	var h header
	var hs handshake
	if h.unmarshal(b) != nil || !h.ctrl || h.typ != ctrlHandshake || hs.unmarshal(b[headerSize:]) != nil {
		return 0, false, false
	}
	return hs.socketID, hs.typ == hsInduction, true
}

// Handle reads a datagram from a caller at from, answering through output,
// which it calls with slices valid only for the call. It returns the Conn
// of a caller it accepts, which is connected and has sent its conclusion
// response through its own Config.Output; it returns nil for anything else.
func (l *Listener) Handle(b []byte, from netip.AddrPort, now time.Time, output func([]byte)) *Conn {
	var h header
	var hs handshake
	if h.unmarshal(b) != nil || !h.ctrl || h.typ != ctrlHandshake || hs.unmarshal(b[headerSize:]) != nil {
		return nil
	}
	switch hs.typ {
	case hsInduction:
		resp := handshake{
			version: 5, extField: srtMagic, isn: hs.isn, mtu: hs.mtu, flowWindow: hs.flowWindow,
			typ: hsInduction, socketID: 0, cookie: l.cookie(from, now), peerIP: peerIP(from),
		}
		out := header{ctrl: true, typ: ctrlHandshake, dst: hs.socketID}
		output(resp.append(out.append(nil)))
	case hsConclusion:
		if hs.cookie != l.cookie(from, now) && hs.cookie != l.cookie(from, now.Add(-cookieBucket)) {
			return nil // not from the address the cookie was for, or stale
		}
		return l.conclusion(&h, &hs, from, now, output)
	}
	return nil
}

func peerIP(a netip.AddrPort) [16]byte {
	var ip [16]byte
	addr := a.Addr().Unmap()
	if addr.Is4() {
		// libsrt keeps an IPv4 address in the first 32-bit word in network
		// order and then writes each word little-endian.
		b := addr.As4()
		ip[0], ip[1], ip[2], ip[3] = b[3], b[2], b[1], b[0]
		return ip
	}
	b := addr.As16()
	for i := 0; i < 16; i += 4 {
		ip[i], ip[i+1], ip[i+2], ip[i+3] = b[i+3], b[i+2], b[i+1], b[i]
	}
	return ip
}

// conclusion accepts or rejects a caller's conclusion.
func (l *Listener) conclusion(h *header, hs *handshake, from netip.AddrPort, now time.Time, output func([]byte)) *Conn {
	reject := func(r RejectReason) *Conn {
		resp := *hs
		resp.typ = hsRejectBase + uint32(r)
		resp.hsReq, resp.km, resp.hasSID = nil, nil, false
		out := header{ctrl: true, typ: ctrlHandshake, dst: hs.socketID}
		output(resp.append(out.append(nil)))
		return nil
	}
	if hs.version != 5 || hs.hsReq == nil || hs.hsRsp {
		return reject(RejectVersion)
	}
	req := &Request{
		Remote:       from,
		PeerSocketID: hs.socketID,
		StreamID:     hs.streamID,
		Encrypted:    hs.km != nil,
		Latency:      time.Duration(max(hs.hsReq.rcvDelay, hs.hsReq.sndDelay)) * time.Millisecond,
	}
	cfg, err := l.cfg.Accept(req)
	if err != nil {
		var r RejectReason
		if !errors.As(err, &r) {
			r = RejectPeer
		}
		return reject(r)
	}
	cfg.StreamID = ""
	if err := cfg.setDefaults(); err != nil {
		return reject(RejectSystem)
	}
	c := newConn(cfg, now)
	c.streamID = hs.streamID
	switch {
	case cfg.Passphrase == "" && hs.km != nil, cfg.Passphrase != "" && hs.km == nil:
		return reject(RejectUnsecure)
	case hs.km != nil:
		km, err := unmarshalKM(hs.km, cfg.Passphrase)
		if err != nil {
			return reject(RejectBadSecret)
		}
		if c.crypt, err = newCryptor(km); err != nil {
			return reject(RejectBadSecret)
		}
		c.km, c.kk = km, kkEven
		if km.even == nil {
			c.kk = kkOdd
		}
		c.cfg.PBKeyLen = km.keyLen
	}
	var r [4]byte
	if _, err := io.ReadFull(cfg.Rand, r[:]); err != nil {
		return reject(RejectSystem)
	}
	c.socketID = socketIDOf(binary.BigEndian.Uint32(r[:]))
	c.peerID = hs.socketID
	c.isn = hs.isn
	c.sndBase, c.sndNext = hs.isn, hs.isn
	c.rcvBase, c.rcvMax, c.ackAcked = hs.isn, hs.isn, hs.isn
	c.rcvLatency = max(cfg.Latency, time.Duration(hs.hsReq.sndDelay)*time.Millisecond)
	c.sndLatency = max(cfg.Latency, time.Duration(hs.hsReq.rcvDelay)*time.Millisecond)
	if hs.flowWindow > 0 {
		c.peerFlowWindow = int(min(hs.flowWindow, 1<<20))
		c.peerAvail = c.peerFlowWindow
	}
	resp := handshake{
		version: 5, extField: extFlagHSReq, isn: hs.isn, mtu: hs.mtu, flowWindow: uint32(cfg.FlowWindow),
		typ: hsConclusion, socketID: c.socketID, cookie: hs.cookie, peerIP: peerIP(from),
		hsReq: &hsExt{
			version: srtVersion, flags: c.srtFlags(),
			rcvDelay: uint16(c.rcvLatency / time.Millisecond), sndDelay: uint16(c.sndLatency / time.Millisecond),
		},
		hsRsp: true,
	}
	if hs.km != nil {
		resp.encryption = hs.encryption
		resp.extField |= extFlagKMReq
		resp.km, resp.kmRsp = hs.km, true
	}
	out := header{ctrl: true, typ: ctrlHandshake, ts: c.timestamp(now), dst: hs.socketID}
	c.hsResp = resp.append(out.append(nil))
	c.output(c.hsResp, now)
	c.connected(now, h.ts)
	return c
}
