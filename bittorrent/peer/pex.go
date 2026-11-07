package peer

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// PexFlags are what added.f says of each peer added (BEP 11).
type PexFlags uint8

// The flags of a peer exchange.
const (
	PexEncryption PexFlags = 0x01 // prefers MSE
	PexSeed       PexFlags = 0x02 // a seed, or upload only
	PexUTP        PexFlags = 0x04 // supports uTP
	PexHolepunch  PexFlags = 0x08 // supports ut_holepunch
	PexReachable  PexFlags = 0x10 // reached by an outgoing connection
)

// Limits of BEP 11.
const (
	// PexInterval is the least time between two PEX messages to a peer.
	PexInterval = time.Minute
	// PexMaxPeers is the most peers added, and the most dropped, in one
	// message, each family counting on its own.
	PexMaxPeers = 50
)

// PexPeer is a peer a PEX message adds.
type PexPeer struct {
	Addr  netip.AddrPort
	Flags PexFlags
}

// Pex is a ut_pex message: the peers connected since the last one, and
// those gone.
type Pex struct {
	Added   []PexPeer
	Dropped []netip.AddrPort
}

// pexWire is a ut_pex message as it goes: compact peers, IPv4 in 6 bytes
// and IPv6 in 18, with a byte of flags for each added.
type pexWire struct {
	Added    []byte `bencode:"added"`
	AddedF   []byte `bencode:"added.f"`
	Added6   []byte `bencode:"added6"`
	Added6F  []byte `bencode:"added6.f"`
	Dropped  []byte `bencode:"dropped"`
	Dropped6 []byte `bencode:"dropped6"`
}

// ErrPex is a malformed ut_pex message.
var ErrPex = errors.New("peer: malformed ut_pex message")

// ParsePex parses a ut_pex message's payload. Missing flags read as zero.
func ParsePex(b []byte) (*Pex, error) {
	var w pexWire
	if err := bencode.Unmarshal(b, &w); err != nil {
		return nil, err
	}
	p := new(Pex)
	var err error
	if p.Added, err = parseAdded(p.Added, w.Added, w.AddedF, 4); err != nil {
		return nil, err
	}
	if p.Added, err = parseAdded(p.Added, w.Added6, w.Added6F, 16); err != nil {
		return nil, err
	}
	if p.Dropped, err = parseCompact(p.Dropped, w.Dropped, 4); err != nil {
		return nil, err
	}
	if p.Dropped, err = parseCompact(p.Dropped, w.Dropped6, 16); err != nil {
		return nil, err
	}
	return p, nil
}

func parseCompact(dst []netip.AddrPort, b []byte, ipLen int) ([]netip.AddrPort, error) {
	n := ipLen + 2
	if len(b)%n != 0 {
		return nil, ErrPex
	}
	for ; len(b) > 0; b = b[n:] {
		a, _ := netip.AddrFromSlice(b[:ipLen])
		dst = append(dst, netip.AddrPortFrom(a, binary.BigEndian.Uint16(b[ipLen:])))
	}
	return dst, nil
}

func parseAdded(dst []PexPeer, b, flags []byte, ipLen int) ([]PexPeer, error) {
	addrs, err := parseCompact(nil, b, ipLen)
	if err != nil {
		return nil, err
	}
	for i, a := range addrs {
		var f PexFlags
		if i < len(flags) {
			f = PexFlags(flags[i])
		}
		dst = append(dst, PexPeer{Addr: a, Flags: f})
	}
	return dst, nil
}

// AppendCompact appends a's compact form: 4 or 16 bytes of address and 2
// of port.
func AppendCompact(dst []byte, a netip.AddrPort) []byte {
	ip := a.Addr().Unmap()
	dst = append(dst, ip.AsSlice()...)
	return binary.BigEndian.AppendUint16(dst, a.Port())
}

// Marshal encodes p, every key present as clients send them.
func (p *Pex) Marshal() ([]byte, error) {
	w := pexWire{Added: []byte{}, AddedF: []byte{}, Added6: []byte{}, Added6F: []byte{}, Dropped: []byte{}, Dropped6: []byte{}}
	for _, a := range p.Added {
		if a.Addr.Addr().Unmap().Is4() {
			w.Added = AppendCompact(w.Added, a.Addr)
			w.AddedF = append(w.AddedF, byte(a.Flags))
		} else {
			w.Added6 = AppendCompact(w.Added6, a.Addr)
			w.Added6F = append(w.Added6F, byte(a.Flags))
		}
	}
	for _, a := range p.Dropped {
		if a.Addr().Unmap().Is4() {
			w.Dropped = AppendCompact(w.Dropped, a)
		} else {
			w.Dropped6 = AppendCompact(w.Dropped6, a)
		}
	}
	return bencode.Marshal(&w)
}

// PexState keeps one connection's peer exchange within BEP 11's limits:
// what was last sent to the peer, so that each message carries only the
// change, at most PexMaxPeers added and dropped per family and at most one
// message per PexInterval; and when the peer last sent one, so that a
// peer sending more often is not heeded.
type PexState struct {
	sent     map[netip.AddrPort]PexFlags
	lastSent time.Time
	lastRecv time.Time
}

// Next returns the message to send at now, given the peers connected now
// (other than the one this state is for), and false when it is too soon
// since the last or nothing changed. Changes past the limits wait for the
// next message.
func (s *PexState) Next(now time.Time, current []PexPeer) (*Pex, bool) {
	if !s.lastSent.IsZero() && now.Sub(s.lastSent) < PexInterval {
		return nil, false
	}
	if s.sent == nil {
		s.sent = make(map[netip.AddrPort]PexFlags)
	}
	cur := make(map[netip.AddrPort]bool, len(current))
	p := new(Pex)
	var added4, added6, dropped4, dropped6 int
	for _, c := range current {
		cur[c.Addr] = true
		if f, ok := s.sent[c.Addr]; ok && f == c.Flags {
			continue
		}
		n := &added4
		if !c.Addr.Addr().Unmap().Is4() {
			n = &added6
		}
		if *n >= PexMaxPeers {
			continue
		}
		*n++
		p.Added = append(p.Added, c)
		s.sent[c.Addr] = c.Flags
	}
	for a := range s.sent {
		if cur[a] {
			continue
		}
		n := &dropped4
		if !a.Addr().Unmap().Is4() {
			n = &dropped6
		}
		if *n >= PexMaxPeers {
			continue
		}
		*n++
		p.Dropped = append(p.Dropped, a)
		delete(s.sent, a)
	}
	if len(p.Added) == 0 && len(p.Dropped) == 0 {
		return nil, false
	}
	s.lastSent = now
	return p, true
}

// Received reports whether a PEX message the peer sent at now is to be
// heeded: not when it came sooner than PexInterval after the last one
// heeded, less a quarter for timers' slack. It also cuts p down to the
// limits of a message.
func (s *PexState) Received(now time.Time, p *Pex) bool {
	if !s.lastRecv.IsZero() && now.Sub(s.lastRecv) < PexInterval*3/4 {
		return false
	}
	s.lastRecv = now
	p.Added = limitFamily(p.Added, func(x PexPeer) netip.Addr { return x.Addr.Addr() })
	p.Dropped = limitFamily(p.Dropped, func(x netip.AddrPort) netip.Addr { return x.Addr() })
	return true
}

func limitFamily[T any](s []T, addr func(T) netip.Addr) []T {
	var n4, n6 int
	out := s[:0]
	for _, x := range s {
		n := &n4
		if !addr(x).Unmap().Is4() {
			n = &n6
		}
		if *n < PexMaxPeers {
			*n++
			out = append(out, x)
		}
	}
	return out
}
