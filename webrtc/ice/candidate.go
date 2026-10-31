package ice

import (
	"errors"
	"fmt"
	"hash/fnv"
	"net/netip"
	"strconv"
	"strings"
)

var (
	ErrCandidateSyntax      = errors.New("ice: malformed candidate")
	ErrUnsupportedTransport = errors.New("ice: candidate transport is not UDP")
	ErrUnresolved           = errors.New("ice: candidate address is a hostname")
	ErrComponent            = errors.New("ice: candidate is not for component 1")
)

// CandidateType is how a candidate's address was obtained, which ranks it:
// a host address is preferred over one a NAT or a relay stands for.
type CandidateType uint8

const (
	Host CandidateType = iota
	ServerReflexive
	PeerReflexive
	Relay
)

func (t CandidateType) String() string {
	switch t {
	case Host:
		return "host"
	case ServerReflexive:
		return "srflx"
	case PeerReflexive:
		return "prflx"
	case Relay:
		return "relay"
	}
	return "CandidateType(" + strconv.Itoa(int(t)) + ")"
}

// typePreference is RFC 8445 section 5.1.2.2's recommendation.
func (t CandidateType) typePreference() uint32 {
	switch t {
	case Host:
		return 126
	case PeerReflexive:
		return 110
	case ServerReflexive:
		return 100
	}
	return 0
}

// Priority is a candidate's priority as RFC 8445 section 5.1.2.1 computes it
// from its type, a local preference that ranks the candidates of one type
// among themselves, and its component.
func Priority(t CandidateType, localPref uint16, component uint16) uint32 {
	return t.typePreference()<<24 | uint32(localPref)<<8 | uint32(256-int(component))&0xFF
}

// Extension is a name and value of a candidate attribute beyond those RFC
// 8839 defines, such as generation, ufrag or network-id, which browsers add.
type Extension struct {
	Name, Value string
}

// LocalID names one of the agent's transports, chosen by its caller: a UDP
// socket, or a TURN allocation for a relay candidate. The agent sends
// through it by naming it and knows a packet's transport by the LocalID its
// caller passes along.
type LocalID uint32

// Candidate is a transport address an agent may be reached at, as an
// a=candidate line carries it (RFC 8839 section 5.1).
type Candidate struct {
	Foundation string
	Component  uint16
	// Transport is "udp" for every candidate the agent uses; others, such
	// as the TCP candidates of RFC 6544, parse and print but are not used.
	Transport string
	Priority  uint32
	// Address is the candidate's IP address, unless Hostname is set: a
	// browser hides its host addresses behind mDNS names ending in ".local",
	// which its caller resolves into Address before adding the candidate.
	Address  netip.Addr
	Hostname string
	Port     uint16
	Type     CandidateType
	// RelatedAddress is the address a reflexive or relayed candidate was
	// obtained from, raddr and rport, which browsers zero for privacy.
	RelatedAddress  netip.AddrPort
	RelatedHostname string
	Extensions      []Extension

	// LocalID is the transport of a local candidate: its base, whose
	// socket a host or server-reflexive candidate's packets go out of, or
	// the allocation of a relay candidate. It is not part of the line.
	LocalID LocalID
}

// AddrPort is the candidate's transport address.
func (c *Candidate) AddrPort() netip.AddrPort { return netip.AddrPortFrom(c.Address, c.Port) }

// Extension returns the value of the extension name.
func (c *Candidate) Extension(name string) (string, bool) {
	for _, e := range c.Extensions {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// String is the candidate as the value of an a=candidate attribute,
// starting "candidate:", as RTCIceCandidate.candidate carries it too.
func (c *Candidate) String() string { return string(c.Append(nil)) }

// Append appends String to dst.
func (c *Candidate) Append(dst []byte) []byte {
	dst = append(dst, "candidate:"...)
	dst = append(dst, c.Foundation...)
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, uint64(c.Component), 10)
	dst = append(dst, ' ')
	if c.Transport == "" {
		dst = append(dst, "udp"...)
	} else {
		dst = append(dst, c.Transport...)
	}
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, uint64(c.Priority), 10)
	dst = append(dst, ' ')
	if c.Hostname != "" {
		dst = append(dst, c.Hostname...)
	} else {
		dst = c.Address.WithZone("").AppendTo(dst)
	}
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, uint64(c.Port), 10)
	dst = append(dst, " typ "...)
	dst = append(dst, c.Type.String()...)
	if c.RelatedHostname != "" || c.RelatedAddress.IsValid() {
		dst = append(dst, " raddr "...)
		if c.RelatedHostname != "" {
			dst = append(dst, c.RelatedHostname...)
		} else {
			dst = c.RelatedAddress.Addr().WithZone("").AppendTo(dst)
		}
		dst = append(dst, " rport "...)
		dst = strconv.AppendUint(dst, uint64(c.RelatedAddress.Port()), 10)
	}
	for _, e := range c.Extensions {
		dst = append(dst, ' ')
		dst = append(dst, e.Name...)
		dst = append(dst, ' ')
		dst = append(dst, e.Value...)
	}
	return dst
}

// ParseCandidate parses a candidate attribute: "candidate:" and what
// follows, with or without a leading "a=", as SDP and trickle signaling
// carry it.
func ParseCandidate(s string) (Candidate, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "a=")
	rest, ok := strings.CutPrefix(s, "candidate:")
	if !ok {
		return Candidate{}, fmt.Errorf("%w: no candidate: prefix", ErrCandidateSyntax)
	}
	f := strings.Fields(rest)
	if len(f) < 8 {
		return Candidate{}, fmt.Errorf("%w: %d fields", ErrCandidateSyntax, len(f))
	}
	var c Candidate
	c.Foundation = f[0]
	if len(c.Foundation) > 32 || !isICEChars(c.Foundation) {
		return Candidate{}, fmt.Errorf("%w: foundation %q", ErrCandidateSyntax, c.Foundation)
	}
	comp, err := strconv.ParseUint(f[1], 10, 16)
	if err != nil || comp == 0 || comp > 256 {
		return Candidate{}, fmt.Errorf("%w: component %q", ErrCandidateSyntax, f[1])
	}
	c.Component = uint16(comp)
	c.Transport = strings.ToLower(f[2])
	prio, err := strconv.ParseUint(f[3], 10, 32)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: priority %q", ErrCandidateSyntax, f[3])
	}
	c.Priority = uint32(prio)
	if ip, err := netip.ParseAddr(f[4]); err == nil {
		c.Address = ip.Unmap()
	} else if f[4] != "" && !strings.ContainsAny(f[4], ":%") {
		c.Hostname = f[4]
	} else {
		return Candidate{}, fmt.Errorf("%w: address %q", ErrCandidateSyntax, f[4])
	}
	port, err := strconv.ParseUint(f[5], 10, 16)
	if err != nil {
		return Candidate{}, fmt.Errorf("%w: port %q", ErrCandidateSyntax, f[5])
	}
	c.Port = uint16(port)
	if f[6] != "typ" {
		return Candidate{}, fmt.Errorf("%w: %q where typ belongs", ErrCandidateSyntax, f[6])
	}
	switch f[7] {
	case "host":
		c.Type = Host
	case "srflx":
		c.Type = ServerReflexive
	case "prflx":
		c.Type = PeerReflexive
	case "relay":
		c.Type = Relay
	default:
		return Candidate{}, fmt.Errorf("%w: type %q", ErrCandidateSyntax, f[7])
	}
	f = f[8:]
	var raddr netip.Addr
	var rport uint16
	related := false
	for len(f) > 0 {
		if len(f) < 2 {
			return Candidate{}, fmt.Errorf("%w: %q has no value", ErrCandidateSyntax, f[0])
		}
		name, value := f[0], f[1]
		f = f[2:]
		switch name {
		case "raddr":
			related = true
			if ip, err := netip.ParseAddr(value); err == nil {
				raddr = ip.Unmap()
			} else {
				c.RelatedHostname = value
			}
		case "rport":
			related = true
			p, err := strconv.ParseUint(value, 10, 16)
			if err != nil {
				return Candidate{}, fmt.Errorf("%w: rport %q", ErrCandidateSyntax, value)
			}
			rport = uint16(p)
		default:
			c.Extensions = append(c.Extensions, Extension{name, value})
		}
	}
	if related {
		if !raddr.IsValid() && c.RelatedHostname == "" {
			raddr = netip.IPv4Unspecified()
		}
		c.RelatedAddress = netip.AddrPortFrom(raddr, rport)
	}
	return c, nil
}

// isICEChars reports whether s is made of ice-chars, ALPHA, DIGIT, "+" and
// "/", as foundations, ufrags and passwords are (RFC 8839 section 5.1).
func isICEChars(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if !('a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || '0' <= b && b <= '9' || b == '+' || b == '/') {
			return false
		}
	}
	return true
}

// foundation derives a foundation from what RFC 8445 section 5.1.1.3 says
// two candidates must share to share one: their type, the IP address of
// their base, and the server they were obtained from.
func foundation(t CandidateType, base netip.Addr, server netip.AddrPort) string {
	h := fnv.New32a()
	h.Write([]byte{byte(t)})
	b, _ := base.MarshalBinary()
	h.Write(b)
	b, _ = server.MarshalBinary()
	h.Write(b)
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}
