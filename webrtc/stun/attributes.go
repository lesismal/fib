package stun

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"time"
)

// AttrType is the type of an attribute. Types below 0x8000 are
// comprehension-required: a receiver that does not know one must reject the
// message with a 420 error listing it in UNKNOWN-ATTRIBUTES.
type AttrType uint16

// The attributes of STUN (RFC 8489), ICE (RFC 8445), TURN (RFC 8656, RFC
// 6062) and the NAT behavior discovery of RFC 5780, from the IANA registry.
const (
	AttrMappedAddress          AttrType = 0x0001
	AttrUsername               AttrType = 0x0006
	AttrMessageIntegrity       AttrType = 0x0008
	AttrErrorCode              AttrType = 0x0009
	AttrUnknownAttributes      AttrType = 0x000A
	AttrChannelNumber          AttrType = 0x000C
	AttrLifetime               AttrType = 0x000D
	AttrXORPeerAddress         AttrType = 0x0012
	AttrData                   AttrType = 0x0013
	AttrRealm                  AttrType = 0x0014
	AttrNonce                  AttrType = 0x0015
	AttrXORRelayedAddress      AttrType = 0x0016
	AttrRequestedAddressFamily AttrType = 0x0017
	AttrEvenPort               AttrType = 0x0018
	AttrRequestedTransport     AttrType = 0x0019
	AttrDontFragment           AttrType = 0x001A
	AttrMessageIntegritySHA256 AttrType = 0x001C
	AttrPasswordAlgorithm      AttrType = 0x001D
	AttrUserhash               AttrType = 0x001E
	AttrXORMappedAddress       AttrType = 0x0020
	AttrReservationToken       AttrType = 0x0022
	AttrPriority               AttrType = 0x0024
	AttrUseCandidate           AttrType = 0x0025
	AttrPadding                AttrType = 0x0026
	AttrResponsePort           AttrType = 0x0027
	AttrConnectionID           AttrType = 0x002A

	AttrAdditionalAddressFamily    AttrType = 0x8000
	AttrAddressErrorCode           AttrType = 0x8001
	AttrPasswordAlgorithms         AttrType = 0x8002
	AttrAlternateDomain            AttrType = 0x8003
	AttrICMP                       AttrType = 0x8004
	AttrSoftware                   AttrType = 0x8022
	AttrAlternateServer            AttrType = 0x8023
	AttrTransactionTransmitCounter AttrType = 0x8025
	AttrCacheTimeout               AttrType = 0x8027
	AttrFingerprint                AttrType = 0x8028
	AttrICEControlled              AttrType = 0x8029
	AttrICEControlling             AttrType = 0x802A
	AttrResponseOrigin             AttrType = 0x802B
	AttrOtherAddress               AttrType = 0x802C
)

// Required reports whether t is comprehension-required.
func (t AttrType) Required() bool { return t < 0x8000 }

var attrNames = map[AttrType]string{
	AttrMappedAddress:              "MAPPED-ADDRESS",
	AttrUsername:                   "USERNAME",
	AttrMessageIntegrity:           "MESSAGE-INTEGRITY",
	AttrErrorCode:                  "ERROR-CODE",
	AttrUnknownAttributes:          "UNKNOWN-ATTRIBUTES",
	AttrChannelNumber:              "CHANNEL-NUMBER",
	AttrLifetime:                   "LIFETIME",
	AttrXORPeerAddress:             "XOR-PEER-ADDRESS",
	AttrData:                       "DATA",
	AttrRealm:                      "REALM",
	AttrNonce:                      "NONCE",
	AttrXORRelayedAddress:          "XOR-RELAYED-ADDRESS",
	AttrRequestedAddressFamily:     "REQUESTED-ADDRESS-FAMILY",
	AttrEvenPort:                   "EVEN-PORT",
	AttrRequestedTransport:         "REQUESTED-TRANSPORT",
	AttrDontFragment:               "DONT-FRAGMENT",
	AttrMessageIntegritySHA256:     "MESSAGE-INTEGRITY-SHA256",
	AttrPasswordAlgorithm:          "PASSWORD-ALGORITHM",
	AttrUserhash:                   "USERHASH",
	AttrXORMappedAddress:           "XOR-MAPPED-ADDRESS",
	AttrReservationToken:           "RESERVATION-TOKEN",
	AttrPriority:                   "PRIORITY",
	AttrUseCandidate:               "USE-CANDIDATE",
	AttrPadding:                    "PADDING",
	AttrResponsePort:               "RESPONSE-PORT",
	AttrConnectionID:               "CONNECTION-ID",
	AttrAdditionalAddressFamily:    "ADDITIONAL-ADDRESS-FAMILY",
	AttrAddressErrorCode:           "ADDRESS-ERROR-CODE",
	AttrPasswordAlgorithms:         "PASSWORD-ALGORITHMS",
	AttrAlternateDomain:            "ALTERNATE-DOMAIN",
	AttrICMP:                       "ICMP",
	AttrSoftware:                   "SOFTWARE",
	AttrAlternateServer:            "ALTERNATE-SERVER",
	AttrTransactionTransmitCounter: "TRANSACTION-TRANSMIT-COUNTER",
	AttrCacheTimeout:               "CACHE-TIMEOUT",
	AttrFingerprint:                "FINGERPRINT",
	AttrICEControlled:              "ICE-CONTROLLED",
	AttrICEControlling:             "ICE-CONTROLLING",
	AttrResponseOrigin:             "RESPONSE-ORIGIN",
	AttrOtherAddress:               "OTHER-ADDRESS",
}

func (t AttrType) String() string {
	if s, ok := attrNames[t]; ok {
		return s
	}
	return fmt.Sprintf("AttrType(0x%04x)", uint16(t))
}

// The error codes of RFC 8489 section 14.8, RFC 8445 and RFC 8656.
const (
	CodeTryAlternate                 = 300
	CodeBadRequest                   = 400
	CodeUnauthorized                 = 401
	CodeForbidden                    = 403
	CodeMobilityForbidden            = 405
	CodeUnknownAttribute             = 420
	CodeAllocationMismatch           = 437
	CodeStaleNonce                   = 438
	CodeAddressFamilyNotSupported    = 440
	CodeWrongCredentials             = 441
	CodeUnsupportedTransportProtocol = 442
	CodePeerAddressFamilyMismatch    = 443
	CodeAllocationQuotaReached       = 486
	CodeRoleConflict                 = 487
	CodeServerError                  = 500
	CodeInsufficientCapacity         = 508
)

// Address families as the address attributes encode them.
const (
	familyIPv4 = 0x01
	familyIPv6 = 0x02
)

// Protocol numbers REQUESTED-TRANSPORT carries.
const (
	ProtoUDP = 17
	ProtoTCP = 6
)

// AddString adds an attribute whose value is s: USERNAME, REALM, NONCE,
// SOFTWARE.
func (m *Message) AddString(t AttrType, s string) {
	m.Raw = m.grow(t, len(s))
	copy(m.Raw[len(m.Raw)-pad4(len(s)):], s)
}

// AddFlag adds an attribute with no value: USE-CANDIDATE, DONT-FRAGMENT.
func (m *Message) AddFlag(t AttrType) { m.Raw = m.grow(t, 0) }

// AddUint32 adds a 4-byte attribute: PRIORITY, LIFETIME (in seconds).
func (m *Message) AddUint32(t AttrType, v uint32) {
	m.Raw = m.grow(t, 4)
	binary.BigEndian.PutUint32(m.Raw[len(m.Raw)-4:], v)
}

// AddUint64 adds an 8-byte attribute: ICE-CONTROLLING, ICE-CONTROLLED,
// RESERVATION-TOKEN.
func (m *Message) AddUint64(t AttrType, v uint64) {
	m.Raw = m.grow(t, 8)
	binary.BigEndian.PutUint64(m.Raw[len(m.Raw)-8:], v)
}

// AddLifetime adds LIFETIME, in whole seconds.
func (m *Message) AddLifetime(d time.Duration) { m.AddUint32(AttrLifetime, uint32(d/time.Second)) }

// AddChannelNumber adds CHANNEL-NUMBER, whose last two bytes are RFFU zeros.
func (m *Message) AddChannelNumber(n uint16) { m.AddUint32(AttrChannelNumber, uint32(n)<<16) }

// AddRequestedTransport adds REQUESTED-TRANSPORT for the IP protocol proto,
// ProtoUDP for a TURN allocation that relays UDP.
func (m *Message) AddRequestedTransport(proto uint8) {
	m.AddUint32(AttrRequestedTransport, uint32(proto)<<24)
}

// AddAddress adds an address attribute in the plain form of MAPPED-ADDRESS,
// ALTERNATE-SERVER, RESPONSE-ORIGIN and OTHER-ADDRESS.
func (m *Message) AddAddress(t AttrType, a netip.AddrPort) { m.addAddress(t, a, false) }

// AddXORAddress adds an address attribute XORed with the magic cookie and
// the transaction ID, as XOR-MAPPED-ADDRESS, XOR-PEER-ADDRESS and
// XOR-RELAYED-ADDRESS are, so that NATs that rewrite addresses they find in
// packets leave it alone. Build must have set the transaction ID first.
func (m *Message) AddXORAddress(t AttrType, a netip.AddrPort) { m.addAddress(t, a, true) }

func (m *Message) addAddress(t AttrType, a netip.AddrPort, xor bool) {
	ip := a.Addr().Unmap()
	n := 8
	family := byte(familyIPv4)
	if ip.Is6() {
		n = 20
		family = familyIPv6
	}
	m.Raw = m.grow(t, n)
	v := m.Raw[len(m.Raw)-n:]
	v[0] = 0
	v[1] = family
	port := a.Port()
	if xor {
		port ^= MagicCookie >> 16
	}
	binary.BigEndian.PutUint16(v[2:4], port)
	if family == familyIPv4 {
		b := ip.As4()
		copy(v[4:], b[:])
	} else {
		b := ip.As16()
		copy(v[4:], b[:])
	}
	if xor {
		m.xorAddr(v[4:])
	}
}

// xorAddr XORs an address with the magic cookie followed by the transaction
// ID, both ways.
func (m *Message) xorAddr(b []byte) {
	var key [16]byte
	binary.BigEndian.PutUint32(key[:4], MagicCookie)
	copy(key[4:], m.TransactionID[:])
	for i := range b {
		b[i] ^= key[i]
	}
}

// AddErrorCode adds ERROR-CODE with code, 300 to 699, and a reason phrase.
func (m *Message) AddErrorCode(code int, reason string) {
	n := 4 + len(reason)
	m.Raw = m.grow(AttrErrorCode, n)
	v := m.Raw[len(m.Raw)-pad4(n):]
	v[0], v[1] = 0, 0
	v[2] = byte(code / 100 & 0x07)
	v[3] = byte(code % 100)
	copy(v[4:], reason)
}

// AddUnknownAttributes adds UNKNOWN-ATTRIBUTES listing types.
func (m *Message) AddUnknownAttributes(types []AttrType) {
	m.Raw = m.grow(AttrUnknownAttributes, 2*len(types))
	v := m.Raw[len(m.Raw)-pad4(2*len(types)):]
	for i, t := range types {
		binary.BigEndian.PutUint16(v[2*i:], uint16(t))
	}
}

// Uint32 returns the value of a 4-byte attribute.
func (m *Message) Uint32(t AttrType) (uint32, error) {
	v, ok := m.Get(t)
	if !ok {
		return 0, fmt.Errorf("%w: %v", ErrAttrNotFound, t)
	}
	if len(v) != 4 {
		return 0, fmt.Errorf("%w: %v", ErrAttrSize, t)
	}
	return binary.BigEndian.Uint32(v), nil
}

// Uint64 returns the value of an 8-byte attribute.
func (m *Message) Uint64(t AttrType) (uint64, error) {
	v, ok := m.Get(t)
	if !ok {
		return 0, fmt.Errorf("%w: %v", ErrAttrNotFound, t)
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("%w: %v", ErrAttrSize, t)
	}
	return binary.BigEndian.Uint64(v), nil
}

// Lifetime returns LIFETIME.
func (m *Message) Lifetime() (time.Duration, error) {
	v, err := m.Uint32(AttrLifetime)
	return time.Duration(v) * time.Second, err
}

// ChannelNumber returns CHANNEL-NUMBER.
func (m *Message) ChannelNumber() (uint16, error) {
	v, err := m.Uint32(AttrChannelNumber)
	return uint16(v >> 16), err
}

// RequestedTransport returns the protocol number of REQUESTED-TRANSPORT.
func (m *Message) RequestedTransport() (uint8, error) {
	v, err := m.Uint32(AttrRequestedTransport)
	return uint8(v >> 24), err
}

// Address returns the value of a plain address attribute.
func (m *Message) Address(t AttrType) (netip.AddrPort, error) { return m.address(t, false) }

// XORAddress returns the value of an XORed address attribute.
func (m *Message) XORAddress(t AttrType) (netip.AddrPort, error) { return m.address(t, true) }

func (m *Message) address(t AttrType, xor bool) (netip.AddrPort, error) {
	v, ok := m.Get(t)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("%w: %v", ErrAttrNotFound, t)
	}
	if len(v) < 4 {
		return netip.AddrPort{}, fmt.Errorf("%w: %v", ErrAttrSize, t)
	}
	port := binary.BigEndian.Uint16(v[2:4])
	if xor {
		port ^= MagicCookie >> 16
	}
	var b [16]byte
	switch v[1] {
	case familyIPv4:
		if len(v) != 8 {
			return netip.AddrPort{}, fmt.Errorf("%w: %v", ErrAttrSize, t)
		}
		copy(b[:4], v[4:8])
		if xor {
			m.xorAddr(b[:4])
		}
		return netip.AddrPortFrom(netip.AddrFrom4([4]byte(b[:4])), port), nil
	case familyIPv6:
		if len(v) != 20 {
			return netip.AddrPort{}, fmt.Errorf("%w: %v", ErrAttrSize, t)
		}
		copy(b[:], v[4:20])
		if xor {
			m.xorAddr(b[:])
		}
		return netip.AddrPortFrom(netip.AddrFrom16(b), port), nil
	}
	return netip.AddrPort{}, fmt.Errorf("%w: %v family %d", ErrBadFamily, t, v[1])
}

// ErrorCode returns the code and reason phrase of ERROR-CODE. The reason
// aliases Raw.
func (m *Message) ErrorCode() (code int, reason []byte, err error) {
	v, ok := m.Get(AttrErrorCode)
	if !ok {
		return 0, nil, fmt.Errorf("%w: %v", ErrAttrNotFound, AttrErrorCode)
	}
	if len(v) < 4 {
		return 0, nil, fmt.Errorf("%w: %v", ErrAttrSize, AttrErrorCode)
	}
	return int(v[2]&0x07)*100 + int(v[3]), v[4:], nil
}

// UnknownAttributes appends the types UNKNOWN-ATTRIBUTES lists to dst.
func (m *Message) UnknownAttributes(dst []AttrType) ([]AttrType, error) {
	v, ok := m.Get(AttrUnknownAttributes)
	if !ok {
		return dst, fmt.Errorf("%w: %v", ErrAttrNotFound, AttrUnknownAttributes)
	}
	if len(v)%2 != 0 {
		return dst, fmt.Errorf("%w: %v", ErrAttrSize, AttrUnknownAttributes)
	}
	for i := 0; i < len(v); i += 2 {
		dst = append(dst, AttrType(binary.BigEndian.Uint16(v[i:])))
	}
	return dst, nil
}
