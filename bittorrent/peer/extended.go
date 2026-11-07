package peer

import (
	"net/netip"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// Names of the extensions of BEP 10 this package implements.
const (
	// ExtMetadata is ut_metadata, BEP 9: fetching the info dictionary.
	ExtMetadata = "ut_metadata"
	// ExtPex is ut_pex, BEP 11: peer exchange.
	ExtPex = "ut_pex"
)

// ExtendedHandshake is the dictionary of the extended handshake (BEP 10),
// the extended message of ID 0.
type ExtendedHandshake struct {
	// M maps each extension the sender supports to the ID it is to be sent
	// with; 0 says the sender has turned it off.
	M map[string]int `bencode:"m"`
	// Port is the sender's listening port.
	Port int `bencode:"p,omitempty"`
	// Version is the sender's client name and version.
	Version string `bencode:"v,omitempty"`
	// YourIP is the receiver's address as the sender sees it, 4 or 16
	// bytes.
	YourIP []byte `bencode:"yourip,omitempty"`
	// IPv4 and IPv6 are the sender's own addresses, if it knows them.
	IPv4 []byte `bencode:"ipv4,omitempty"`
	IPv6 []byte `bencode:"ipv6,omitempty"`
	// Reqq is how many requests the sender keeps queued without dropping.
	Reqq int `bencode:"reqq,omitempty"`
	// MetadataSize is the size of the info dictionary (BEP 9).
	MetadataSize int `bencode:"metadata_size,omitempty"`
	// UploadOnly is non-zero for a peer that will not download (BEP 21).
	UploadOnly int `bencode:"upload_only,omitempty"`
	// Encryption is non-zero for a peer that prefers MSE.
	Encryption int `bencode:"e,omitempty"`
}

// ParseExtendedHandshake parses an extended handshake's payload.
func ParseExtendedHandshake(b []byte) (*ExtendedHandshake, error) {
	h := new(ExtendedHandshake)
	if err := bencode.Unmarshal(b, h); err != nil {
		return nil, err
	}
	return h, nil
}

// Marshal encodes h, an empty m dictionary when M is nil, as BEP 10
// requires one.
func (h *ExtendedHandshake) Marshal() ([]byte, error) {
	if h.M == nil {
		c := *h
		c.M = map[string]int{}
		h = &c
	}
	return bencode.Marshal(h)
}

// ID is the ID the sender wants extension name sent with: 0 when it does
// not support it.
func (h *ExtendedHandshake) ID(name string) uint8 {
	id := h.M[name]
	if id <= 0 || id > 255 {
		return 0
	}
	return uint8(id)
}

// YourAddr is YourIP as an address; invalid when absent or malformed.
func (h *ExtendedHandshake) YourAddr() netip.Addr {
	a, _ := netip.AddrFromSlice(h.YourIP)
	return a
}

// CompactAddr is a's 4 or 16 bytes, for YourIP, IPv4 and IPv6.
func CompactAddr(a netip.Addr) []byte {
	a = a.Unmap()
	if !a.IsValid() {
		return nil
	}
	return a.AsSlice()
}
