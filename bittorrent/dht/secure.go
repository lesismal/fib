package dht

import (
	"crypto/rand"
	"hash/crc32"
	"net/netip"
	"time"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// secureCRC is the CRC32-C BEP 42 derives the top 21 bits of a node ID
// from: of the IP's first 4 (IPv4) or 8 (IPv6) bytes, masked, with r in
// the top three bits.
func secureCRC(ip netip.Addr, r byte) uint32 {
	ip = ip.Unmap()
	var b []byte
	var mask []byte
	if ip.Is4() {
		a := ip.As4()
		b, mask = a[:], []byte{0x03, 0x0f, 0x3f, 0xff}
	} else {
		a := ip.As16()
		b, mask = a[:8], []byte{0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff}
	}
	for i := range b {
		b[i] &= mask[i]
	}
	b[0] |= (r & 7) << 5
	return crc32.Checksum(b, castagnoli)
}

// SecureID returns a random node ID that BEP 42 ties to the external
// address ip, the one other nodes see us at.
func SecureID(ip netip.Addr) ID {
	id := RandomID()
	crc := secureCRC(ip, id[19]&7)
	id[0] = byte(crc >> 24)
	id[1] = byte(crc >> 16)
	id[2] = byte(crc>>8)&0xf8 | id[2]&7
	return id
}

// IDSecure reports whether id is one BEP 42 allows a node at ip to have.
// Nodes at local addresses (loopback, private, link-local) may have any
// ID, as the BEP exempts them.
func IDSecure(id ID, ip netip.Addr) bool {
	if isLocal(ip) {
		return true
	}
	crc := secureCRC(ip, id[19]&7)
	return id[0] == byte(crc>>24) && id[1] == byte(crc>>16) && id[2]&0xf8 == byte(crc>>8)&0xf8
}

// ipVoter learns our external address from the "ip" key of responses
// (BEP 42): each node that answers votes for the address it saw us at, one
// vote a voter IP, the latest standing. The address with the most votes,
// and at least minVotes, wins.
type ipVoter struct {
	votes map[netip.Addr]vote // by voter IP
}

type vote struct {
	addr netip.Addr
	at   time.Time
}

const (
	maxVoters = 64
	minVotes  = 3
)

func (v *ipVoter) add(voter, addr netip.Addr, now time.Time) {
	if v.votes == nil {
		v.votes = map[netip.Addr]vote{}
	}
	if _, ok := v.votes[voter]; !ok && len(v.votes) >= maxVoters {
		var oldest netip.Addr
		var at time.Time
		for k, x := range v.votes {
			if at.IsZero() || x.at.Before(at) {
				oldest, at = k, x.at
			}
		}
		delete(v.votes, oldest)
	}
	v.votes[voter] = vote{addr: addr.Unmap(), at: now}
}

// winner returns the external address the votes agree on, if any.
func (v *ipVoter) winner() (netip.Addr, bool) {
	tally := map[netip.Addr]int{}
	var best netip.Addr
	for _, x := range v.votes {
		tally[x.addr]++
		if n := tally[x.addr]; n > tally[best] {
			best = x.addr
		}
	}
	return best, tally[best] >= minVotes
}

// randomBytes fills b from crypto/rand.
func randomBytes(b []byte) { _, _ = rand.Read(b) }
