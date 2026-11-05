package tls

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	stdtls "crypto/tls"
	"encoding/binary"
	"slices"
	"sync"
	"time"
)

// Session tickets for the TLS 1.3 server that runs without a worker (RFC 8446
// section 4.6.1). A ticket carries everything the server needs to resume the
// session, sealed under a key only the server holds, so the server keeps no
// state per session. What a ticket carries is private to this package; a
// client only hands it back.

const (
	// ticketLifetime is how long a ticket is good for, the longest TLS 1.3
	// allows.
	ticketLifetime = 7 * 24 * time.Hour
	// ticketKeyLife is how long a key seals new tickets before the next one
	// takes over; it opens tickets for as long as they live.
	ticketKeyLife = 24 * time.Hour
	ticketNameLen = 16
)

// ticketKey is one key tickets are sealed under.
type ticketKey struct {
	name    [ticketNameLen]byte
	aead    cipher.AEAD
	created time.Time
}

func newTicketKey(secret []byte, created time.Time) (*ticketKey, error) {
	sum := sha256.Sum256(append([]byte("fib tls ticket key"), secret...))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	k := &ticketKey{aead: aead, created: created}
	name := sha256.Sum256(append([]byte("fib tls ticket name"), secret...))
	copy(k.name[:], name[:])
	return k, nil
}

// ticketKeys are a Handler's keys: a static one from Config.SessionTicketKey
// if it has one, otherwise keys made at random and replaced daily, the old
// ones kept as long as the tickets they sealed can live.
type ticketKeys struct {
	mu     sync.Mutex
	static *ticketKey
	keys   []*ticketKey
	// used are the tickets that have carried early data, until they expire:
	// early data can be replayed, so a ticket gets it once (RFC 8446 section
	// 8, RFC 9001 section 9.2). It is this instance's alone, a Handler's or a
	// QUICServer's, even where another opens the same tickets with a key
	// they share; see Handler.EarlyData.
	used map[[32]byte]time.Time
}

// maxUsedTickets bounds the register; when it is full of live tickets no
// more early data is accepted, which costs a round trip and nothing else. It
// is a variable so that a test can fill it without a million entries.
var maxUsedTickets = 1 << 20

// firstUse reports whether ticket has not carried early data before, and
// records that it does now.
func (t *ticketKeys) firstUse(ticket []byte, now time.Time) bool {
	key := sha256.Sum256(ticket)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, seen := t.used[key]; seen {
		return false
	}
	if t.used == nil {
		t.used = make(map[[32]byte]time.Time)
	}
	if len(t.used) >= maxUsedTickets {
		for k, expires := range t.used {
			if now.After(expires) {
				delete(t.used, k)
			}
		}
		if len(t.used) >= maxUsedTickets {
			return false
		}
	}
	t.used[key] = now.Add(ticketLifetime + time.Minute)
	return true
}

func newTicketKeys(config *stdtls.Config) *ticketKeys {
	if config.SessionTicketsDisabled {
		return nil
	}
	t := &ticketKeys{}
	// crypto/tls fills an unset SessionTicketKey, under its own lock, the first
	// time a server it runs on the Config needs a ticket key; Clone reads it
	// under that lock. A key it made starts with "DEPRECATED" and is never
	// used to seal tickets, so it is not the user's.
	key := config.Clone().SessionTicketKey
	if key != ([32]byte{}) && !bytes.HasPrefix(key[:], []byte("DEPRECATED")) {
		if k, err := newTicketKey(key[:], time.Time{}); err == nil {
			t.static = k
		}
	}
	return t
}

// current is the key new tickets are sealed under.
func (t *ticketKeys) current(now time.Time) *ticketKey {
	if t.static != nil {
		return t.static
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.keys) == 0 || now.Sub(t.keys[0].created) >= ticketKeyLife {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil
		}
		k, err := newTicketKey(secret, now)
		if err != nil {
			return nil
		}
		t.keys = append([]*ticketKey{k}, t.keys...)
		// Keys that can no longer open a live ticket are dropped.
		for len(t.keys) > 1 && now.Sub(t.keys[len(t.keys)-1].created) > ticketLifetime+ticketKeyLife {
			t.keys = t.keys[:len(t.keys)-1]
		}
	}
	return t.keys[0]
}

func (t *ticketKeys) find(name []byte) *ticketKey {
	if t.static != nil {
		if string(name) == string(t.static.name[:]) {
			return t.static
		}
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range t.keys {
		if string(name) == string(k.name[:]) {
			return k
		}
	}
	return nil
}

// ticketState is what a ticket holds.
type ticketState struct {
	suite   uint16
	created time.Time
	ageAdd  uint32
	psk     []byte
	server  string
	// For a QUIC session that may be resumed with 0-RTT data: whether it
	// may, the protocol it was agreed for, and what the QUIC layer asked to
	// have kept with it (the transport parameters the server then had).
	earlyData bool
	alpn      string
	extra     [][]byte
	// certs are the certificates the client authenticated with, if it did,
	// and chains those they were verified with.
	certs  [][]byte
	chains [][][]byte
}

// sealRaw seals plain under the current key.
func (t *ticketKeys) sealRaw(plain []byte, now time.Time) ([]byte, bool) {
	k := t.current(now)
	if k == nil {
		return nil, false
	}
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, false
	}
	ticket := append(append([]byte(nil), k.name[:]...), nonce...)
	return k.aead.Seal(ticket, nonce, plain, k.name[:]), true
}

// openRaw opens a ticket sealed by sealRaw, or reports false for one that is
// not ours or was altered.
func (t *ticketKeys) openRaw(ticket []byte) ([]byte, bool) {
	if len(ticket) < ticketNameLen {
		return nil, false
	}
	k := t.find(ticket[:ticketNameLen])
	if k == nil || len(ticket) < ticketNameLen+k.aead.NonceSize()+k.aead.Overhead() {
		return nil, false
	}
	rest := ticket[ticketNameLen:]
	plain, err := k.aead.Open(nil, rest[:k.aead.NonceSize()], rest[k.aead.NonceSize():], ticket[:ticketNameLen])
	return plain, err == nil
}

// seal makes the ticket for s, a TLS 1.3 session.
func (t *ticketKeys) seal(s ticketState, now time.Time) ([]byte, bool) {
	plain := []byte{1}
	plain = binary.BigEndian.AppendUint16(plain, s.suite)
	plain = binary.BigEndian.AppendUint64(plain, uint64(s.created.Unix()))
	plain = binary.BigEndian.AppendUint32(plain, s.ageAdd)
	plain = append(plain, byte(len(s.psk)))
	plain = append(plain, s.psk...)
	plain = binary.BigEndian.AppendUint16(plain, uint16(len(s.server)))
	plain = append(plain, s.server...)
	if s.earlyData || len(s.extra) != 0 || len(s.certs) != 0 {
		// After the server name: whether early data is allowed, the protocol,
		// and the extra data of the QUIC layer; then the client's
		// certificates, if it sent any.
		early := byte(0)
		if s.earlyData {
			early = 1
		}
		plain = append(plain, early, byte(len(s.alpn)))
		plain = append(plain, s.alpn...)
		plain = append(plain, byte(len(s.extra)))
		for _, e := range s.extra {
			plain = binary.BigEndian.AppendUint16(plain, uint16(len(e)))
			plain = append(plain, e...)
		}
		var ok bool
		if plain, ok = appendPeer(plain, s.certs, s.chains); !ok {
			return nil, false
		}
	}
	return t.sealRaw(plain, now)
}

// appendPeer appends the client's certificates and their verified chains, if
// it sent any: a count and the certificates, each with its length, then a
// count of chains and each chain the same way. It reports false if they do
// not fit a ticket, which is then not issued: a session resumed without them
// would lose who the client is.
func appendPeer(plain []byte, certs [][]byte, chains [][][]byte) ([]byte, bool) {
	if len(certs) == 0 {
		return plain, true
	}
	if len(certs) > 255 || len(chains) > 255 || slices.ContainsFunc(chains, func(c [][]byte) bool { return len(c) > 255 }) {
		return plain, false
	}
	appendList := func(plain []byte, list [][]byte) []byte {
		plain = append(plain, byte(len(list)))
		for _, der := range list {
			plain = append(plain, byte(len(der)>>16), byte(len(der)>>8), byte(len(der)))
			plain = append(plain, der...)
		}
		return plain
	}
	plain = appendList(plain, certs)
	plain = append(plain, byte(len(chains)))
	for _, chain := range chains {
		plain = appendList(plain, chain)
	}
	// The ticket goes in a field of up to 64 KiB, with its key name, nonce
	// and tag.
	return plain, len(plain) <= maxTicketPlain
}

const maxTicketPlain = 0xffff - ticketNameLen - 64

// readPeer reads back what appendPeer wrote, which is all that is left of
// plain, or nothing.
func readPeer(plain []byte) (certs [][]byte, chains [][][]byte, ok bool) {
	if len(plain) == 0 {
		return nil, nil, true
	}
	r := newReader(plain)
	readList := func() [][]byte {
		n := int(r.u8())
		list := make([][]byte, 0, n)
		for i := 0; i < n && r.ok; i++ {
			list = append(list, bytes.Clone(r.vec24()))
		}
		return list
	}
	certs = readList()
	n := int(r.u8())
	for i := 0; i < n && r.ok; i++ {
		chains = append(chains, readList())
	}
	if !r.empty() || len(certs) == 0 {
		return nil, nil, false
	}
	return certs, chains, true
}

// ticketState12 is what a TLS 1.2 ticket holds (RFC 5077): the session's
// master secret, what must match for it to be used again, and the client's
// certificates, as ticketState has them.
type ticketState12 struct {
	suite   uint16
	created time.Time
	master  []byte
	ems     bool
	server  string
	certs   [][]byte
	chains  [][][]byte
}

func (t *ticketKeys) seal12(s ticketState12, now time.Time) ([]byte, bool) {
	plain := []byte{2}
	plain = binary.BigEndian.AppendUint16(plain, s.suite)
	plain = binary.BigEndian.AppendUint64(plain, uint64(s.created.Unix()))
	ems := byte(0)
	if s.ems {
		ems = 1
	}
	plain = append(plain, ems, byte(len(s.master)))
	plain = append(plain, s.master...)
	plain = binary.BigEndian.AppendUint16(plain, uint16(len(s.server)))
	plain = append(plain, s.server...)
	plain, ok := appendPeer(plain, s.certs, s.chains)
	if !ok {
		return nil, false
	}
	return t.sealRaw(plain, now)
}

func (t *ticketKeys) open12(ticket []byte, now time.Time) (ticketState12, bool) {
	plain, ok := t.openRaw(ticket)
	if !ok || len(plain) < 1+2+8+1+1 || plain[0] != 2 {
		return ticketState12{}, false
	}
	var s ticketState12
	s.suite = binary.BigEndian.Uint16(plain[1:])
	s.created = time.Unix(int64(binary.BigEndian.Uint64(plain[3:])), 0)
	s.ems = plain[11] == 1
	n := int(plain[12])
	plain = plain[13:]
	if len(plain) < n+2 {
		return ticketState12{}, false
	}
	s.master, plain = plain[:n], plain[n:]
	m := int(binary.BigEndian.Uint16(plain))
	if len(plain) < 2+m {
		return ticketState12{}, false
	}
	s.server = string(plain[2 : 2+m])
	if s.certs, s.chains, ok = readPeer(plain[2+m:]); !ok {
		return ticketState12{}, false
	}
	if age := now.Sub(s.created); age < -time.Minute || age > ticketLifetime {
		return ticketState12{}, false
	}
	return s, true
}

// open reads a TLS 1.3 ticket back. It reports false for one that is not
// ours, was altered, or has outlived its lifetime.
func (t *ticketKeys) open(ticket []byte, now time.Time) (ticketState, bool) {
	plain, ok := t.openRaw(ticket)
	if !ok || len(plain) < 1+2+8+4+1 || plain[0] != 1 {
		return ticketState{}, false
	}
	var s ticketState
	s.suite = binary.BigEndian.Uint16(plain[1:])
	s.created = time.Unix(int64(binary.BigEndian.Uint64(plain[3:])), 0)
	s.ageAdd = binary.BigEndian.Uint32(plain[11:])
	n := int(plain[15])
	plain = plain[16:]
	if len(plain) < n+2 {
		return ticketState{}, false
	}
	s.psk, plain = plain[:n], plain[n:]
	if len(plain) < 2 {
		return ticketState{}, false
	}
	m := int(binary.BigEndian.Uint16(plain))
	if len(plain) < 2+m {
		return ticketState{}, false
	}
	s.server, plain = string(plain[2:2+m]), plain[2+m:]
	if len(plain) != 0 {
		if len(plain) < 3 {
			return ticketState{}, false
		}
		s.earlyData = plain[0] == 1
		a := int(plain[1])
		if len(plain) < 2+a+1 {
			return ticketState{}, false
		}
		s.alpn, plain = string(plain[2:2+a]), plain[2+a:]
		count := int(plain[0])
		plain = plain[1:]
		for i := 0; i < count; i++ {
			if len(plain) < 2 {
				return ticketState{}, false
			}
			l := int(binary.BigEndian.Uint16(plain))
			if len(plain) < 2+l {
				return ticketState{}, false
			}
			s.extra = append(s.extra, bytes.Clone(plain[2:2+l]))
			plain = plain[2+l:]
		}
		if s.certs, s.chains, ok = readPeer(plain); !ok {
			return ticketState{}, false
		}
	}
	if age := now.Sub(s.created); age < -time.Minute || age > ticketLifetime {
		return ticketState{}, false
	}
	return s, true
}
