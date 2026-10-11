package tls

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	stdtls "crypto/tls"
	"encoding/binary"
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
}

func newTicketKeys(config *stdtls.Config) *ticketKeys {
	if config.SessionTicketsDisabled {
		return nil
	}
	t := &ticketKeys{}
	if config.SessionTicketKey != ([32]byte{}) {
		if k, err := newTicketKey(config.SessionTicketKey[:], time.Time{}); err == nil {
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
	return t.sealRaw(plain, now)
}

// ticketState12 is what a TLS 1.2 ticket holds (RFC 5077): the session's
// master secret, and what must match for it to be used again.
type ticketState12 struct {
	suite   uint16
	created time.Time
	master  []byte
	ems     bool
	server  string
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
	if len(plain) != 2+m {
		return ticketState12{}, false
	}
	s.server = string(plain[2:])
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
	m := int(binary.BigEndian.Uint16(plain))
	if len(plain) != 2+m {
		return ticketState{}, false
	}
	s.server = string(plain[2:])
	if age := now.Sub(s.created); age < -time.Minute || age > ticketLifetime {
		return ticketState{}, false
	}
	return s, true
}
