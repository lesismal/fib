package mse

import (
	"bytes"
	"crypto/rc4"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
)

// protocolHeader is how a plaintext BitTorrent handshake begins, which a
// receiver tells MSE apart by: the odds of a public key beginning so are
// 2^-160.
const protocolHeader = "\x13BitTorrent protocol"

// KeyFinder finds the SKEY whose Req2 is req2: how a receiver serving many
// torrents learns which one an initiator wants.
type KeyFinder interface {
	FindKey(req2 [20]byte) (skey [20]byte, ok bool)
}

// KeyFunc is a function that is a KeyFinder.
type KeyFunc func(req2 [20]byte) ([20]byte, bool)

// FindKey calls f.
func (f KeyFunc) FindKey(req2 [20]byte) ([20]byte, bool) { return f(req2) }

// KeyTable is a KeyFinder of a set of SKEYs, info hashes, safe for
// concurrent use: a lookup is a map access, however many torrents there
// are, rather than a hash of each.
type KeyTable struct {
	mu   sync.RWMutex
	keys map[[20]byte][20]byte
}

// Add adds skey.
func (t *KeyTable) Add(skey [20]byte) {
	t.mu.Lock()
	if t.keys == nil {
		t.keys = make(map[[20]byte][20]byte)
	}
	t.keys[Req2(skey)] = skey
	t.mu.Unlock()
}

// Remove removes skey.
func (t *KeyTable) Remove(skey [20]byte) {
	t.mu.Lock()
	delete(t.keys, Req2(skey))
	t.mu.Unlock()
}

// Len is the number of SKEYs in the table.
func (t *KeyTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.keys)
}

// FindKey implements KeyFinder.
func (t *KeyTable) FindKey(req2 [20]byte) ([20]byte, bool) {
	t.mu.RLock()
	skey, ok := t.keys[req2]
	t.mu.RUnlock()
	return skey, ok
}

// Initiator is the initiator's side of the handshake (A), without I/O:
// Start gives what to send first, and Feed takes what the receiver sends
// and gives what to send in answer, until it reports done.
type Initiator struct {
	skey    [20]byte
	provide CryptoMethod
	ia      []byte
	rnd     io.Reader

	key    *keyPair
	state  int
	buf    []byte
	evc    [8]byte
	dec    *rc4.Cipher
	enc    *rc4.Cipher
	method CryptoMethod
	pad    int
	result *Result
}

const (
	iStart = iota
	iYb
	iVC
	iSelect
	iPadD
	iDone
	iFailed
)

// NewInitiator returns the initiator of a handshake for skey, the info
// hash of the torrent wanted, providing provide, with ia as the initial
// payload: typically the BitTorrent handshake, so that it goes with the
// third message rather than a round trip later.
func NewInitiator(skey [20]byte, provide CryptoMethod, ia []byte) *Initiator {
	return &Initiator{skey: skey, provide: provide & Both, ia: ia}
}

// Start returns the first message, Ya and PadA.
func (h *Initiator) Start() ([]byte, error) {
	if h.state != iStart {
		return nil, errors.New("mse: Start called twice")
	}
	if h.provide == 0 {
		return nil, ErrNoMethod
	}
	if len(h.ia) > math.MaxUint16 {
		return nil, errors.New("mse: initial payload too long")
	}
	rnd := randReader(h.rnd)
	key, err := newKeyPair(rnd)
	if err != nil {
		return nil, err
	}
	pad, err := padding(rnd, MaxPad)
	if err != nil {
		return nil, err
	}
	h.key = key
	h.state = iYb
	return append(key.y[:], pad...), nil
}

// Feed takes bytes from the receiver and returns bytes to send to it, and
// done once the handshake is complete, when Result says how to go on. An
// error ends the handshake.
func (h *Initiator) Feed(in []byte) (out []byte, done bool, err error) {
	switch h.state {
	case iStart:
		return nil, false, errors.New("mse: Feed before Start")
	case iDone:
		return nil, true, errors.New("mse: handshake already done")
	case iFailed:
		return nil, false, errors.New("mse: handshake failed")
	}
	h.buf = append(h.buf, in...)
	out, err = h.step()
	if err != nil {
		h.state = iFailed
		h.buf = nil
		return nil, false, err
	}
	return out, h.state == iDone, nil
}

func (h *Initiator) step() (out []byte, err error) {
	for {
		switch h.state {
		case iYb:
			if len(h.buf) < KeySize {
				return out, nil
			}
			s, err := h.key.secret(h.buf[:KeySize])
			if err != nil {
				return nil, err
			}
			h.buf = h.buf[KeySize:]
			req1 := hash([]byte("req1"), s[:])
			req2 := Req2(h.skey)
			req3 := hash([]byte("req3"), s[:])
			out = append(out, req1[:]...)
			for i := range req2 {
				out = append(out, req2[i]^req3[i])
			}
			// ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA)), ENCRYPT(IA):
			// PadC is left empty, as is the practice.
			start := len(out)
			out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
			out = binary.BigEndian.AppendUint32(out, uint32(h.provide))
			out = binary.BigEndian.AppendUint16(out, 0)
			out = binary.BigEndian.AppendUint16(out, uint16(len(h.ia)))
			out = append(out, h.ia...)
			h.enc = newCipher("keyA", s[:], h.skey)
			h.enc.XORKeyStream(out[start:], out[start:])
			// The receiver's VC, encrypted, is the first eight bytes of
			// its keystream, which finding it consumes.
			h.dec = newCipher("keyB", s[:], h.skey)
			h.dec.XORKeyStream(h.evc[:], h.evc[:])
			h.state = iVC
		case iVC:
			// VC follows Yb's padding, of up to MaxPad bytes.
			n := min(len(h.buf), MaxPad+8)
			i := bytes.Index(h.buf[:n], h.evc[:])
			if i < 0 {
				if len(h.buf) >= MaxPad+8 {
					return nil, ErrNoSync
				}
				return out, nil
			}
			h.buf = h.buf[i+8:]
			h.state = iSelect
		case iSelect:
			if len(h.buf) < 6 {
				return out, nil
			}
			h.dec.XORKeyStream(h.buf[:6], h.buf[:6])
			sel := CryptoMethod(binary.BigEndian.Uint32(h.buf))
			h.pad = int(binary.BigEndian.Uint16(h.buf[4:]))
			h.buf = h.buf[6:]
			if (sel != Plaintext && sel != RC4) || sel&h.provide == 0 {
				return nil, ErrNoMethod
			}
			if h.pad > MaxPad {
				return nil, ErrPadding
			}
			h.method = sel
			h.state = iPadD
		case iPadD:
			if len(h.buf) < h.pad {
				return out, nil
			}
			h.dec.XORKeyStream(h.buf[:h.pad], h.buf[:h.pad])
			rest := h.buf[h.pad:]
			r := &Result{Method: h.method, SKey: h.skey}
			if len(rest) > 0 {
				r.Rest = append([]byte(nil), rest...)
			}
			if h.method == RC4 {
				r.Encrypt, r.Decrypt = h.enc, h.dec
				r.Decrypt.XORKeyStream(r.Rest, r.Rest)
			}
			h.result = r
			h.buf = nil
			h.state = iDone
			return out, nil
		default:
			return out, nil
		}
	}
}

// Result is the outcome once Feed has reported done, nil before.
func (h *Initiator) Result() *Result { return h.result }

// Receiver is the receiver's side of the handshake (B), without I/O: Feed
// takes what the initiator sends and gives what to send in answer, until
// it reports done. It also recognizes a plaintext BitTorrent handshake,
// which its policy accepts or refuses.
type Receiver struct {
	keys   KeyFinder
	policy Policy
	rnd    io.Reader

	key    *keyPair
	state  int
	buf    []byte
	s      [KeySize]byte
	req1   [20]byte
	skey   [20]byte
	dec    *rc4.Cipher
	method CryptoMethod
	pad    int
	ia     []byte
	result *Result
}

const (
	rDetect = iota
	rYa
	rReq1
	rReq2
	rVC
	rPadC
	rLenIA
	rIA
	rDone
	rFailed
)

// NewReceiver returns the receiver of a handshake, finding the initiator's
// SKEY with keys and treating plaintext and the methods provided as policy
// says.
func NewReceiver(keys KeyFinder, policy Policy) *Receiver {
	return &Receiver{keys: keys, policy: policy}
}

// Feed takes bytes from the initiator and returns bytes to send to it, and
// done once the handshake is complete, when Result says how to go on. An
// error ends the handshake.
func (h *Receiver) Feed(in []byte) (out []byte, done bool, err error) {
	switch h.state {
	case rDone:
		return nil, true, errors.New("mse: handshake already done")
	case rFailed:
		return nil, false, errors.New("mse: handshake failed")
	}
	h.buf = append(h.buf, in...)
	out, err = h.step()
	if err != nil {
		h.state = rFailed
		h.buf = nil
		return nil, false, err
	}
	return out, h.state == rDone, nil
}

func (h *Receiver) step() (out []byte, err error) {
	for {
		switch h.state {
		case rDetect:
			n := min(len(h.buf), len(protocolHeader))
			if string(h.buf[:n]) != protocolHeader[:n] {
				h.state = rYa
				continue
			}
			if n < len(protocolHeader) {
				return out, nil
			}
			if h.policy == Require {
				return nil, ErrPlaintextRefused
			}
			h.result = &Result{PlainHandshake: true, Rest: h.buf}
			h.buf = nil
			h.state = rDone
			return out, nil
		case rYa:
			if len(h.buf) < KeySize {
				return out, nil
			}
			rnd := randReader(h.rnd)
			key, err := newKeyPair(rnd)
			if err != nil {
				return nil, err
			}
			if h.s, err = key.secret(h.buf[:KeySize]); err != nil {
				return nil, err
			}
			pad, err := padding(rnd, MaxPad)
			if err != nil {
				return nil, err
			}
			h.key = key
			h.buf = h.buf[KeySize:]
			h.req1 = hash([]byte("req1"), h.s[:])
			out = append(out, key.y[:]...)
			out = append(out, pad...)
			h.state = rReq1
		case rReq1:
			// HASH('req1', S) follows Ya's padding, of up to MaxPad bytes.
			n := min(len(h.buf), MaxPad+20)
			i := bytes.Index(h.buf[:n], h.req1[:])
			if i < 0 {
				if len(h.buf) >= MaxPad+20 {
					return nil, ErrNoSync
				}
				return out, nil
			}
			h.buf = h.buf[i+20:]
			h.state = rReq2
		case rReq2:
			if len(h.buf) < 20 {
				return out, nil
			}
			req3 := hash([]byte("req3"), h.s[:])
			var req2 [20]byte
			for i := range req2 {
				req2[i] = h.buf[i] ^ req3[i]
			}
			skey, ok := h.keys.FindKey(req2)
			if !ok {
				return nil, ErrUnknownKey
			}
			h.skey = skey
			h.buf = h.buf[20:]
			h.dec = newCipher("keyA", h.s[:], skey)
			h.state = rVC
		case rVC:
			if len(h.buf) < 14 {
				return out, nil
			}
			h.dec.XORKeyStream(h.buf[:14], h.buf[:14])
			if !bytes.Equal(h.buf[:8], make([]byte, 8)) {
				return nil, ErrBadVC
			}
			provided := CryptoMethod(binary.BigEndian.Uint32(h.buf[8:]))
			h.pad = int(binary.BigEndian.Uint16(h.buf[12:]))
			h.buf = h.buf[14:]
			sel := h.policy.Select(provided & Both)
			if sel != Plaintext && sel != RC4 {
				return nil, ErrNoMethod
			}
			if h.pad > MaxPad {
				return nil, ErrPadding
			}
			h.method = sel
			h.state = rPadC
		case rPadC:
			if len(h.buf) < h.pad {
				return out, nil
			}
			h.dec.XORKeyStream(h.buf[:h.pad], h.buf[:h.pad])
			h.buf = h.buf[h.pad:]
			h.state = rLenIA
		case rLenIA:
			if len(h.buf) < 2 {
				return out, nil
			}
			h.dec.XORKeyStream(h.buf[:2], h.buf[:2])
			h.pad = int(binary.BigEndian.Uint16(h.buf))
			h.buf = h.buf[2:]
			h.state = rIA
		case rIA:
			if len(h.buf) < h.pad {
				return out, nil
			}
			ia := h.buf[:h.pad]
			h.dec.XORKeyStream(ia, ia)
			rest := h.buf[h.pad:]
			r := &Result{Method: h.method, SKey: h.skey}
			if len(ia) > 0 {
				r.IA = append([]byte(nil), ia...)
			}
			if len(rest) > 0 {
				r.Rest = append([]byte(nil), rest...)
			}
			// ENCRYPT(VC, crypto_select, len(padD), padD)
			pad, err := padding(randReader(h.rnd), MaxPad)
			if err != nil {
				return nil, err
			}
			start := len(out)
			out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
			out = binary.BigEndian.AppendUint32(out, uint32(h.method))
			out = binary.BigEndian.AppendUint16(out, uint16(len(pad)))
			out = append(out, make([]byte, len(pad))...)
			enc := newCipher("keyB", h.s[:], h.skey)
			enc.XORKeyStream(out[start:], out[start:])
			if h.method == RC4 {
				r.Encrypt, r.Decrypt = enc, h.dec
				r.Decrypt.XORKeyStream(r.Rest, r.Rest)
			}
			h.result = r
			h.buf = nil
			h.state = rDone
			return out, nil
		default:
			return out, nil
		}
	}
}

// Result is the outcome once Feed has reported done, nil before.
func (h *Receiver) Result() *Result { return h.result }
