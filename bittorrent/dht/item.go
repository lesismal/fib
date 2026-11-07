package dht

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha1"
	"errors"
	"strconv"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// BEP 44 bounds.
const (
	MaxItemValue = 1000 // bytes of a bencoded value
	MaxItemSalt  = 64
	// ItemTTL is how long a stored item is kept without being put again.
	ItemTTL = 2 * time.Hour
)

// Item is an item of the DHT's storage (BEP 44). An immutable item is its
// value, found at the SHA-1 of it. A mutable item belongs to an ed25519
// key: it is found at the SHA-1 of the public key and salt, and replaced by
// any item of the same key and salt with a higher sequence number and a
// valid signature.
type Item struct {
	// V is the value, bencoded, at most MaxItemValue bytes.
	V bencode.RawMessage
	// K is the public key of a mutable item, zero for an immutable one.
	K    [32]byte
	Salt []byte
	Seq  int64
	Sig  [64]byte
}

// NewImmutableItem returns the immutable item of v, which it bencodes.
func NewImmutableItem(v any) (*Item, error) {
	b, err := bencode.Marshal(v)
	if err != nil {
		return nil, err
	}
	it := &Item{V: b}
	return it, it.Verify()
}

// NewMutableItem returns a mutable item of v, which it bencodes, signed
// with key.
func NewMutableItem(v any, salt []byte, seq int64, key ed25519.PrivateKey) (*Item, error) {
	b, err := bencode.Marshal(v)
	if err != nil {
		return nil, err
	}
	it := &Item{V: b, Salt: salt, Seq: seq}
	it.Sign(key)
	return it, it.Verify()
}

// Mutable reports whether the item is a mutable one.
func (it *Item) Mutable() bool { return it.K != [32]byte{} }

// Target returns the ID the item is stored at.
func (it *Item) Target() ID {
	if it.Mutable() {
		return MutableTarget(it.K, it.Salt)
	}
	return ID(sha1.Sum(it.V))
}

// MutableTarget returns the target of the mutable items of key k and salt.
func MutableTarget(k [32]byte, salt []byte) ID {
	h := sha1.New()
	h.Write(k[:])
	h.Write(salt)
	return ID(h.Sum(nil))
}

// signedBytes returns what a mutable item's signature covers: the salt,
// sequence number and value as bencoded dictionary entries, without the
// dictionary's "d" and "e".
func (it *Item) signedBytes() []byte {
	b := make([]byte, 0, len(it.V)+len(it.Salt)+40)
	if len(it.Salt) > 0 {
		b = append(b, "4:salt"...)
		b = strconv.AppendInt(b, int64(len(it.Salt)), 10)
		b = append(b, ':')
		b = append(b, it.Salt...)
	}
	b = append(b, "3:seqi"...)
	b = strconv.AppendInt(b, it.Seq, 10)
	b = append(b, "e1:v"...)
	return append(b, it.V...)
}

// Sign makes the item a mutable one of key, signing it.
func (it *Item) Sign(key ed25519.PrivateKey) {
	copy(it.K[:], key.Public().(ed25519.PublicKey))
	copy(it.Sig[:], ed25519.Sign(key, it.signedBytes()))
}

// Item errors, as the KRPC errors that report them.
var (
	ErrItemTooBig      = &Error{Code: ErrCodeTooBig, Msg: "message (v field) too big"}
	ErrItemSaltTooBig  = &Error{Code: ErrCodeSaltTooBig, Msg: "salt (salt field) too big"}
	ErrItemSignature   = &Error{Code: ErrCodeBadSignature, Msg: "invalid signature"}
	ErrItemCASMismatch = &Error{Code: ErrCodeCASMismatch, Msg: "the CAS hash mismatched, re-read value and try again"}
	ErrItemSeqTooLow   = &Error{Code: ErrCodeSeqTooLow, Msg: "sequence number less than current"}
)

// Verify checks the item's size, its value's bencoding and, for a mutable
// item, its signature.
func (it *Item) Verify() error {
	if len(it.V) == 0 || !bencode.Valid(it.V) {
		return errors.New("dht: an item's value must be one bencoded value")
	}
	if len(it.V) > MaxItemValue {
		return ErrItemTooBig
	}
	if !it.Mutable() {
		if len(it.Salt) > 0 || it.Seq != 0 {
			return errors.New("dht: an immutable item has no salt or seq")
		}
		return nil
	}
	if len(it.Salt) > MaxItemSalt {
		return ErrItemSaltTooBig
	}
	if !ed25519.Verify(it.K[:], it.signedBytes(), it.Sig[:]) {
		return ErrItemSignature
	}
	return nil
}

// itemStore keeps the items put to us, bounded in number.
type itemStore struct {
	items map[ID]*storedItem
	max   int
}

type storedItem struct {
	Item
	stored time.Time
}

// put stores an item that passed Verify, under BEP 44's rules for
// replacing a mutable one: cas, when given, must be the stored sequence
// number, and the new one must not be lower, nor equal with another value.
func (s *itemStore) put(it *Item, cas *int64, now time.Time) error {
	target := it.Target()
	old := s.items[target]
	if old != nil && it.Mutable() {
		if cas != nil && *cas != old.Seq {
			return ErrItemCASMismatch
		}
		if it.Seq < old.Seq || it.Seq == old.Seq && !bytes.Equal(it.V, old.V) {
			return ErrItemSeqTooLow
		}
	}
	if old == nil && len(s.items) >= s.max {
		// Full: the stalest item makes way.
		var victim ID
		var at time.Time
		for t, x := range s.items {
			if at.IsZero() || x.stored.Before(at) {
				victim, at = t, x.stored
			}
		}
		delete(s.items, victim)
	}
	s.items[target] = &storedItem{Item: *it, stored: now}
	return nil
}

func (s *itemStore) get(target ID, now time.Time) *Item {
	x := s.items[target]
	if x == nil || now.Sub(x.stored) >= ItemTTL {
		return nil
	}
	it := x.Item
	return &it
}

func (s *itemStore) expire(now time.Time) {
	for t, x := range s.items {
		if now.Sub(x.stored) >= ItemTTL {
			delete(s.items, t)
		}
	}
}
