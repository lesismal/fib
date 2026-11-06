package hls

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

// Encryption encrypts a Muxer's media segments, and its Low-Latency parts,
// with AES-128 (METHOD=AES-128 of RFC 8216 section 4.3.2.4): AES-128 in CBC
// mode with PKCS#7 padding, under keys the caller supplies, a key for every
// Rotation segments (a key period, or epoch). Initialization sections stay
// clear.
//
// The media playlist carries an EXT-X-KEY wherever the key changes, before
// the segment's first EXT-X-PART, naming the key by the URI Key returned and
// no IV, so that a segment's IV is its media sequence number; an EXT-X-MAP
// after a key is preceded by EXT-X-KEY:METHOD=NONE, and the key named again
// after it. A part is encrypted on its own, as a resource of its own, with
// the key and the IV of its segment (its media sequence number): the
// specification has EXT-X-KEY apply to a segment's parts, written before
// the first, and says nothing of a part's IV, which players (hls.js) take
// as their segment's.
//
// The Muxer serves no key: the caller answers the URIs it names, with
// whatever access control it has, and may tell a key period's URI from one
// not yet used with Muxer.Session and Muxer.KeyEpoch.
type Encryption struct {
	// Rotation is how many segments one key encrypts: segment msn is
	// encrypted with the key of epoch msn / Rotation. 0 is one key, epoch 0,
	// for the Muxer's life.
	Rotation int
	// Key returns the key of epoch of the Muxer whose session is session
	// (Muxer.Session), and the URI the media playlist names it by, relative
	// to the playlist, as "key-<session>-<epoch>.key"; it must not hold a
	// double quote, a CR or an LF. It is called on the publisher's
	// goroutine, with the Muxer's lock held, once an epoch, as its first
	// segment begins; it must not call the Muxer. An error, or a URI it
	// cannot carry, ends the stream (EXT-X-ENDLIST), clear media never
	// being served in place of encrypted. The Muxer keeps the key only as
	// the cipher it makes of it.
	Key func(session string, epoch uint64) (key [16]byte, uri string, err error)
}

// errKeyURI is a URI a playlist cannot carry in a quoted string.
var errKeyURI = errors.New("hls: an Encryption.Key URI holds a double quote or a line break")

// errNoKey is an Encryption without a Key.
var errNoKey = errors.New("hls: Encryption.Key is nil")

// epochKey is the key of a key period, as the ciphers made of it: seg
// encrypts the segment being written, part a part. It neither prints nor
// keeps the key.
type epochKey struct {
	epoch uint64
	uri   string
	block cipher.Block
	seg   cipher.BlockMode
	part  cipher.BlockMode
}

// String names the key period and nothing of its key.
func (k *epochKey) String() string {
	return "hls.key(epoch " + strconv.FormatUint(k.epoch, 10) + ")"
}

// GoString is String, for %#v.
func (k *epochKey) GoString() string { return k.String() }

// ivSetter is a CBC mode whose IV can be set again, as crypto/aes's is,
// sparing an allocation a segment and a part.
type ivSetter interface{ SetIV([]byte) }

// msnIV returns the IV of media sequence number msn: msn as a 128-bit
// big-endian number.
func msnIV(msn uint64) [aes.BlockSize]byte {
	var iv [aes.BlockSize]byte
	binary.BigEndian.PutUint64(iv[8:], msn)
	return iv
}

// cbcAt returns mode, or a new CBC encrypter of block, set to IV iv.
func cbcAt(mode cipher.BlockMode, block cipher.Block, iv [aes.BlockSize]byte) cipher.BlockMode {
	if s, ok := mode.(ivSetter); ok {
		s.SetIV(iv[:])
		return mode
	}
	return cipher.NewCBCEncrypter(block, iv[:])
}

// epochKey returns the key of segment msn, asking Encryption.Key for it
// when its epoch is not the last one's. m.mu is held.
func (m *Muxer) epochKey(msn uint64) (*epochKey, error) {
	epoch := uint64(0)
	if r := m.enc.Rotation; r > 0 {
		epoch = msn / uint64(r)
	}
	if k := m.key; k != nil && k.epoch == epoch {
		return k, nil
	}
	if m.enc.Key == nil {
		return nil, errNoKey
	}
	key, uri, err := m.enc.Key(m.session, epoch)
	if err != nil {
		clear(key[:])
		return nil, err
	}
	if strings.ContainsAny(uri, "\"\r\n") {
		clear(key[:])
		return nil, errKeyURI
	}
	block, err := aes.NewCipher(key[:])
	clear(key[:])
	if err != nil {
		return nil, err
	}
	var iv [aes.BlockSize]byte
	m.key = &epochKey{
		epoch: epoch,
		uri:   uri,
		block: block,
		seg:   cipher.NewCBCEncrypter(block, iv[:]),
		part:  cipher.NewCBCEncrypter(block, iv[:]),
	}
	return m.key, nil
}

// pad appends the PKCS#7 padding of a message n bytes long to b.
func pad(b []byte, n int) []byte {
	p := aes.BlockSize - n%aes.BlockSize
	for range p {
		b = append(b, byte(p))
	}
	return b
}

// encryptSegment encrypts, in place, the whole blocks of s's data not yet
// encrypted, chaining on from the last; final pads the segment first and
// encrypts it to its end. s is the open segment, whose data nobody reads
// while it is open: its parts are served from their own ciphertext.
func encryptSegment(s *segment, final bool) {
	if final {
		s.data = pad(s.data, len(s.data))
	}
	n := (len(s.data) - s.encN) &^ (aes.BlockSize - 1)
	if n == 0 {
		return
	}
	b := s.data[s.encN : s.encN+n]
	s.key.seg.CryptBlocks(b, b)
	s.encN += n
}

// appendPart appends clear, a part of s, encrypted on its own with the key
// and IV of s, to s.penc, and returns where it is; size is what the parts
// of the last segment took.
func appendPart(s *segment, clear []byte, size int) (off, n int) {
	off = len(s.penc)
	if s.penc == nil && size > 0 {
		// As long as the last segment's, and a little more: one
		// allocation a segment.
		s.penc = make([]byte, 0, size+size/4)
	}
	s.penc = append(s.penc, clear...)
	s.penc = pad(s.penc, len(clear))
	b := s.penc[off:]
	s.key.part = cbcAt(s.key.part, s.key.block, msnIV(s.msn))
	s.key.part.CryptBlocks(b, b)
	return off, len(b)
}

// Session returns the Muxer's session: the part of its file names that
// tells them from those of every other Muxer, and what Encryption.Key is
// given.
func (m *Muxer) Session() string { return m.session }

// Next returns the media sequence number of the segment being cut, or of
// the next one when none is.
func (m *Muxer) Next() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next
}

// KeyEpoch returns the key period of the segment being cut, or of the next
// one, Next() / Encryption.Rotation, and false when the Muxer encrypts
// nothing: a playlist has named no key of a later epoch, nor will until
// that segment ends, so that a key server can refuse the keys of segments
// not cut yet.
func (m *Muxer) KeyEpoch() (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.enc == nil {
		return 0, false
	}
	if r := m.enc.Rotation; r > 0 {
		return m.next / uint64(r), true
	}
	return 0, true
}
