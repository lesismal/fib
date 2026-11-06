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
// Nothing is encrypted, nor any key asked for, with the Muxer's lock held,
// nor on the publisher's goroutine: the Muxer cuts a segment or part clear
// and hands it to Run, and lists it, and serves it, once Run has encrypted
// it. Playlist and file requests never wait on a cipher; a segment appears
// in the playlist as late as its encryption takes. Should the encryption
// fall behind by more than a few segments, the stream ends.
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
	// double quote, a CR or an LF. It is called where Run runs the
	// encryption, without the Muxer's lock, once an epoch, before its first
	// segment or part is encrypted. An error, or a URI it cannot carry,
	// ends the stream (EXT-X-ENDLIST), clear media never being served in
	// place of encrypted. The Muxer keeps the key only as the cipher it
	// makes of it.
	Key func(session string, epoch uint64) (key [16]byte, uri string, err error)
	// Run runs job, which encrypts what the Muxer has cut until nothing is
	// left, and reports whether it will: on a pool of the caller's, say.
	// It is called by the publisher's goroutine without the Muxer's lock,
	// and may run job there and then, holding the publisher up for as long.
	// Nil, or false, runs job on a goroutine of its own.
	Run func(job func()) bool
}

// maxPending is how many segments may wait for their encryption: one more
// ends the stream, rather than holding media without bound.
const maxPending = 4

// The errors that end an encrypted stream.
var (
	errKeyURI  = errors.New("hls: an Encryption.Key URI holds a double quote or a line break")
	errNoKey   = errors.New("hls: Encryption.Key is nil")
	errBacklog = errors.New("hls: the encryption of segments fell behind")
)

// newCipher makes the cipher of a key; tests make slow ones.
var newCipher = aes.NewCipher

// epochKey is the key of a key period, as the ciphers made of it: seg
// encrypts a segment, part a part. It neither prints nor keeps the key.
type epochKey struct {
	epoch uint64
	uri   string
	block cipher.Block
	seg   cipher.BlockMode
	part  cipher.BlockMode
	iv    [aes.BlockSize]byte // where an IV is set from, which a local would escape
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

// cbc returns mode, one of k's, or a new CBC encrypter of k's, set to the
// IV of msn.
func (k *epochKey) cbc(mode cipher.BlockMode, msn uint64) cipher.BlockMode {
	k.iv = msnIV(msn)
	if s, ok := mode.(ivSetter); ok {
		s.SetIV(k.iv[:])
		return mode
	}
	return cipher.NewCBCEncrypter(k.block, k.iv[:])
}

// pad appends the PKCS#7 padding of a message n bytes long to b.
func pad(b []byte, n int) []byte {
	p := aes.BlockSize - n%aes.BlockSize
	for range p {
		b = append(b, byte(p))
	}
	return b
}

// encJob is what is left to encrypt of a segment: its part i, of the clear
// bytes in data, or, with i < 0, the whole segment, data.
type encJob struct {
	s    *segment
	i    int
	data []byte
}

// enqueue queues j, and has the encryption run once the lock is released
// (unlock) when it is not running. m.mu is held.
func (m *Muxer) enqueue(j encJob) {
	m.jobs = append(m.jobs, j)
	if !m.running {
		m.running, m.kick = true, true
	}
}

// unlock releases m.mu, and starts the encryption of what was cut while it
// was held.
func (m *Muxer) unlock() {
	kick := m.kick
	m.kick = false
	m.mu.Unlock()
	if kick {
		if run := m.enc.Run; run == nil || !run(m.encryptFn) {
			go m.encrypt()
		}
	}
}

// encrypt encrypts the queued segments and parts in order, without the
// lock, publishing each once it is: a part into its segment's ciphertext
// and the playlist, a segment into the playlist. One runs at a time
// (m.running), which owns m.key, m.pencSize and the key's ciphers.
func (m *Muxer) encrypt() {
	m.mu.Lock()
	for len(m.jobs) > 0 && m.encErr == nil {
		j := m.jobs[0]
		copy(m.jobs, m.jobs[1:])
		m.jobs[len(m.jobs)-1] = encJob{}
		m.jobs = m.jobs[:len(m.jobs)-1]
		penc := j.s.penc
		m.mu.Unlock()

		k, err := m.keyOf(j.s.msn)
		var out []byte
		var off int
		if err == nil {
			if j.i >= 0 {
				off, out = len(penc), m.sealPart(k, j.s.msn, penc, j.data)
			} else {
				out = sealSegment(k, j.s.msn, j.data)
			}
		}

		m.mu.Lock()
		if m.encErr != nil {
			break
		}
		if err != nil {
			m.fail(err)
			break
		}
		s := j.s
		s.key = k
		if j.i >= 0 {
			s.penc = out
			s.parts[j.i].eoff, s.parts[j.i].en = off, len(out)-off
			s.pubParts = j.i + 1
		} else {
			// m.cut[0] is s: segments are queued in order.
			s.data = out
			copy(m.cut, m.cut[1:])
			m.cut[len(m.cut)-1] = nil
			m.cut = m.cut[:len(m.cut)-1]
			m.publish(s)
		}
		m.wake()
	}
	m.running = false
	m.mu.Unlock()
}

// fail ends the stream for err: what is cut and not encrypted is dropped,
// and every request answered. m.mu is held.
func (m *Muxer) fail(err error) {
	m.encErr = err
	m.ended = true
	m.open = nil
	clear(m.cut)
	m.cut = m.cut[:0]
	clear(m.jobs)
	m.jobs = m.jobs[:0]
	m.wake()
}

// keyOf returns the key of segment msn, asking Encryption.Key for it when
// its epoch is not the last one's. The running encryption calls it,
// without the lock.
func (m *Muxer) keyOf(msn uint64) (*epochKey, error) {
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
	block, err := newCipher(key[:])
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

// sealSegment returns data, a whole segment msn, encrypted in place with
// its padding, or in a copy when its capacity has no room for the padding
// or more room than it needs: nobody reads a segment before it is
// encrypted.
func sealSegment(k *epochKey, msn uint64, data []byte) []byte {
	p := aes.BlockSize - len(data)%aes.BlockSize
	if c := cap(data) - len(data); c < p || c > len(data)/8+aes.BlockSize {
		data = append(make([]byte, 0, len(data)+p), data...)
	}
	data = pad(data, len(data))
	k.seg = k.cbc(k.seg, msn)
	k.seg.CryptBlocks(data, data)
	return data
}

// sealPart appends clear, a part of segment msn, encrypted on its own with
// the key and IV of its segment, to penc, the ciphertext of the parts
// before it, and returns penc. The bytes of penc already there are not
// written: requests may be reading them.
func (m *Muxer) sealPart(k *epochKey, msn uint64, penc, clear []byte) []byte {
	off := len(penc)
	if penc == nil && m.pencSize > 0 {
		// As long as the last segment's, and a little more: one
		// allocation a segment.
		penc = make([]byte, 0, m.pencSize+m.pencSize/4)
	}
	penc = append(penc, clear...)
	penc = pad(penc, len(clear))
	b := penc[off:]
	k.part = k.cbc(k.part, msn)
	k.part.CryptBlocks(b, b)
	return penc
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
