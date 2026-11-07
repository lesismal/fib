package peer

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

// MetadataPieceSize is the size of every piece of the info dictionary
// ut_metadata sends, but the last (BEP 9).
const MetadataPieceSize = 16 << 10

// DefaultMaxMetadataSize bounds the info dictionary a MetadataAssembler
// takes when its max is zero: a metadata_size is the peer's word, and a
// lie should not cost more than this.
const DefaultMaxMetadataSize = 16 << 20

// MetadataMessageType is the msg_type of a ut_metadata message.
type MetadataMessageType int

// The ut_metadata messages.
const (
	MetadataRequest MetadataMessageType = 0
	MetadataData    MetadataMessageType = 1
	MetadataReject  MetadataMessageType = 2
)

// MetadataMessage is a ut_metadata message: a request for a piece of the
// info dictionary, the piece, or a refusal.
type MetadataMessage struct {
	Type  MetadataMessageType `bencode:"msg_type"`
	Piece int                 `bencode:"piece"`
	// TotalSize is the size of the info dictionary, in a data message.
	TotalSize int `bencode:"total_size,omitempty"`
	// Data is the piece, in a data message, which follows the dictionary.
	Data []byte `bencode:"-"`
}

// ParseMetadataMessage parses a ut_metadata message's payload. Data
// aliases b.
func ParseMetadataMessage(b []byte) (*MetadataMessage, error) {
	d := bencode.NewBytesDecoder(b)
	d.NoCopy()
	m := new(MetadataMessage)
	if err := d.Decode(m); err != nil {
		return nil, err
	}
	if m.Type < MetadataRequest || m.Type > MetadataReject || m.Piece < 0 {
		return nil, fmt.Errorf("peer: ut_metadata message type %d piece %d", m.Type, m.Piece)
	}
	if m.Type == MetadataData {
		m.Data = b[d.InputOffset():]
	} else if d.InputOffset() != int64(len(b)) {
		return nil, errors.New("peer: data after a ut_metadata request or reject")
	}
	return m, nil
}

// AppendMetadataMessage appends m's payload to dst: the dictionary, then
// Data for a data message.
func AppendMetadataMessage(dst []byte, m *MetadataMessage) []byte {
	// The struct of ints always encodes.
	dst, _ = bencode.Append(dst, m)
	if m.Type == MetadataData {
		dst = append(dst, m.Data...)
	}
	return dst
}

// MetadataPieces is the number of pieces an info dictionary of size bytes
// is sent in.
func MetadataPieces(size int) int {
	return (size + MetadataPieceSize - 1) / MetadataPieceSize
}

// MetadataPiece is piece i of info, the info dictionary's bytes, as a data
// message carries it; false when there is no piece i.
func MetadataPiece(info []byte, i int) ([]byte, bool) {
	if i < 0 || i >= MetadataPieces(len(info)) {
		return nil, false
	}
	start := i * MetadataPieceSize
	return info[start:min(start+MetadataPieceSize, len(info))], true
}

// Errors of a MetadataAssembler.
var (
	// ErrMetadataSize is a metadata_size or total_size out of bounds, or
	// another than the one the assembly began with.
	ErrMetadataSize = errors.New("peer: bad metadata size")
	// ErrMetadataPiece is a piece of the wrong number or length.
	ErrMetadataPiece = errors.New("peer: bad metadata piece")
	// ErrMetadataHash is an info dictionary whose hash is not the info
	// hash: a peer sent a wrong piece.
	ErrMetadataHash = errors.New("peer: metadata does not match the info hash")
)

// MetadataAssembler puts together the info dictionary from the pieces of
// ut_metadata data messages, possibly from several peers, and checks it
// against the info hash.
type MetadataAssembler struct {
	data  []byte
	have  []bool
	count int
}

// NewMetadataAssembler returns an assembler of an info dictionary of size
// bytes, as an extended handshake's metadata_size says; size must be
// positive and at most max, DefaultMaxMetadataSize when max is zero.
func NewMetadataAssembler(size, max int) (*MetadataAssembler, error) {
	if max <= 0 {
		max = DefaultMaxMetadataSize
	}
	if size <= 0 || size > max {
		return nil, ErrMetadataSize
	}
	return &MetadataAssembler{data: make([]byte, size), have: make([]bool, MetadataPieces(size))}, nil
}

// Size is the info dictionary's size.
func (a *MetadataAssembler) Size() int { return len(a.data) }

// Pieces is the number of pieces.
func (a *MetadataAssembler) Pieces() int { return len(a.have) }

// Have reports piece i.
func (a *MetadataAssembler) Have(i int) bool { return i >= 0 && i < len(a.have) && a.have[i] }

// Missing appends the pieces not had yet to dst.
func (a *MetadataAssembler) Missing(dst []int) []int {
	for i, ok := range a.have {
		if !ok {
			dst = append(dst, i)
		}
	}
	return dst
}

// Complete reports every piece had.
func (a *MetadataAssembler) Complete() bool { return a.count == len(a.have) }

// Add takes m, a data message: its piece must be in range with the right
// length, and its total_size, when given, the assembler's size.
func (a *MetadataAssembler) Add(m *MetadataMessage) error {
	if m.Type != MetadataData {
		return ErrMetadataPiece
	}
	if m.TotalSize != 0 && m.TotalSize != len(a.data) {
		return ErrMetadataSize
	}
	want, ok := MetadataPiece(a.data, m.Piece)
	if !ok || len(m.Data) != len(want) {
		return ErrMetadataPiece
	}
	copy(want, m.Data)
	if !a.have[m.Piece] {
		a.have[m.Piece] = true
		a.count++
	}
	return nil
}

// Reset forgets every piece, to fetch them again.
func (a *MetadataAssembler) Reset() {
	clear(a.have)
	a.count = 0
}

// Info checks the complete info dictionary against v1, its SHA-1, and v2,
// its SHA-256, whichever are not zero, and parses it. On a mismatch it
// resets, so that the pieces are fetched again, and reports
// ErrMetadataHash.
func (a *MetadataAssembler) Info(v1 metainfo.InfoHash, v2 metainfo.InfoHashV2) (*metainfo.Info, error) {
	if !a.Complete() {
		return nil, errors.New("peer: metadata incomplete")
	}
	if v1.IsZero() && v2.IsZero() {
		return nil, errors.New("peer: no info hash to check metadata against")
	}
	if !v1.IsZero() {
		if h := sha1.Sum(a.data); !bytes.Equal(h[:], v1[:]) {
			a.Reset()
			return nil, ErrMetadataHash
		}
	}
	if !v2.IsZero() {
		if h := sha256.Sum256(a.data); !bytes.Equal(h[:], v2[:]) {
			a.Reset()
			return nil, ErrMetadataHash
		}
	}
	return metainfo.ParseInfo(a.data)
}
