package metainfo

import (
	"crypto/sha256"
	"errors"
	"math/bits"
)

// BlockSize is the size of the leaves of a v2 file's merkle tree (BEP 52),
// and of the blocks peers request.
const BlockSize = 16 << 10

func hashPair(a, b [32]byte) [32]byte {
	var buf [64]byte
	copy(buf[:32], a[:])
	copy(buf[32:], b[:])
	return sha256.Sum256(buf[:])
}

var padHashes = func() (p [64][32]byte) {
	for i := 1; i < len(p); i++ {
		p[i] = hashPair(p[i-1], p[i-1])
	}
	return p
}()

// PadHash returns the root of a subtree of 2^height leaves that are all
// zero hashes, which is what pads a v2 merkle tree beyond the end of a
// file: height 0 is the zero leaf itself, and PadHash(log2(piece
// length/BlockSize)) pads a piece layer.
func PadHash(height int) [32]byte {
	if height < 0 || height >= len(padHashes) {
		return [32]byte{}
	}
	return padHashes[height]
}

// nextPow2 returns the smallest power of two at least n (1 for n <= 1).
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

func log2(n int64) int {
	return bits.Len64(uint64(n)) - 1
}

// MerkleRoot returns the root of the binary SHA-256 tree whose bottom
// layer is nodes padded on the right to width nodes, each pad being pad;
// pads higher up are the hashes of the pads below. Width is rounded up to
// a power of two at least len(nodes). With leaf hashes and a zero pad it
// is a file's pieces root; with a piece layer and PadHash of a piece's
// height it is the same root from the piece layer.
func MerkleRoot(nodes [][32]byte, width int, pad [32]byte) [32]byte {
	if len(nodes) == 0 {
		return merkleRootInPlace(nil, width, pad)
	}
	return merkleRootInPlace(append([][32]byte(nil), nodes...), width, pad)
}

// merkleRootInPlace is MerkleRoot overwriting nodes as it goes.
func merkleRootInPlace(layer [][32]byte, width int, pad [32]byte) [32]byte {
	width = max(nextPow2(width), nextPow2(len(layer)))
	if len(layer) == 0 {
		for ; width > 1; width /= 2 {
			pad = hashPair(pad, pad)
		}
		return pad
	}
	for ; width > 1; width /= 2 {
		n := (len(layer) + 1) / 2
		for i := 0; i < n; i++ {
			r := pad
			if 2*i+1 < len(layer) {
				r = layer[2*i+1]
			}
			layer[i] = hashPair(layer[2*i], r)
		}
		layer = layer[:n]
		pad = hashPair(pad, pad)
	}
	return layer[0]
}

// fileHasher makes a v2 file's pieces root and piece layer from its leaf
// hashes as they come. It keeps the leaves of the piece being hashed and
// the roots of the pieces before it, folding each piece's leaves into its
// root as the next piece starts: 32 bytes per piece, not per 16 KiB leaf.
type fileHasher struct {
	perPiece int        // leaves per piece
	leaves   [][32]byte // the current piece's leaves, at most perPiece
	pieces   [][32]byte // the roots of the pieces before it
}

func newFileHasher(pieceLength int64) *fileHasher {
	n := int(pieceLength / BlockSize)
	return &fileHasher{perPiece: n, leaves: make([][32]byte, 0, n)}
}

// add adds the next leaf hash.
func (h *fileHasher) add(leaf [32]byte) {
	if len(h.leaves) == h.perPiece {
		h.pieces = append(h.pieces, merkleRootInPlace(h.leaves, h.perPiece, [32]byte{}))
		h.leaves = h.leaves[:0]
	}
	h.leaves = append(h.leaves, leaf)
}

// finish returns the pieces root of the leaves added since the last
// finish, and their piece layer if they span more than one piece, and
// readies the hasher for the next file. A file of one piece or less is the
// root of its leaves padded to a power of two; a larger one the root of its
// piece layer padded with PadHash of a piece's height.
func (h *fileHasher) finish() (root [32]byte, layer []byte) {
	defer func() {
		h.leaves = h.leaves[:0]
		h.pieces = h.pieces[:0]
	}()
	if len(h.pieces) == 0 {
		return merkleRootInPlace(h.leaves, len(h.leaves), [32]byte{}), nil
	}
	h.pieces = append(h.pieces, merkleRootInPlace(h.leaves, h.perPiece, [32]byte{}))
	layer = make([]byte, 0, len(h.pieces)*32)
	for i := range h.pieces {
		layer = append(layer, h.pieces[i][:]...)
	}
	return merkleRootInPlace(h.pieces, len(h.pieces), PadHash(log2(int64(h.perPiece)))), layer
}

// HashBlocks returns the SHA-256 of each BlockSize block of data, the last
// one possibly shorter: the leaves of a v2 merkle tree.
func HashBlocks(data []byte) [][32]byte {
	hashes := make([][32]byte, 0, (len(data)+BlockSize-1)/BlockSize)
	for len(data) > 0 {
		n := min(len(data), BlockSize)
		hashes = append(hashes, sha256.Sum256(data[:n]))
		data = data[n:]
	}
	return hashes
}

// LayerHashes splits a piece layer, concatenated 32-byte hashes as the
// "piece layers" dictionary holds them, into hashes.
func LayerHashes(layer []byte) ([][32]byte, error) {
	if len(layer)%32 != 0 {
		return nil, errors.New("metainfo: piece layer is not a whole number of hashes")
	}
	hashes := make([][32]byte, len(layer)/32)
	for i := range hashes {
		copy(hashes[i][:], layer[i*32:])
	}
	return hashes, nil
}

// LayerRoot returns the pieces root a file's piece layer hashes to, for
// the given piece length (a power of two at least BlockSize).
func LayerRoot(layer []byte, pieceLength int64) ([32]byte, error) {
	hashes, err := LayerHashes(layer)
	if err != nil {
		return [32]byte{}, err
	}
	return MerkleRoot(hashes, len(hashes), PadHash(log2(pieceLength/BlockSize))), nil
}

// VerifyProof reports whether nodes, a run of len(nodes) (a power of two)
// nodes of one layer of a merkle tree starting at index (a multiple of
// len(nodes)), combine with uncles, the sibling hashes from the bottom up,
// into want: the root, or the node the last uncle reaches. This checks the
// hashes of a BEP 52 "hashes" message against a root or a layer already
// verified.
func VerifyProof(want [32]byte, index int, nodes, uncles [][32]byte) bool {
	n := len(nodes)
	if n == 0 || n&(n-1) != 0 || index < 0 || index%n != 0 {
		return false
	}
	cur := MerkleRoot(nodes, n, [32]byte{})
	pos := index / n
	for _, u := range uncles {
		if pos&1 == 0 {
			cur = hashPair(cur, u)
		} else {
			cur = hashPair(u, cur)
		}
		pos >>= 1
	}
	return cur == want
}
