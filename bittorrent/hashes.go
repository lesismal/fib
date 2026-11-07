package bittorrent

import (
	"bytes"
	"crypto/sha256"
	"math/bits"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// BEP 52's hash requests: a v2 torrent from a magnet link has no piece
// layers, the hashes its pieces are checked against below each file's
// pieces root, and asks peers for them; and this side answers the
// requests of peers from the layers it has.

const (
	// maxHashesPerRequest is how many hashes one request asks for, as
	// libtorrent and anacrolix ask.
	maxHashesPerRequest = 512
	// maxHashesServed bounds the hashes one request may ask of this side.
	maxHashesServed  = 8192
	hashRequestRetry = 10 * time.Second
)

// hashKey is a request for part of a file's piece layer.
type hashKey struct {
	root  [32]byte
	index uint32
}

type hashReq struct {
	pc *peerConn
	at time.Time
}

// layerFetch is a piece layer being put together from hashes messages.
type layerFetch struct {
	hashes [][32]byte
	got    []bool // per request of maxHashesPerRequest
}

func log2(n int64) int { return bits.Len64(uint64(n)) - 1 }

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// pieceLayerIndex is the layer of the merkle trees whose nodes each cover
// a piece, counted from the 16 KiB leaves as BEP 52's base layer is.
func (t *Torrent) pieceLayerIndex() int { return log2(t.info.PieceLength / metainfo.BlockSize) }

// fileByRoot finds the file whose pieces root is root: its index, or -1.
func (t *Torrent) fileByRoot(root [32]byte) int {
	for i, f := range t.info.FileList() {
		if f.PiecesRoot == root && f.Length > 0 {
			return i
		}
	}
	return -1
}

func filePieceCount(f metainfo.File, pl int64) int {
	return int((f.Length + pl - 1) / pl)
}

// requestHashesLocked asks v2 peers for the piece layers a v2-only
// torrent is missing, in requests of up to 512 hashes without proofs, as
// anacrolix asks; the whole layer is checked against the pieces root once
// it is all there.
func (t *Torrent) requestHashesLocked() {
	if t.info == nil || !t.info.HasV2() || t.info.HasV1() || t.checking {
		return
	}
	now := time.Now()
	base := uint32(t.pieceLayerIndex())
	for _, f := range t.info.FileList() {
		if f.IsPad() || f.Length <= t.info.PieceLength {
			continue
		}
		if _, ok := t.layers[f.PiecesRoot]; ok {
			continue
		}
		n := filePieceCount(f, t.info.PieceLength)
		lf := t.layerParts[f.PiecesRoot]
		if lf == nil {
			lf = &layerFetch{hashes: make([][32]byte, n), got: make([]bool, (n+maxHashesPerRequest-1)/maxHashesPerRequest)}
			t.layerParts[f.PiecesRoot] = lf
		}
		for c := range lf.got {
			if lf.got[c] {
				continue
			}
			idx := c * maxHashesPerRequest
			key := hashKey{f.PiecesRoot, uint32(idx)}
			prev, asked := t.hashReqs[key]
			if asked && now.Sub(prev.at) < hashRequestRetry {
				continue
			}
			pc := t.hashPeerLocked(prev.pc)
			if pc == nil {
				return
			}
			length := max(2, nextPow2(min(maxHashesPerRequest, n-idx)))
			err := pc.c.Send(&peer.Message{
				Type: peer.MsgHashRequest, PiecesRoot: f.PiecesRoot,
				BaseLayer: base, Index: uint32(idx), Length: uint32(length),
			})
			if err == nil {
				t.hashReqs[key] = hashReq{pc: pc, at: now}
			}
		}
	}
}

// hashPeerLocked picks a peer of the torrent's v2 side to ask for hashes,
// a seed if there is one, other than not.
func (t *Torrent) hashPeerLocked(not *peerConn) *peerConn {
	var best *peerConn
	for _, pc := range t.peers {
		if pc == not || !pc.c.V2() || pc.closed {
			continue
		}
		if best == nil || (pc.seed && !best.seed) {
			best = pc
		}
	}
	if best == nil && not != nil && !not.closed {
		best = not
	}
	return best
}

func (t *Torrent) onHashesLocked(pc *peerConn, m *peer.Message) {
	key := hashKey{m.PiecesRoot, m.Index}
	req, ok := t.hashReqs[key]
	if !ok || req.pc != pc || t.info == nil {
		return
	}
	delete(t.hashReqs, key)
	lf := t.layerParts[m.PiecesRoot]
	if lf == nil || int(m.Length)*32 > len(m.Data) || m.Index%maxHashesPerRequest != 0 {
		return
	}
	for k := 0; k < int(m.Length); k++ {
		i := int(m.Index) + k
		if i < len(lf.hashes) {
			copy(lf.hashes[i][:], m.Data[k*32:])
		}
	}
	lf.got[m.Index/maxHashesPerRequest] = true
	for _, g := range lf.got {
		if !g {
			return
		}
	}
	layer := make([]byte, 0, 32*len(lf.hashes))
	for _, h := range lf.hashes {
		layer = append(layer, h[:]...)
	}
	root, err := metainfo.LayerRoot(layer, t.info.PieceLength)
	if err != nil || root != m.PiecesRoot {
		// Wrong hashes: ask again, from another peer.
		clear(lf.got)
		return
	}
	delete(t.layerParts, m.PiecesRoot)
	t.setLayerLocked(m.PiecesRoot, layer)
}

// setLayerLocked keeps a file's verified piece layer, and checks the
// pieces that waited for it.
func (t *Torrent) setLayerLocked(root [32]byte, layer []byte) {
	if t.layers == nil {
		t.layers = metainfo.PieceLayers{}
	}
	t.layers[root] = layer
	for i, p := range t.partials {
		if p.waitHashes {
			p.waitHashes = false
			if p.complete() && p.writing == 0 && !p.busy {
				p.busy = true
				go t.verifyPiece(i, p)
			}
		}
	}
}

func (t *Torrent) onHashRejectLocked(pc *peerConn, m *peer.Message) {
	key := hashKey{m.PiecesRoot, m.Index}
	if req, ok := t.hashReqs[key]; ok && req.pc == pc {
		// Ask another peer at once.
		req.at = time.Time{}
		t.hashReqs[key] = req
		t.requestHashesLocked()
	}
}

// onHashRequestLocked answers a peer's hash request from the piece layers
// this side has: hashes of the piece layer only, padded past the file's
// last piece, followed by the uncles that prove them up to proof layers
// above the base, the subtree the hashes span not counted.
func (t *Torrent) onHashRequestLocked(pc *peerConn, m *peer.Message) {
	reply := *m
	reply.Type = peer.MsgHashReject
	reply.Data = nil
	defer func() { _ = pc.c.Send(&reply) }()
	if t.info == nil || !t.info.HasV2() {
		return
	}
	layer, ok := t.layers[m.PiecesRoot]
	if !ok || int(m.BaseLayer) != t.pieceLayerIndex() {
		return
	}
	n := len(layer) / 32
	width := nextPow2(n)
	length, index := int(m.Length), int(m.Index)
	if length < 1 || length > maxHashesServed || length&(length-1) != 0 || index%length != 0 || index+length > width {
		return
	}
	pad := metainfo.PadHash(t.pieceLayerIndex())
	nodes := make([][32]byte, width)
	for i := range nodes {
		if i < n {
			copy(nodes[i][:], layer[i*32:])
		} else {
			nodes[i] = pad
		}
	}
	data := make([]byte, 0, 32*(length+int(m.ProofLayers)))
	for _, h := range nodes[index : index+length] {
		data = append(data, h[:]...)
	}
	// Walk up from the layer of the subtree's root.
	h := log2(int64(length))
	for range h {
		nodes = parentLayer(nodes)
	}
	pos := index / length
	for level := h; level < int(m.ProofLayers) && len(nodes) > 1; level++ {
		sib := nodes[pos^1]
		data = append(data, sib[:]...)
		nodes = parentLayer(nodes)
		pos /= 2
	}
	reply.Type = peer.MsgHashes
	reply.Data = data
}

func parentLayer(nodes [][32]byte) [][32]byte {
	up := make([][32]byte, len(nodes)/2)
	var buf [64]byte
	for i := range up {
		copy(buf[:32], nodes[2*i][:])
		copy(buf[32:], nodes[2*i+1][:])
		up[i] = sha256.Sum256(buf[:])
	}
	return up
}

// computeLayers derives the piece layers of the files a hybrid torrent
// from a magnet link downloaded, from their data, so that this side can
// answer v2 peers' hash requests as a seed: each piece's subtree root,
// checked against the file's pieces root.
func (t *Torrent) computeLayers() {
	t.mu.Lock()
	info, st := t.info, t.storage
	var todo []int
	if info != nil && info.HasV2() && st != nil {
		for f, file := range info.FileList() {
			if file.IsPad() || file.Length <= info.PieceLength {
				continue
			}
			if _, ok := t.layers[file.PiecesRoot]; !ok {
				todo = append(todo, f)
			}
		}
	}
	t.mu.Unlock()
	if len(todo) == 0 {
		return
	}
	width := int(info.PieceLength / metainfo.BlockSize)
	buf := make([]byte, info.PieceLength)
	for _, f := range todo {
		file := info.FileList()[f]
		n := filePieceCount(file, info.PieceLength)
		layer := make([]byte, 0, 32*n)
		ok := true
		for k := range n {
			off := file.Offset + int64(k)*info.PieceLength
			data := buf[:min(info.PieceLength, file.Offset+file.Length-off)]
			if err := readData(st, info, off, data); err != nil {
				ok = false
				break
			}
			r := metainfo.MerkleRoot(metainfo.HashBlocks(data), width, [32]byte{})
			layer = append(layer, r[:]...)
		}
		if !ok {
			continue
		}
		if root, err := metainfo.LayerRoot(layer, info.PieceLength); err == nil && bytes.Equal(root[:], file.PiecesRoot[:]) {
			t.mu.Lock()
			if t.layers == nil {
				t.layers = metainfo.PieceLayers{}
			}
			t.layers[file.PiecesRoot] = layer
			t.mu.Unlock()
		}
	}
}
