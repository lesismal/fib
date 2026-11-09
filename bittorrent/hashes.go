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
	// maxHashAnswers bounds a peer's hash requests being answered at
	// once.
	maxHashAnswers = 4
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
	// checking is set while the assembled layer is checked against the
	// file's root on the disk pool.
	checking bool
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
	if lf.checking {
		return
	}
	layer := make([]byte, 0, 32*len(lf.hashes))
	for _, h := range lf.hashes {
		layer = append(layer, h[:]...)
	}
	// The layer's root, a hash of every hash in it, is worked out on the
	// disk pool.
	lf.checking = true
	want, pieceLength := m.PiecesRoot, t.info.PieceLength
	t.c.disk.run(func() {
		root, err := metainfo.LayerRoot(layer, pieceLength)
		t.mu.Lock()
		defer t.mu.Unlock()
		lf.checking = false
		if t.layerParts[want] != lf {
			return
		}
		if err != nil || root != want {
			// Wrong hashes: ask again, from another peer.
			clear(lf.got)
			return
		}
		delete(t.layerParts, want)
		t.setLayerLocked(want, layer)
	})
}

// setLayerLocked keeps a file's verified piece layer, and checks the
// pieces that waited for it.
func (t *Torrent) setLayerLocked(root [32]byte, layer []byte) {
	if t.layers == nil {
		t.layers = metainfo.PieceLayers{}
	}
	t.layers[root] = layer
	t.kickWaitingLocked()
}

// kickWaitingLocked checks the pieces that waited for a piece layer now
// known.
func (t *Torrent) kickWaitingLocked() {
	for i, p := range t.partials {
		if !p.waitHashes || t.pieceLayersFor(i) == nil {
			continue
		}
		p.waitHashes = false
		if p.complete() && p.writing == 0 && !p.busy {
			p.busy = true
			t.c.disk.run(func() { t.verifyPiece(i, p) })
		}
	}
}

func (t *Torrent) onHashRejectLocked(pc *peerConn, m *peer.Message) {
	key := hashKey{m.PiecesRoot, m.Index}
	if req, ok := t.hashReqs[key]; ok && req.pc == pc {
		if o := t.hashPeerLocked(pc); o != nil && o != pc {
			// Ask another peer at once; the same one only after
			// hashRequestRetry, never in a loop of rejects.
			req.at = time.Time{}
			t.hashReqs[key] = req
			t.requestHashesLocked()
		}
	}
}

// onHashRequestLocked answers a peer's hash request from the piece layers
// this side has: hashes of the piece layer only, padded past the file's
// last piece, followed by the uncles that prove them up to proof layers
// above the base, the subtree the hashes span not counted. The proof,
// which hashes the whole layer, is made on the disk pool, at most
// maxHashAnswers at a time for a peer; past them the request is rejected.
func (t *Torrent) onHashRequestLocked(pc *peerConn, m *peer.Message) {
	reply := *m
	reply.Type = peer.MsgHashReject
	reply.Data = nil
	if t.info == nil || !t.info.HasV2() || pc.hashing >= maxHashAnswers {
		_ = pc.c.Send(&reply)
		return
	}
	// A layer, once kept, is never written to again.
	layer, ok := t.layers[m.PiecesRoot]
	base := t.pieceLayerIndex()
	length, index := int(m.Length), int(m.Index)
	if !ok || int(m.BaseLayer) != base || length < 1 || length > maxHashesServed || length&(length-1) != 0 ||
		index%length != 0 || index+length > nextPow2(len(layer)/32) {
		_ = pc.c.Send(&reply)
		if !ok {
			// A file had whole whose layer is not known yet (a hybrid
			// from a magnet link, resumed): it is made for the next ask.
			t.computeLayersLocked(true)
		}
		return
	}
	// m is the parser's, reused for the next message: the job has reply.
	pc.hashing++
	t.c.disk.run(func() {
		reply.Type = peer.MsgHashes
		reply.Data = hashProof(layer, base, index, length, int(reply.ProofLayers))
		_ = pc.c.Send(&reply)
		t.mu.Lock()
		pc.hashing--
		t.mu.Unlock()
	})
}

// hashProof is the answer to a hash request of the piece layer layer,
// whose index in the file's tree is base: length hashes from index, then
// the uncles up to proofLayers above the base.
func hashProof(layer []byte, base, index, length, proofLayers int) []byte {
	n := len(layer) / 32
	width := nextPow2(n)
	pad := metainfo.PadHash(base)
	nodes := make([][32]byte, width)
	for i := range nodes {
		if i < n {
			copy(nodes[i][:], layer[i*32:])
		} else {
			nodes[i] = pad
		}
	}
	data := make([]byte, 0, 32*(length+proofLayers))
	for _, h := range nodes[index : index+length] {
		data = append(data, h[:]...)
	}
	// Walk up from the layer of the subtree's root.
	h := log2(int64(length))
	for range h {
		nodes = parentLayer(nodes)
	}
	pos := index / length
	for level := h; level < proofLayers && len(nodes) > 1; level++ {
		sib := nodes[pos^1]
		data = append(data, sib[:]...)
		nodes = parentLayer(nodes)
		pos /= 2
	}
	return data
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

// layerBatchBytes bounds what one job of computeLayers reads before it
// goes to the back of the disk queue, as uploadBatch bounds an upload's.
const layerBatchBytes = 4 << 20

// layerJob is computeLayers' progress: the files whose layers it makes,
// the one it is at, and the piece of it.
type layerJob struct {
	files []int
	f, k  int
	layer []byte
	buf   []byte
}

// computeLayersLocked starts computeLayers on the disk pool, unless it
// runs already, when a v2 torrent lacks the piece layer of a file it has
// every piece of, checked: a hybrid torrent from a magnet link, whose
// info dictionary does not hold them, once downloaded or found complete
// by its check. Pieces all taken from resume data, which were not read,
// are not read for it either, unless onDemand: a peer asked for hashes
// of the layer.
func (t *Torrent) computeLayersLocked(onDemand bool) {
	if (!onDemand && !t.hashedData) || t.layering || t.storage == nil || t.checking || t.info == nil || !t.info.HasV2() {
		return
	}
	var todo []int
	for f, file := range t.info.FileList() {
		if file.IsPad() || file.Length <= t.info.PieceLength {
			continue
		}
		if _, ok := t.layers[file.PiecesRoot]; ok {
			continue
		}
		b, e := t.info.FilePieces(f)
		all := true
		for i := b; i < e && all; i++ {
			all = t.pieces[i].have && !t.pieces[i].trusted
		}
		if all {
			todo = append(todo, f)
		}
	}
	if len(todo) == 0 {
		return
	}
	t.layering = true
	j := &layerJob{files: todo, buf: make([]byte, t.info.PieceLength)}
	t.c.disk.run(func() { t.computeLayers(j) })
}

// computeLayers derives the piece layers of the files of j from their
// data, so that this side can answer v2 peers' hash requests (BEP 52) as
// a seed: each piece's subtree root, the layer checked against the file's
// pieces root. It reads up to layerBatchBytes, then queues itself again;
// it stops when the storage is closed, to start again with Start.
func (t *Torrent) computeLayers(j *layerJob) {
	st, info, done := t.useStorage()
	if st == nil {
		done()
		t.mu.Lock()
		t.layering = false
		t.mu.Unlock()
		return
	}
	width := int(info.PieceLength / metainfo.BlockSize)
	var got map[[32]byte][]byte
	read := 0
	for j.f < len(j.files) && read < layerBatchBytes {
		file := info.FileList()[j.files[j.f]]
		n := filePieceCount(file, info.PieceLength)
		off := file.Offset + int64(j.k)*info.PieceLength
		data := j.buf[:min(info.PieceLength, file.Offset+file.Length-off)]
		read += len(data)
		if err := readData(st, info, off, data); err != nil {
			j.f, j.k, j.layer = j.f+1, 0, nil
			continue
		}
		r := metainfo.MerkleRoot(metainfo.HashBlocks(data), width, [32]byte{})
		j.layer = append(j.layer, r[:]...)
		if j.k++; j.k < n {
			continue
		}
		if root, err := metainfo.LayerRoot(j.layer, info.PieceLength); err == nil && bytes.Equal(root[:], file.PiecesRoot[:]) {
			if got == nil {
				got = map[[32]byte][]byte{}
			}
			got[file.PiecesRoot] = j.layer
		}
		j.f, j.k, j.layer = j.f+1, 0, nil
	}
	done()
	t.mu.Lock()
	defer t.mu.Unlock()
	for root, layer := range got {
		t.setLayerLocked(root, layer)
	}
	if j.f < len(j.files) {
		t.c.disk.run(func() { t.computeLayers(j) })
		return
	}
	t.layering = false
}
