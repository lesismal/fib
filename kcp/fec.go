package kcp

import "encoding/binary"

// github.com/xtaci/kcp-go's forward error correction: every packet goes
// out as a data shard, after a header of a sequence number and a type and
// the 2-byte size of what follows (the size included), and every
// DataShards of them are followed by ParityShards parity shards of their
// Reed-Solomon code, so that a receiver missing up to ParityShards of a
// set recovers them without waiting for a retransmission.
const (
	fecHeaderSize      = 6 // seqid(4) type(2)
	fecHeaderSizePlus2 = fecHeaderSize + 2
	typeData           = 0xf1
	typeParity         = 0xf2
	typeOOB            = 0xf3 // kcp-go's unreliable packets, which a KCP session drops
	// maxShardSets is how many sets behind the newest one a set may be
	// and still be completed.
	maxShardSets = 3
	// fecEncodeLatency is how long, in ms, the shards of a set may take
	// coming and still get parity: parity of a set that took longer
	// would repair it too late to be worth sending. kcp-go's
	// maxFECEncodeLatency.
	fecEncodeLatency = 500
)

type fecEncoder struct {
	rs      *rsCodec
	total   int
	paws    uint32 // seqids wrap here, a multiple of total
	next    uint32
	count   int
	maxSize int
	shards  [][]byte
	lastMs  int64
	started bool
}

func newFECEncoder(data, parity int) (*fecEncoder, error) {
	rs, err := newRS(data, parity)
	if err != nil {
		return nil, err
	}
	total := data + parity
	return &fecEncoder{
		rs:     rs,
		total:  total,
		paws:   0xffffffff / uint32(total) * uint32(total),
		shards: make([][]byte, total),
	}, nil
}

// add stamps the data shard hdr (the FEC header of a packet, its size
// field and the packet after it) with its seqid, and returns the parity
// shards, each the room of a header and then the parity of the sizes and
// packets, once hdr completes a set. The parity is the encoder's until the
// next call.
func (e *fecEncoder) add(hdr []byte, nowMs int64) [][]byte {
	binary.LittleEndian.PutUint32(hdr, e.next)
	binary.LittleEndian.PutUint16(hdr[4:], typeData)
	binary.LittleEndian.PutUint16(hdr[fecHeaderSize:], uint16(len(hdr)-fecHeaderSize))
	e.next = (e.next + 1) % e.paws
	payload := hdr[fecHeaderSize:]
	s := e.shards[e.count]
	s = append(s[:0], payload...)
	e.shards[e.count] = s
	e.count++
	e.maxSize = max(e.maxSize, len(payload))
	last := e.lastMs
	e.lastMs = nowMs
	if e.count < e.rs.data {
		return nil
	}
	size := e.maxSize
	e.count, e.maxSize = 0, 0
	if e.started && nowMs-last >= fecEncodeLatency {
		e.next = (e.next + uint32(e.rs.parity)) % e.paws
		return nil
	}
	e.started = true
	shards := make([][]byte, e.total)
	for i := range e.rs.data {
		s := e.shards[i]
		if cap(s) < size {
			s = append(s[:cap(s)], make([]byte, size-cap(s))...)
		}
		s = s[:size]
		clear(s[len(e.shards[i]):])
		e.shards[i] = s
		shards[i] = s
	}
	out := make([][]byte, e.rs.parity)
	for p := range e.rs.parity {
		i := e.rs.data + p
		b := e.shards[i]
		if cap(b) < fecHeaderSize+size {
			b = make([]byte, fecHeaderSize+size, fecHeaderSize+size+64)
		}
		b = b[:fecHeaderSize+size]
		e.shards[i] = b
		shards[i] = b[fecHeaderSize:]
		binary.LittleEndian.PutUint32(b, e.next)
		binary.LittleEndian.PutUint16(b[4:], typeParity)
		e.next = (e.next + 1) % e.paws
		out[p] = b
	}
	e.rs.encode(shards)
	return out
}

type fecSet struct {
	shards [][]byte
	have   int
	done   bool
}

type fecDecoder struct {
	rs     *rsCodec
	total  int
	paws   uint32
	sets   map[uint32]*fecSet
	newest uint32
}

func newFECDecoder(data, parity int) (*fecDecoder, error) {
	rs, err := newRS(data, parity)
	if err != nil {
		return nil, err
	}
	total := data + parity
	return &fecDecoder{
		rs:    rs,
		total: total,
		paws:  0xffffffff / uint32(total) * uint32(total),
		sets:  map[uint32]*fecSet{},
	}, nil
}

// decode takes a shard, its FEC header first, and returns the payloads of
// the data shards it lets the decoder recover: each a size field and the
// packet.
func (d *fecDecoder) decode(b []byte) [][]byte {
	seqid := binary.LittleEndian.Uint32(b)
	isData := binary.LittleEndian.Uint16(b[4:]) == typeData
	if seqid >= d.paws {
		return nil
	}
	idx := int(seqid % uint32(d.total))
	if (idx < d.rs.data) != isData {
		return nil // the peer's shards are not ours
	}
	id := seqid / uint32(d.total)
	set := d.sets[id]
	if set == nil {
		set = &fecSet{shards: make([][]byte, d.total)}
		d.sets[id] = set
		if len(d.sets) == 1 || timediff(id*uint32(d.total), d.newest*uint32(d.total)) > 0 {
			d.newest = id
		}
		d.discard()
		if d.sets[id] == nil {
			return nil
		}
	}
	if set.done || set.shards[idx] != nil {
		return nil
	}
	set.shards[idx] = append([]byte(nil), b[fecHeaderSize:]...)
	set.have++
	if set.have < d.rs.data {
		return nil
	}
	set.done = true
	shards := set.shards
	set.shards = nil
	size, missing := 0, false
	for i, s := range shards {
		size = max(size, len(s))
		missing = missing || (i < d.rs.data && s == nil)
	}
	if !missing {
		return nil
	}
	for i, s := range shards {
		if s != nil && len(s) < size {
			shards[i] = append(s, make([]byte, size-len(s))...)
		}
	}
	lost := make([]bool, d.rs.data)
	for i := range lost {
		lost[i] = shards[i] == nil
	}
	if d.rs.reconstructData(shards, size) != nil {
		return nil
	}
	var out [][]byte
	for i, l := range lost {
		if l {
			out = append(out, shards[i])
		}
	}
	return out
}

// discard forgets the sets too far behind the newest.
func (d *fecDecoder) discard() {
	t := uint32(d.total)
	for id := range d.sets {
		if timediff(d.newest*t, id*t) > maxShardSets*int32(t) {
			delete(d.sets, id)
		}
	}
}
