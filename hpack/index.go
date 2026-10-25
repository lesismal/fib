package hpack

import "hash/maphash"

// MaxEncoderTableSize is the largest dynamic table an Encoder uses, whatever
// SetTableSizeLimit and the peer allow: an entry counts at least 32 bytes
// against the table, so it holds fewer entries than its index tells apart.
const MaxEncoderTableSize = 1 << 20

// A slot of a fieldIndex is empty when zero. Otherwise its top bit is set, the
// next fifteen bits are the top of the key's hash, to skip most entries with
// another key without comparing strings, and the low sixteen bits are the
// number of entries added before this one, modulo 65536, which tells the
// table's entries apart since it holds fewer than that.
const (
	slotUsed = 0x80000000
	slotTag  = 0xffff0000
)

// fieldIndex finds the newest entry of an encoder's dynamic table with a name
// and value, and the newest with a name, without searching the table. It is
// two hash tables with linear probing, one per key, holding a slot for each
// distinct key in the table. Each has a power of two slots, at least twice
// the entries the table can hold at the size it was made for, so that a probe
// soon meets an empty slot; a table allowed to grow past that is indexed
// afresh.
type fieldIndex struct {
	pairs, names []uint32
	mask         uint
}

// newFieldIndex returns an index for a table of up to maxSize bytes.
func newFieldIndex(maxSize int) *fieldIndex {
	n := 32
	for n < 2*(maxSize/32) {
		n *= 2
	}
	slots := make([]uint32, 2*n)
	return &fieldIndex{pairs: slots[:n:n], names: slots[n:], mask: uint(n - 1)}
}

// fits reports whether the index has room for a table of maxSize bytes.
func (x *fieldIndex) fits(maxSize int) bool { return len(x.pairs) >= 2*(maxSize/32) }

var indexSeed = maphash.MakeSeed()

func hashName(name string) uint64 { return maphash.String(indexSeed, name) }

// hashPair hashes a name and value from the name's hash.
func hashPair(nameHash uint64, value string) uint64 {
	return nameHash ^ maphash.String(indexSeed, value)*0x9e3779b97f4a7c15
}

func slotOf(h uint64, id uint16) uint32 {
	return slotUsed | uint32(h>>49)<<16 | uint32(id)
}

// slotPos is the position in entries of the entry a slot refers to.
func (t *dynamicTable) slotPos(s uint32) int { return int(uint16(s) - t.first) }

// find returns the position in entries of the newest entry with the key h
// hashes, the name alone or with value when pair is set, or -1.
func (t *dynamicTable) find(slots []uint32, h uint64, name, value string, pair bool) int {
	tag, mask := slotOf(h, 0), t.index.mask
	for i := uint(h); ; i++ {
		s := slots[i&mask]
		if s == 0 {
			return -1
		}
		if s&slotTag != tag {
			continue
		}
		p := t.slotPos(s)
		if f := t.entries[p]; f.Name == name && (!pair || f.Value == value) {
			return p
		}
	}
}

// link points the key h hashes at the entry at position p, the newest, in
// place of an older entry with the key if there is one.
func (t *dynamicTable) link(slots []uint32, h uint64, p int, pair bool) {
	tag, mask := slotOf(h, 0), t.index.mask
	f := t.entries[p]
	for i := uint(h); ; i++ {
		s := &slots[i&mask]
		if *s != 0 {
			if *s&slotTag != tag {
				continue
			}
			if g := t.entries[t.slotPos(*s)]; g.Name != f.Name || (pair && g.Value != f.Value) {
				continue
			}
		}
		*s = slotOf(h, t.first+uint16(p))
		return
	}
}

// unlink drops the key h hashes if it still points at the entry at position
// p, which is leaving the table. A newer entry with the same key has taken
// the slot over otherwise. The slots after the one emptied move back into it
// as linear probing requires, which takes their keys' hashes again.
func (t *dynamicTable) unlink(slots []uint32, h uint64, p int, pair bool) {
	mask := t.index.mask
	hole := uint(h) & mask
	for ; ; hole = (hole + 1) & mask {
		s := slots[hole]
		if s == 0 {
			return
		}
		if t.slotPos(s) == p {
			break
		}
	}
	for j := (hole + 1) & mask; slots[j] != 0; j = (j + 1) & mask {
		f := t.entries[t.slotPos(slots[j])]
		home := hashName(f.Name)
		if pair {
			home = hashPair(home, f.Value)
		}
		if (j-uint(home))&mask >= (j-hole)&mask {
			slots[hole] = slots[j]
			hole = j
		}
	}
	slots[hole] = 0
}

// indexFrom is the number of entries past which the encoder looks fields up
// in the index rather than searching the table, which is quicker than
// hashing while the table is small.
const indexFrom = 16

// addIndexed adds f to an encoder's table and to its index. hashed says
// whether the name and pair hashes are given; they are taken here when the
// index needs them. The index is made when the table first grows past
// indexFrom entries, so an encoder whose table stays small goes without, and
// made again when the table has been allowed to outgrow it.
func (t *dynamicTable) addIndexed(f HeaderField, nameHash, pairHash uint64, hashed bool) {
	t.size += f.size()
	t.entries = append(t.entries, f)
	switch {
	case t.index != nil:
		if !hashed {
			nameHash = hashName(f.Name)
			pairHash = hashPair(nameHash, f.Value)
		}
		p := len(t.entries) - 1
		t.link(t.index.names, nameHash, p, false)
		t.link(t.index.pairs, pairHash, p, true)
	case len(t.entries) > indexFrom:
		t.index = newFieldIndex(t.maxSize)
		for p, g := range t.entries {
			h := hashName(g.Name)
			t.link(t.index.names, h, p, false)
			t.link(t.index.pairs, hashPair(h, g.Value), p, true)
		}
	}
	t.evict()
}

// unindex drops the oldest n entries, about to be evicted, from the index.
func (t *dynamicTable) unindex(n int) {
	for p := range n {
		f := t.entries[p]
		h := hashName(f.Name)
		t.unlink(t.index.names, h, p, false)
		t.unlink(t.index.pairs, hashPair(h, f.Value), p, true)
	}
}
