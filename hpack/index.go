package hpack

import "hash/maphash"

// An encoder's dynamic table never holds more than this many entries: the
// encoder caps the table at the default size, and an entry counts at least 32
// bytes against it.
const maxIndexedEntries = DefaultTableSize / 32

// indexSlots is the size of each hash table of a fieldIndex, twice the
// entries it has to hold, so a probe soon meets an empty slot.
const indexSlots = 2 * maxIndexedEntries

// A slot of a fieldIndex is empty when zero. Otherwise its top bit is set, the
// next seven bits are the top of the key's hash, to skip most entries with
// another key without comparing strings, and the low byte is the number of
// entries added before this one, modulo 256, which tells the table's entries
// apart since it holds fewer than that.
const (
	slotUsed = 0x8000
	slotTag  = 0xff00
)

// fieldIndex finds the newest entry of an encoder's dynamic table with a name
// and value, and the newest with a name, without searching the table. It is
// two hash tables with linear probing, one per key, holding a slot for each
// distinct key in the table.
type fieldIndex struct {
	pairs, names [indexSlots]uint16
}

var indexSeed = maphash.MakeSeed()

func hashName(name string) uint64 { return maphash.String(indexSeed, name) }

// hashPair hashes a name and value from the name's hash.
func hashPair(nameHash uint64, value string) uint64 {
	return nameHash ^ maphash.String(indexSeed, value)*0x9e3779b97f4a7c15
}

func slotOf(h uint64, id uint8) uint16 {
	return slotUsed | uint16(h>>57)<<8 | uint16(id)
}

// slotPos is the position in entries of the entry a slot refers to.
func (t *dynamicTable) slotPos(s uint16) int { return int(uint8(s) - t.first) }

// find returns the position in entries of the newest entry with the key h
// hashes, the name alone or with value when pair is set, or -1.
func (t *dynamicTable) find(slots *[indexSlots]uint16, h uint64, name, value string, pair bool) int {
	tag := slotOf(h, 0)
	for i := uint(h); ; i++ {
		s := slots[i%indexSlots]
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
func (t *dynamicTable) link(slots *[indexSlots]uint16, h uint64, p int, pair bool) {
	tag := slotOf(h, 0)
	f := t.entries[p]
	for i := uint(h); ; i++ {
		s := &slots[i%indexSlots]
		if *s != 0 {
			if *s&slotTag != tag {
				continue
			}
			if g := t.entries[t.slotPos(*s)]; g.Name != f.Name || (pair && g.Value != f.Value) {
				continue
			}
		}
		*s = slotOf(h, t.first+uint8(p))
		return
	}
}

// unlink drops the key h hashes if it still points at the entry at position
// p, which is leaving the table. A newer entry with the same key has taken
// the slot over otherwise. The slots after the one emptied move back into it
// as linear probing requires, which takes their keys' hashes again.
func (t *dynamicTable) unlink(slots *[indexSlots]uint16, h uint64, p int, pair bool) {
	hole := uint(h) % indexSlots
	for ; ; hole = (hole + 1) % indexSlots {
		s := slots[hole]
		if s == 0 {
			return
		}
		if t.slotPos(s) == p {
			break
		}
	}
	for j := (hole + 1) % indexSlots; slots[j] != 0; j = (j + 1) % indexSlots {
		f := t.entries[t.slotPos(slots[j])]
		home := hashName(f.Name)
		if pair {
			home = hashPair(home, f.Value)
		}
		if (j-uint(home))%indexSlots >= (j-hole)%indexSlots {
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
// indexFrom entries, so an encoder whose table stays small goes without.
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
		t.link(&t.index.names, nameHash, p, false)
		t.link(&t.index.pairs, pairHash, p, true)
	case len(t.entries) > indexFrom:
		t.index = new(fieldIndex)
		for p, g := range t.entries {
			h := hashName(g.Name)
			t.link(&t.index.names, h, p, false)
			t.link(&t.index.pairs, hashPair(h, g.Value), p, true)
		}
	}
	t.evict()
}

// unindex drops the oldest n entries, about to be evicted, from the index.
func (t *dynamicTable) unindex(n int) {
	for p := range n {
		f := t.entries[p]
		h := hashName(f.Name)
		t.unlink(&t.index.names, h, p, false)
		t.unlink(&t.index.pairs, hashPair(h, f.Value), p, true)
	}
}
