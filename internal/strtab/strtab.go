// Package strtab is a fixed set of strings to look up, such as the field
// names a protocol knows, for the packages of this module: a lookup hashes a
// few of the key's bytes rather than all of them, as a map does, and
// compares the key with one or two candidates, which takes a header field's
// name half the time a map takes.
package strtab

// Table maps a fixed set of strings to values. It is built once and only
// read after, by any number of goroutines.
type Table[V any] struct {
	slots []slot[V]
	mask  uint32
}

type slot[V any] struct {
	key  string
	val  V
	used bool
}

// New builds a table of keys, with vals[i] the value of keys[i]. A key given
// twice keeps its first value.
func New[V any](keys []string, vals []V) *Table[V] {
	size := 16
	for size < 4*len(keys) {
		size *= 2
	}
	t := &Table[V]{slots: make([]slot[V], size), mask: uint32(size - 1)}
	for i, key := range keys {
		if key == "" {
			panic("strtab: empty key")
		}
		for j := hash(key) & t.mask; ; j = (j + 1) & t.mask {
			s := &t.slots[j]
			if !s.used {
				*s = slot[V]{key: key, val: vals[i], used: true}
				break
			}
			if s.key == key {
				break
			}
		}
	}
	return t
}

// Get returns key's value, and whether the table has key.
func (t *Table[V]) Get(key string) (V, bool) {
	if key != "" {
		for j := hash(key) & t.mask; ; j = (j + 1) & t.mask {
			s := &t.slots[j]
			if !s.used {
				break
			}
			if s.key == key {
				return s.val, true
			}
		}
	}
	var zero V
	return zero, false
}

// hash mixes the length of s, which is not empty, with its first, middle and
// last bytes: names that share a prefix, as content-length and content-type
// do, differ in length or at the end.
func hash(s string) uint32 {
	n := len(s)
	h := uint32(n)<<24 | uint32(s[0])<<16 | uint32(s[n>>1])<<8 | uint32(s[n-1])
	h *= 0x9e3779b1
	return h ^ h>>15
}
