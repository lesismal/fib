package metainfo

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// FileTree is a node of a v2 torrent's file tree (BEP 52): a directory of
// named entries, or, with File set, a file. In bencoding a file is a
// dictionary whose only key is the empty string.
type FileTree struct {
	File *TreeFile
	Dir  map[string]*FileTree
}

// TreeFile is a file of a v2 file tree.
type TreeFile struct {
	Length int64
	// PiecesRoot is the root of the file's merkle tree, zero for an empty
	// file.
	PiecesRoot [32]byte
	// Attr holds the file's BEP 47 attributes.
	Attr string
}

type treeFileWire struct {
	Attr       string `bencode:"attr,omitempty"`
	Length     int64  `bencode:"length"`
	PiecesRoot []byte `bencode:"pieces root,omitempty"`
}

// Add puts a file at path, making the directories on the way.
func (t *FileTree) Add(path []string, f TreeFile) error {
	if len(path) == 0 {
		return errors.New("metainfo: empty path")
	}
	for i, name := range path {
		if t.File != nil {
			return fmt.Errorf("metainfo: %q is a file", path[:i])
		}
		if t.Dir == nil {
			t.Dir = map[string]*FileTree{}
		}
		next := t.Dir[name]
		if next == nil {
			next = &FileTree{}
			t.Dir[name] = next
		}
		t = next
	}
	if t.File != nil || t.Dir != nil {
		return fmt.Errorf("metainfo: %q already exists", path)
	}
	t.File = &f
	return nil
}

// Walk calls fn for each file below t in the tree's order (names sorted
// bytewise, depth first) with its path, until fn returns false.
func (t *FileTree) Walk(fn func(path []string, f *TreeFile) bool) {
	t.walk(nil, fn)
}

func (t *FileTree) walk(path []string, fn func([]string, *TreeFile) bool) bool {
	if t.File != nil {
		return fn(append([]string(nil), path...), t.File)
	}
	for _, name := range sortedKeys(t.Dir) {
		if !t.Dir[name].walk(append(path, name), fn) {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]*FileTree) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// MarshalBencode encodes the tree.
func (t *FileTree) MarshalBencode() ([]byte, error) {
	return t.appendTo(nil)
}

func (t *FileTree) appendTo(b []byte) ([]byte, error) {
	b = append(b, 'd')
	if t.File != nil {
		w := treeFileWire{Attr: t.File.Attr, Length: t.File.Length}
		if t.File.PiecesRoot != ([32]byte{}) {
			w.PiecesRoot = t.File.PiecesRoot[:]
		}
		b = append(b, "0:"...)
		var err error
		if b, err = bencode.Append(b, &w); err != nil {
			return nil, err
		}
		return append(b, 'e'), nil
	}
	for _, name := range sortedKeys(t.Dir) {
		if name == "" {
			return nil, errors.New("metainfo: file tree entry with an empty name")
		}
		b = strconv.AppendInt(b, int64(len(name)), 10)
		b = append(b, ':')
		b = append(b, name...)
		var err error
		if b, err = t.Dir[name].appendTo(b); err != nil {
			return nil, err
		}
	}
	return append(b, 'e'), nil
}

// UnmarshalBencode decodes a tree. Its nesting is bounded by the
// decoder's depth limit, which the bytes have already passed.
func (t *FileTree) UnmarshalBencode(data []byte) error {
	var v any
	if err := bencode.Unmarshal(data, &v); err != nil {
		return err
	}
	node, err := treeFrom(v)
	if err != nil {
		return err
	}
	*t = *node
	return nil
}

var errBadTree = errors.New("metainfo: malformed file tree")

func treeFrom(v any) (*FileTree, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errBadTree
	}
	if fv, ok := m[""]; ok {
		if len(m) != 1 {
			return nil, errBadTree
		}
		fm, ok := fv.(map[string]any)
		if !ok {
			return nil, errBadTree
		}
		var f TreeFile
		if f.Length, ok = fm["length"].(int64); !ok || f.Length < 0 {
			return nil, errBadTree
		}
		if root, ok := fm["pieces root"]; ok {
			s, ok := root.(string)
			if !ok || len(s) != 32 {
				return nil, errBadTree
			}
			copy(f.PiecesRoot[:], s)
		}
		if attr, ok := fm["attr"].(string); ok {
			f.Attr = attr
		}
		return &FileTree{File: &f}, nil
	}
	t := &FileTree{Dir: make(map[string]*FileTree, len(m))}
	for name, child := range m {
		c, err := treeFrom(child)
		if err != nil {
			return nil, err
		}
		t.Dir[name] = c
	}
	return t, nil
}
