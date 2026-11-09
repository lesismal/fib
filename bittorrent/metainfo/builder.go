package metainfo

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Version is the kind of torrent a Builder makes.
type Version int

const (
	// Hybrid torrents have both v1 pieces and a v2 file tree, padding each
	// file to a piece boundary, so that v1 and v2 clients share them. It
	// is the default.
	Hybrid Version = iota
	// V1 torrents (BEP 3) hash pieces across file boundaries with SHA-1.
	V1
	// V2 torrents (BEP 52) hash each file into a SHA-256 merkle tree.
	V2
)

// Builder makes a torrent of files by hashing them.
type Builder struct {
	// Version is the kind of torrent, Hybrid by default.
	Version Version
	// PieceLength is the piece length; 0 picks the smallest power of two
	// of at least 16 KiB that makes fewer than 2048 pieces, up to 16 MiB.
	// A v2 or hybrid torrent needs a power of two of at least 16 KiB.
	PieceLength int64
	// Name is the torrent's name; by default the base name of the file or
	// directory.
	Name string
	// Trackers are tiers of tracker URLs (BEP 12); the first is also the
	// "announce" of BEP 3.
	Trackers [][]string
	// WebSeeds are web seed URLs (BEP 19).
	WebSeeds []string
	// Nodes are DHT nodes for a torrent without trackers (BEP 5).
	Nodes []Node
	// Private marks the torrent private (BEP 27).
	Private bool
	// Source is the info dictionary's "source", which gives a torrent of
	// the same files a different info hash per private tracker.
	Source       string
	Comment      string
	CreatedBy    string
	CreationDate time.Time // left out when zero
}

// maxAutoPieceLength caps the piece length Builder picks.
const maxAutoPieceLength = 16 << 20

// ChoosePieceLength returns the piece length Builder picks for total
// bytes of data.
func ChoosePieceLength(total int64) int64 {
	pl := int64(BlockSize)
	for total/pl >= 2048 && pl < maxAutoPieceLength {
		pl <<= 1
	}
	return pl
}

type sourceFile struct {
	name   string   // path in the fs.FS
	path   []string // path in the torrent, nil for a single file
	length int64
}

// BuildPath makes a torrent of the file or directory at path in the
// operating system's file system.
func (b *Builder) BuildPath(p string) (*MetaInfo, error) {
	return b.BuildPathContext(context.Background(), p)
}

// BuildPathContext is BuildPath stopping with ctx.Err() once ctx is done.
func (b *Builder) BuildPathContext(ctx context.Context, p string) (*MetaInfo, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	bb := *b
	if bb.Name == "" {
		bb.Name = filepath.Base(abs)
	}
	if st.IsDir() {
		return bb.BuildContext(ctx, os.DirFS(abs), ".")
	}
	return bb.BuildContext(ctx, os.DirFS(filepath.Dir(abs)), filepath.Base(abs))
}

// Build makes a torrent of root in fsys: a single-file torrent if root is
// a file, a multi-file one of the regular files below it otherwise, in
// the order of their paths (as v2 requires), symbolic links to files
// followed and other entries and empty directories left out.
//
// It reads each file once, in 16 KiB blocks, and keeps 32 bytes per piece
// of a v2 file (the piece layer it outputs) and one piece's leaf hashes,
// so its memory does not grow with the files' sizes beyond that.
func (b *Builder) Build(fsys fs.FS, root string) (*MetaInfo, error) {
	return b.BuildContext(context.Background(), fsys, root)
}

// BuildContext is Build stopping with ctx.Err() once ctx is done. It looks
// at ctx between files and every megabyte read, so it returns within a
// megabyte of hashing; a read of fsys that blocks is not interrupted.
func (b *Builder) BuildContext(ctx context.Context, fsys fs.FS, root string) (*MetaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.Version < Hybrid || b.Version > V2 {
		return nil, fmt.Errorf("metainfo: unknown version %d", b.Version)
	}
	name := b.Name
	if name == "" {
		name = path.Base(root)
	}
	if !validName(name) {
		return nil, fmt.Errorf("metainfo: invalid torrent name %q; set Builder.Name", name)
	}
	files, single, err := listFiles(ctx, fsys, root)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, f := range files {
		total += f.length
	}
	if total == 0 {
		return nil, errors.New("metainfo: no data to make a torrent of")
	}
	pl := b.PieceLength
	if pl == 0 {
		pl = ChoosePieceLength(total)
	}
	if pl <= 0 || b.Version != V1 && (pl < BlockSize || pl&(pl-1) != 0) {
		return nil, fmt.Errorf("metainfo: piece length %d is not a power of two of at least 16 KiB", pl)
	}

	info := &Info{Name: name, PieceLength: pl, Private: b.Private, Source: b.Source}
	hasV1, hasV2 := b.Version != V2, b.Version != V1
	var v1 *v1Hasher
	if hasV1 {
		v1 = &v1Hasher{pl: pl, h: sha1.New()}
	}
	layers := PieceLayers{}
	if hasV2 {
		info.MetaVersion = 2
		info.FileTree = &FileTree{}
	}
	var v2 *fileHasher
	if hasV2 {
		v2 = newFileHasher(pl)
	}
	blk := make([]byte, BlockSize)
	for i, sf := range files {
		if err := readFile(ctx, fsys, sf, blk, func(block []byte) {
			if v1 != nil {
				v1.write(block)
			}
			if v2 != nil {
				v2.add(sha256.Sum256(block))
			}
		}); err != nil {
			return nil, err
		}
		var root [32]byte
		if hasV2 && sf.length > 0 {
			var layer []byte
			root, layer = v2.finish()
			if layer != nil {
				layers[root] = layer
			}
		}
		if hasV2 {
			p := sf.path
			if single {
				p = []string{name}
			}
			if err := info.FileTree.Add(p, TreeFile{Length: sf.length, PiecesRoot: root}); err != nil {
				return nil, err
			}
		}
		if hasV1 && !single {
			info.Files = append(info.Files, FileInfo{Length: sf.length, Path: sf.path})
			// A hybrid torrent pads every file but the last to a piece
			// boundary (BEP 47), so that v1 pieces are v2's.
			if rem := sf.length % pl; hasV2 && rem != 0 && i < len(files)-1 {
				pad := pl - rem
				v1.pad(pad)
				info.Files = append(info.Files, FileInfo{
					Length: pad, Path: []string{".pad", strconv.FormatInt(pad, 10)}, Attr: "p",
				})
			}
		}
		if single {
			info.Length = sf.length
		}
	}
	if v1 != nil {
		info.Pieces = v1.finish()
	}

	mi := &MetaInfo{
		URLList:   append(URLList(nil), b.WebSeeds...),
		Nodes:     b.Nodes,
		Comment:   b.Comment,
		CreatedBy: b.CreatedBy,
	}
	if !b.CreationDate.IsZero() {
		mi.CreationDate = b.CreationDate.Unix()
	}
	for _, tier := range b.Trackers {
		if len(tier) > 0 {
			mi.AnnounceList = append(mi.AnnounceList, append([]string(nil), tier...))
		}
	}
	if len(mi.AnnounceList) > 0 {
		mi.Announce = mi.AnnounceList[0][0]
		if len(mi.AnnounceList) == 1 && len(mi.AnnounceList[0]) == 1 {
			mi.AnnounceList = nil
		}
	}
	if len(layers) > 0 {
		mi.PieceLayers = layers
	}
	if err := mi.SetInfo(info); err != nil {
		return nil, err
	}
	return mi, nil
}

// listFiles lists the regular files at or below root, sorted by path
// component by component, which is the order of a v2 file tree.
func listFiles(ctx context.Context, fsys fs.FS, root string) (files []sourceFile, single bool, err error) {
	st, err := fs.Stat(fsys, root)
	if err != nil {
		return nil, false, err
	}
	if st.Mode().IsRegular() {
		return []sourceFile{{name: root, length: st.Size()}}, true, nil
	}
	if !st.IsDir() {
		return nil, false, fmt.Errorf("metainfo: %s is neither a file nor a directory", root)
	}
	// fs.WalkDir visits each directory's entries sorted by name, depth
	// first, which is the tree's order.
	err = fs.WalkDir(fsys, root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		var size int64
		switch {
		case d.Type().IsRegular():
			fi, err := d.Info()
			if err != nil {
				return err
			}
			size = fi.Size()
		case d.Type()&fs.ModeSymlink != 0:
			// Links to files are followed; links to directories are not,
			// as they could loop.
			fi, err := fs.Stat(fsys, p)
			if err != nil || !fi.Mode().IsRegular() {
				return nil
			}
			size = fi.Size()
		default:
			return nil
		}
		rel := p
		if root != "." {
			rel = strings.TrimPrefix(p, root+"/")
		}
		files = append(files, sourceFile{name: p, path: strings.Split(rel, "/"), length: size})
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if len(files) == 0 {
		return nil, false, fmt.Errorf("metainfo: no files in %s", root)
	}
	return files, false, nil
}

// ctxCheckBlocks is how many blocks readFile hashes between looks at its
// context: a megabyte.
const ctxCheckBlocks = (1 << 20) / BlockSize

// readFile feeds the file to fn in BlockSize blocks, the last one
// shorter, checking that it still has the length it was listed with, and
// returns ctx.Err() once ctx is done.
func readFile(ctx context.Context, fsys fs.FS, sf sourceFile, blk []byte, fn func([]byte)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := fsys.Open(sf.name)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var n int64
	for blocks := 1; ; blocks++ {
		if blocks%ctxCheckBlocks == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		k, err := io.ReadFull(r, blk)
		if k > 0 {
			n += int64(k)
			if n > sf.length {
				return fmt.Errorf("metainfo: %s grew while being hashed", sf.name)
			}
			fn(blk[:k])
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if n != sf.length {
		return fmt.Errorf("metainfo: %s shrank while being hashed", sf.name)
	}
	return nil
}

// v1Hasher hashes a stream of data into v1 pieces.
type v1Hasher struct {
	pl     int64
	h      hash.Hash
	n      int64 // bytes of the current piece hashed
	pieces []byte
}

func (v *v1Hasher) write(p []byte) {
	for len(p) > 0 {
		k := min(int64(len(p)), v.pl-v.n)
		v.h.Write(p[:k])
		v.n += k
		p = p[k:]
		if v.n == v.pl {
			v.pieces = v.h.Sum(v.pieces)
			v.h.Reset()
			v.n = 0
		}
	}
}

var zeros [BlockSize]byte

func (v *v1Hasher) pad(n int64) {
	for n > 0 {
		k := min(n, int64(len(zeros)))
		v.write(zeros[:k])
		n -= k
	}
}

func (v *v1Hasher) finish() []byte {
	if v.n > 0 {
		v.pieces = v.h.Sum(v.pieces)
		v.n = 0
	}
	if v.pieces == nil {
		return []byte{}
	}
	return v.pieces
}
