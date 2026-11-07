package metainfo

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// Info is a torrent's info dictionary: what the info hash names. It has
// the fields of v1 (BEP 3) and v2 (BEP 52); a hybrid torrent has both.
//
// An Info comes from ParseInfo (or Parse) or a Builder, which keep the
// bytes it was encoded as, from which its hashes and file layout are
// computed once. To change one, edit a copy and pass it to
// MetaInfo.SetInfo, which encodes and parses it again.
type Info struct {
	// Name is the file's name for a single-file torrent and the
	// directory's for a multi-file one.
	Name string
	// PieceLength is the number of bytes in each piece; a power of two of
	// at least BlockSize in a v2 torrent.
	PieceLength int64
	// Pieces is the concatenated SHA-1s of the v1 pieces, nil in a
	// v2-only torrent.
	Pieces []byte
	// Length is a v1 single-file torrent's length; Files is nil then.
	Length int64
	// Attr is a v1 single-file torrent's BEP 47 attributes.
	Attr string
	// Files is a v1 multi-file torrent's files in order, padding files
	// included.
	Files []FileInfo
	// Private (BEP 27) says peers come only from the torrent's trackers:
	// no DHT, PEX or local discovery.
	Private bool
	// Source distinguishes otherwise identical torrents of different
	// private trackers.
	Source string
	// MetaVersion is 2 for a v2 or hybrid torrent.
	MetaVersion int
	// FileTree is a v2 torrent's files, nil in a v1 one.
	FileTree *FileTree

	raw    []byte
	hashV1 InfoHash
	hashV2 InfoHashV2
	files  []File
	pieces int
	total  int64
}

// FileInfo is an entry of a v1 info dictionary's "files" list.
type FileInfo struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
	// PathUTF8 is the path in UTF-8, which some creators add when Path is
	// in another encoding.
	PathUTF8 []string `bencode:"path.utf-8,omitempty"`
	// Attr holds BEP 47 attributes: 'p' padding, 'x' executable, 'h'
	// hidden, 'l' symbolic link (to SymlinkPath).
	Attr        string   `bencode:"attr,omitempty"`
	SymlinkPath []string `bencode:"symlink path,omitempty"`
	SHA1        []byte   `bencode:"sha1,omitempty"`
	MD5Sum      string   `bencode:"md5sum,omitempty"`
}

// File is one file of a torrent in the order and at the place it has in
// the torrent's pieces, as Info.FileList returns them.
type File struct {
	// Path is the file's path below the torrent's directory, nil for the
	// file of a single-file torrent (named Info.Name).
	Path []string
	// Length is the file's size in bytes.
	Length int64
	// Offset is where the file starts in the torrent's data: in a v1
	// torrent right after the previous file, in a v2 one at the next
	// piece boundary.
	Offset int64
	// Attr holds the file's BEP 47 attributes.
	Attr string
	// SymlinkPath is a symbolic link's target, when Attr has 'l'.
	SymlinkPath []string
	// PiecesRoot is the root of the file's v2 merkle tree, zero in a
	// v1-only torrent and for an empty file.
	PiecesRoot [32]byte
}

// IsPad reports whether f is a BEP 47 padding file, whose content is
// zeros that are never stored.
func (f *File) IsPad() bool { return strings.Contains(f.Attr, "p") }

// Segment is the part of a file a range of the torrent's data covers.
type Segment struct {
	File   int   // index in Info.FileList
	Offset int64 // offset in the file
	Length int64
}

var (
	// ErrPieceHash is a piece whose data does not hash to the torrent's
	// hash of it.
	ErrPieceHash = errors.New("metainfo: piece hash mismatch")
	// ErrNoPieceLayer is a v2 piece check for a file whose piece layer is
	// not known yet, as for a torrent from a magnet link before the layer
	// is fetched from peers.
	ErrNoPieceLayer = errors.New("metainfo: no piece layer for the file")
)

// maxLength bounds file and torrent lengths so that offsets never
// overflow.
const maxLength = 1 << 60

// infoWire is the info dictionary as bencoded. Pointers tell a key that
// is absent from one that is empty.
type infoWire struct {
	Name        string     `bencode:"name"`
	PieceLength int64      `bencode:"piece length"`
	Pieces      *[]byte    `bencode:"pieces"`
	Length      *int64     `bencode:"length"`
	Attr        string     `bencode:"attr,omitempty"`
	Files       []FileInfo `bencode:"files,omitempty"`
	Private     bool       `bencode:"private,omitempty"`
	Source      string     `bencode:"source,omitempty"`
	MetaVersion int        `bencode:"meta version,omitempty"`
	FileTree    *FileTree  `bencode:"file tree"`
}

// HasV1 reports whether the torrent has v1 pieces: v1 or hybrid.
func (info *Info) HasV1() bool { return info.MetaVersion != 2 || info.Pieces != nil }

// HasV2 reports whether the torrent has a v2 file tree: v2 or hybrid.
func (info *Info) HasV2() bool { return info.MetaVersion == 2 && info.FileTree != nil }

// MarshalBencode encodes info, giving back the bytes it was parsed from if
// it was.
func (info *Info) MarshalBencode() ([]byte, error) {
	if info.raw != nil {
		return info.raw, nil
	}
	return info.encode()
}

func (info *Info) encode() ([]byte, error) {
	w := infoWire{
		Name:        info.Name,
		PieceLength: info.PieceLength,
		Attr:        info.Attr,
		Files:       info.Files,
		Private:     info.Private,
		Source:      info.Source,
		MetaVersion: info.MetaVersion,
	}
	if info.HasV1() {
		pieces := info.Pieces
		if pieces == nil {
			pieces = []byte{}
		}
		w.Pieces = &pieces
		if info.Files == nil {
			w.Length = &info.Length
		}
	}
	if info.MetaVersion == 2 {
		w.FileTree = info.FileTree
	}
	return bencode.Marshal(&w)
}

// UnmarshalBencode parses an info dictionary as ParseInfo does.
func (info *Info) UnmarshalBencode(data []byte) error {
	p, err := ParseInfo(data)
	if err != nil {
		return err
	}
	*info = *p
	return nil
}

// ParseInfo parses and checks an info dictionary, such as the one a
// magnet link's metadata exchange (BEP 9) fetches. It keeps a copy of raw,
// over which the info hashes are taken.
//
// Keys it does not know are kept in the bytes and ignored otherwise. It
// rejects what would make the torrent unusable or unsafe: piece hashes
// that do not match the length, file paths with empty, "." or ".."
// components or a '/' or NUL in one, files that clash, and hybrid
// torrents whose v1 and v2 files differ.
func ParseInfo(raw []byte) (*Info, error) {
	var w infoWire
	if err := bencode.Unmarshal(raw, &w); err != nil {
		// A value of the wrong type leaves its field empty, which the
		// checks below reject for any field that matters.
		var te *bencode.UnmarshalTypeError
		if !errors.As(err, &te) {
			return nil, fmt.Errorf("metainfo: info: %w", err)
		}
	}
	info := &Info{
		Name:        w.Name,
		PieceLength: w.PieceLength,
		Attr:        w.Attr,
		Files:       w.Files,
		Private:     w.Private,
		Source:      w.Source,
		MetaVersion: w.MetaVersion,
		raw:         bytes.Clone(raw),
	}
	if w.Pieces != nil {
		info.Pieces = *w.Pieces
		if info.Pieces == nil {
			info.Pieces = []byte{}
		}
	}
	if w.Length != nil {
		info.Length = *w.Length
	}
	if info.MetaVersion == 2 {
		info.FileTree = w.FileTree
	}
	if err := info.check(w.Length != nil); err != nil {
		return nil, err
	}
	info.hashV1 = sha1.Sum(info.raw)
	info.hashV2 = sha256.Sum256(info.raw)
	return info, nil
}

// Bytes returns the encoded info dictionary, which must not be modified.
func (info *Info) Bytes() []byte { return info.raw }

// InfoHashV1 returns the SHA-1 of the info dictionary, the hash of a v1 or
// hybrid torrent.
func (info *Info) InfoHashV1() InfoHash { return info.hashV1 }

// InfoHashV2 returns the SHA-256 of the info dictionary, the hash of a v2
// or hybrid torrent.
func (info *Info) InfoHashV2() InfoHashV2 { return info.hashV2 }

// validName reports whether s can be one component of a file's path.
func validName(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\x00")
}

func validPath(p []string) bool {
	if len(p) == 0 {
		return false
	}
	for _, c := range p {
		if !validName(c) {
			return false
		}
	}
	return true
}

func (info *Info) check(hasLength bool) error {
	if !validName(info.Name) {
		return fmt.Errorf("metainfo: invalid name %q", info.Name)
	}
	if info.PieceLength <= 0 || info.PieceLength > maxLength {
		return fmt.Errorf("metainfo: invalid piece length %d", info.PieceLength)
	}
	switch info.MetaVersion {
	case 0, 1, 2:
	default:
		return fmt.Errorf("metainfo: unsupported meta version %d", info.MetaVersion)
	}
	if info.MetaVersion == 2 {
		if info.FileTree == nil {
			return errors.New("metainfo: v2 info without a file tree")
		}
		if info.PieceLength < BlockSize || info.PieceLength&(info.PieceLength-1) != 0 {
			return fmt.Errorf("metainfo: v2 piece length %d is not a power of two of at least 16 KiB", info.PieceLength)
		}
	}
	var v1, v2 []File
	if info.HasV1() {
		var err error
		if v1, err = info.v1Files(hasLength); err != nil {
			return err
		}
	}
	if info.HasV2() {
		var err error
		if v2, err = info.v2Files(); err != nil {
			return err
		}
	}
	switch {
	case v1 != nil && v2 != nil:
		if err := info.matchHybrid(v1, v2); err != nil {
			return err
		}
		info.files = v1
		info.pieces = len(info.Pieces) / sha1.Size
	case v1 != nil:
		info.files = v1
		info.pieces = len(info.Pieces) / sha1.Size
	default:
		info.files = v2
		var end int64
		if n := len(v2); n > 0 {
			end = v2[n-1].Offset + v2[n-1].Length
		}
		info.pieces = int((end + info.PieceLength - 1) / info.PieceLength)
	}
	for i := range info.files {
		info.total += info.files[i].Length
	}
	return checkClashes(info.files)
}

func (info *Info) v1Files(hasLength bool) ([]File, error) {
	if len(info.Pieces)%sha1.Size != 0 {
		return nil, errors.New("metainfo: pieces is not a whole number of SHA-1 hashes")
	}
	var files []File
	switch {
	case info.Files != nil && hasLength:
		return nil, errors.New("metainfo: info has both length and files")
	case info.Files != nil:
		if len(info.Files) == 0 {
			return nil, errors.New("metainfo: empty files list")
		}
		files = make([]File, len(info.Files))
		for i, fi := range info.Files {
			if !validPath(fi.Path) {
				return nil, fmt.Errorf("metainfo: invalid file path %q", fi.Path)
			}
			files[i] = File{Path: fi.Path, Length: fi.Length, Attr: fi.Attr, SymlinkPath: fi.SymlinkPath}
		}
	case hasLength:
		files = []File{{Length: info.Length, Attr: info.Attr}}
	default:
		return nil, errors.New("metainfo: info has neither length nor files")
	}
	var off int64
	for i := range files {
		f := &files[i]
		if f.Length < 0 || f.Length > maxLength-off {
			return nil, fmt.Errorf("metainfo: invalid file length %d", f.Length)
		}
		f.Offset = off
		off += f.Length
	}
	if want := (off + info.PieceLength - 1) / info.PieceLength; int64(len(info.Pieces)/sha1.Size) != want {
		return nil, fmt.Errorf("metainfo: %d piece hashes for %d pieces", len(info.Pieces)/sha1.Size, want)
	}
	return files, nil
}

// v2Files lists the file tree's files in order, each at a piece boundary.
func (info *Info) v2Files() ([]File, error) {
	var files []File
	t := info.FileTree
	if t.File != nil {
		return nil, errors.New("metainfo: file tree is a file")
	}
	if len(t.Dir) == 1 && t.Dir[info.Name] != nil && t.Dir[info.Name].File != nil {
		// A single-file torrent: the tree's one entry is the file, named
		// as the torrent.
		tf := t.Dir[info.Name].File
		files = []File{{Length: tf.Length, Attr: tf.Attr, PiecesRoot: tf.PiecesRoot}}
	} else {
		t.Walk(func(path []string, tf *TreeFile) bool {
			files = append(files, File{
				Path: path, Length: tf.Length, Attr: tf.Attr, PiecesRoot: tf.PiecesRoot,
			})
			return true
		})
	}
	if len(files) == 0 {
		return nil, errors.New("metainfo: empty file tree")
	}
	var off int64
	for i := range files {
		f := &files[i]
		if files[i].Path != nil && !validPath(f.Path) {
			return nil, fmt.Errorf("metainfo: invalid file path %q", f.Path)
		}
		if f.Length < 0 || f.Length > maxLength-off {
			return nil, fmt.Errorf("metainfo: invalid file length %d", f.Length)
		}
		if f.Length > 0 && f.PiecesRoot == ([32]byte{}) {
			return nil, fmt.Errorf("metainfo: file %q has no pieces root", f.Path)
		}
		f.Offset = off
		off += (f.Length + info.PieceLength - 1) / info.PieceLength * info.PieceLength
	}
	return files, nil
}

// matchHybrid checks that a hybrid torrent's v1 files, without their
// padding, are its v2 files, each starting at a piece boundary, and copies
// the pieces roots across.
func (info *Info) matchHybrid(v1, v2 []File) error {
	j := 0
	for i := range v1 {
		f := &v1[i]
		if f.IsPad() {
			continue
		}
		if j >= len(v2) || !equalPath(f.Path, v2[j].Path) || f.Length != v2[j].Length {
			return errors.New("metainfo: hybrid torrent's v1 and v2 files differ")
		}
		if f.Length > 0 && f.Offset%info.PieceLength != 0 {
			return fmt.Errorf("metainfo: hybrid torrent's file %q is not piece aligned", f.Path)
		}
		f.PiecesRoot = v2[j].PiecesRoot
		j++
	}
	if j != len(v2) {
		return errors.New("metainfo: hybrid torrent's v1 and v2 files differ")
	}
	return nil
}

func equalPath(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// checkClashes rejects two files at one path, or a file at the path of
// another's directory, which storage could not hold. Padding files are
// never stored, so they may repeat.
func checkClashes(files []File) error {
	if len(files) < 2 {
		return nil
	}
	paths := make(map[string]bool, len(files))
	for i := range files {
		if files[i].IsPad() {
			continue
		}
		p := strings.Join(files[i].Path, "/")
		if paths[p] {
			return fmt.Errorf("metainfo: two files at %q", p)
		}
		paths[p] = true
	}
	for p := range paths {
		for i := strings.IndexByte(p, '/'); i >= 0; {
			if paths[p[:i]] {
				return fmt.Errorf("metainfo: file %q is also a directory", p[:i])
			}
			j := strings.IndexByte(p[i+1:], '/')
			if j < 0 {
				break
			}
			i += 1 + j
		}
	}
	return nil
}

// FileList returns the torrent's files in order, padding files included,
// which must not be modified.
func (info *Info) FileList() []File { return info.files }

// TotalLength returns the sum of the files' lengths, padding included.
func (info *Info) TotalLength() int64 { return info.total }

// PieceCount returns the number of pieces.
func (info *Info) PieceCount() int { return info.pieces }

// PieceOffset returns where piece i starts in the torrent's data.
func (info *Info) PieceOffset(i int) int64 { return int64(i) * info.PieceLength }

// PieceSize returns the length of piece i: the piece length but for the
// last piece, and in a v2-only torrent the last piece of each file. It is
// 0 for an index out of range.
func (info *Info) PieceSize(i int) int64 {
	if i < 0 || i >= info.pieces {
		return 0
	}
	start := info.PieceOffset(i)
	var end int64
	if info.HasV1() {
		end = info.TotalLength()
	} else {
		f := &info.files[info.fileAt(start)]
		end = f.Offset + f.Length
	}
	return min(info.PieceLength, end-start)
}

// fileAt returns the index of the non-empty file holding the byte at off,
// or of the first file after it, or len(files).
func (info *Info) fileAt(off int64) int {
	return sort.Search(len(info.files), func(i int) bool {
		f := &info.files[i]
		return f.Offset+f.Length > off
	})
}

// Segments returns the parts of files the range [off, off+n) of the
// torrent's data covers, padding files included and empty files left out.
func (info *Info) Segments(off, n int64) []Segment {
	var segs []Segment
	end := off + n
	for i := info.fileAt(off); i < len(info.files); i++ {
		f := &info.files[i]
		if f.Offset >= end {
			break
		}
		if f.Length == 0 {
			continue
		}
		s, e := max(off, f.Offset), min(end, f.Offset+f.Length)
		segs = append(segs, Segment{File: i, Offset: s - f.Offset, Length: e - s})
	}
	return segs
}

// PieceSegments returns the parts of files piece i covers.
func (info *Info) PieceSegments(i int) []Segment {
	return info.Segments(info.PieceOffset(i), info.PieceSize(i))
}

// FilePieces returns the range [begin, end) of the pieces file f overlaps.
func (info *Info) FilePieces(f int) (begin, end int) {
	file := &info.files[f]
	if file.Length == 0 {
		b := int(file.Offset / info.PieceLength)
		return b, b
	}
	return int(file.Offset / info.PieceLength), int((file.Offset + file.Length + info.PieceLength - 1) / info.PieceLength)
}

// PieceHashV1 returns the SHA-1 of v1 piece i.
func (info *Info) PieceHashV1(i int) (h [20]byte) {
	copy(h[:], info.Pieces[i*sha1.Size:])
	return h
}

// VerifyPieceV1 checks piece i's data against its v1 SHA-1.
func (info *Info) VerifyPieceV1(i int, data []byte) error {
	if !info.HasV1() || i < 0 || i >= info.pieces {
		return fmt.Errorf("metainfo: no v1 piece %d", i)
	}
	if sha1.Sum(data) != info.PieceHashV1(i) {
		return ErrPieceHash
	}
	return nil
}

// VerifyPieceV2 checks piece i's data against the merkle tree of its file:
// against the file's piece layer from layers when the file is larger than
// a piece, against its pieces root otherwise. In a hybrid torrent data may
// include the padding after the file's end, which is left out.
func (info *Info) VerifyPieceV2(i int, data []byte, layers PieceLayers) error {
	if !info.HasV2() || i < 0 || i >= info.pieces {
		return fmt.Errorf("metainfo: no v2 piece %d", i)
	}
	start := info.PieceOffset(i)
	fi := info.fileAt(start)
	if fi == len(info.files) || info.files[fi].Offset > start || info.files[fi].IsPad() {
		return fmt.Errorf("metainfo: piece %d holds no file data", i)
	}
	f := &info.files[fi]
	n := min(info.PieceLength, f.Offset+f.Length-start)
	if int64(len(data)) < n {
		return ErrPieceHash
	}
	data = data[:n]
	if f.Length <= info.PieceLength {
		leaves := HashBlocks(data)
		if MerkleRoot(leaves, len(leaves), [32]byte{}) != f.PiecesRoot {
			return ErrPieceHash
		}
		return nil
	}
	layer, ok := layers[f.PiecesRoot]
	if !ok {
		return ErrNoPieceLayer
	}
	j := int((start - f.Offset) / info.PieceLength)
	if (j+1)*32 > len(layer) {
		return ErrNoPieceLayer
	}
	got := MerkleRoot(HashBlocks(data), int(info.PieceLength/BlockSize), [32]byte{})
	if !bytes.Equal(got[:], layer[j*32:(j+1)*32]) {
		return ErrPieceHash
	}
	return nil
}

// VerifyPiece checks piece i's data against every hash the torrent has of
// it: v1, v2, or both in a hybrid torrent. A hybrid torrent's v2 check is
// skipped while its piece layer is not known, the v1 check standing.
func (info *Info) VerifyPiece(i int, data []byte, layers PieceLayers) error {
	if info.HasV1() {
		if err := info.VerifyPieceV1(i, data); err != nil {
			return err
		}
	}
	if info.HasV2() {
		err := info.VerifyPieceV2(i, data, layers)
		if err == ErrNoPieceLayer && info.HasV1() {
			return nil
		}
		return err
	}
	return nil
}

// PieceLayers maps the pieces root of each v2 file larger than a piece to
// its piece layer: the concatenated hashes of the merkle tree's nodes that
// each cover one piece.
type PieceLayers map[[32]byte][]byte

// ValidatePieceLayers checks that every layer in layers for one of the
// torrent's files has one hash per piece of the file and hashes to the
// file's pieces root. It reports which files larger than a piece have no
// layer; Parse requires none to be missing from a .torrent file, while a
// torrent from a magnet link fetches them from peers.
func (info *Info) ValidatePieceLayers(layers PieceLayers) (missing int, err error) {
	if !info.HasV2() {
		return 0, nil
	}
	for i := range info.files {
		f := &info.files[i]
		if f.IsPad() || f.Length <= info.PieceLength {
			continue
		}
		layer, ok := layers[f.PiecesRoot]
		if !ok {
			missing++
			continue
		}
		pieces := (f.Length + info.PieceLength - 1) / info.PieceLength
		if int64(len(layer)) != pieces*32 {
			return missing, fmt.Errorf("metainfo: piece layer of %q has %d bytes for %d pieces", f.Path, len(layer), pieces)
		}
		root, _ := LayerRoot(layer, info.PieceLength)
		if root != f.PiecesRoot {
			return missing, fmt.Errorf("metainfo: piece layer of %q does not hash to its pieces root", f.Path)
		}
	}
	return missing, nil
}
