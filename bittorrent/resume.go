package bittorrent

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// resumeVersion is the version of the resume data ResumeData writes; data
// of a newer one is refused.
const resumeVersion = 1

// ErrResumeData is the Err of the EventError that reports resume data
// passed in AddOptions.Resume that was not used: not valid, of another
// torrent, of a newer version, or on storage that is not a StatStorage.
// The torrent's storage is then hash-checked in full.
var ErrResumeData = errors.New("bittorrent: resume data not used")

// errChecking is what ResumeData returns while the storage is checked.
var errChecking = errors.New("bittorrent: no resume data while the torrent checks its storage")

// resumeBlob is resume data as it is bencoded.
type resumeBlob struct {
	Version  int64        `bencode:"fib resume"`
	InfoHash []byte       `bencode:"info hash,omitempty"`
	V2       []byte       `bencode:"info hash v2,omitempty"`
	Pieces   []byte       `bencode:"pieces"`
	Files    []resumeFile `bencode:"files"`
}

// resumeFile is a file's stamp in resume data: its size and modification
// time in nanoseconds since the Unix epoch, or a size of -1 for a file
// that was not there. Padding and empty files have a zero stamp.
type resumeFile struct {
	Size  int64 `bencode:"size"`
	MTime int64 `bencode:"mtime"`
}

// ResumeData returns the torrent's resume data, for AddOptions.Resume when
// it is added again: a small, versioned bencoded dictionary of its info
// hashes, the bitfield of the pieces it has (checked, and so written to
// storage and read back), and the size and modification time of each of
// its files as its storage reports them now. Adding the torrent with it
// takes the pieces it lists as had without hashing them, in the files
// whose size and modification time have not changed.
//
// It is an error before the info dictionary is known, while the storage
// is checked (keep the resume data saved before), and on storage that is
// not a StatStorage. A stopped torrent's storage is opened again for the
// file stamps, and closed.
//
// The file stamps are taken after the bitfield, so that a piece the
// bitfield lists was written before them; a write that lands after them
// (a block of a piece not yet had) changes its file's stamp, and that
// file's pieces are checked when the torrent is added again. Save the
// resume data when ResumeGen changes, and once more after Stop or
// Client.Close, when no writes are left.
func (t *Torrent) ResumeData() ([]byte, error) {
	t.mu.Lock()
	info := t.info
	if info == nil {
		t.mu.Unlock()
		return nil, errors.New("bittorrent: no resume data before the info dictionary")
	}
	if t.checking {
		t.mu.Unlock()
		return nil, errChecking
	}
	have := t.have.Clone()
	opener := t.opener
	ih, ih2 := t.ih, t.ih2
	t.mu.Unlock()

	// The storage in use is held so that Stop or Close cannot close it
	// under the Stat calls (see useStorage).
	st, _, done := t.useStorage()
	defer done()
	if st == nil {
		s, err := opener(info)
		if err != nil {
			return nil, err
		}
		defer s.Close()
		st = s
	}
	ss, ok := st.(StatStorage)
	if !ok {
		return nil, errors.New("bittorrent: no resume data on storage that cannot report file stamps")
	}
	b := resumeBlob{Version: resumeVersion, Pieces: have.Bytes()}
	if info.HasV1() {
		b.InfoHash = ih[:]
	}
	if info.HasV2() {
		b.V2 = ih2[:]
	}
	files := info.FileList()
	b.Files = make([]resumeFile, len(files))
	for i, f := range files {
		if f.IsPad() || f.Length == 0 {
			continue
		}
		size, mtime, err := ss.Stat(i)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			b.Files[i] = resumeFile{Size: -1}
		case err != nil:
			return nil, err
		default:
			b.Files[i] = resumeFile{Size: size, MTime: mtime.UnixNano()}
		}
	}
	return bencode.Marshal(&b)
}

// ResumeGen is a count that changes whenever the bitfield in ResumeData
// does, which is when a piece passes its check: a caller saves resume data
// when it differs from the count at the last save. It is not a reason to
// skip the save after Stop or Client.Close, as writes to pieces not yet
// had change the file stamps without changing it.
func (t *Torrent) ResumeGen() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return uint64(t.haveCount)
}

// What the check does with a piece, as resume data plans it.
const (
	resumeCheck int8 = iota // hash it: a file of it changed
	resumeHave              // had, and its files have not changed
	resumeSkip              // not had, or a file of it is missing
)

// resumePlan takes the resume data of AddOptions.Resume, once, and plans
// the check from it: nil, when there is none or it is not used, checks
// every piece. Data that is not used is reported. It runs on the check's
// goroutine, as it asks storage for every file's stamp.
func (t *Torrent) resumePlan(st Storage, info *metainfo.Info) []int8 {
	t.mu.Lock()
	data := t.opts.Resume
	t.opts.Resume = nil
	t.mu.Unlock()
	if len(data) == 0 {
		return nil
	}
	plan, err := planResume(st, info, data)
	if err != nil {
		t.c.emit(Event{Type: EventError, Torrent: t, Err: fmt.Errorf("%w: %v", ErrResumeData, err)})
		return nil
	}
	return plan
}

func planResume(st Storage, info *metainfo.Info, data []byte) ([]int8, error) {
	ss, ok := st.(StatStorage)
	if !ok {
		return nil, errors.New("storage cannot report file stamps")
	}
	var b resumeBlob
	if err := bencode.Unmarshal(data, &b); err != nil {
		return nil, err
	}
	switch {
	case b.Version <= 0:
		return nil, errors.New("not resume data")
	case b.Version > resumeVersion:
		return nil, fmt.Errorf("version %d, newer than %d", b.Version, resumeVersion)
	}
	matched := false
	if len(b.InfoHash) > 0 {
		ih := info.InfoHashV1()
		if !info.HasV1() || string(b.InfoHash) != string(ih[:]) {
			return nil, errors.New("of another torrent")
		}
		matched = true
	}
	if len(b.V2) > 0 {
		ih2 := info.InfoHashV2()
		if !info.HasV2() || string(b.V2) != string(ih2[:]) {
			return nil, errors.New("of another torrent")
		}
		matched = true
	}
	if !matched {
		return nil, errors.New("no info hash")
	}
	n := info.PieceCount()
	have, err := peer.ParseBitfield(b.Pieces, n)
	if err != nil {
		return nil, errors.New("bitfield does not fit the torrent")
	}
	files := info.FileList()
	if len(b.Files) != len(files) {
		return nil, fmt.Errorf("%d files, the torrent has %d", len(b.Files), len(files))
	}
	// Each file now: as it was, changed, or missing. A piece takes the
	// last of its files': one with a file missing cannot pass, and is not
	// read.
	const (
		same = iota
		changed
		missing
	)
	state := make([]int8, len(files))
	for i, f := range files {
		if f.IsPad() || f.Length == 0 {
			continue
		}
		size, mtime, err := ss.Stat(i)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			state[i] = missing
		case err != nil:
			state[i] = changed
		case b.Files[i].Size >= 0 && size == b.Files[i].Size && mtime.UnixNano() == b.Files[i].MTime:
			state[i] = same
		default:
			state[i] = changed
		}
	}
	plan := make([]int8, n)
	for i := range n {
		worst := int8(same)
		for _, seg := range info.Segments(info.PieceOffset(i), info.PieceSize(i)) {
			if !files[seg.File].IsPad() {
				worst = max(worst, state[seg.File])
			}
		}
		switch {
		case worst == missing:
			plan[i] = resumeSkip
		case worst == changed:
			plan[i] = resumeCheck
		case have.Has(i):
			plan[i] = resumeHave
		default:
			plan[i] = resumeSkip
		}
	}
	return plan, nil
}

// resumePiece does what plan says for piece i, and reports whether that
// leaves the check nothing to do for it.
func (t *Torrent) resumePiece(plan []int8, i int) bool {
	if plan == nil {
		return false
	}
	switch plan[i] {
	case resumeHave:
		t.mu.Lock()
		t.markHaveLocked(i)
		t.mu.Unlock()
		return true
	case resumeSkip:
		return true
	}
	return false
}
