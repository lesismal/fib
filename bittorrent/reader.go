package bittorrent

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

// DefaultReadahead is how far ahead of its position a Reader has pieces
// fetched first.
const DefaultReadahead = 4 << 20

// Reader reads a torrent's data, or a file's, as it downloads: a read
// waits for the piece it needs, and the pieces from the position to the
// readahead go before all others, nearest first, so that a file plays
// while it streams in. It is an io.ReadSeekCloser; one goroutine at a
// time may use it.
type Reader struct {
	t     *Torrent
	start int64
	// length is -1 for a reader of the whole torrent made before its info
	// dictionary was known.
	length int64

	mu        sync.Mutex
	pos       int64
	readahead int64
	ctx       context.Context
	first     int
	last      int
	closed    bool
}

// NewReader returns a Reader of the torrent's data, all its files one
// after another as its pieces hold them (zeros where padding files and
// the gaps between a v2 torrent's files are). A torrent from a magnet
// link reads once its info dictionary is known.
func (t *Torrent) NewReader() *Reader {
	length := int64(-1)
	t.mu.Lock()
	if t.info != nil {
		length = t.info.TotalLength()
	}
	t.mu.Unlock()
	return t.newReader(0, length)
}

func (t *Torrent) newReader(start, length int64) *Reader {
	return &Reader{t: t, start: start, length: length, readahead: DefaultReadahead, ctx: context.Background(), first: -1, last: -1}
}

// SetReadahead sets how many bytes past the position are fetched first.
func (r *Reader) SetReadahead(n int64) {
	r.mu.Lock()
	r.readahead = max(n, 1)
	r.mu.Unlock()
}

// SetContext sets the context that bounds Read's waits.
func (r *Reader) SetContext(ctx context.Context) {
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
}

// Read reads at the position, waiting for the piece there.
func (r *Reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, net.ErrClosed
	}
	t := r.t
	select {
	case <-t.gotInfo:
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-t.removed:
		return 0, errTorrentGone
	}
	t.mu.Lock()
	info := t.info
	t.mu.Unlock()
	if r.length < 0 {
		r.length = info.TotalLength()
	}
	if r.pos >= r.length {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	off := r.start + r.pos
	i := int(off / info.PieceLength)
	end := min(info.PieceOffset(i)+info.PieceLength, r.start+r.length)
	n := int(min(int64(len(p)), end-off))
	t.setWindow(r, off, min(r.start+r.length, off+r.readahead))
	for {
		t.mu.Lock()
		have := t.pieces[i].have
		st := t.storage
		sig := t.pieceSig
		t.mu.Unlock()
		if have && st != nil {
			if err := readData(st, info, off, p[:n]); err != nil {
				return 0, err
			}
			r.pos += int64(n)
			return n, nil
		}
		select {
		case <-sig:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-t.removed:
			return 0, errTorrentGone
		}
	}
}

var errTorrentGone = errors.New("bittorrent: torrent removed")

// Seek sets the position, as io.Seeker.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.length < 0 {
		select {
		case <-r.t.gotInfo:
			r.length = r.t.Info().TotalLength()
		default:
			if whence == io.SeekEnd {
				return 0, errors.New("bittorrent: length not known before the info dictionary")
			}
		}
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.length + offset
	default:
		return 0, errors.New("bittorrent: bad whence")
	}
	if abs < 0 {
		return 0, errors.New("bittorrent: negative position")
	}
	r.pos = abs
	return abs, nil
}

// Close stops the reader's priority over other pieces.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	t := r.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.readers[r]; ok {
		delete(t.readers, r)
		t.recomputeUrgentLocked()
	}
	return nil
}

// setWindow makes the pieces of [from, to) the reader's urgent ones.
func (t *Torrent) setWindow(r *Reader, from, to int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.info == nil {
		return
	}
	first := int(from / t.info.PieceLength)
	last := int((max(to, from+1) - 1) / t.info.PieceLength)
	last = min(last, len(t.pieces)-1)
	if _, ok := t.readers[r]; ok && r.first == first && r.last == last {
		return
	}
	r.first, r.last = first, last
	t.readers[r] = struct{}{}
	t.recomputeUrgentLocked()
}

// recomputeUrgentLocked gives the pieces in the readers' windows their
// urgent priorities, the nearest piece of each window the highest.
func (t *Torrent) recomputeUrgentLocked() {
	clear(t.urgent)
	for r := range t.readers {
		for i := r.first; i <= r.last && i >= 0; i++ {
			p := priorityUrgent + max(0, 1000-(i-r.first))
			if p > t.urgent[i] {
				t.urgent[i] = p
			}
		}
	}
	t.recomputePrioritiesLocked()
	for _, pc := range t.peers {
		t.updateInterestLocked(pc)
		t.fillRequestsLocked(pc)
	}
}
