package bittorrent

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Defaults of the disk pool's Config fields.
const (
	// DefaultMaxDiskWorkers caps Config.DiskWorkers' default, which is
	// GOMAXPROCS.
	DefaultMaxDiskWorkers = 8
	// DefaultDiskQueueBytes is Config.DiskQueueBytes when zero.
	DefaultDiskQueueBytes = 64 << 20
)

// uploadBatch is how many blocks one upload job of a peer's reads and
// sends before it goes to the back of the disk queue, so that a peer that
// keeps up does not keep a disk worker to itself.
const uploadBatch = 16

// errNoStorage is a disk job's when its torrent's storage closed while it
// waited.
var errNoStorage = errors.New("bittorrent: storage closed")

// readHolder is what reads come from: a peer.Conn.
type readHolder interface{ HoldReads(hold bool) }

// diskPool runs the client's storage reads and writes and its piece
// hashing, which must not run on a fib worker: a fixed number of
// goroutines take jobs from one queue, in order. Writes count their bytes
// until they land; past limit, the connections whose blocks are queued
// stop reading until the queue drains to half of it.
type diskPool struct {
	limit int64
	holds atomic.Int64 // how many times reads were held, for tests

	mu     sync.Mutex
	cond   sync.Cond
	jobs   []func()
	queued int64
	held   map[readHolder]struct{}
	closed bool
	wg     sync.WaitGroup
}

func newDiskPool(workers int, limit int64) *diskPool {
	d := &diskPool{limit: limit, held: map[readHolder]struct{}{}}
	d.cond.L = &d.mu
	d.wg.Add(workers)
	for range workers {
		go d.worker()
	}
	return d
}

// diskWorkers is Config.DiskWorkers' default.
func diskWorkers() int { return min(runtime.GOMAXPROCS(0), DefaultMaxDiskWorkers) }

func (d *diskPool) worker() {
	defer d.wg.Done()
	for {
		d.mu.Lock()
		for len(d.jobs) == 0 && !d.closed {
			d.cond.Wait()
		}
		if len(d.jobs) == 0 {
			d.mu.Unlock()
			return
		}
		job := d.jobs[0]
		d.jobs[0] = nil
		d.jobs = d.jobs[1:]
		d.mu.Unlock()
		job()
	}
}

// run queues fn. Once the pool is closed fn runs on a goroutine of its
// own (the caller may hold t.mu): the client has detached every storage
// by then, so it does no I/O.
func (d *diskPool) run(fn func()) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		go fn()
		return
	}
	d.jobs = append(d.jobs, fn)
	d.cond.Signal()
	d.mu.Unlock()
}

// runBytes queues fn, which holds n bytes until it returns. It never
// waits: with the queue past its bound it holds the reads of from, to be
// resumed once the queue has drained.
func (d *diskPool) runBytes(n int64, from readHolder, fn func()) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		go fn()
		return
	}
	d.queued += n
	d.jobs = append(d.jobs, func() {
		fn()
		d.release(n)
	})
	d.cond.Signal()
	if d.queued >= d.limit && from != nil {
		if _, ok := d.held[from]; !ok {
			d.held[from] = struct{}{}
			d.holds.Add(1)
			// Under d.mu, so that a release cannot resume it first.
			from.HoldReads(true)
		}
	}
	d.mu.Unlock()
}

// release returns a write's bytes, and resumes the connections held once
// the queue is down to half its bound.
func (d *diskPool) release(n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queued -= n
	if d.queued > d.limit/2 || len(d.held) == 0 {
		return
	}
	for h := range d.held {
		h.HoldReads(false)
	}
	clear(d.held)
}

// queuedBytes is how many bytes the writes queued and running hold.
func (d *diskPool) queuedBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.queued
}

// close runs the jobs still queued and stops the workers.
func (d *diskPool) close() {
	d.mu.Lock()
	d.closed = true
	d.cond.Broadcast()
	held := d.held
	d.held = map[readHolder]struct{}{}
	for h := range held {
		h.HoldReads(false)
	}
	d.mu.Unlock()
	d.wg.Wait()
}

// useStorage returns the torrent's storage and info for an operation,
// nil when it has none, and done, which the operation calls when it is
// over: until then the storage is not closed. It must not be called with
// t.mu held, nor again before done.
func (t *Torrent) useStorage() (st Storage, info *metainfo.Info, done func()) {
	t.diskMu.RLock()
	t.mu.Lock()
	st, info = t.storage, t.info
	t.mu.Unlock()
	if st == nil {
		t.diskMu.RUnlock()
		return nil, nil, func() {}
	}
	return st, info, t.diskMu.RUnlock
}

// closeStorage closes st, which the torrent no longer holds, once the
// operations using it are over; with remove it deletes what st stored
// when it can (see StorageRemover). It must not be called with t.mu held.
func (t *Torrent) closeStorage(st Storage, remove bool) error {
	t.diskMu.Lock()
	defer t.diskMu.Unlock()
	if r, ok := st.(StorageRemover); ok && remove {
		return r.Remove()
	}
	return st.Close()
}
