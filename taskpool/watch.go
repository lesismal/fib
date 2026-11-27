package taskpool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/log"
)

// A pool's queue is where its tasks wait for a worker, and what it holds
// says how far the pool is behind: a queue that fills turns TryGo away and
// makes Go wait. A queue that fills shows in QueueStats, and in a warning
// logged as it rises past each of WarnLevels.
//
// Nothing looks at a queue while it is shallow. A submission that leaves a
// queue (or one shard of it) at the first of WarnLevels or deeper arms the
// pool's watch, and so does a TryGo turned away by a full queue; the queue's
// depth is known to the submission already, from the head and tail it read to
// queue its task, so the check costs it one compare and writes nothing a
// shallow queue's submissions share. An armed pool is sampled every
// watchInterval by a watcher goroutine, which warns as the queue rises past
// each level, and disarms the pool once it has calmed: below the first level
// less warnHysteresis for releaseCalm, or below the first level, with no
// level warned at, for releaseQuiet. The watcher runs only while some pool is
// armed, and leaves once none is, so a process whose queues stay shallow runs
// no timer for them.
//
// MaxQueued is the deepest queue the samples found, the depth that armed the
// watch, the whole queue once TryGo has been turned away, and, for a queue
// that grows its storage as it fills, about the depth that made it grow; a
// burst that came and went between two samples of an armed pool may not show
// in it.

// WarnLevels are the shares of a pool's queue, in percent, that the watcher
// warns about as the queue rises past each. A level warns again only once
// the queue has fallen below it by warnHysteresis percent, so a queue that
// hovers around a level does not warn on every sample. The first is also
// where a submission arms the pool's watch.
var WarnLevels = [...]int{50, 75, 90, 100}

// warnHysteresis is how far, in percent of the queue, a queue falls below a
// level it has warned at before it may warn there again.
const warnHysteresis = 10

// watchInterval is how often the watcher samples the armed pools' queues,
// and warnEvery the least time between two warnings a pool logs at one
// level; the ones held back meanwhile are counted, and the next says how
// many. releaseCalm and releaseQuiet are how long a pool stays calm before
// the watcher disarms it; see the top of this file.
var (
	watchInterval = 100 * time.Millisecond
	warnEvery     = 10 * time.Second
	releaseCalm   = time.Second
	releaseQuiet  = 10 * time.Second
)

// QueueStats describes a pool's queue; see TaskPool.QueueStats.
type QueueStats struct {
	// Queued is how many tasks wait for a worker now, and QueueSize how many
	// the queue holds. Both are 0 for a ModeInline pool, which has none.
	Queued    int
	QueueSize int
	// MaxQueued is the deepest the queue has been seen: by the watcher, which
	// samples it once it is deep, by the submission that found it deep, or by
	// a TryGo turned away, which found it full. A queue that grows its
	// storage, as ModeAdaptive's does, also reports about the depth that made
	// it grow, so a queue never deep enough to be watched shows roughly how
	// deep it got.
	MaxQueued int
	// Rejected counts the TryGo calls turned away with the queue full,
	// leaving out those turned away because the pool had stopped.
	Rejected uint64
	// Warnings counts, for each of WarnLevels, the times the queue rose
	// past that level.
	Warnings [len(WarnLevels)]uint64
}

// queueReporter is a backend that can say how many tasks wait in its queue
// and how many it holds.
type queueReporter interface {
	queueLen() (queued, size int)
	// queueState is queueLen with how full the backend's fullest shard is,
	// in percent of that shard's size, read in the same pass.
	queueState() (queued, size, fullest int)
	// grownPast is about the deepest the queue has been as far as its
	// growth tells (see taskRing.grownPast), or 0.
	grownPast() int
}

// queueWatch is what a pool keeps of its queue's history. The pool, its
// backend (through the executor) and, while it is armed, the watcher hold it.
type queueWatch struct {
	// armed is set by the submission that arms the watch, and cleared by the
	// watcher as it disarms it; only the first arms it, so a deep queue's
	// submissions read it and write nothing.
	armed     atomic.Bool
	stopped   atomic.Bool
	maxQueued atomic.Int64
	rejected  atomic.Uint64
	warnings  [len(WarnLevels)]atomic.Uint64

	executor *executor
	// queue is the backend, if it has a queue; workers counts its workers
	// for the warnings.
	queue   queueReporter
	workers func() int

	// Only the watcher touches the rest. rejectsSeen is rejected as the
	// last sample saw it; level is the index of the highest of WarnLevels
	// warned at and not yet left, or -1; lastWarn and held, per level, when
	// it last logged and how many warnings it has held back since; calmSince
	// and quietSince when the queue was last found to have calmed, by
	// releaseCalm's measure and releaseQuiet's.
	mu          sync.Mutex
	rejectsSeen uint64
	level       int
	lastWarn    [len(WarnLevels)]time.Time
	held        [len(WarnLevels)]int
	calmSince   time.Time
	quietSince  time.Time
}

// newQueueWatch makes the watch of a pool whose backend is not built yet;
// see setBackend.
func newQueueWatch(executor *executor) *queueWatch {
	return &queueWatch{executor: executor, level: -1}
}

// setBackend tells the watch the backend it watches, before the pool takes
// its first task.
func (w *queueWatch) setBackend(b backend) {
	w.workers = b.workerCount
	if r, ok := b.(queueReporter); ok {
		w.queue = r
	}
}

// QueueStats reports the pool's queue: what waits in it now, the deepest it
// has been, and the warnings and turn-aways that say the pool fell behind.
func (tp *TaskPool) QueueStats() QueueStats {
	w := tp.watch
	q, size := tp.queueLen()
	s := QueueStats{
		Queued:    q,
		QueueSize: size,
		MaxQueued: int(w.maxQueued.Load()),
		Rejected:  w.rejected.Load(),
	}
	for i := range s.Warnings {
		s.Warnings[i] = w.warnings[i].Load()
	}
	if w.queue != nil {
		s.MaxQueued = max(s.MaxQueued, w.queue.grownPast())
	}
	s.MaxQueued = max(s.MaxQueued, q)
	return s
}

// Queued reports how many tasks wait for a worker now, and the queue's size.
func (tp *TaskPool) Queued() (queued, size int) { return tp.queueLen() }

func (tp *TaskPool) queueLen() (queued, size int) {
	if r := tp.watch.queue; r != nil {
		return r.queueLen()
	}
	return 0, 0
}

// rejectedFull records a TryGo turned away by a full queue: the queue was as
// deep as it goes, and the watch is armed for the watcher to warn of it.
func (tp *TaskPool) rejectedFull() {
	w := tp.watch
	w.rejected.Add(1)
	if _, size := tp.queueLen(); size > 0 {
		raiseMax(&w.maxQueued, int64(size))
		w.arm(size)
	}
}

// checkDeep arms e's watch if queue, a pool's channel, will be at the first
// of WarnLevels or deeper once the task about to be sent is in it. It reads
// the channel's header before the send, which takes that line anyway;
// read after, it could find the line taken by the worker receiving.
func checkDeep(e *executor, queue chan Task) {
	if size := cap(queue); size > 0 {
		if q := len(queue) + 1; q*100 >= WarnLevels[0]*size {
			e.watch.arm(q)
		}
	}
}

// chanState is queueState for a backend whose queue is the one channel.
func chanState(queue chan Task) (queued, size, fullest int) {
	if size = cap(queue); size > 0 {
		queued = len(queue)
		return queued, size, queued * 100 / size
	}
	return 0, 0, 0
}

// arm has the watcher sample the pool's queue, which a submission found
// depth deep, from now until it calms. Arming one already armed reads armed
// and writes nothing.
func (w *queueWatch) arm(depth int) {
	if w.armed.Load() || w.queue == nil || !w.armed.CompareAndSwap(false, true) {
		return
	}
	raiseMax(&w.maxQueued, int64(depth))
	watcher.mu.Lock()
	watcher.pools = append(watcher.pools, w)
	if !watcher.running {
		watcher.running = true
		go watchLoop()
	}
	watcher.mu.Unlock()
}

// raiseMax sets m to v if v is more.
func raiseMax(m *atomic.Int64, v int64) {
	for {
		cur := m.Load()
		if v <= cur || m.CompareAndSwap(cur, v) {
			return
		}
	}
}

// sample looks at the pool's queue once, for the watcher, and reports whether
// the pool has calmed, or stopped, and should be disarmed.
func (w *queueWatch) sample(now time.Time) (calmed bool) {
	if w.stopped.Load() || w.queue == nil {
		return true
	}
	q, size, fullest := w.queue.queueState()
	if size <= 0 {
		return true
	}
	raiseMax(&w.maxQueued, int64(q))
	if r := w.rejected.Load(); r != w.rejectsSeen {
		// A TryGo found the queue full since the last sample, which the
		// sample itself may have missed.
		w.rejectsSeen, q, fullest = r, size, 100
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	reached := levelOf(q, size, 0)
	// A level is left once the queue falls warnHysteresis below it.
	if kept := levelOf(q, size, warnHysteresis); kept < w.level {
		w.level = kept
	}
	for w.level < reached {
		w.level++
		w.warnings[w.level].Add(1)
		if now.Sub(w.lastWarn[w.level]) < warnEvery {
			w.held[w.level]++
			continue
		}
		held := w.held[w.level]
		w.lastWarn[w.level], w.held[w.level] = now, 0
		args := []any{
			"pool", w.executor.name, "id", w.executor.id, "level", WarnLevels[w.level],
			"queued", q, "queueSize", size, "maxQueued", w.maxQueued.Load(),
			"workers", w.workers(), "rejected", w.rejected.Load(), "held", held,
		}
		if WarnLevels[w.level] >= 100 {
			log.Error("taskpool: queue full", args...)
		} else {
			log.Warn("taskpool: queue filling", args...)
		}
	}
	// The pool calms by the fuller of its queue and its fullest shard, since
	// a shard that deep arms it again.
	share := max(q*100/size, fullest)
	calmed = calmFor(&w.calmSince, share < WarnLevels[0]-warnHysteresis, now, releaseCalm)
	if calmFor(&w.quietSince, share < WarnLevels[0] && w.level < 0, now, releaseQuiet) {
		calmed = true
	}
	if calmed {
		w.calmSince, w.quietSince = time.Time{}, time.Time{}
	}
	return calmed
}

// calmFor keeps in since when calm began, and reports whether it has lasted
// d by now.
func calmFor(since *time.Time, calm bool, now time.Time, d time.Duration) bool {
	if !calm {
		*since = time.Time{}
		return false
	}
	if since.IsZero() {
		*since = now
	}
	return now.Sub(*since) >= d
}

// levelOf is the index of the highest of WarnLevels that queued of size
// reaches once slack percent of size is added to it, or -1.
func levelOf(queued, size, slack int) int {
	level := -1
	for i, pct := range WarnLevels {
		if (queued*100 + slack*size) >= pct*size {
			level = i
		}
	}
	return level
}

// watcher holds the armed pools, and whether a watchLoop runs for them. A
// pool is added by the submission that arms it and removed, with armed
// cleared, by the watcher under mu, so a submission that arms the pool again
// after adds it after the removal, and one watchLoop runs while any is armed.
var watcher struct {
	mu      sync.Mutex
	pools   []*queueWatch
	running bool
}

// watchLoop samples every armed pool every watchInterval, disarms the ones
// that have calmed, and returns once none is left armed.
func watchLoop() {
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()
	var pools, calmed []*queueWatch
	for now := range ticker.C {
		watcher.mu.Lock()
		pools = append(pools[:0], watcher.pools...)
		watcher.mu.Unlock()
		calmed = calmed[:0]
		for _, w := range pools {
			if w.sample(now) {
				calmed = append(calmed, w)
			}
		}
		clear(pools)
		if !disarm(calmed) {
			clear(calmed)
			return
		}
		clear(calmed)
	}
}

// disarm takes the pools calmed off the watcher, and reports whether any is
// left armed; when none is, the watcher is marked not running, and its loop
// returns.
func disarm(calmed []*queueWatch) bool {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if len(calmed) > 0 {
		kept := watcher.pools[:0]
		for _, w := range watcher.pools {
			if !contains(calmed, w) {
				kept = append(kept, w)
				continue
			}
			w.armed.Store(false)
		}
		clear(watcher.pools[len(kept):])
		watcher.pools = kept
	}
	if len(watcher.pools) == 0 {
		watcher.running = false
		return false
	}
	return true
}

func contains(ws []*queueWatch, w *queueWatch) bool {
	for _, x := range ws {
		if x == w {
			return true
		}
	}
	return false
}
