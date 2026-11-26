package taskpool

import (
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/lesismal/fib/log"
)

// A pool's queue is where its tasks wait for a worker, and what it holds
// says how far the pool is behind: a queue that fills turns TryGo away and
// makes Go wait. A watcher looks at every pool's queue every watchInterval,
// so that one which fills shows: in QueueStats, and in a warning logged as
// it rises past each of WarnLevels.
//
// The watcher samples rather than counting on every submission, which would
// have each submission write a counter every core shares: MaxQueued is the
// deepest queue a sample found, or the whole queue once TryGo has been
// turned away, so a burst that came and went between two samples may not
// show in it.

// WarnLevels are the shares of a pool's queue, in percent, that the watcher
// warns about as the queue rises past each. A level warns again only once
// the queue has fallen below it by warnHysteresis percent, so a queue that
// hovers around a level does not warn on every sample.
var WarnLevels = [...]int{50, 75, 90, 100}

// warnHysteresis is how far, in percent of the queue, a queue falls below a
// level it has warned at before it may warn there again.
const warnHysteresis = 10

// watchInterval is how often the watcher samples the pools' queues, and
// warnEvery the least time between two warnings a pool logs at one level;
// the ones held back meanwhile are counted, and the next says how many.
var (
	watchInterval = 100 * time.Millisecond
	warnEvery     = 10 * time.Second
)

// QueueStats describes a pool's queue; see TaskPool.QueueStats.
type QueueStats struct {
	// Queued is how many tasks wait for a worker now, and QueueSize how many
	// the queue holds. Both are 0 for a ModeInline pool, which has none.
	Queued    int
	QueueSize int
	// MaxQueued is the deepest the queue has been seen: by the watcher,
	// which samples it, or by a TryGo turned away, which found it full.
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
}

// queueWatch is what a pool keeps of its queue's history.
type queueWatch struct {
	maxQueued atomic.Int64
	rejected  atomic.Uint64
	warnings  [len(WarnLevels)]atomic.Uint64

	// Only the watcher touches the rest. rejectsSeen is rejected as the
	// last sample saw it; level is the index of the highest of WarnLevels
	// warned at and not yet left, or -1; lastWarn and held, per level, when
	// it last logged and how many warnings it has held back since.
	mu          sync.Mutex
	rejectsSeen uint64
	level       int
	lastWarn    [len(WarnLevels)]time.Time
	held        [len(WarnLevels)]int
}

// QueueStats reports the pool's queue: what waits in it now, the deepest it
// has been, and the warnings and turn-aways that say the pool fell behind.
func (tp *TaskPool) QueueStats() QueueStats {
	q, size := tp.queueLen()
	s := QueueStats{
		Queued:    q,
		QueueSize: size,
		MaxQueued: int(tp.watch.maxQueued.Load()),
		Rejected:  tp.watch.rejected.Load(),
	}
	for i := range s.Warnings {
		s.Warnings[i] = tp.watch.warnings[i].Load()
	}
	if q > s.MaxQueued {
		s.MaxQueued = q
	}
	return s
}

// Queued reports how many tasks wait for a worker now, and the queue's size.
func (tp *TaskPool) Queued() (queued, size int) { return tp.queueLen() }

func (tp *TaskPool) queueLen() (queued, size int) {
	if r, ok := tp.backend.(queueReporter); ok {
		return r.queueLen()
	}
	return 0, 0
}

// rejectedFull records a TryGo turned away by a full queue: the queue was as
// deep as it goes, and the watcher's next sample warns of it.
func (tp *TaskPool) rejectedFull() {
	tp.watch.rejected.Add(1)
	if _, size := tp.queueLen(); size > 0 {
		raiseMax(&tp.watch.maxQueued, int64(size))
	}
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

// sample looks at the pool's queue once, for the watcher.
func (tp *TaskPool) sample(now time.Time) {
	q, size := tp.queueLen()
	if size <= 0 {
		return
	}
	raiseMax(&tp.watch.maxQueued, int64(q))
	w := &tp.watch
	if r := w.rejected.Load(); r != w.rejectsSeen {
		// A TryGo found the queue full since the last sample, which the
		// sample itself may have missed.
		w.rejectsSeen, q = r, size
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
			"pool", tp.executor.name, "id", tp.executor.id, "level", WarnLevels[w.level],
			"queued", q, "queueSize", size, "maxQueued", w.maxQueued.Load(),
			"workers", tp.backend.workerCount(), "rejected", w.rejected.Load(), "held", held,
		}
		if WarnLevels[w.level] >= 100 {
			log.Error("taskpool: queue full", args...)
		} else {
			log.Warn("taskpool: queue filling", args...)
		}
	}
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

// The pools the watcher looks at, held weakly so that a pool nothing else
// holds is collected; one that has stopped, or been collected, is dropped.
var (
	watchMu    sync.Mutex
	watched    []weak.Pointer[TaskPool]
	watchStart sync.Once
)

// watchPool has the watcher look at tp's queue from now on.
func watchPool(tp *TaskPool) {
	tp.watch.level = -1
	if _, ok := tp.backend.(queueReporter); !ok {
		return
	}
	watchMu.Lock()
	watched = append(watched, weak.Make(tp))
	watchMu.Unlock()
	watchStart.Do(func() { go watchLoop() })
}

// watchLoop samples every pool watched, every watchInterval, for as long as
// the process runs: one goroutine for all of them.
func watchLoop() {
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()
	var pools []*TaskPool
	for now := range ticker.C {
		pools = livePools(pools[:0])
		for _, tp := range pools {
			tp.sample(now)
		}
		clear(pools)
	}
}

// livePools appends to pools the pools still watched, and drops those that
// have stopped or been collected.
func livePools(pools []*TaskPool) []*TaskPool {
	watchMu.Lock()
	defer watchMu.Unlock()
	kept := watched[:0]
	for _, p := range watched {
		tp := p.Value()
		if tp == nil || tp.stopped.Load() {
			continue
		}
		kept = append(kept, p)
		pools = append(pools, tp)
	}
	clear(watched[len(kept):])
	watched = kept
	return pools
}
