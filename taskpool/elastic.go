package taskpool

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// idleLinger is how long a worker stays available after finishing its task
// before giving up its slot. A worker that exits the moment the queue looks
// empty is replaced by a fork on the very next submission, and a forked
// goroutine starts on a fresh stack that has to grow again through the whole
// handler chain: before workers lingered, newstack and copystack were 29% of a
// 100k-connection echo profile. Submissions arrive in epoll-round bursts, so a
// short linger is enough to carry a worker from one burst to the next.
const idleLinger = 50 * time.Millisecond

// The states of an elasticWorker, which the worker and whoever takes it off
// the idle stack move it between.
const (
	workerBusy int32 = iota
	workerIdle
	workerRetired
)

// elasticWorker is a lingering worker's own wake-up. Whoever takes it off the
// idle stack, moving it from idle to busy, sends it one task, and nil tells
// it to leave; the buffer of one means the send never blocks.
type elasticWorker struct {
	wake  chan Task
	state atomic.Int32
	home  *elasticShard
}

// elasticPool is nbio v1.7.0's taskpool with lingering workers: a submission
// forks a worker while a slot is free and queues the task otherwise, and a
// worker that runs out of tasks waits idleLinger for another one before it
// gives its slot back.
//
// A lingering worker waits on its own wake-up channel and its own timer, and
// on nothing it shares with the other workers. Thousands of workers waiting
// in a select on channels they all share made every one of them lock those
// channels each time it parked or woke, and that locking was the pool's
// cost. The lingering workers sit instead on a stack, the one parked last on
// top, where a submission takes the warmest of them and sends it its task.
// A worker whose linger runs out retires by moving its own state from idle
// to retired; if a submission took it first, the state is busy by then and
// the worker waits for the task on its way instead. A retired worker stays
// on the stack as a tombstone, skipped by whoever pops it, until enough of
// them pile up for the stack to be compacted.
//
// The dispatcher takes queued tasks to workers and never runs one itself, so
// a blocking task cannot stop the queue from being consumed. It hands a task
// to a lingering worker if there is one, forks if a slot is free, and waits
// otherwise for either, told by a worker that parks or one that leaves.
type elasticPool struct {
	executor   *executor
	maxWorkers int64
	tasks      chan Task
	// kicked wakes the dispatcher when it waits for a worker or a slot.
	kicked chan struct{}
	closed chan struct{}
	// shards split the idle stack and the count of pending tasks, one for
	// each P, so that the submissions and workers of different cores do not
	// queue on one lock or bounce one counter's line between them.
	shards []elasticShard

	_ cacheLinePad
	// concurrent counts the workers, each of which holds an execution slot.
	concurrent atomic.Int64
	waiting    atomic.Bool
	stopped    atomic.Bool
	_          cacheLinePad

	stopOnce sync.Once
	workerWG sync.WaitGroup
}

// elasticShard is one part of the idle stack, and of the count of tasks
// accepted and not yet run, which stop waits out. A worker parks on its home
// shard and counts its tasks out there; a submission counts its task in on
// the shard it starts looking for a worker at. So one shard's count may go
// negative while another's stays positive, and only their sum means anything.
type elasticShard struct {
	_       cacheLinePad
	pending atomic.Int64
	// count is the lingering workers on the stack, not the tombstones, so
	// that a submission can skip the lock when there are none.
	count atomic.Int32
	mu    sync.Mutex
	idle  []*elasticWorker
	// dead counts the tombstones on the stack.
	dead int
}

func newElasticPool(executor *executor, maxConcurrent, queueSize int) *elasticPool {
	p := &elasticPool{
		executor: executor, maxWorkers: int64(maxConcurrent),
		tasks: make(chan Task, queueSize), kicked: make(chan struct{}, 1),
		closed: make(chan struct{}), shards: make([]elasticShard, runtime.GOMAXPROCS(0)),
	}
	p.workerWG.Add(1)
	go p.dispatch()
	return p
}

// acquire tries to take an execution slot.
func (p *elasticPool) acquire() bool {
	if p.concurrent.Add(1) <= p.maxWorkers {
		return true
	}
	p.concurrent.Add(-1)
	return false
}

// kick wakes the dispatcher if it is waiting for a worker or a slot.
func (p *elasticPool) kick() {
	if p.waiting.Load() {
		select {
		case p.kicked <- struct{}{}:
		default:
		}
	}
}

// release gives back an execution slot.
func (p *elasticPool) release() {
	p.concurrent.Add(-1)
	p.kick()
}

// popIdle takes a lingering worker off the stacks, moving it to busy, or
// reports nil when there is none. It looks from shard start on, and on each
// shard takes the worker that parked last.
func (p *elasticPool) popIdle(start int) *elasticWorker {
	n := len(p.shards)
	for i := 0; i < n; i++ {
		if w := p.shards[(start+i)%n].pop(); w != nil {
			return w
		}
	}
	return nil
}

func (s *elasticShard) pop() *elasticWorker {
	if s.count.Load() == 0 {
		return nil
	}
	s.mu.Lock()
	for n := len(s.idle); n > 0; n = len(s.idle) {
		w := s.idle[n-1]
		s.idle[n-1] = nil
		s.idle = s.idle[:n-1]
		if w.state.CompareAndSwap(workerIdle, workerBusy) {
			s.count.Add(-1)
			s.mu.Unlock()
			return w
		}
		s.dead--
	}
	s.mu.Unlock()
	return nil
}

// pushIdle puts w on its home shard's stack as a lingering worker.
func (p *elasticPool) pushIdle(w *elasticWorker) {
	w.state.Store(workerIdle)
	s := w.home
	s.mu.Lock()
	s.idle = append(s.idle, w)
	s.mu.Unlock()
	s.count.Add(1)
	p.kick()
}

// retire moves w from idle to retired, leaving it on its stack as a
// tombstone, and reports false if a submission took it first.
func (p *elasticPool) retire(w *elasticWorker) bool {
	if !w.state.CompareAndSwap(workerIdle, workerRetired) {
		return false
	}
	s := w.home
	s.count.Add(-1)
	s.mu.Lock()
	s.dead++
	if s.dead > len(s.idle)/2 {
		live := s.idle[:0]
		for _, w := range s.idle {
			if w.state.Load() != workerRetired {
				live = append(live, w)
			}
		}
		clear(s.idle[len(live):])
		s.idle, s.dead = live, 0
	}
	s.mu.Unlock()
	return true
}

// pendingTasks sums the shards' counts of pending tasks. Once stopped is set
// no count rises for good, so a sum read shard by shard is never below the
// true count at the end of the read, and a sum of zero means every task ran.
func (p *elasticPool) pendingTasks() int64 {
	var total int64
	for i := range p.shards {
		total += p.shards[i].pending.Load()
	}
	return total
}

func (p *elasticPool) submit(task Task) bool {
	// A submission counts itself in before it looks at stopped, and stop sets
	// stopped before it looks at the count, so stop waits for every task a
	// submission saw the pool running to accept.
	start := rand.IntN(len(p.shards))
	shard := &p.shards[start]
	shard.pending.Add(1)
	if p.stopped.Load() {
		shard.pending.Add(-1)
		return false
	}
	// Hand the task to a lingering worker before forking a new one. Forking
	// is only worth its fresh stack when there is nobody waiting.
	if w := p.popIdle(start); w != nil {
		w.wake <- task
		return true
	}
	if p.fork(task) {
		return true
	}
	p.tasks <- task
	return true
}

func (p *elasticPool) submitBatch(tasks []Task) int {
	for i, task := range tasks {
		if !p.submit(task) {
			return i
		}
	}
	return len(tasks)
}

// stop refuses new tasks, waits for the accepted ones to run, and then tells
// the dispatcher and the lingering workers to go without waiting out their
// linger.
func (p *elasticPool) stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		for p.pendingTasks() > 0 {
			time.Sleep(100 * time.Microsecond)
		}
		close(p.closed)
		// A worker that parks after this sweep sees stopped and leaves by
		// itself; see run.
		for w := p.popIdle(0); w != nil; w = p.popIdle(0) {
			w.wake <- nil
		}
		p.workerWG.Wait()
	})
}

func (p *elasticPool) workerCount() int { return int(p.concurrent.Load()) }

func (p *elasticPool) attrs() []any {
	return []any{
		"workers", p.workerCount(), "maxWorkers", p.maxWorkers,
		"queueSize", cap(p.tasks), "idleLinger", idleLinger,
	}
}

func (p *elasticPool) fork(first Task) bool {
	if !p.acquire() {
		return false
	}
	p.workerWG.Add(1)
	go p.run(first)
	return true
}

func (p *elasticPool) run(task Task) {
	defer p.workerWG.Done()
	defer p.release()
	self := &elasticWorker{wake: make(chan Task, 1), home: &p.shards[rand.IntN(len(p.shards))]}
	// One timer per worker, reset per wait, rather than a fresh timer per idle
	// spell: this is the hot path the linger exists to keep warm. Since Go
	// 1.23 Reset discards a value the timer may have left unread.
	timer := time.NewTimer(idleLinger)
	defer timer.Stop()
	for {
		p.execute(self, task)
		// A single-channel poll first: it is the cheapest way to take the next
		// task while the queue has one, and it never blocks.
		select {
		case task = <-p.tasks:
			continue
		default:
		}
		p.pushIdle(self)
		if p.stopped.Load() && p.retire(self) {
			// Parked after stop swept the stack, so nobody would wake it.
			return
		}
		timer.Reset(idleLinger)
		select {
		case task = <-self.wake:
		case <-timer.C:
			if p.retire(self) {
				return
			}
			// A submission took this worker off the stack as its linger ran
			// out, and its task is on the way.
			task = <-self.wake
		}
		if task == nil {
			// Told to leave by stop.
			return
		}
	}
}

// dispatch takes queued tasks to lingering workers, or to new ones.
func (p *elasticPool) dispatch() {
	defer p.workerWG.Done()
	for {
		select {
		case task := <-p.tasks:
			if !p.place(task) {
				return
			}
		case <-p.closed:
			return
		}
	}
}

// place hands task to a lingering worker or forks one for it, waiting for
// either while there is neither. It reports false if the pool closed first.
func (p *elasticPool) place(task Task) bool {
	for {
		// Raised before looking, so that a worker parking or leaving after
		// the look sees it and kicks.
		p.waiting.Store(true)
		if w := p.popIdle(rand.IntN(len(p.shards))); w != nil {
			p.waiting.Store(false)
			w.wake <- task
			return true
		}
		if p.fork(task) {
			p.waiting.Store(false)
			return true
		}
		select {
		case <-p.kicked:
		case <-p.closed:
			return false
		}
	}
}

func (p *elasticPool) execute(w *elasticWorker, task Task) {
	defer w.home.pending.Add(-1)
	p.executor.call(task)
}
