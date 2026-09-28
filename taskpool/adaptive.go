package taskpool

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe" // for go:linkname
)

// defaultShrinkInterval is how often an adaptive pool looks for workers it
// did not need. It is long next to the elastic pool's idle linger on purpose:
// a resident worker is kept for its warm stack, and giving one up only to fork
// its replacement a moment later is the churn this pool exists to avoid.
const defaultShrinkInterval = time.Second

// AdaptiveConfig sizes a ModeAdaptive pool.
type AdaptiveConfig struct {
	// Name labels the pool in what it logs.
	Name string
	// MinWorkers is the resident floor: the pool starts this many workers and
	// never retires below it. Zero lets the pool retire every worker while it
	// is idle, and start them again when work arrives.
	MinWorkers int
	// MaxWorkers is the ceiling the pool grows to under load. It must be
	// greater than zero and not below MinWorkers.
	MaxWorkers int
	// QueueSize bounds the tasks waiting for a worker. A submission that finds
	// the queue full waits for room.
	QueueSize int
	// ShrinkInterval is how often the pool retires workers that stayed idle
	// for the whole of the previous interval. Zero means one second.
	ShrinkInterval time.Duration
}

// NewAdaptive creates a ModeAdaptive pool.
//
// Its workers park between tasks, but their number follows
// the load. Tasks that find every worker busy and none on its way to the
// queue start a new one, up to MaxWorkers, so a burst of tasks that block is
// absorbed by more workers rather than a longer queue, while short tasks are
// left to the workers already running them. Once per ShrinkInterval the pool
// looks at the fewest workers that sat idle at any moment of the interval:
// those were never needed, and half of them are retired, never taking the pool
// below MinWorkers. Halving rather than retiring them all lets a pool that
// grew for a burst step back down over a few intervals instead of dropping
// the workers the next burst would want. See adaptivePool for how workers are
// woken.
func NewAdaptive(config AdaptiveConfig) *TaskPool {
	validateAdaptiveRange(config.MinWorkers, config.MaxWorkers)
	if config.QueueSize < 0 {
		panic("taskpool: queueSize must not be negative")
	}
	return start(config.Name, ModeAdaptive, func(executor *executor) backend {
		return newAdaptiveBackend(executor, config)
	})
}

func validateAdaptiveRange(minWorkers, maxWorkers int) {
	if maxWorkers <= 0 {
		panic("taskpool: maxWorkers must be greater than zero")
	}
	if minWorkers < 0 || minWorkers > maxWorkers {
		panic("taskpool: minWorkers must be between zero and maxWorkers")
	}
}

// adaptiveBackend spreads an adaptive pool over shards, since one queue and
// lock shared by every producer and worker would be the limit on many cores,
// and runs the one goroutine that shrinks all of them.
//
// The shards share one ceiling rather than splitting it, and a shard that
// needs a worker the ceiling will not start takes a parked one from another
// shard, so that tasks do not wait on one shard while workers sit idle on
// the rest. A submission goes to the lighter of two shards; see pick.
type adaptiveBackend struct {
	shards      []*adaptivePool
	budget      workerBudget
	interval    time.Duration
	workerWG    sync.WaitGroup
	stopJanitor chan struct{}
	janitorDone chan struct{}
	stopOnce    sync.Once
}

func newAdaptiveBackend(executor *executor, config AdaptiveConfig) *adaptiveBackend {
	interval := config.ShrinkInterval
	if interval <= 0 {
		interval = defaultShrinkInterval
	}
	queueSize := config.QueueSize
	shards := shardCount(config.MaxWorkers)
	if queueSize < shards {
		// Every shard needs a slot to queue into.
		queueSize = shards
	}
	b := &adaptiveBackend{interval: interval, stopJanitor: make(chan struct{}), janitorDone: make(chan struct{})}
	for i := 0; i < shards; i++ {
		p := newAdaptivePool(b, executor, share(queueSize, shards, i))
		b.shards = append(b.shards, p)
	}
	b.resize(config.MinWorkers, config.MaxWorkers)
	go b.janitor(interval)
	return b
}

// share is shard i's part of total, with the remainder spread over the
// leading shards so the parts add up to total.
func share(total, shards, i int) int {
	part := total / shards
	if i < total%shards {
		part++
	}
	return part
}

// pick chooses the shard a submission goes to: the one of the P the
// submitter runs on, so that a submitter keeps feeding the shard whose
// workers are already running its tasks and take the next without being
// woken, and submitters on different Ps do not contend for one shard or one
// counter. A shard that has tasks queued and no parked worker to take them
// is weighed against another, chosen at random, and the one with the less
// work waiting on it is taken, so that a shard its tasks have tied up, or
// whose queue is full, is not handed more while others are idle.
func (b *adaptiveBackend) pick() *adaptivePool {
	n := len(b.shards)
	i := procPin() % n
	procUnpin()
	p := b.shards[i]
	if n == 1 || p.idleCount.Load() > 0 || p.ring.len() == 0 {
		return p
	}
	if q := b.shards[(i+1+rand.IntN(n-1))%n]; q.backlog() < p.backlog() {
		return q
	}
	return p
}

func (b *adaptiveBackend) submit(task Task) bool { return b.pick().submit(task) }

func (b *adaptiveBackend) submitBatch(tasks []Task) int { return b.pick().submitBatch(tasks) }

func (b *adaptiveBackend) stop() {
	b.stopOnce.Do(func() {
		close(b.stopJanitor)
		<-b.janitorDone
		for _, p := range b.shards {
			p.stop()
		}
		b.workerWG.Wait()
	})
}

func (b *adaptiveBackend) workerCount() int {
	total := 0
	for _, p := range b.shards {
		total += p.workerCount()
	}
	return total
}

func (b *adaptiveBackend) attrs() []any {
	minWorkers, queueSize := 0, 0
	for _, p := range b.shards {
		p.mu.Lock()
		minWorkers += p.minWorkers
		p.mu.Unlock()
		queueSize += int(p.ring.limit)
	}
	return []any{
		"shards", len(b.shards), "workers", b.workerCount(), "minWorkers", minWorkers,
		"maxWorkers", int(b.budget.ceiling.Load()), "queueSize", queueSize, "shrinkInterval", b.interval,
	}
}

// resize splits a new floor over the shards and moves the ceiling they
// share. The shard count was fixed when the pool was built, so a ceiling
// below it still leaves every shard one worker, which a shard needs to run
// what lands on it.
func (b *adaptiveBackend) resize(minWorkers, maxWorkers int) {
	b.budget.ceiling.Store(int32(maxWorkers))
	for i, p := range b.shards {
		p.resize(share(minWorkers, len(b.shards), i))
	}
}

// adopt moves a parked worker from another shard to p, for p to run what the
// ceiling will not start a worker for. It takes one from any shard with more
// than its first worker. The floor is the pool's, not a shard's, so a worker
// taken from a shard at its part of the floor takes that part along, and p
// keeps it resident instead. The worker comes back counted out of its shard
// and into p; the caller sends it to p. It reports nil when no shard has one
// to spare.
func (b *adaptiveBackend) adopt(p *adaptivePool) *adaptiveWorker {
	n := len(b.shards)
	from := rand.IntN(n)
	for i := 0; i < n; i++ {
		q := b.shards[(from+i)%n]
		if q == p || q.idleCount.Load() == 0 {
			continue
		}
		q.mu.Lock()
		var w *adaptiveWorker
		floor := false
		if !q.stopped.Load() && q.workers.Load() > 1 {
			w = q.popIdleLocked()
		}
		if w != nil {
			if floor = int(q.workers.Load()) <= q.minWorkers; floor {
				q.minWorkers--
			}
			q.count(-1)
		}
		q.mu.Unlock()
		if w == nil {
			continue
		}
		if floor {
			p.mu.Lock()
			p.minWorkers++
			p.mu.Unlock()
		}
		// It takes the budget q just gave back, or is p's first worker.
		p.count(1)
		return w
	}
	return nil
}

// rescue sends a worker to each shard other than except that has tasks
// queued and none on the way to them. Such a shard asked for a worker while
// the budget was spent and got none, and its tasks wait for one of its own
// busy workers to finish, which for tasks that block may be never. So
// whoever gives budget back, a worker retiring over the ceiling or a shrink,
// passes it on to the shards that ran short of it.
func (b *adaptiveBackend) rescue(except *adaptivePool) {
	for _, p := range b.shards {
		if p != except && p.ring.len() > 0 && p.waking.Load() == 0 {
			p.kick()
		}
	}
}

// workerBudget is the ceiling an adaptive pool's shards share. A shard with
// no worker may always start one, so that a task that lands on it always has
// a worker to run it; a shard's further workers draw on the budget, which is
// the ceiling less the shards' first workers. The first worker of a shard
// that had none may thus take the pool over its ceiling for a while, until a
// shard with workers to spare lets one go; see retireOverCeiling.
type workerBudget struct {
	_ cacheLinePad
	// extra counts the workers past each shard's first, and firsts the
	// shards that have a worker.
	extra   atomic.Int32
	firsts  atomic.Int32
	ceiling atomic.Int32
	_       cacheLinePad
}

// take draws one worker past a shard's first on the budget, reporting false
// when it is spent.
func (b *workerBudget) take() bool {
	for {
		extra := b.extra.Load()
		if extra >= b.ceiling.Load()-b.firsts.Load() {
			return false
		}
		if b.extra.CompareAndSwap(extra, extra+1) {
			return true
		}
	}
}

// room reports whether take would draw a worker now.
func (b *workerBudget) room() bool {
	return b.extra.Load() < b.ceiling.Load()-b.firsts.Load()
}

// excess is how many workers the pool runs over its ceiling.
func (b *workerBudget) excess() int32 {
	return b.extra.Load() + b.firsts.Load() - b.ceiling.Load()
}

func (b *adaptiveBackend) janitor(interval time.Duration) {
	defer close(b.janitorDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, p := range b.shards {
				p.shrink()
			}
			// Shrinking gave budget back, and a shard may have been left
			// waiting for it; see rescue.
			b.rescue(nil)
		case <-b.stopJanitor:
			return
		}
	}
}

// maxWaking bounds the workers a shard has woken or started that have not yet
// reached its queue. See adaptivePool. One fans out a hop at a time, which is
// slow to spread a burst of blocking tasks; more than two wakes workers that
// arrive to find the queue drained.
const maxWaking = 2

// adaptiveWorker is a parked worker's own wake-up. A worker is taken off the
// idle stack by whoever wakes it, so it is never woken twice and a send never
// blocks: a shard sends it to that shard's queue, which is another shard's
// when adopt moved it, and nil retires it.
type adaptiveWorker struct{ wake chan *adaptivePool }

func newAdaptiveWorker() *adaptiveWorker {
	return &adaptiveWorker{wake: make(chan *adaptivePool, 1)}
}

// adaptivePool is one shard: a bounded queue, and workers that park on a
// stack of their own wake-ups between tasks.
//
// A worker is woken for the queue rather than for a task, and whichever
// worker reaches the queue takes the tasks there until it is empty. Waking
// one worker per task, as signalling a condition variable per task does,
// makes a batch of n tasks cost n wake-ups although a woken worker usually
// finds the batch half drained by the ones before it; every wake-up readies a
// goroutine and often a whole thread, and on a busy server that churn was
// most of the pool's CPU time.
// So a shard keeps at most maxWaking workers on their way to the queue, and
// each worker that takes a task and still sees more waiting than are coming
// for them wakes the next one. The pool fans out one worker per hop for as
// long as tasks are left behind, which is how fast it fans out for tasks that
// block, and stops as soon as the workers already running keep up, which is
// what short tasks need.
//
// Only a task that finds every worker busy and none coming grows the pool. A
// worker woken but not yet running counts as coming, so a burst grows the
// pool by the workers it keeps busy rather than by one per task.
//
// The stack is last in, first out: the worker woken is the one that parked
// last, whose stack and cache are warm, and the ones at the bottom are those
// the load has not needed for longest, which are the ones shrink retires.
//
// Submitters queue tasks and workers take them without the lock; the lock
// covers the idle stack and waiting for room. A busy shard's submitters and
// workers thus never queue behind each other, nor behind a worker parking: a
// mutex that a parking worker has waited on for long hands itself over in
// turn to every waiter, and a submitter taking it per task then parked and
// was woken per task. What the lock no
// longer orders, two checks cover, each made after the change it races with
// so that the sequentially consistent atomics let one side or the other see
// both: a worker that parks looks at the queue again once it is on the idle
// stack, and a waker that gives up its reservation looks at the idle stack
// again once it has.
type adaptivePool struct {
	backend  *adaptiveBackend
	budget   *workerBudget
	executor *executor
	// workerWG is the backend's, since a worker may move between shards.
	workerWG *sync.WaitGroup
	ring     *taskRing
	_        cacheLinePad
	// The counters a wake-up reads and writes share a line, so that a hop
	// of the fan-out moves one line between cores rather than one per
	// counter.
	//
	// waking counts the workers woken or started that have not reached the
	// queue yet. Until they do they are neither idle nor busy: they will take
	// the next tasks queued without being told.
	waking atomic.Int32
	// workers counts the live workers, including those just started and not
	// yet running, and excluding those told to retire. Those past the first
	// are on the budget; see addWorker.
	workers   atomic.Int32
	idleCount atomic.Int32
	// roomWanted is set by a submitter that waits for room, and cleared by
	// the one worker that wakes the waiters.
	roomWanted atomic.Bool
	_          cacheLinePad
	// What every submission reads and writes has a line of its own, so that
	// submitting does not move the line the wake-ups use.
	//
	// stopped is set under the lock, and read without it too.
	stopped atomic.Bool
	// submitting counts the submissions in flight, which stop waits out
	// before it sets draining.
	submitting atomic.Int32
	// draining tells the workers to leave once they find the queue empty.
	// It is set under the lock, after stopped and once no submission can
	// queue another task.
	draining atomic.Bool
	_        cacheLinePad

	mu      sync.Mutex
	notFull *sync.Cond
	// idle holds the parked workers, the one parked last on top. idleCount
	// mirrors its length for the checks made without the lock.
	idle       []*adaptiveWorker
	minWorkers int
	// lowIdle is the fewest workers parked at any moment since the last
	// shrink: the ones at the bottom of the stack that no task reached.
	lowIdle int
}

func newAdaptivePool(b *adaptiveBackend, executor *executor, queueSize int) *adaptivePool {
	p := &adaptivePool{backend: b, budget: &b.budget, executor: executor, workerWG: &b.workerWG,
		ring: newTaskRing(queueSize)}
	p.notFull = sync.NewCond(&p.mu)
	return p
}

// backlog is how much work waits on the shard for a worker: the tasks queued,
// less the parked workers that would take them at once.
func (p *adaptivePool) backlog() int { return p.ring.len() - int(p.idleCount.Load()) }

// addWorker counts a new worker in, drawing on the budget unless it is the
// shard's first. It reports false, counting nothing, when the budget is spent.
//
// What it draws it keeps across a lost race for the count, which another
// worker joining or leaving the shard causes, rather than handing it back
// and taking it again: the budget is shared, and a shard that looked at it in
// between would find it spent although it was not, and leave its queued
// tasks with no worker coming for them.
func (p *adaptivePool) addWorker() bool {
	taken := false
	for {
		workers := p.workers.Load()
		if workers == 0 {
			if taken {
				// The shard lost its last worker meanwhile, so this one is
				// its first and not on the budget.
				p.budget.extra.Add(-1)
				taken = false
			}
			if p.workers.CompareAndSwap(0, 1) {
				p.budget.firsts.Add(1)
				return true
			}
			continue
		}
		if !taken {
			if !p.budget.take() {
				return false
			}
			taken = true
		}
		if p.workers.CompareAndSwap(workers, workers+1) {
			return true
		}
	}
}

// count moves the shard's worker count by delta, keeping the budget's counts
// of first and further workers in step with it.
func (p *adaptivePool) count(delta int32) {
	after := p.workers.Add(delta)
	before := after - delta
	p.budget.extra.Add(max(0, after-1) - max(0, before-1))
	if (before > 0) != (after > 0) {
		if after > 0 {
			p.budget.firsts.Add(1)
		} else {
			p.budget.firsts.Add(-1)
		}
	}
}

// wakeups is the workers a caller has reserved and sets going once it has
// released the lock, so that the wake-ups and forks do not extend the hold
// time the submitters queue behind.
type wakeups struct {
	workers [maxWaking]*adaptiveWorker
	woken   int
	spawn   int
	// adopt records that the ceiling kept a worker from starting, so that
	// kick, which may take the other shards' locks, looks for one to adopt.
	adopt bool
}

func (w *wakeups) run(p *adaptivePool) {
	for i := 0; i < w.woken; i++ {
		w.workers[i].wake <- p
		w.workers[i] = nil
	}
	for ; w.spawn > 0; w.spawn-- {
		go p.worker(newAdaptiveWorker())
	}
	w.woken = 0
	if w.adopt {
		w.adopt = false
		p.kick()
	}
}

// reserveWaking claims a place among the workers on their way to the queue
// if the queue holds more tasks than are coming for them and fewer than
// maxWaking are.
func (p *adaptivePool) reserveWaking() bool {
	for {
		waking := p.waking.Load()
		if waking >= maxWaking || int(waking) >= p.ring.len() {
			return false
		}
		if p.waking.CompareAndSwap(waking, waking+1) {
			return true
		}
	}
}

// spawnReserve reserves a new worker if the ceiling allows one. The WaitGroup
// is raised with it, while the caller either holds the lock or is itself a
// counted worker, so that stop cannot find the count at zero before it.
func (p *adaptivePool) spawnReserve() bool {
	if !p.addWorker() {
		return false
	}
	p.workerWG.Add(1)
	return true
}

func (p *adaptivePool) popIdleLocked() *adaptiveWorker {
	n := len(p.idle)
	if n == 0 {
		return nil
	}
	worker := p.idle[n-1]
	p.idle[n-1] = nil
	p.idle = p.idle[:n-1]
	p.idleCount.Add(-1)
	p.lowIdle = min(p.lowIdle, n-1)
	return worker
}

// kickLocked sends workers to the queue until as many are on their way as it
// holds tasks, or maxWaking are. It takes a parked worker if there is one
// and otherwise starts one while the ceiling allows. The workers are only
// reserved here; the caller runs w after unlocking. Holding the lock, it
// needs no second look at the idle stack when it gives a reservation up: a
// worker can only park after it, and looks at the queue again when it does.
func (p *adaptivePool) kickLocked(w *wakeups) {
	for w.woken+w.spawn < maxWaking && p.reserveWaking() {
		if worker := p.popIdleLocked(); worker != nil {
			w.workers[w.woken] = worker
			w.woken++
		} else if p.spawnReserve() {
			w.spawn++
		} else {
			p.waking.Add(-1)
			w.adopt = true
			return
		}
	}
}

// kick is kickLocked for a submitter or a worker, which does not hold the
// lock and takes it only when there is a parked worker to wake.
func (p *adaptivePool) kick() {
	for p.reserveWaking() {
		if p.idleCount.Load() > 0 {
			p.mu.Lock()
			worker := p.popIdleLocked()
			p.mu.Unlock()
			if worker != nil {
				worker.wake <- p
				continue
			}
		}
		if p.spawnReserve() {
			go p.worker(newAdaptiveWorker())
			continue
		}
		if worker := p.backend.adopt(p); worker != nil {
			worker.wake <- p
			continue
		}
		p.waking.Add(-1)
		// A worker that parked since the idle stack was looked at may have
		// seen this reservation and left the queue to it, and budget given
		// back since it was looked at may have been left to it too: rescue
		// passes budget on only to a shard with nothing on the way, which
		// this reservation made this one look not to be.
		if p.idleCount.Load() == 0 && !p.budget.room() {
			return
		}
	}
}

// lowWater is how far the queue drains before the submitters waiting for
// room are woken. Waking one each time a cell frees would take the lock once
// per task, with the waiters queued behind it and filling the queue again one
// cell at a time; waking them all once half the queue is free lets them queue
// a run of tasks each.
func (p *adaptivePool) lowWater() int { return int(p.ring.limit / 2) }

// waitForRoomLocked parks a submitter until the queue has drained to its low
// water mark, making sure first that a worker is on its way to drain it. A
// worker that takes a task signals only if it sees the submitter counted, so
// the count goes up before the queue is looked at again; a waiter that finds
// the queue full is then woken by whichever worker takes it down to the mark.
func (p *adaptivePool) waitForRoomLocked() {
	var w wakeups
	p.kickLocked(&w)
	if w.woken > 0 || w.spawn > 0 || w.adopt {
		p.mu.Unlock()
		w.run(p)
		p.mu.Lock()
	}
	p.roomWanted.Store(true)
	if !p.stopped.Load() && p.ring.len() >= int(p.ring.limit) {
		p.notFull.Wait()
	}
}

// pushLocked queues task, waiting for room while the queue is full, and
// reports false if the pool stopped first.
func (p *adaptivePool) pushLocked(task Task) bool {
	for !p.stopped.Load() {
		if p.ring.push(task) {
			return true
		}
		p.waitForRoomLocked()
	}
	return false
}

// submit queues task without the lock, which it takes only to wait for room
// when the queue is full, and sends a worker to the queue for it the way a
// worker that took a task sends the next one; see kick.
func (p *adaptivePool) submit(task Task) bool {
	p.submitting.Add(1)
	defer p.submitting.Add(-1)
	if p.stopped.Load() {
		return false
	}
	if !p.ring.push(task) && !p.pushSlow(task) {
		return false
	}
	p.kick()
	return true
}

// pushSlow queues task once the queue was found full, waiting for room under
// the lock, and reports false if the pool stopped first.
func (p *adaptivePool) pushSlow(task Task) bool {
	p.mu.Lock()
	ok := p.pushLocked(task)
	p.mu.Unlock()
	return ok
}

// submitBatch queues tasks and then sends workers to the queue for them,
// which fan out from there. It returns how many tasks were accepted; a
// shorter count means the pool stopped mid-batch.
func (p *adaptivePool) submitBatch(tasks []Task) int {
	p.submitting.Add(1)
	defer p.submitting.Add(-1)
	submitted := 0
	for _, task := range tasks {
		if p.stopped.Load() || !p.ring.push(task) && !p.pushSlow(task) {
			break
		}
		submitted++
	}
	if submitted > 0 {
		p.kick()
	}
	return submitted
}

// retireOverCeiling counts the calling worker out if the pool runs more
// workers than its ceiling, which a resize that lowered it or another shard's
// first worker leaves it doing, and reports whether it did. A shard's first
// worker is not on the budget, so it stays.
func (p *adaptivePool) retireOverCeiling() bool {
	for p.budget.excess() > 0 {
		workers := p.workers.Load()
		if workers <= 1 {
			return false
		}
		if p.workers.CompareAndSwap(workers, workers-1) {
			p.budget.extra.Add(-1)
			return true
		}
	}
	return false
}

func (p *adaptivePool) worker(self *adaptiveWorker) {
	defer p.workerWG.Done()
	for {
		// This worker was on its way to the queue and has reached it.
		p.waking.Add(-1)
		for {
			if p.retireOverCeiling() {
				// Leave even with work queued, passing it on to a worker
				// that stays, and the budget on to a shard that ran short.
				p.kick()
				p.backend.rescue(p)
				return
			}
			task, ok := p.ring.pop()
			if !ok {
				break
			}
			p.kick()
			if p.roomWanted.Load() && p.ring.len() <= p.lowWater() && p.roomWanted.Swap(false) {
				p.mu.Lock()
				p.notFull.Broadcast()
				p.mu.Unlock()
			}
			p.executor.call(task)
		}
		p.mu.Lock()
		if p.draining.Load() {
			if p.ring.len() > 0 {
				// A task queued since the queue was found empty may have
				// been left to this worker by a submitter that saw it
				// running.
				p.mu.Unlock()
				p.waking.Add(1)
				continue
			}
			p.count(-1)
			p.mu.Unlock()
			return
		}
		if p.retireOverCeiling() {
			// A resize that ran while this worker was busy could not retire
			// it. A task queued since the queue was found empty may have
			// been left to it, so it is passed on, and the budget it gives
			// back on to a shard that ran short.
			var w wakeups
			p.kickLocked(&w)
			p.mu.Unlock()
			w.run(p)
			p.backend.rescue(p)
			return
		}
		p.idle = append(p.idle, self)
		p.idleCount.Add(1)
		// A task queued after the queue was found empty may have been left
		// to this worker by a submitter that saw it running.
		var w wakeups
		p.kickLocked(&w)
		p.mu.Unlock()
		w.run(p)
		next := <-self.wake
		if next == nil {
			// Retired by shrink, resize or stop, which counted it out.
			return
		}
		// Sent to its own shard, or to another that adopted it.
		p = next
	}
}

// retireLocked takes up to n workers off the bottom of the idle stack, the
// ones parked longest, and counts them out. The caller tells them to leave
// once it has released the lock.
func (p *adaptivePool) retireLocked(n int) []*adaptiveWorker {
	n = min(n, len(p.idle))
	if n <= 0 {
		return nil
	}
	retired := make([]*adaptiveWorker, n)
	copy(retired, p.idle)
	remaining := copy(p.idle, p.idle[n:])
	clear(p.idle[remaining:])
	p.idle = p.idle[:remaining]
	p.idleCount.Add(int32(-n))
	p.count(int32(-n))
	p.lowIdle = min(p.lowIdle, remaining)
	return retired
}

func dismiss(workers []*adaptiveWorker) {
	for _, w := range workers {
		w.wake <- nil
	}
}

// shrink retires half of the workers that stayed idle through the whole
// interval since the last call, keeping the pool at or above its floor.
func (p *adaptivePool) shrink() {
	p.mu.Lock()
	idle := p.lowIdle
	var retired []*adaptiveWorker
	if surplus := int(p.workers.Load()) - p.minWorkers; idle > 0 && surplus > 0 && !p.stopped.Load() {
		retired = p.retireLocked(min((idle+1)/2, surplus))
	}
	p.lowIdle = len(p.idle)
	p.mu.Unlock()
	dismiss(retired)
}

// resize moves the floor, once the backend has moved the ceiling. Raising the
// floor starts the missing workers at once; a ceiling lowered below the
// workers there are retires this shard's parked workers past its first now,
// while busy ones leave as they finish their task.
func (p *adaptivePool) resize(minWorkers int) {
	p.mu.Lock()
	p.minWorkers = minWorkers
	spawn := 0
	for !p.stopped.Load() && int(p.workers.Load()) < minWorkers && p.spawnReserve() {
		p.waking.Add(1)
		spawn++
	}
	retired := p.retireLocked(min(int(p.budget.excess()), int(p.workers.Load())-1))
	p.mu.Unlock()
	dismiss(retired)
	for ; spawn > 0; spawn-- {
		go p.worker(newAdaptiveWorker())
	}
}

func (p *adaptivePool) workerCount() int { return int(p.workers.Load()) }

// stop rejects new tasks and sends every parked worker to the queue, where
// each one runs what is left and then leaves, as the workers still running do
// once they find the queue empty. The backend waits for all of them.
func (p *adaptivePool) stop() {
	p.mu.Lock()
	p.stopped.Store(true)
	p.notFull.Broadcast()
	p.mu.Unlock()
	// A submission counts itself in before it looks at stopped, and stop
	// sets stopped before it looks at the count, so a submission either sees
	// stopped or is waited for here, and its task is queued before the
	// workers are told to drain. One waiting for room was woken above.
	for p.submitting.Load() > 0 {
		runtime.Gosched()
	}
	p.mu.Lock()
	p.draining.Store(true)
	idle := p.idle
	p.idle = nil
	p.idleCount.Store(0)
	p.lowIdle = 0
	p.waking.Add(int32(len(idle)))
	p.mu.Unlock()
	for _, w := range idle {
		w.wake <- p
	}
}

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()
