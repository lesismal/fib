package taskpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// NewAdaptiveChan creates a ModeAdaptiveChan pool.
//
// It grows and shrinks as NewAdaptive's pool does: a task that finds every
// worker busy and none on its way to the queue starts a new one, up to
// MaxWorkers, and once per ShrinkInterval half of the fewest workers that sat
// idle at any moment of the interval are retired, never taking the pool below
// MinWorkers. What differs is how its workers wait: on the queue itself, a
// buffered channel, where ModeAdaptive parks them on a mutex-guarded stack and
// wakes a submitter waiting for room with a condition variable. See
// chanAdaptivePool.
func NewAdaptiveChan(config AdaptiveConfig) *TaskPool {
	validateAdaptiveRange(config.MinWorkers, config.MaxWorkers)
	if config.QueueSize < 0 {
		panic("taskpool: queueSize must not be negative")
	}
	return start(config.Name, ModeAdaptiveChan, func(executor *executor) backend {
		return newChanAdaptivePool(executor, config)
	})
}

// chanAdaptivePool is ModeAdaptive with channels in place of its mutex and
// condition variables.
//
// The queue is a buffered channel, so the runtime does what the lock did:
// a task sent while a worker waits on the channel goes straight to it, and a
// submitter that finds the queue full blocks on the send until a worker takes
// a task. A worker waits on the queue and on retire. retire is unbuffered, so
// a non-blocking send on it reaches a waiting worker or nobody: that is how
// shrink and resize take an idle worker off without a stack to pop it from.
// A worker waiting never has tasks queued in front of it, since a task sent
// to a channel a receiver waits on goes to the receiver, so it cannot take
// retire while tasks are left behind.
//
// Growth follows ModeAdaptive's. waking counts the workers started that have
// not reached the queue yet; they will take the next tasks queued without
// being told. A submission whose task is left queued starts a worker only if
// no worker is idle and fewer are coming than tasks are queued, at most
// maxWaking at a time, and each worker that takes a task and still sees tasks
// left starts the next one. A burst thus fans out one worker per hop for as
// long as tasks are left behind, which is what tasks that block need, and
// stops as soon as the running workers keep up, which is what short ones need.
//
// Unlike ModeAdaptive's stack, the channel wakes the worker that has waited
// longest rather than the one that parked last. That spares the pool the lock
// the stack needs, at the cost of the warmest worker's cache.
//
// Resize, shrink and stop take ctl, a one-slot channel used as a lock; the
// submission and worker paths never do.
type chanAdaptivePool struct {
	executor *executor
	interval time.Duration
	tasks    chan Task
	retire   chan struct{}
	ctl      chan struct{}
	done     chan struct{}
	janitor  chan struct{}

	_ cacheLinePad
	// workers counts the live workers, including those started and not yet
	// running, and excluding those told to retire.
	workers atomic.Int32
	// idle counts the workers waiting on the queue, and lowIdle the fewest
	// that did at any moment since the last shrink: the ones no task reached.
	idle    atomic.Int32
	lowIdle atomic.Int32
	waking  atomic.Int32
	_       cacheLinePad

	minWorkers atomic.Int32
	maxWorkers atomic.Int32
	// submitting counts the submissions in flight, which stop waits out
	// before it closes the queue.
	submitting atomic.Int32
	stopped    atomic.Bool
	stopOnce   sync.Once
	workerWG   sync.WaitGroup
}

func newChanAdaptivePool(executor *executor, config AdaptiveConfig) *chanAdaptivePool {
	interval := config.ShrinkInterval
	if interval <= 0 {
		interval = defaultShrinkInterval
	}
	p := &chanAdaptivePool{
		executor: executor, interval: interval,
		// Growth is keyed to the tasks left queued, which an unbuffered queue
		// never holds.
		tasks: make(chan Task, max(1, config.QueueSize)), retire: make(chan struct{}),
		ctl: make(chan struct{}, 1), done: make(chan struct{}), janitor: make(chan struct{}),
	}
	p.resize(config.MinWorkers, config.MaxWorkers)
	go p.runJanitor()
	return p
}

func (p *chanAdaptivePool) lock()   { p.ctl <- struct{}{} }
func (p *chanAdaptivePool) unlock() { <-p.ctl }

// addWorker counts a new worker in, reporting false when the ceiling is
// reached.
func (p *chanAdaptivePool) addWorker() bool {
	for {
		workers := p.workers.Load()
		if workers >= p.maxWorkers.Load() {
			return false
		}
		if p.workers.CompareAndSwap(workers, workers+1) {
			return true
		}
	}
}

// reserveWaking claims a place among the workers on their way to the queue
// if the queue holds more tasks than are coming for them and fewer than
// maxWaking are.
func (p *chanAdaptivePool) reserveWaking() bool {
	for {
		waking := p.waking.Load()
		if waking >= maxWaking || int(waking) >= len(p.tasks) {
			return false
		}
		if p.waking.CompareAndSwap(waking, waking+1) {
			return true
		}
	}
}

// grow starts workers for the tasks queued while none is idle and fewer are
// coming for them than they number, as the ceiling allows. The caller is a
// submission in flight or a counted worker, so stop cannot find the
// WaitGroup at zero before the Add.
func (p *chanAdaptivePool) grow() {
	for p.idle.Load() == 0 && p.reserveWaking() {
		if !p.addWorker() {
			p.waking.Add(-1)
			return
		}
		p.workerWG.Add(1)
		go p.worker(true)
	}
}

func (p *chanAdaptivePool) submit(task Task) bool {
	p.submitting.Add(1)
	defer p.submitting.Add(-1)
	if p.stopped.Load() {
		return false
	}
	p.push(task)
	return true
}

func (p *chanAdaptivePool) submitBatch(tasks []Task) int {
	p.submitting.Add(1)
	defer p.submitting.Add(-1)
	if p.stopped.Load() {
		return 0
	}
	for _, task := range tasks {
		p.push(task)
	}
	return len(tasks)
}

// push queues task, making sure a worker is on its way to it, and blocks
// while the queue is full.
func (p *chanAdaptivePool) push(task Task) {
	select {
	case p.tasks <- task:
	default:
		p.grow()
		p.tasks <- task
	}
	p.grow()
}

// noteIdleDrop lowers lowIdle to idle if idle is below it.
func (p *chanAdaptivePool) noteIdleDrop(idle int32) {
	for {
		low := p.lowIdle.Load()
		if idle >= low || p.lowIdle.CompareAndSwap(low, idle) {
			return
		}
	}
}

// retireOverCeiling counts the calling worker out if the pool runs more
// workers than its ceiling, which a resize that lowered it leaves it doing,
// and reports whether it did.
func (p *chanAdaptivePool) retireOverCeiling() bool {
	for {
		workers := p.workers.Load()
		if workers <= p.maxWorkers.Load() {
			return false
		}
		if p.workers.CompareAndSwap(workers, workers-1) {
			return true
		}
	}
}

func (p *chanAdaptivePool) worker(waking bool) {
	defer p.workerWG.Done()
	if waking {
		// This worker was on its way to the queue and has reached it.
		p.waking.Add(-1)
	}
	for {
		var task Task
		var ok bool
		// A single-channel poll first: it is the cheapest way to take the next
		// task while the queue has one, and it never blocks.
		select {
		case task, ok = <-p.tasks:
		default:
			p.idle.Add(1)
			select {
			case task, ok = <-p.tasks:
				p.noteIdleDrop(p.idle.Add(-1))
			case <-p.retire:
				// Told to retire by shrink or resize, which counted it out. A
				// submission that raced the message may have queued a task
				// while this worker still counted as idle, and so left it to
				// this worker; it is passed on.
				p.noteIdleDrop(p.idle.Add(-1))
				p.grow()
				return
			}
		}
		if !ok {
			// Closed by stop once no submission can add to it.
			p.workers.Add(-1)
			return
		}
		// Fan out while tasks are left behind this one.
		p.grow()
		p.executor.call(task)
		if p.retireOverCeiling() {
			// Pass what is left queued on to a worker that stays.
			p.grow()
			return
		}
	}
}

// retireIdle tells up to n waiting workers to leave, counting out each one
// that took the message, and reports how many did.
func (p *chanAdaptivePool) retireIdle(n int) int {
	retired := 0
	for ; retired < n; retired++ {
		select {
		case p.retire <- struct{}{}:
			p.workers.Add(-1)
		default:
			return retired
		}
	}
	return retired
}

func (p *chanAdaptivePool) runJanitor() {
	defer close(p.done)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.shrink()
		case <-p.janitor:
			return
		}
	}
}

// shrink retires half of the workers that stayed idle through the whole
// interval since the last call, keeping the pool at or above its floor.
func (p *chanAdaptivePool) shrink() {
	p.lock()
	idle := int(p.lowIdle.Load())
	if surplus := int(p.workers.Load() - p.minWorkers.Load()); idle > 0 && surplus > 0 {
		p.retireIdle(min((idle+1)/2, surplus))
	}
	p.lowIdle.Store(p.idle.Load())
	p.unlock()
}

// resize moves the floor and the ceiling. Raising the floor starts the
// missing workers at once; a ceiling lowered below the workers there are
// retires waiting ones now, while busy ones leave as they finish their task.
func (p *chanAdaptivePool) resize(minWorkers, maxWorkers int) {
	p.lock()
	defer p.unlock()
	if p.stopped.Load() {
		return
	}
	p.maxWorkers.Store(int32(maxWorkers))
	p.minWorkers.Store(int32(minWorkers))
	for int(p.workers.Load()) < minWorkers && p.addWorker() {
		p.workerWG.Add(1)
		go p.worker(false)
	}
	if excess := int(p.workers.Load()) - maxWorkers; excess > 0 {
		p.retireIdle(excess)
	}
}

func (p *chanAdaptivePool) workerCount() int { return int(p.workers.Load()) }

func (p *chanAdaptivePool) attrs() []any {
	return []any{
		"workers", p.workerCount(), "minWorkers", int(p.minWorkers.Load()),
		"maxWorkers", int(p.maxWorkers.Load()), "queueSize", cap(p.tasks), "shrinkInterval", p.interval,
	}
}

// stop rejects new tasks, waits for the submissions in flight to queue
// theirs, and closes the queue: the workers run what is left in it and
// leave once it is empty.
func (p *chanAdaptivePool) stop() {
	p.stopOnce.Do(func() {
		close(p.janitor)
		<-p.done
		p.lock()
		p.stopped.Store(true)
		p.unlock()
		// A submission counts itself in before it looks at stopped, and stop
		// sets stopped before it looks at the count, so a submission either
		// sees stopped or is waited for here. One blocked on a full queue is
		// waited for as the workers drain it.
		for p.submitting.Load() > 0 {
			runtime.Gosched()
		}
		close(p.tasks)
		p.workerWG.Wait()
	})
}
