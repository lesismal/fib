package taskpool

import (
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

// elasticPool is nbio v1.7.0's taskpool with lingering workers: a submission
// forks a worker while a slot is free and queues the task otherwise, and a
// worker that runs out of tasks waits idleLinger for another one before it
// gives its slot back.
//
// A lingering worker waits on handoff, not on the queue. handoff is
// unbuffered, so a non-blocking send on it succeeds only when a worker is
// waiting there, which is how a submission reuses a warm worker instead of
// forking. Were the workers to wait on the queue, the dispatcher, which waits
// on it too, would take such a task as often as they did and fork for it.
//
// The dispatcher takes queued tasks to workers and never runs one itself, so
// a blocking task cannot stop the queue from being consumed. While no slot is
// free it offers the task on handoff as well: the slots may all be held by
// workers lingering there, which would otherwise keep the task waiting for
// their linger to run out.
type elasticPool struct {
	executor   *executor
	maxWorkers int64
	// concurrent counts the workers, each of which holds an execution slot.
	concurrent atomic.Int64
	tasks      chan Task
	handoff    chan Task
	slotted    chan struct{}
	closed     chan struct{}

	mu       sync.Mutex
	stopped  bool
	stopOnce sync.Once
	taskWG   sync.WaitGroup
	workerWG sync.WaitGroup
}

func newElasticPool(executor *executor, maxConcurrent, queueSize int) *elasticPool {
	p := &elasticPool{
		executor: executor, maxWorkers: int64(maxConcurrent),
		tasks: make(chan Task, queueSize), handoff: make(chan Task),
		slotted: make(chan struct{}, 1), closed: make(chan struct{}),
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

// release gives back an execution slot and wakes the dispatcher if it is
// waiting for one.
func (p *elasticPool) release() {
	p.concurrent.Add(-1)
	select {
	case p.slotted <- struct{}{}:
	default:
	}
}

func (p *elasticPool) submit(task Task) bool {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.taskWG.Add(1)
	// Hand the task to a lingering worker before forking a new one. Forking
	// is only worth its fresh stack when there is nobody waiting.
	select {
	case p.handoff <- task:
		p.mu.Unlock()
		return true
	default:
	}
	if p.fork(task) {
		p.mu.Unlock()
		return true
	}
	p.mu.Unlock()
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

// stop refuses new tasks, waits for the accepted ones to run, and then lets
// the dispatcher and the lingering workers go without waiting out their
// linger.
func (p *elasticPool) stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
		p.taskWG.Wait()
		close(p.closed)
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
	// One timer per worker, reset per wait, rather than a fresh timer per idle
	// spell: this is the hot path the linger exists to keep warm. Since Go
	// 1.23 Reset discards a value the timer may have left unread.
	timer := time.NewTimer(idleLinger)
	defer timer.Stop()
	for {
		p.execute(task)
		// A single-channel poll first: it is the cheapest way to take the next
		// task while the queue has one, and it never blocks.
		select {
		case task = <-p.tasks:
			continue
		default:
		}
		timer.Reset(idleLinger)
		select {
		case task = <-p.handoff:
		case <-timer.C:
			return
		case <-p.closed:
			return
		}
	}
}

// dispatch takes queued tasks to new workers, or to lingering ones while no
// slot is free.
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

// place forks a worker for task, or hands it to a lingering one, whichever a
// freed slot or a waiting worker allows first. It reports false if the pool
// closed first.
func (p *elasticPool) place(task Task) bool {
	for !p.fork(task) {
		select {
		case p.handoff <- task:
			return true
		case <-p.slotted:
		case <-p.closed:
			return false
		}
	}
	return true
}

func (p *elasticPool) execute(task Task) {
	defer p.taskWG.Done()
	p.executor.call(task)
}
