//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// handDeadline bounds every wait in these tests: a hand that deadlocks fails
// the test rather than hanging it.
const handDeadline = 10 * time.Second

// newHandEngine runs an engine with one loop, whose hands keep their order.
func newHandEngine(t *testing.T) *Engine {
	t.Helper()
	config := DefaultConfig()
	config.EventLoopCount = 1
	engine, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	return engine
}

// waitHand fails the test unless ch is closed within handDeadline.
func waitHand(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(handDeadline):
		t.Fatalf("%s: not within %v", what, handDeadline)
	}
}

// A task of a pool with one worker and one slot, the slot taken, hands more
// work to its own pool. Go would wait for room forever there; Hand returns at
// once, and the work runs once the task has finished and the pool has room,
// in the order it was handed.
func TestHandFromAFullPoolsOwnWorker(t *testing.T) {
	engine := newHandEngine(t)
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "hand-full", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()

	release := make(chan struct{})
	running := make(chan struct{})
	if !pool.Go(func() {
		close(running)
		<-release
	}) {
		t.Fatal("the pool turned the first task away")
	}
	waitHand(t, running, "the first task to start")
	// The one slot is taken; the next Go would wait.
	if !pool.Go(func() {}) {
		t.Fatal("the pool turned the queued task away")
	}

	const handed = 5
	var order []int // appended to by the pool's one worker, one at a time
	all := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		// Hand must not wait however full the pool is.
		for i := range handed {
			if !engine.Hand(pool, func() {
				order = append(order, i)
				if len(order) == handed {
					close(all)
				}
			}) {
				t.Error("Hand reported the engine closed")
			}
		}
		close(returned)
	}()
	waitHand(t, returned, "Hand to return with the pool full")
	close(release)
	waitHand(t, all, "the handed functions to run")
	for i, v := range order {
		if v != i {
			t.Fatalf("ran in the order %v, want the order handed", order)
		}
	}
}

// The task that hands work to its own pool is that pool's only worker: with
// Go it would wait on itself; with Hand the work runs after it returns.
func TestHandFromTheOnlyWorker(t *testing.T) {
	engine := newHandEngine(t)
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "hand-self", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()

	ran := make(chan struct{})
	taskDone := make(chan struct{})
	if !pool.Go(func() {
		defer close(taskDone)
		// Fill the one slot from the worker itself, as a pool's task
		// handing work on would, then hand more.
		if !pool.TryGo(func() {}) {
			t.Error("the empty queue turned a task away")
		}
		for range 3 {
			if !engine.Hand(pool, func() {}) {
				t.Error("Hand reported the engine closed")
			}
		}
		if !engine.Hand(pool, func() { close(ran) }) {
			t.Error("Hand reported the engine closed")
		}
	}) {
		t.Fatal("the pool turned the task away")
	}
	waitHand(t, taskDone, "the task that hands to its own pool to return")
	waitHand(t, ran, "the handed function to run")
}

// A pool that has stopped takes nothing; what is handed to it runs on a
// goroutine of its own instead.
func TestHandToAStoppedPool(t *testing.T) {
	engine := newHandEngine(t)
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "hand-stopped", MaxWorkers: 1, QueueSize: 1})
	pool.Stop()
	if !pool.Stopped() {
		t.Fatal("Stopped is false after Stop")
	}
	ran := make(chan struct{})
	if !engine.Hand(pool, func() { close(ran) }) {
		t.Fatal("Hand reported the engine closed")
	}
	waitHand(t, ran, "the function handed to a stopped pool to run")
}

// A pool that is no taskpool.TaskPool, as an application may give, gets what
// is handed to it from a goroutine of its own.
func TestHandToAnApplicationPool(t *testing.T) {
	engine := newHandEngine(t)
	pool := &handTestPool{}
	ran := make(chan struct{})
	if !engine.Hand(pool, func() { close(ran) }) {
		t.Fatal("Hand reported the engine closed")
	}
	waitHand(t, ran, "the function handed to an application's pool to run")
	if pool.tasks.Load() != 1 {
		t.Fatalf("the pool was given %d tasks, want 1", pool.tasks.Load())
	}
}

type handTestPool struct{ tasks atomic.Int32 }

func (p *handTestPool) GoTask(task taskpool.Task) bool {
	p.tasks.Add(1)
	go task.RunTask()
	return true
}

func (p *handTestPool) GoTasks(tasks []taskpool.Task) int {
	for _, task := range tasks {
		p.GoTask(task)
	}
	return len(tasks)
}

// Once the engine has closed, Hand reports false and runs nothing.
func TestHandAfterClose(t *testing.T) {
	engine, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	engine.Stop()
	select {
	case <-runDone:
	case <-time.After(handDeadline):
		t.Fatal("Run did not return after Stop")
	}
	_ = engine.Close()
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "hand-closed", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()
	if engine.Hand(pool, func() { t.Error("ran after the engine closed") }) {
		t.Fatal("Hand reported true after the engine closed")
	}
}

// fullPool returns a pool with one worker, held until release is closed, and
// its one slot taken, so that it turns away whatever is offered to it.
func fullPool(t *testing.T, name string) (pool *taskpool.TaskPool, release func()) {
	t.Helper()
	pool = taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: name, MaxWorkers: 1, QueueSize: 1})
	held := make(chan struct{})
	running := make(chan struct{})
	if !pool.Go(func() { close(running); <-held }) {
		t.Fatal("the pool turned the first task away")
	}
	waitHand(t, running, "the first task to start")
	if !pool.Go(func() {}) {
		t.Fatal("the pool turned the queued task away")
	}
	var once atomic.Bool
	release = func() {
		if once.CompareAndSwap(false, true) {
			close(held)
		}
	}
	t.Cleanup(func() { release(); pool.Stop() })
	return pool, release
}

// A full pool holds up what is handed to it, and nothing handed to another
// pool on the same loop: that runs at once.
func TestHandFullPoolHoldsUpOnlyItsOwn(t *testing.T) {
	engine := newHandEngine(t)
	full, release := fullPool(t, "hand-held")
	free := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "hand-free", MaxWorkers: 4, QueueSize: 16})
	defer free.Stop()

	heldRan := make(chan struct{})
	if !engine.Hand(full, func() { close(heldRan) }) {
		t.Fatal("Hand reported the engine closed")
	}
	deadline := time.Now().Add(handDeadline)
	for engine.Stats().HandQueued != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("HandQueued %d, want 1 waiting for the full pool", engine.Stats().HandQueued)
		}
		time.Sleep(time.Millisecond)
	}
	freeRan := make(chan struct{})
	if !engine.Hand(free, func() { close(freeRan) }) {
		t.Fatal("Hand reported the engine closed")
	}
	// Well within one retry of the held pool, which would not take it anyway.
	waitHand(t, freeRan, "a hand to a pool with room, with another pool full")
	select {
	case <-heldRan:
		t.Fatal("the hand to the full pool ran before it had room")
	default:
	}
	release()
	waitHand(t, heldRan, "the hand to the full pool once it had room")
	for engine.Stats().HandQueued != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("HandQueued %d once drained, want 0", engine.Stats().HandQueued)
		}
		time.Sleep(time.Millisecond)
	}
	if s := engine.Stats(); s.HandMaxQueued < 1 {
		t.Fatalf("HandMaxQueued %d, want at least 1", s.HandMaxQueued)
	}
}

// TryHand turns work away, with ErrPoolBusy, while the pool's queue is full
// or functions it turned away wait on the loop, counts it, and hands it on
// once the pool has room.
func TestTryHandTurnsAwayWhileFull(t *testing.T) {
	engine := newHandEngine(t)
	pool, release := fullPool(t, "tryhand-full")
	if err := engine.TryHand(pool, func() { t.Error("TryHand ran work it turned away") }); err != ErrPoolBusy {
		t.Fatalf("TryHand with the queue full: %v, want ErrPoolBusy", err)
	}
	if got := engine.Stats().HandRejected; got != 1 {
		t.Fatalf("HandRejected %d, want 1", got)
	}
	release()
	ran := make(chan struct{})
	deadline := time.Now().Add(handDeadline)
	for {
		err := engine.TryHand(pool, func() { close(ran) })
		if err == nil {
			break
		}
		if err != ErrPoolBusy || time.Now().After(deadline) {
			t.Fatalf("TryHand once the pool drains: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	waitHand(t, ran, "the work TryHand took")
}
