package sidepool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// Callbacks posted while the pool's one worker is busy and its queue full
// wait in the backlog, start no goroutine, and all run once the worker is
// free, each once.
func TestPostOverflow(t *testing.T) {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "post-test", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()
	var b backlog
	release := make(chan struct{})
	started := make(chan struct{})
	post(pool, &b, func() { close(started); <-release })
	<-started
	var mu sync.Mutex
	var order []int
	before := runtime.NumGoroutine()
	const posts = 50
	for i := range posts {
		post(pool, &b, func() {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		})
	}
	if grew := runtime.NumGoroutine() - before; grew > 2 {
		t.Fatalf("posting to a full pool started %d goroutines", grew)
	}
	b.mu.Lock()
	waiting := len(b.fns)
	b.mu.Unlock()
	if waiting == 0 {
		t.Fatal("nothing waited in the backlog with the queue full")
	}
	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == posts {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d callbacks ran", n, posts)
		}
		time.Sleep(time.Millisecond)
	}
	seen := make(map[int]bool, posts)
	for _, i := range order {
		if seen[i] {
			t.Fatalf("callback %d ran twice: %v", i, order)
		}
		seen[i] = true
	}
}

// A callback that panics, whether the pool queued it or it waited in the
// backlog, is reported to the pool's panic handler, and the callbacks behind
// it in the backlog still run.
func TestPostPanicKeepsDraining(t *testing.T) {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "post-panic-test", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()
	var panics atomic.Int32
	pool.SetPanicHandler(func(any, []byte) { panics.Add(1) })
	var b backlog
	release := make(chan struct{})
	started := make(chan struct{})
	post(pool, &b, func() { close(started); <-release; panic("first") })
	<-started
	var ran atomic.Int32
	const posts = 20
	for i := range posts {
		post(pool, &b, func() {
			ran.Add(1)
			if i%2 == 0 {
				panic("again")
			}
		})
	}
	b.mu.Lock()
	waiting := len(b.fns)
	b.mu.Unlock()
	if waiting == 0 {
		t.Fatal("nothing waited in the backlog with the queue full")
	}
	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for ran.Load() != posts || panics.Load() != 1+posts/2 {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d callbacks ran, %d panics reported", ran.Load(), posts, panics.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// Post runs its callback on the notify pool.
func TestPost(t *testing.T) {
	var ran atomic.Bool
	done := make(chan struct{})
	Post(func() { ran.Store(true); close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the callback never ran")
	}
	if first, again := Notify(), Notify(); !ran.Load() || first != again {
		t.Fatal("Post did not run on one notify pool")
	}
}
