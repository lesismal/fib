package sidepool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// Callbacks posted while the pool's one worker is busy and its queue full
// are neither dropped nor waited for: each runs once, on a goroutine of its
// own, and is counted.
func TestPostOverflow(t *testing.T) {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "post-test", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()
	release := make(chan struct{})
	var releaseOnce sync.Once
	free := func() { releaseOnce.Do(func() { close(release) }) }
	defer free()
	started := make(chan struct{})
	post(pool, func() { close(started); <-release })
	<-started
	before := PostSpilled()
	var mu sync.Mutex
	seen := map[int]int{}
	var ran atomic.Int32
	const posts = 50
	var wg sync.WaitGroup
	wg.Add(posts)
	for i := range posts {
		post(pool, func() {
			defer wg.Done()
			ran.Add(1)
			mu.Lock()
			seen[i]++
			mu.Unlock()
		})
	}
	// The queue holds one; the rest ran on goroutines of their own, while
	// the worker is still held.
	spilled := int32(PostSpilled() - before)
	if spilled < posts-2 {
		t.Fatalf("%d of %d callbacks spilled with the queue full", spilled, posts)
	}
	deadline := time.Now().Add(10 * time.Second)
	for ran.Load() < spilled {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d spilled callbacks ran while the worker was held", ran.Load(), spilled)
		}
		time.Sleep(time.Millisecond)
	}
	free()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%d of %d callbacks ran", ran.Load(), posts)
	}
	for i := range posts {
		if seen[i] != 1 {
			t.Fatalf("callback %d ran %d times", i, seen[i])
		}
	}
}

// A callback that panics, whether the pool queued it or it ran on a
// goroutine of its own, is reported to the pool's panic handler.
func TestPostPanic(t *testing.T) {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "post-panic-test", MaxWorkers: 1, QueueSize: 1})
	defer pool.Stop()
	var panics atomic.Int32
	pool.SetPanicHandler(func(any, []byte) { panics.Add(1) })
	release := make(chan struct{})
	var releaseOnce sync.Once
	free := func() { releaseOnce.Do(func() { close(release) }) }
	defer free()
	started := make(chan struct{})
	post(pool, func() { close(started); <-release; panic("first") })
	<-started
	const posts = 20
	for range posts {
		post(pool, func() { panic("again") })
	}
	free()
	deadline := time.Now().Add(10 * time.Second)
	for panics.Load() != 1+posts {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d panics reported", panics.Load(), 1+posts)
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
