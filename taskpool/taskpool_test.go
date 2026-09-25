package taskpool

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyBoundAndDrain(t *testing.T) {
	const limit = 4
	tp := New("test", limit, 32)
	release := make(chan struct{})
	var running atomic.Int64
	var peak atomic.Int64
	for i := 0; i < 32; i++ {
		if !tp.Go(func() {
			current := running.Add(1)
			for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
			}
			<-release
			running.Add(-1)
		}) {
			t.Fatal("submission rejected before Stop")
		}
	}
	deadline := time.Now().Add(time.Second)
	for peak.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	tp.Stop()
	if got := peak.Load(); got > limit || got < 2 {
		t.Fatalf("peak concurrency = %d, want 2..%d", got, limit)
	}
	if tp.Go(func() {}) {
		t.Fatal("submission accepted after Stop")
	}
}

func TestStopWaitsAndRecoversPanics(t *testing.T) {
	tp := New("test", 2, 2)
	started := make(chan struct{})
	finish := make(chan struct{})
	var panicSeen atomic.Bool
	tp.SetPanicHandler(func(any, []byte) { panicSeen.Store(true) })
	tp.Go(func() { panic("boom") })
	tp.Go(func() { close(started); <-finish })
	<-started
	done := make(chan struct{})
	go func() { tp.Stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("Stop returned while a task was running")
	case <-time.After(10 * time.Millisecond):
	}
	close(finish)
	<-done
	if !panicSeen.Load() {
		t.Fatal("panic handler was not called")
	}
}

func TestEachTaskRunsOnce(t *testing.T) {
	tp := New("test", 8, 64)
	var counts [100]atomic.Int64
	var submitted sync.WaitGroup
	for i := range counts {
		submitted.Add(1)
		go func(index int) {
			defer submitted.Done()
			if !tp.Go(func() { counts[index].Add(1) }) {
				t.Errorf("task %d rejected", index)
			}
		}(i)
	}
	submitted.Wait()
	tp.Stop()
	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("task %d ran %d times", i, got)
		}
	}
}

func TestGoTasksRunsBatchesOnce(t *testing.T) {
	for _, mode := range []Mode{ModeElastic, ModeCond, ModeAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			// A queue smaller than the batch forces submitBatch through its
			// queue-full wait path.
			tp := NewWithMode("test", mode, 4, 2)
			var counts [100]atomic.Int64
			tasks := make([]Task, len(counts))
			for i := range tasks {
				index := i
				tasks[i] = taskFunc(func() { counts[index].Add(1) })
			}
			if got := tp.GoTasks(tasks); got != len(tasks) {
				t.Fatalf("GoTasks accepted %d tasks, want %d", got, len(tasks))
			}
			tp.Stop()
			for i := range counts {
				if got := counts[i].Load(); got != 1 {
					t.Fatalf("task %d ran %d times", i, got)
				}
			}
			if got := tp.GoTasks(tasks); got != 0 {
				t.Fatalf("GoTasks accepted %d tasks after Stop, want 0", got)
			}
		})
	}
}

func TestAllModesExecuteAndStop(t *testing.T) {
	for _, mode := range []Mode{ModeElastic, ModeCond, ModeAdaptive} {
		t.Run(mode.String(), func(t *testing.T) {
			tp := NewWithMode("test", mode, 4, 16)
			var count atomic.Int64
			for i := 0; i < 100; i++ {
				if !tp.Go(func() { count.Add(1) }) {
					t.Fatal("task rejected before Stop")
				}
			}
			tp.Stop()
			if got := count.Load(); got != 100 {
				t.Fatalf("executed %d tasks, want 100", got)
			}
			if tp.Go(func() {}) {
				t.Fatal("task accepted after Stop")
			}
		})
	}
}

// syncBuffer is a bytes.Buffer the pool's goroutines can log to.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A pool logs under its name when it is created and started, once
// SetLogStatus has switched that on, and when a task panics with no panic
// handler set.
func TestLogsCarryThePoolName(t *testing.T) {
	var out syncBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
	defer slog.SetDefault(previous)
	SetLogStatus(true)
	defer SetLogStatus(false)
	for _, mode := range []Mode{ModeCond, ModeElastic, ModeAdaptive} {
		out.buf.Reset()
		tp := NewWithMode("named-"+mode.String(), mode, 2, 4)
		if got := tp.Name(); got != "named-"+mode.String() {
			t.Fatalf("Name() = %q", got)
		}
		tp.Go(func() { panic("boom") })
		tp.Stop()
		logged := out.String()
		for _, msg := range []string{"taskpool: created", "taskpool: started", "taskpool: task panicked"} {
			want := "msg=\"" + msg + "\" pool=named-" + mode.String()
			if !strings.Contains(logged, want) {
				t.Fatalf("%v: no %q in the log:\n%s", mode, want, logged)
			}
		}
	}
}

// By default a pool logs nothing when it is created and started, but still
// logs a task that panics with no panic handler set.
func TestStatusLogsAreOffByDefault(t *testing.T) {
	var out syncBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
	defer slog.SetDefault(previous)
	tp := NewWithMode("quiet", ModeCond, 2, 4)
	tp.Go(func() { panic("boom") })
	tp.Stop()
	logged := out.String()
	for _, msg := range []string{"taskpool: created", "taskpool: started"} {
		if strings.Contains(logged, msg) {
			t.Fatalf("%q logged by default:\n%s", msg, logged)
		}
	}
	if !strings.Contains(logged, "taskpool: task panicked") {
		t.Fatalf("no panic in the log:\n%s", logged)
	}
}
