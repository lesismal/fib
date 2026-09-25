package taskpool

import (
	"sync/atomic"
	"testing"
)

func TestInlineRunsOnSubmitterAndRecovers(t *testing.T) {
	tp := NewWithMode("inline", ModeInline, 0, 0)
	if tp.Mode() != ModeInline || tp.Workers() != 0 {
		t.Fatalf("mode %v with %d workers, want inline with none", tp.Mode(), tp.Workers())
	}
	var ran atomic.Int64
	var panicked atomic.Int64
	tp.SetPanicHandler(func(any, []byte) { panicked.Add(1) })
	// Running on the submitter means the task has finished by the time Go
	// returns, with no synchronisation of its own.
	if !tp.Go(func() { ran.Add(1) }) || ran.Load() != 1 {
		t.Fatalf("Go ran %d tasks before returning, want 1", ran.Load())
	}
	tasks := []Task{taskFunc(func() { ran.Add(1) }), taskFunc(func() { panic("boom") }), taskFunc(func() { ran.Add(1) })}
	if n := tp.GoTasks(tasks); n != len(tasks) {
		t.Fatalf("GoTasks accepted %d of %d", n, len(tasks))
	}
	if ran.Load() != 3 || panicked.Load() != 1 {
		t.Fatalf("ran %d and recovered %d panics, want 3 and 1", ran.Load(), panicked.Load())
	}
	tp.Stop()
	if tp.Go(func() { ran.Add(1) }) || tp.GoTasks(tasks) != 0 || ran.Load() != 3 {
		t.Fatal("a stopped inline pool ran work")
	}
}
