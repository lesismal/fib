package taskpool

import "sync/atomic"

// NewInline creates a ModeInline pool, labelled name in what it logs: one
// that runs each task on the goroutine submitting it, recovering a panic the
// way the other modes' workers do. A submission returns once its tasks have
// run, so a task that blocks holds up its submitter.
func NewInline(name string) *TaskPool {
	return start(name, ModeInline, func(executor *executor) backend {
		return &inlineBackend{executor: executor}
	})
}

// inlineBackend runs tasks where they are submitted. Once stopped it refuses
// them, as a stopped pool of any other mode does.
type inlineBackend struct {
	executor *executor
	stopped  atomic.Bool
}

func (b *inlineBackend) submit(task Task) bool {
	if b.stopped.Load() {
		return false
	}
	b.executor.call(task)
	return true
}

func (b *inlineBackend) submitBatch(tasks []Task) int {
	for i, task := range tasks {
		if b.stopped.Load() {
			return i
		}
		b.executor.call(task)
	}
	return len(tasks)
}

func (b *inlineBackend) stop() { b.stopped.Store(true) }

func (b *inlineBackend) workerCount() int { return 0 }

func (b *inlineBackend) attrs() []any { return []any{"workers", 0} }
