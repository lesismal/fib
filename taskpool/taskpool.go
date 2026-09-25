// Package taskpool provides interchangeable bounded task schedulers.
package taskpool

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type Mode uint8

const (
	// ModeCond runs a fixed set of workers that park on a condition variable
	// between tasks. The worker count is the number of goroutines that exist.
	ModeCond Mode = iota
	// ModeElastic forks a worker per submission while it has capacity, lets
	// idle ones linger briefly, and retires them after that. The worker count
	// is a ceiling rather than a population.
	ModeElastic
	// ModeAdaptive keeps parked workers as ModeCond does, but grows the
	// population when tasks arrive to find every worker busy and retires
	// workers that stay idle, between a floor and a ceiling that Resize can
	// move while the pool runs. See NewAdaptive.
	ModeAdaptive
	// ModeInline runs each task on the goroutine that submits it, under the
	// same panic recovery as the other modes, and has no workers or queue of
	// its own. It suits a submitter that is itself one of many goroutines
	// spreading the load, such as one event loop among several, where handing
	// the task to another goroutine would only add a wake-up. See NewInline.
	ModeInline
)

func (m Mode) String() string {
	switch m {
	case ModeElastic:
		return "elastic"
	case ModeCond:
		return "cond"
	case ModeAdaptive:
		return "adaptive"
	case ModeInline:
		return "inline"
	default:
		return fmt.Sprintf("Mode(%d)", m)
	}
}

func (m Mode) Valid() bool {
	return m == ModeElastic || m == ModeCond || m == ModeAdaptive || m == ModeInline
}

type Task interface{ RunTask() }

type taskFunc func()

func (f taskFunc) RunTask() { f() }

type backend interface {
	submit(Task) bool
	submitBatch([]Task) int
	stop()
	workerCount() int
	// attrs describes what the backend runs once built, as slog key-value
	// pairs.
	attrs() []any
}

// executor runs a pool's tasks, and knows which pool it runs them for so
// that what it logs says so.
type executor struct {
	name    string
	id      uint64
	mu      sync.RWMutex
	handler func(any, []byte)
}

func (e *executor) setPanicHandler(handler func(any, []byte)) {
	e.mu.Lock()
	e.handler = handler
	e.mu.Unlock()
}

func (e *executor) call(task Task) {
	defer func() {
		if recovered := recover(); recovered != nil {
			e.mu.RLock()
			handler := e.handler
			e.mu.RUnlock()
			if handler != nil {
				handler(recovered, debug.Stack())
				return
			}
			slog.Error("taskpool: task panicked", "pool", e.name, "id", e.id,
				"panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
		}
	}()
	task.RunTask()
}

type TaskPool struct {
	executor *executor
	backend  backend
	mode     Mode
}

// New creates a ModeAdaptive pool that grows to maxConcurrent workers under
// load and retires every one of them when idle. name labels the pool in what
// it logs.
func New(name string, maxConcurrent, queueSize int) *TaskPool {
	return NewWithMode(name, ModeAdaptive, maxConcurrent, queueSize)
}

// NewWithMode creates a pool of the given mode, labelled name in what it
// logs. For ModeAdaptive, maxConcurrent is the ceiling and the floor is
// zero; NewAdaptive sets both. ModeInline ignores both sizes.
func NewWithMode(name string, mode Mode, maxConcurrent, queueSize int) *TaskPool {
	if mode == ModeInline {
		return NewInline(name)
	}
	if maxConcurrent <= 0 {
		panic("taskpool: maxConcurrent must be greater than zero")
	}
	if queueSize < 0 {
		panic("taskpool: queueSize must not be negative")
	}
	params := []any{"maxConcurrent", maxConcurrent, "queueSize", queueSize}
	switch mode {
	case ModeElastic:
		return start(name, mode, params, func(executor *executor) backend {
			return newElasticPool(executor, maxConcurrent, queueSize)
		})
	case ModeCond:
		return start(name, mode, params, func(executor *executor) backend {
			return newCondBackend(executor, maxConcurrent, queueSize)
		})
	case ModeAdaptive:
		return NewAdaptive(AdaptiveConfig{
			Name: name, MaxWorkers: maxConcurrent, QueueSize: queueSize,
		})
	default:
		panic("taskpool: invalid mode")
	}
}

// poolIDs numbers the pools built, so that the lines one pool logs can be
// told from another's even when the two share a name.
var poolIDs atomic.Uint64

// start builds a pool's backend. It logs the parameters the pool was created
// with before building it, and what the pool runs once it has started: the
// shards, workers and queue those parameters resolved to.
func start(name string, mode Mode, params []any, build func(*executor) backend) *TaskPool {
	executor := &executor{name: name, id: poolIDs.Add(1)}
	label := []any{"pool", name, "id", executor.id, "mode", mode.String()}
	slog.Info("taskpool: created", append(label, params...)...)
	pool := &TaskPool{executor: executor, backend: build(executor), mode: mode}
	slog.Info("taskpool: started", append(label[:len(label):len(label)], pool.backend.attrs()...)...)
	return pool
}

// Name reports the name the pool was created with.
func (tp *TaskPool) Name() string { return tp.executor.name }

// Mode reports the mode the pool was created with.
func (tp *TaskPool) Mode() Mode { return tp.mode }

// SetPanicHandler sets what a task that panics is reported to, with the
// value it panicked with and its stack. Without one, the pool logs the panic
// under its name; the pool goes on running either way.
func (tp *TaskPool) SetPanicHandler(handler func(any, []byte)) {
	tp.executor.setPanicHandler(handler)
}

func (tp *TaskPool) Go(f func()) bool {
	if f == nil {
		return true
	}
	return tp.GoTask(taskFunc(f))
}

func (tp *TaskPool) GoTask(task Task) bool {
	if task == nil {
		return true
	}
	return tp.backend.submit(task)
}

// GoTasks submits tasks in order and returns how many were accepted. Tasks
// must be non-nil. A short count means the pool stopped; the suffix
// tasks[n:] was not accepted.
func (tp *TaskPool) GoTasks(tasks []Task) int {
	if len(tasks) == 0 {
		return 0
	}
	return tp.backend.submitBatch(tasks)
}

func (tp *TaskPool) Call(f func()) { tp.executor.call(taskFunc(f)) }

func (tp *TaskPool) Stop() { tp.backend.stop() }

// Workers reports how many workers the pool is running: the fixed count under
// ModeCond, the forked workers under ModeElastic, the current population
// under ModeAdaptive, and none under ModeInline.
func (tp *TaskPool) Workers() int { return tp.backend.workerCount() }

// Resize moves a ModeAdaptive pool's floor and ceiling while it runs. Raising
// the floor starts workers at once, and lowering the ceiling retires the
// workers over it: idle ones at once, busy ones as they finish their task.
// It reports false, and changes nothing, for a pool of any other mode. It
// panics if maxWorkers is not positive or minWorkers is not between zero and
// maxWorkers.
func (tp *TaskPool) Resize(minWorkers, maxWorkers int) bool {
	adaptive, ok := tp.backend.(*adaptiveBackend)
	if !ok {
		return false
	}
	validateAdaptiveRange(minWorkers, maxWorkers)
	adaptive.resize(minWorkers, maxWorkers)
	return true
}
