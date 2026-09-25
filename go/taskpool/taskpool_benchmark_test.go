package taskpool

import (
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkModes = []Mode{ModeElastic, ModeCond, ModeAdaptive}

type benchmarkTask struct{ done chan struct{} }

func (t *benchmarkTask) RunTask() { t.done <- struct{}{} }

// BenchmarkGoTask measures one handoff at a time: a submission to an idle
// pool and the wait for its task to run, which is the latency of a lightly
// loaded server.
func BenchmarkGoTask(b *testing.B) {
	for _, mode := range benchmarkModes {
		b.Run(mode.String(), func(b *testing.B) {
			tp := NewWithMode("test", mode, 1, 1)
			defer tp.Stop()
			task := &benchmarkTask{done: make(chan struct{}, 1)}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !tp.GoTask(task) {
					b.Fatal("task rejected")
				}
				<-task.done
			}
		})
	}
}

// spinTask stands in for a connection's round: a little CPU work, then a
// mark that it is done.
type spinTask struct {
	work int
	wg   *sync.WaitGroup
}

var spinSink atomic.Uint64

func spin(n int) {
	x := uint64(n)
	for i := 0; i < n; i++ {
		x = x*6364136223846793005 + 1442695040888963407
	}
	if x == 0 {
		spinSink.Add(1)
	}
}

func (t *spinTask) RunTask() {
	spin(t.work)
	t.wg.Done()
}

// reportCPU adds the process CPU time spent per task, which is what a pool
// costs beyond the wall clock: spinning and scheduler churn show up here even
// when they do not slow the benchmark down.
func reportCPU(b *testing.B, start time.Duration, tasks int) {
	b.ReportMetric(float64(cpuTime()-start)/float64(tasks), "cpu-ns/task")
}

func newBenchPool(mode Mode) *TaskPool {
	sizing := 1000 * runtime.GOMAXPROCS(0)
	if mode == ModeCond {
		sizing = 100 * runtime.GOMAXPROCS(0)
	}
	return NewWithMode("test", mode, sizing, 100000)
}

// BenchmarkLoopBatch submits the way an engine's event loops do: a few
// loops, each handing its round's ready connections over in one batch and
// going straight back for the next round.
func BenchmarkLoopBatch(b *testing.B) {
	for _, work := range []int{50, 1000} {
		for _, batch := range []int{8, 64, 512} {
			for _, mode := range benchmarkModes {
				name := mode.String() + "/batch=" + strconv.Itoa(batch) + "/work=" + strconv.Itoa(work)
				b.Run(name, func(b *testing.B) {
					tp := newBenchPool(mode)
					defer tp.Stop()
					loops := max(1, runtime.GOMAXPROCS(0)/4)
					var wg sync.WaitGroup
					per := (b.N + loops - 1) / loops
					wg.Add(per * loops)
					start := cpuTime()
					b.ResetTimer()
					var loopsDone sync.WaitGroup
					for l := 0; l < loops; l++ {
						loopsDone.Add(1)
						go func() {
							defer loopsDone.Done()
							storage := make([]spinTask, per)
							tasks := make([]Task, 0, batch)
							for i := range storage {
								storage[i] = spinTask{work: work, wg: &wg}
								tasks = append(tasks, &storage[i])
								if len(tasks) == batch || i == len(storage)-1 {
									tp.GoTasks(tasks)
									tasks = tasks[:0]
								}
							}
						}()
					}
					loopsDone.Wait()
					wg.Wait()
					b.StopTimer()
					reportCPU(b, start, per*loops)
				})
			}
		}
	}
}

// BenchmarkParallelGo submits single tasks from every P at once.
func BenchmarkParallelGo(b *testing.B) {
	for _, mode := range benchmarkModes {
		b.Run(mode.String(), func(b *testing.B) {
			tp := newBenchPool(mode)
			defer tp.Stop()
			var wg sync.WaitGroup
			var count atomic.Int64
			start := cpuTime()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				task := &spinTask{work: 100, wg: &wg}
				for pb.Next() {
					wg.Add(1)
					count.Add(1)
					tp.GoTask(task)
				}
			})
			wg.Wait()
			b.StopTimer()
			reportCPU(b, start, int(count.Load()))
		})
	}
}

type latencyTask struct {
	submitted time.Time
	delay     *time.Duration
	wg        *sync.WaitGroup
}

func (t *latencyTask) RunTask() {
	*t.delay = time.Since(t.submitted)
	spin(200)
	t.wg.Done()
}

// BenchmarkLatency reports how long a task waits between its submission and
// the start of its run while loops keep the pool moderately busy.
func BenchmarkLatency(b *testing.B) {
	for _, mode := range benchmarkModes {
		b.Run(mode.String(), func(b *testing.B) {
			tp := newBenchPool(mode)
			defer tp.Stop()
			loops := max(1, runtime.GOMAXPROCS(0)/4)
			per := (b.N + loops - 1) / loops
			delays := make([]time.Duration, per*loops)
			var wg sync.WaitGroup
			wg.Add(len(delays))
			start := cpuTime()
			b.ResetTimer()
			var loopsDone sync.WaitGroup
			for l := 0; l < loops; l++ {
				loopsDone.Add(1)
				go func(base int) {
					defer loopsDone.Done()
					const batch = 16
					storage := make([]latencyTask, per)
					tasks := make([]Task, 0, batch)
					for i := range storage {
						storage[i] = latencyTask{delay: &delays[base+i], wg: &wg}
						tasks = append(tasks, &storage[i])
						if len(tasks) == batch || i == len(storage)-1 {
							now := time.Now()
							for _, task := range tasks {
								task.(*latencyTask).submitted = now
							}
							tp.GoTasks(tasks)
							tasks = tasks[:0]
							// Leave the pool room to go idle between
							// rounds, as a loop waiting on epoll does.
							time.Sleep(20 * time.Microsecond)
						}
					}
				}(l * per)
			}
			loopsDone.Wait()
			wg.Wait()
			b.StopTimer()
			reportCPU(b, start, len(delays))
			slices.Sort(delays)
			b.ReportMetric(float64(delays[len(delays)/2]), "p50-ns")
			b.ReportMetric(float64(delays[len(delays)*99/100]), "p99-ns")
		})
	}
}

// BenchmarkBlockingFanOut submits rounds of tasks that each block for a
// while, as handlers waiting on a database do. Every task needs a worker of
// its own at once, so a round is only as short as the pool is quick to fan out.
// ModeCond is left out: its fixed workers are split over shards too small to
// hold a round.
func BenchmarkBlockingFanOut(b *testing.B) {
	for _, mode := range []Mode{ModeElastic, ModeAdaptive} {
		b.Run(mode.String(), func(b *testing.B) {
			tp := newBenchPool(mode)
			defer tp.Stop()
			const round = 256
			release := make(chan struct{})
			var started, done sync.WaitGroup
			tasks := make([]Task, round)
			for i := range tasks {
				tasks[i] = taskFunc(func() {
					started.Done()
					<-release
					done.Done()
				})
			}
			start := cpuTime()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started.Add(round)
				done.Add(round)
				tp.GoTasks(tasks)
				// A round is over when every task holds a worker at once.
				started.Wait()
				b.StopTimer()
				close(release)
				done.Wait()
				release = make(chan struct{})
				b.StartTimer()
			}
			b.StopTimer()
			reportCPU(b, start, b.N*round)
		})
	}
}
