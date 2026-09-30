// Package benchmark compares fib's task pools with other goroutine pools in
// the same scenarios. It is a module of its own so that the pools it compares
// against never become dependencies of fib; see README.md for the scripts
// that run it.
package benchmark

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/gopkg/util/gopool"
	fnetpool "github.com/linfeip/fnet/pool"
	"github.com/panjf2000/ants/v2"

	fibpool "github.com/lesismal/fib/taskpool"
	nbiopool "github.com/lesismal/nbio/taskpool"
)

// pool is what every scenario submits to.
type pool interface {
	Go(func())
	Stop()
}

// queueSize is the queue given to the pools that take one.
const queueSize = 100000

// maxWorkers is the ceiling every pool gets: fib's default of 1000 workers
// per P.
func maxWorkers() int { return 1000 * runtime.GOMAXPROCS(0) }

type nbioPool struct{ *nbiopool.TaskPool }

type fibPool struct{ *fibpool.TaskPool }

func (p fibPool) Go(f func()) { p.TaskPool.Go(f) }

// antsPool runs on ants' defaults: Submit blocks while every worker is busy,
// and idle workers expire after a second.
type antsPool struct{ *ants.Pool }

func (p antsPool) Go(f func()) {
	if err := p.Submit(f); err != nil {
		panic(err)
	}
}

func (p antsPool) Stop() { p.Release() }

// gopoolPool runs on gopool's defaults: tasks wait in an unbounded list, and
// a worker leaves as soon as the list is empty. gopool has no Stop.
type gopoolPool struct{ gopool.Pool }

func (gopoolPool) Stop() {}

// fnetPool runs on fnet's defaults apart from the ceiling: per-core shards,
// an unbounded queue per shard, and idle workers exit after 5s.
type fnetPool struct{ *fnetpool.Pool }

func (p fnetPool) Go(f func()) {
	if err := p.Submit(f); err != nil {
		panic(err)
	}
}

func (p fnetPool) Stop() { p.Close() }

var pools = []struct {
	name string
	new  func() pool
}{
	{"nbio", func() pool { return nbioPool{nbiopool.New(maxWorkers(), queueSize)} }},
	{"ants", func() pool {
		p, err := ants.NewPool(maxWorkers())
		if err != nil {
			panic(err)
		}
		return antsPool{p}
	}},
	{"gopool", func() pool {
		return gopoolPool{gopool.NewPool("benchmark", int32(maxWorkers()), gopool.NewConfig())}
	}},
	{"fnet", func() pool { return fnetPool{fnetpool.New(fnetpool.Config{MaxWorkers: maxWorkers()})} }},
	{"fib-adaptive", func() pool {
		return fibPool{fibpool.NewWithMode("benchmark", fibpool.ModeAdaptive, maxWorkers(), queueSize)}
	}},
	{"fib-adaptive-chan", func() pool {
		return fibPool{fibpool.NewWithMode("benchmark", fibpool.ModeAdaptiveChan, maxWorkers(), queueSize)}
	}},
	{"fib-elastic", func() pool {
		return fibPool{fibpool.NewWithMode("benchmark", fibpool.ModeElastic, maxWorkers(), queueSize)}
	}},
}

// scenarios run in this order; each is run against every pool.
var scenarios = []struct {
	name string
	run  func(b *testing.B, p pool)
}{
	{"Handoff", handoff},
	{"ParallelTiny", parallelTiny},
	{"LoopCPUShort", func(b *testing.B, p pool) { loops(b, p, func() { spin(50) }) }},
	{"LoopCPULong", func(b *testing.B, p pool) { loops(b, p, func() { spin(2000) }) }},
	{"LoopBlocking", func(b *testing.B, p pool) { loops(b, p, func() { time.Sleep(time.Millisecond) }) }},
	{"LoopMixed", loopMixed},
	{"Bursts", bursts},
}

// BenchmarkPools runs every scenario against every pool, as
// BenchmarkPools/<scenario>/<pool>. Besides ns/op it reports cpu-ns/op, the
// process CPU time spent per task, which is what a pool costs beyond the wall
// clock: spinning and scheduler churn show up there even when they do not
// slow the benchmark down.
func BenchmarkPools(b *testing.B) {
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			for _, pc := range pools {
				b.Run(pc.name, func(b *testing.B) {
					p := pc.new()
					defer p.Stop()
					runtime.GC()
					start := cpuTime()
					b.ResetTimer()
					sc.run(b, p)
					b.StopTimer()
					b.ReportMetric(float64(cpuTime()-start)/float64(b.N), "cpu-ns/op")
				})
			}
		})
	}
}

var sink atomic.Uint64

// spin burns CPU for n rounds of a generator the compiler cannot drop.
func spin(n int) {
	x := uint64(n)
	for i := 0; i < n; i++ {
		x = x*6364136223846793005 + 1442695040888963407
	}
	sink.Add(x & 1)
}

// handoff submits one task at a time and waits for it: the latency of a
// lightly loaded server.
func handoff(b *testing.B, p pool) {
	done := make(chan struct{})
	for i := 0; i < b.N; i++ {
		p.Go(func() { done <- struct{}{} })
		<-done
	}
}

// parallelTiny has every P submit empty tasks at once: raw contention on the
// pool's own structures.
func parallelTiny(b *testing.B, p pool) {
	var wg sync.WaitGroup
	wg.Add(b.N)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Go(wg.Done)
		}
	})
	wg.Wait()
}

// loops submits b.N tasks from GOMAXPROCS/4 goroutines, as an engine's event
// loops do, and waits for them all.
func loops(b *testing.B, p pool, task func()) {
	n := max(1, runtime.GOMAXPROCS(0)/4)
	per := (b.N + n - 1) / n
	var wg sync.WaitGroup
	wg.Add(per * n)
	f := func() { task(); wg.Done() }
	for l := 0; l < n; l++ {
		go func() {
			for i := 0; i < per; i++ {
				p.Go(f)
			}
		}()
	}
	wg.Wait()
}

// loopMixed is loops with one task in ten blocking for 500µs and the rest
// CPU-bound.
func loopMixed(b *testing.B, p pool) {
	var count atomic.Uint64
	loops(b, p, func() {
		if count.Add(1)%10 == 0 {
			time.Sleep(500 * time.Microsecond)
		} else {
			spin(500)
		}
	})
}

// bursts submits rounds of 256 short tasks separated by an idle gap, which is
// left out of the timing. A pool that parks or lingers its workers serves the
// next round from warm goroutines; one that retires them at once forks again.
func bursts(b *testing.B, p pool) {
	const round, gap = 256, 2 * time.Millisecond
	var wg sync.WaitGroup
	f := func() { spin(200); wg.Done() }
	for done := 0; done < b.N; done += round {
		wg.Add(round)
		for i := 0; i < round; i++ {
			p.Go(f)
		}
		wg.Wait()
		b.StopTimer()
		time.Sleep(gap)
		b.StartTimer()
	}
}
