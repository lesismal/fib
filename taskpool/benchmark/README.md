# taskpool benchmarks

[English](README.md) | [简体中文](README.zh-CN.md)

Compares fib's task pools with other goroutine pools under the same
scenarios. This directory is a Go module of its own
(`github.com/lesismal/fib/taskpool/benchmark`, with `replace` pointing at the
fib checkout two levels up), so the pools it compares against are never
dependencies of fib itself.

## Pools

| name | pool | settings |
|---|---|---|
| `nbio` | `github.com/lesismal/nbio/taskpool` v1.7.0 | `New(ceiling, 100000)` |
| `ants` | `github.com/panjf2000/ants/v2` v2.12.1 | `NewPool(ceiling)`, defaults: `Submit` blocks while every worker is busy |
| `gopool` | `github.com/bytedance/gopkg/util/gopool` v0.1.4 | `NewPool(name, ceiling, NewConfig())`, unbounded task list |
| `fib-adaptive` | `taskpool.ModeAdaptive` | floor 0, queue 100000 |
| `fib-adaptive-chan` | `taskpool.ModeAdaptiveChan` | floor 0, queue 100000 |
| `fib-elastic` | `taskpool.ModeElastic` | queue 100000 |

Every pool gets the same ceiling, fib's default of 1000 workers per P.

## Scenarios

| name | what it measures |
|---|---|
| `Handoff` | one submitter, one task at a time, waiting for each: latency on a lightly loaded server |
| `ParallelTiny` | every P submits empty tasks at once: contention on the pool itself |
| `LoopCPUShort` | GOMAXPROCS/4 submitters, as event loops, feeding CPU-bound tasks of ~50 rounds |
| `LoopCPULong` | the same with ~2000 rounds |
| `LoopBlocking` | the same with tasks that sleep 1ms: how fast the pool fans out |
| `LoopMixed` | one task in ten sleeps 500µs, the rest spin 500 rounds |
| `Bursts` | rounds of 256 short tasks with a 2ms idle gap, left out of the timing: reuse of warm workers |

Each benchmark is `BenchmarkPools/<scenario>/<pool>` and reports `ns/op`
and `cpu-ns/op`, the process CPU time spent per task (getrusage on Linux and
macOS, GetProcessTimes on Windows).

## Running

Linux and macOS:

```bash
./bench.sh                                          # everything, once each
./bench.sh -s Handoff,Bursts -p nbio,fib-elastic -c 3
./bench.sh -cpu 4,8 -s LoopBlocking                 # one row per GOMAXPROCS
./bench.sh -docker -cpus 8                          # in a golang:1.27 Linux container
./bench.sh -l                                       # list scenarios and pools
./bench.sh -h
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File bench.ps1
.\bench.ps1 -Scenarios Handoff,Bursts -Pools nbio,fib-elastic -Count 3
.\bench.ps1 -Cpu 4,8 -Scenarios LoopBlocking
.\bench.ps1 -List
```

The raw `go test` output goes to `results/<os>-<arch>-<time>.txt` (or `-o` /
`-Out`), and a Markdown table of medians follows it: `ns/op / cpu-ns/op`,
the fastest pool of each scenario in bold, and `*` on cells whose runs
spread more than 1.3x. To summarize earlier output again:

```bash
go run ./summarize results/*.txt
```

A full run of all scenarios and pools, once each, takes about 3 minutes;
`-c` / `-Count` runs each more times, which is what the medians and the
`*` spread notes need.
Keep the machine otherwise idle while it runs; the blocking and burst
scenarios are the most sensitive to noise.
