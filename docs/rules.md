# fib development rules

English | [简体中文](rules.zh-CN.md)

Rules every change to fib follows, beside how the code is organised and verified
([roadmap](roadmap.md), [load tests](loadtest.md)).

## 1. Cleaning up

1. **Every development, test and benchmark run cleans up after itself and frees the space it
   took.** When the work is done (a feature, a fix, a test or benchmark run, a reproduction),
   remove what it left behind, on this machine and on the benchmark host: build output made for it
   (binaries, test binaries from `go test -c`, load test builds), copies of the tree and git
   worktrees, containers, images and volumes it created, and logs and temporary files no longer
   needed. Caches kept on purpose (Go's build and module caches, pinned images) stay, but are
   pruned when they grow. The benchmark host's disk is shared by every session: check its free
   space (`df -h /`) before a large build or run, and never leave it fuller than you found it; a
   full disk stops everyone's builds and runs.

## 2. Feature work and benchmarks

2. **A feature task runs the basic CI tests, not load tests or benchmarks.** A task that develops a
   feature or fixes a bug runs what CI runs: the tests of the packages it touches (and, before a
   push, `go test -race ./...`), vet and the linters CI runs. It runs no load tests, benchmarks, A/B
   comparisons, perf profiles, full benchmark harness runs, or stress and soak runs (high repeat
   counts, long runs under load), neither here nor on the benchmark host. The benchmark host's time
   belongs to the benchmark tasks the maintainer runs separately (HTTPArena and the like); a feature
   task that loads the host wastes it and skews or delays those runs. Where another rule asks for a
   measurement, the feature task leaves it to such a benchmark task and says in its report what
   should be measured. A feature task runs a benchmark only when the user asks for one in that task.
