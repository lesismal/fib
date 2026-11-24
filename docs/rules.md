# fib development rules

English | [简体中文](rules.zh-CN.md)

Rules every change to fib follows, beside how the code is organised and verified
([roadmap](roadmap.md), [dev plan](dev-plan.md), [load tests](loadtest.md)).

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
   A feature task whose feature needs a benchmark (a hot path: engine reads and writes, parsers,
   framing, crypto, flow control, pools) adds or keeps its row in [the dev plan](dev-plan.md),
   marked not benchmarked and not optimized, and names there what should be measured; the
   benchmark task that measures or optimizes it updates the row.

## 3. No run hangs unnoticed

3. **A hung test costs minutes, not hours.** A bug that deadlocks a test must fail the run
   quickly and visibly, never leave it waiting:
   - **Each test binary has a time limit.** Every `go test` run passes an explicit `-timeout` sized
     to what it runs (the whole suite: `-timeout 20m`, as CI uses; a single slow test: a few times
     its usual time), and never `-timeout 0`. When it expires, Go fails the binary with
     `panic: test timed out`, the tests still running and every goroutine's stack, so the hung test
     and where it waits show.
   - **A timeout is a failure like any other**: it is reported, and the test is rerun alone a few
     times (`-run <Test> -count=5 -timeout ...`) to tell a rare hang from a deadlock the change
     brought in.
   - **Whoever waits on a long run watches its progress, not only its end.** The watcher checks
     that the log keeps growing (`go test -v` or `-json` output) and speaks up when it has not grown
     for 15 minutes; then the running processes are looked at (`ps`, and `kill -QUIT <pid>` for a
     goroutine dump of a test binary). Waiting for the end marker alone is not enough.
   - **Tests have their own deadlines.** A test that waits on a connection, a channel or a
     condition sets a read/write deadline, a `select` with a timeout or a context with a deadline,
     so a bug makes it fail rather than hang.
