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
