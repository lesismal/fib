# fib

The development rules below are loaded into every session and bind every change here.

@docs/rules.md

In short: clean up after every development, test and benchmark run (rule 1): binaries, test
binaries, copies, worktrees, containers and temporary files, here and on the benchmark host, whose
disk every session shares.

**Feature tasks run only the basic CI tests** (rule 2): no load tests, benchmarks, A/B
comparisons, profiles or stress runs, here or on the benchmark host, unless the user asks for one in
that task. Benchmarks are the maintainer's separate tasks (HTTPArena and the like); a feature task
says in its report what should be measured.
