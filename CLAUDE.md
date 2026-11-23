# fib

The development rules below are loaded into every session and bind every change here.

@docs/rules.md

In short: clean up after every development, test and benchmark run (rule 1): binaries, test
binaries, copies, worktrees, containers and temporary files, here and on the benchmark host, whose
disk every session shares.
