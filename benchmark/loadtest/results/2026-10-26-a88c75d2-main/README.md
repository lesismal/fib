# Interleaved A/B, 2026-10-26

Two builds of fib, A = `a88c75d2` and B = main at `f03d3dff`, were run against
the same clients one case at a time on the 16-CPU test host (server on CPUs
0-7, load generator on 8-15), with the campaign's 30-second measurement after
a 3-second warm-up and five counted rounds per side:

```sh
./benchmark/loadtest/ab.sh --a /root/dsh/ab-a/bin --b /root/dsh/ab-b/bin \
    --out /tmp/fib-ab/main-5r --rounds 5
```

`ab-fast.tsv` is one row per counted fast-case run: `results.tsv`'s columns
with the side, the round, the one-minute load average and the busy percentage
before and after the run, and the live heap after and once idle.
`loadavg-fast.tsv` has every attempt, counted or not: 16 of the 136 were
discarded because the host was still busy when they finished, and repeated.

`ab-slow.tsv` and `loadavg-slow.tsv` are the same for the three slow cases,
run in a second batch once the host had quieted. All of its 34 attempts were
counted (30 slow-case runs and the four growth runs), and no run was
discarded. `growth/` has each side's `growth.tsv` for the two growth cases:
ten 5-second runs against one server.

The five rounds per side and the verdict are in
[docs/loadtest.md](../../../../docs/loadtest.md). The host's one-minute load
average recorded with a run includes the campaign itself (33 seconds on all
16 CPUs), so the gate was a one-second `/proc/stat` busy sample, not the
average.
