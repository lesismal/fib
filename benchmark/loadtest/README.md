# Load testing

The campaign's code and driver. The measurements, the method and what they
found are in [docs/loadtest.md](../../docs/loadtest.md) (and
[its Chinese version](../../docs/loadtest.zh-CN.md)); this file is the
operator's notes.

## Requirements

- Go 1.27.3 or the version in `go.mod`
- `h2load` from nghttp2-client (`apt-get install -y nghttp2-client`)
- `python3`, `curl`, and on Linux `taskset` and `flock`

Nothing here adds a dependency to fib's `go.mod`; the programs import only the
standard library and fib itself.

## Running

```sh
./benchmark/loadtest/run.sh --mode smoke            # about a minute
./benchmark/loadtest/run.sh --mode full --rounds 3  # the campaign, about 20 minutes
```

From another machine, sync the worktree and run it there; for example with
the DSH test host:

```sh
/tmp/dsh-testhost/rt.sh "$PWD" loadtest \
  'nohup bash benchmark/loadtest/run.sh --mode full --rounds 3 \
     --out /tmp/fib-loadtest > /tmp/fib-loadtest.log 2>&1 &'
```

Use `nohup` or another detached launch: a retried ssh would otherwise start a
second campaign. `run.sh` holds `/tmp/fib-loadtest.lock` with `flock`, so a
duplicate exits instead of double-driving the host.

Useful options:

| Option | Effect |
| --- | --- |
| `--mode smoke\|full` | a short check, or the whole campaign |
| `--duration N` | measurement seconds per case (default 30) |
| `--warmup N` | warm-up seconds (default 3) |
| `--rounds N` | interleaved rounds of the six A/B cases (default 3) |
| `--cases LIST` | only these case names, comma separated |
| `--bin DIR` | run the `server`, `h3load` and `stall` already built in DIR |
| `--growth-runs N` | repeated short runs against one server (default 10) |
| `--out DIR` | output directory |
| `--no-pin` | do not split the CPUs between server and load generator |

The server is pinned to CPUs 0..n/2-1 and the load generator to n/2..n-1
when `taskset` is available, so a Go client and a Go server do not steal each
other's cores. The campaign starts a fresh server per case, records
`/stats?gc=1` before, after and two seconds after each case, and writes one
row per case to `results.tsv`.

## A clean A/B between two revisions

The campaign's rounds compare configurations of one build. To compare two
revisions on a shared host, build each side once from its own tree into a
directory of its own, then interleave the two builds with `ab.sh`:

```sh
(cd /tmp/fib-before && go build -o bin/server ./benchmark/loadtest/server &&
  go build -o bin/h3load ./benchmark/loadtest/h3load &&
  go build -o bin/stall  ./benchmark/loadtest/stall)
# the same in /tmp/fib-after
./benchmark/loadtest/ab.sh --a /tmp/fib-before/bin --b /tmp/fib-after/bin \
  --out /tmp/fib-ab --rounds 5
```

`ab.sh` runs one case at a time, A then B, then B then A, and so on, with a
fresh server for every run and the same CPU split; nothing is rebuilt between
rounds. Before each run it samples `/proc/stat` for one second and waits
while more than `--max-busy` percent (20) of the host is busy with something
else, for up to `--wait-max` seconds (1200) in total. A run whose host was
still busy when it finished is not counted and is repeated.

The one-minute load average cannot be the gate during a campaign: the run
pins all CPUs for 33 seconds, so the average stays between 3 and 20 between
runs even on an otherwise idle host, and waiting for it to fall below 1.5
would add up to a minute and a half to every run and measure the decay of the
campaign's own load. `ab.sh` gates on the one-second busy sample instead and
records the average: `ab.tsv` adds side, round, `load1_before`, the busy
percent before and after, and the live heap after and once idle to the columns
of `results.tsv`, and `loadavg.tsv` has a row for every attempt, counted or
not. `--cases` and `--duration` work as they do for `run.sh`; `growth-*`
cases run once per side by default and their `growth.tsv` is copied to
`growth/`.

The campaign that used it, and the verdict it produced, are in
[docs/loadtest.md](../../docs/loadtest.md); its `ab-fast.tsv`, `ab-slow.tsv`
and `loadavg-*.tsv` are under `results/`.

## Output

```
<out>/
  results.tsv          one row per case
  growth.tsv           repeated short runs against one server
  cases/<name>/        load.out, server.log, before.json, after.json, settle.json, stall-*.out
  cases/<name>-addr.json
  bin/                 server, h3load, stall
```

`results.tsv` columns are, in order: case, protocol, concurrency, requests,
req/s, 2xx, failed, errored, timeout, p50, p99, mean, max, RSS before, RSS
after, RSS settle, goroutines before/after/peak/settle, descriptors
after/peak/settle, open connections, pending bytes, paused reads, stalled
bytes, stalled errors, stalled clients without a response, server warnings,
and the load generator's exit code. Blank latency cells mean the tool does
not report that number (`h2load` has no percentiles).

`report.py` groups repeated rounds by case and prints the median:

```sh
python3 benchmark/loadtest/report.py <out>
```

`ab.sh` writes `runs/<case>/<side>-<round>-a<attempt>/`, one `run.sh` output
directory per run, and its merged `ab.tsv` and `loadavg.tsv` beside them.
`ab.tsv` is `results.tsv` with `side`, `round`, `load1_before`, `busy_before`,
`load1_after` and `busy_after` prepended, and the live heap after the run and
once idle appended; `loadavg.tsv` records every attempt including the ones it
did not count.

## A stalled reader on its own

`stallmem.sh` measures what the load campaign's `slow-*` cases hold open for
30 seconds: one fresh server, N responses of 8MiB never read, and the live
heap and RSS from `/stats?gc=1` while they are still open.

```sh
./benchmark/loadtest/stallmem.sh --proto h3 --streams 1,4,16 --rounds 3
```

`--bin DIR` runs a `server` and `stall` already built instead of building
them, so a run before a change and a run after it can be interleaved against
the same clients:

```sh
./benchmark/loadtest/stallmem.sh --bin /tmp/before --streams 1,4,16 --out /tmp/sm-before-1
./benchmark/loadtest/stallmem.sh --bin /tmp/after  --streams 1,4,16 --out /tmp/sm-after-1
```

It writes one row per measurement to `<out>/stallmem.tsv`.

## Cases

| Name | Load | What it shows |
| --- | --- | --- |
| `h1-16c`, `h1-256c` | h2load `--h1 -c 16|256 -m 1` | HTTP/1 at two connection counts |
| `h1-pipeline-4x16` | h2load `--h1 -c 4 -m 16` | pipelined requests on few connections |
| `h2c-4x128`, `h2-4x128` | 4 connections, 128 streams each | h2c and HTTP/2 over TLS at 512 streams |
| `h2c-64x8`, `h2-64x8` | 64 connections, 8 streams each | the same 512 streams over many connections |
| `h2c-512x1`, `h2-512x1` | 512 connections, one stream each | descriptor and handshake cost |
| `h3-4x128` | 4 connections, 512 streams | HTTP/3 at the HTTP/2 shape |
| `h3-32x16` | 32 connections, 512 streams | HTTP/3 across many connections |
| `h3-1x256` | 1 connection, 256 streams | HTTP/3 on one connection |
| `slow-h1`, `slow-h2c`, `slow-h3` | the fast case plus 8 stalled and 8 slow readers on their own connections | backpressure and the memory a slow reader costs |
| `growth-h2c`, `growth-h3` | 10 x 5 s fast load against one server | RSS, goroutines and descriptors across repeated runs |

The `slow-*` cases hold each stalled response for the duration of the load.
HTTP/3 closes a connection whose stalled stream lets no packets flow after its
30-second idle timeout, which the stalled client reports as `quic: idle
timeout`; that is expected, not a reset.

## Notes for a new host

- Check `uptime` and the top CPU processes first. The campaign is CPU-bound
  at 512 streams and above; another load test on the same host moves the
  numbers.
- The loopback path has no loss. A campaign over a real network should also
  vary `--duration` and body sizes; it is not the same measurement.
- `h2load` does not verify the server's certificate in this version, and
  `h3load` sets `InsecureSkipVerify`, so the server's self-signed certificate
  is enough.
- The HTTP/3 driver uses fib's Go client on the same host. Do not compare its
  request rate with `h2load`'s as if both were external clients; compare the
  protocols under that limitation, or add a non-Go HTTP/3 load generator.
