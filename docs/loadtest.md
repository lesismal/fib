# Load testing

The conformance suites drive one connection, one request or one hand-written
frame at a time. `benchmark/loadtest` drives the servers the way a load
generator does: hundreds of streams over a few connections and hundreds of
connections with a few streams, for tens of seconds, with a subset of clients
that stops reading its responses. This page records the harness, the campaign
and what it found.

## Running the campaign

```sh
./benchmark/loadtest/run.sh --mode full --rounds 3
```

It needs Go, `h2load` from nghttp2-client, `python3`, `curl`, and on Linux
`taskset` and `flock`; on Debian or Ubuntu, `apt-get install -y
nghttp2-client` provides `h2load`. It builds the three programs below, starts
one server per case, drives it for 30 seconds after a 3-second warm-up, and
writes its output under `/tmp/fib-loadtest/<time>/`:

| File | What it holds |
| --- | --- |
| `results.tsv` | one row per case: requests, req/s, errors, latency, RSS, goroutines, descriptors, open connections, queued bytes |
| `growth.tsv` | one row per repeated short run against one server: RSS, goroutines, descriptors |
| `cases/<name>/` | the load generator's output, the server's log, and `before`, `after` and `settle` snapshots of `/stats` |
| `cases/<name>/server.log` | the server's stderr; nothing was logged in any case of the campaign below |

`--mode smoke` runs one short case per protocol and one stalled case, about a
minute in all, and is the check to run before a release or after a change to a
send path. `--cases LIST` runs only the named cases; `report.py` turns
`results.tsv` into Markdown tables:

```sh
python3 benchmark/loadtest/report.py /tmp/fib-loadtest/<time>
```

The campaign is deliberately not a CI job. It needs `h2load` and a quiet
host, and its numbers mean nothing across machines; the repository's
blocking jobs stay deterministic. Run `--mode smoke` on a test host instead.

## What it drives

| Program | What it does |
| --- | --- |
| `benchmark/loadtest/server` | one process serving HTTP/1 and h2c on one TCP port, HTTP/1 and HTTP/2 over TLS on another, and HTTP/3 on a UDP port, all through the same handler. `/plain` answers `ok\n`; `/big?n=N` answers N bytes of a cached body; `/upload` reads a POST body; `/stream?n=N&chunk=C&delayus=D` writes a body in pieces; `/stats` returns the process's goroutines, descriptors, RSS, heap and the engine's counters, with `?gc=1` forcing a collection and `?reset=1` restarting the counters |
| `benchmark/loadtest/h3load` | a sustained HTTP/3 driver on fib's own client: one QUIC connection per client, a fixed number of requests in flight in total, a per-request timeout, and throughput, p50/p90/p99, per-second completions and error samples when the measurement ends |
| `benchmark/loadtest/stall` | a client that requests `/big?n=8388608`, reads the response header and then either reads it slowly (`-delay`) or stops reading it altogether (`-stall`), one connection per stream. It is run beside the fast load to exercise backpressure, flow control and cleanup |
| `benchmark/loadtest/stallmem.sh` | the stalled-reader case on its own: starts a fresh server, holds N responses of `--body` bytes open without reading them, and records the live heap and RSS from `/stats?gc=1` while they are still open. `--bin DIR` takes a `server` and `stall` already built, so a run before a change and a run after it can be interleaved against the same clients |
| `benchmark/loadtest/run.sh` | the campaign: builds the three and pins the server to the first half of the CPUs and the load generator to the second half, or takes `--bin DIR` for a build made already, starts a fresh server per case, and summarises each case |
| `benchmark/loadtest/ab.sh` | the interleaved A/B: runs the campaign's cases against two builds, A then B and then B then A, waits for the host to be idle before each run, and records the side, round, host load and live heap with every counted run |

HTTP/1 and HTTP/2 are driven by `h2load` 1.59.0: `--h1` for HTTP/1,
prior knowledge for h2c, ALPN for HTTP/2 over TLS. HTTP/3 is driven by
`h3load`, because `h2load` in the distribution packages is built without
QUIC.

## Method

The host is the shared 16-thread Ryzen 7 7800X3D used for the package
benchmarks, with Go 1.27.3, Linux socket buffers of 16MiB, and the server on
CPUs 0-7 and the load generator on 8-15. Each case starts a fresh server
process; the measurement runs for 30 seconds after a 3-second warm-up, and
the six A/B configurations are repeated three times, interleaved, with the
median reported. Other workstreams share the host; the load average was
checked at the start of the campaign, and one three-second `http3.test` from
another workstream overlapped the first HTTP/3 case, so that case's first
round is the low one in the per-run rows. The stalled-reader measurements
before and after the HTTP/3 change were interleaved round by round on that
host with `stallmem.sh`, so that a change in its load moved both sides
together.

Comparing two revisions is a different run. Each side is built once from its
own tree into its own directory, and the two builds are interleaved case by
case with `ab.sh`; the run behind the fast cases below used A = `a88c75d2`
and B = main at `f03d3dff`, five counted rounds per side, a fresh server for
every run and the same CPU split. `ab.sh` waits for the host to be idle
before each run, but not by the load average: the run itself pins all 16
CPUs for 33 seconds, so the average stays between 3 and 20 between runs and
would take a minute and a half to fall below 1.5. It samples `/proc/stat` for
one second instead, and records the load average and the busy percentage of
every run beside its throughput; the rows are in
`benchmark/loadtest/results/2026-10-26-a88c75d2-main/`.

HTTP/1 and HTTP/2 requests and their responses are tiny (`/plain`, 3 bytes),
so the numbers are request-rate numbers, not throughput ones. `h2load`
reports only the minimum, mean, maximum and standard deviation of the time
for a request, not percentiles; the p50 and p99 columns below are therefore
only for HTTP/3, which `h3load` measures itself. The HTTP/3 client is fib's
Go client on the load CPUs, while the HTTP/1 and HTTP/2 client is C; the
protocols are compared with each other under that limitation, not against
an external baseline.

## Results

Medians of three interleaved 30-second runs. "4 x 128" is four connections
with 128 requests in flight each, which is 512 streams; `h3load -conns 4
-streams 512` is the same shape.

| Case | Connections x streams | req/s | mean | p99 | max | errors |
| --- | --- | --- | --- | --- | --- | --- |
| h2c-4x128 | 4 x 128 | 1,199,024 | 0.41ms | - | 3.99ms | 0 |
| h2-4x128 | 4 x 128 | 1,092,137 | 0.43ms | - | 4.33ms | 0 |
| h3-4x128 | 4 x 128 | 897,233 | 0.56ms | 1.56ms | 8.60ms | 0 |
| h2c-64x8 | 64 x 8 | 2,836,822 | 0.15ms | - | 16.49ms | 0 |
| h2-64x8 | 64 x 8 | 2,404,461 | 0.16ms | - | 19.99ms | 0 |
| h3-32x16 | 32 x 16 | 689,402 | 0.71ms | 2.90ms | 33.52ms | 0 |

Connection and stream shapes that each protocol handles differently, one
run each:

| Case | Load | req/s | mean | p99 | max | errors |
| --- | --- | --- | --- | --- | --- | --- |
| h1-16c | 16 connections, one request each | 414,508 | 0.04ms | - | 3.40ms | 0 |
| h1-256c | 256 connections, one request each | 799,966 | 0.33ms | - | 19.03ms | 0 |
| h1-pipeline-4x16 | 4 connections, 16 pipelined requests each | 1,760,884 | 0.04ms | - | 3.48ms | 0 |
| h2c-512x1 | 512 connections, one stream each | 762,134 | 0.39ms | - | 20.75ms | 0 |
| h2-512x1 | 512 connections, one stream each | 575,979 | 0.49ms | - | 25.79ms | 0 |
| h3-1x256 | 1 connection, 256 streams | 373,362 | 0.68ms | 1.34ms | 4.40ms | 0 |

Across the three rounds and the connection-scale cases, the campaign drove
just over a billion requests. No request failed, errored or timed out, and no
server logged a warning; a GOAWAY or reset storm would have appeared as
failed or errored requests in that count. The
512-connection cases open 512 TLS handshakes at once; they completed with no
failure, peak 533 descriptors and 45-52MiB RSS, so no handshake pile-up
appeared at this scale.

What the numbers say:

- **HTTP/2 scales with connections.** Four connections carrying 512 streams
  give 1.20M requests a second over h2c; 64 connections with 8 streams each
  give 2.84M. The server is not short of stream concurrency at four
  connections, it is short of reader and worker parallelism: each connection
  is read by one goroutine.
- **TLS costs 9-15%.** h2c is 1.20M against 1.09M over TLS at 4 x 128, and
  2.84M against 2.40M at 64 x 8: the TLS cost is steady across connection
  counts, and the handshake and record work is not the bottleneck at this
  request size over loopback.
- **HTTP/3 peaks at a much smaller connection count.** 897k requests a
  second at 4 x 128, 689k at 32 x 16 and 373k on one connection with 256
  streams. More QUIC connections cost more than they buy here; each has its
  own crypto and congestion state, and the fib client and server share the
  same eight load CPUs, where the C `h2load` does not. At the same shape
  (4 x 128) h3 reaches 75% of h2c's requests a second, or 82% of h2's, and its
  p99 is 1.56ms. At the best shape for each, h3 is 32% of h2c.
- **HTTP/1 pipelining works, and is fast.** Four connections with 16
  pipelined requests each reached 1.76M requests a second, more than 256
  non-pipelined connections (800k), with no failures. It is a shape browsers
  do not use, but it exercises the parser's multiple-requests-per-read path
  under load.
- **Latency grows where throughput does.** The mean is 0.15ms at 64 x 8 and
  0.41ms at 4 x 128 over h2c; HTTP/3's p99 grows from 1.56ms to 2.90ms as
  connections go from 4 to 32, and its maximum from 8.60ms to 33.52ms.

## Slow readers and memory

`stall` was run beside the fast load as two groups of eight connections: one
group stopped reading after the response header, the other read 16KiB every
50ms. All three protocols kept the fast load serving, with no fast request
failing:

| Slow case | Fast load | Fast req/s | Live heap at the after snapshot | Peak RSS | RSS 2s after the clients end |
| --- | --- | --- | --- | --- | --- |
| slow-h1 | 16 connections | 388,639 | 156MiB | 257.6MiB | 232.5MiB |
| slow-h2c | 4 x 128 | 866,056 | 25MiB | 73.4MiB | 39.0MiB |
| slow-h3 | 4 x 128 | 273,167 | 37MiB (after the idle timeout) | 97.4MiB | 78.4MiB |

Those HTTP/3 figures are after the stalled connections were closed by the
30-second idle timeout. Measured while the streams were still open, against
a fresh server with the campaign's `/big` body cached, the responses cost:

| Protocol | Stalled streams | Live heap | RSS |
| --- | --- | --- | --- |
| h1 | 16 | 268MiB | 206MiB |
| h2c | 16 | 45MiB | 158MiB |
| h3 | 1 | 32MiB | 31MiB |
| h3 | 4 | 81MiB | 79MiB |
| h3 | 16 | 275MiB | 260MiB |

The HTTP/1 row was re-measured with `stallmem.sh` for the change described
below; the other rows are the original campaign's. The HTTP/3 figures are the
ones before its send path was changed; the two after it, measured with
`stallmem.sh` (three interleaved rounds of each server, medians, server and
stalled clients on separate halves of the host), are much lower. The body is
the test server's cached one in every row, so it is a single 8MiB copy however
many streams hold it:

| Stalled streams | Before: live heap | Before: RSS | Now: live heap | Now: RSS |
| --- | --- | --- | --- | --- |
| 1 | 32MiB | 29MiB | 16MiB | 21MiB |
| 4 | 83MiB | 78MiB | 19MiB | 46MiB |
| 16 | 275MiB | 209MiB | 19MiB | 113MiB |

The HTTP/1 path was changed the same way later, and measured with the same
script, three interleaved rounds of the server before and after, medians, the
same cached 8MiB body:

| Stalled connections | Before: live heap | Before: RSS | Now: live heap | Now: RSS |
| --- | --- | --- | --- | --- |
| 1 | 27MiB | 28MiB | 11MiB | 20MiB |
| 4 | 75MiB | 76MiB | 11MiB | 44MiB |
| 16 | 268MiB | 206MiB | 12MiB | 85MiB |

Before, each connection held a copy of the body in a 16MiB pool class, so 16
of them cost 16 x 16MiB on top of the cached body. Now the queue holds the
handler's slice, so the 16 hold the one 8MiB the test server cached, and the
live heap stays at 12MiB however many stall. RSS still climbs with the
connection count because each has the socket buffers the kernel keeps, which
is what 85MiB at 16 connections is.

The difference is in how each send path holds a whole response:

- **HTTP/2** frames only what the stream and connection windows allow now,
  and holds the rest as a reference to the handler's `Response.Body`. A
  stalled stream therefore costs about its stream window (1MiB) of frame
  buffers, not the body, which is why 16 stalled streams leave only 45MiB of
  live heap. Its 158MiB RSS is allocator high-water from the frame buffers,
  and falls to 39MiB once the streams end.
- **HTTP/1** handed the head and the body to `Conn.SendParts`, which copied
  what the socket did not take into a buffer from the pool: a stalled
  connection held a copy of the body rounded up to a power-of-two class, and
  16 of them held 16 copies. The response path now keeps a body above 4KiB as
  the handler gave it (`Conn.SendPartsShared`), queued beside the head and
  counted by its length, so what 16 stalled connections hold is the one cached
  8MiB body. A body of 4KiB or less is still copied into the round's send
  buffer, which is what keeps a small reply one buffered write. See
  [HTTP/1](http1.md) and `fibhttp.Response.Body`.
- **HTTP/3** built the response's HEADERS and its whole body into one frame
  buffer and gave it to the QUIC stream with `WriteFinal`, which kept it
  until the peer acknowledged it. `bufferpool` classes are powers of two, so
  an 8MiB body took a 16MiB buffer: 16 stalled streams cost 16 x 16MiB plus
  the one cached 8MiB body, which is the 275MiB of live heap above. There was
  no per-response queue cap on this path.
- **HTTP/3 now** keeps the body where the handler put it, as HTTP/2 does, and
  frames it as the peer's windows, and then its acknowledgements, let it go:
  the stream holds the response as runs (`quic.Stream.WriteFinalParts`), so
  what a stalled stream costs is the frames the flow-control window admits,
  about a fifth of a MiB each, rather than a pool buffer larger than the body.
  The only copy of the body left is the one into the packets, which the AEAD
  needs, whatever the body's size: a small response's frames are still one
  pool buffer, so a small whole response costs no more than a large one does.
  See [HTTP/3](http3.md) and `fibhttp.Response.Body` for what that asks of the
  handler: the body must not be changed until the peer has acknowledged it.

This is a deployment limit rather than a wrong answer: the peers are
well-behaved enough not to be reset, and the fast load carries on. It is
worth planning for:

- Stream a large response with `Context.Write` and `Flush` instead of handing
  it to `Respond` whole. Streamed writes wait once 64KiB is unsent on HTTP/2
  and HTTP/3, and HTTP/1 pauses reads at `Config.WriteBufferHighWatermark`,
  so memory stays bounded by the stream window or watermark rather than by
  the body.
- Set `Config.MaxWriteQueueBytes` to close an HTTP/1 connection whose peer
  will not read, and `Config.MaxPendingBytes` to bound the server's queued
  output as a whole. Both default to unlimited.
- A whole response no longer bounds an HTTP/3 server by its body: what a
  stalled stream holds is what the peer's windows let through, so
  `MaxConcurrentStreams` times the stream window, plus the bodies the handlers
  themselves keep, is the bound. HTTP/1 now holds the handler's body too, one
  copy however many stalled connections were given it, so what a stalled
  HTTP/1 reader costs is the body itself; `MaxWriteQueueBytes` still bounds
  how much may wait, and streaming is what keeps it from being the whole
  body at once.
- The 30-second QUIC idle timeout closes a connection whose stalled stream
  keeps no packets flowing. That is what the campaign's stalled HTTP/3
  clients reported, as `quic: idle timeout`, not as a reset or a GOAWAY; a
  slow reader that keeps consuming sends window updates and stays open.

## Repeated runs

Ten 5-second runs of 4 x 128 fast load were made against one server process,
first over h2c and then over HTTP/3. Descriptors stayed at 22 throughout.
Goroutines after each run stayed at the handler pool's loaded size (about
1,150 to 1,280 for h2c, 735 to 911 for HTTP/3) rather than climbing run by
run; two seconds after the load, the case snapshots showed 296 idle workers
for h2c and 58 to 162 for HTTP/3, so the pool does drain. RSS levelled off
at 33-38MiB for h2c; HTTP/3's rose from 39MiB to a 48-52MiB band. No per-run
descriptor or goroutine leak appeared. The HTTP/3 RSS drift is allocator
high-water rather than proven growth; a longer soak is still worth running
before calling the HTTP/3 heap flat.

The campaign changed no production code. Nothing in it was a concurrency bug
that a small, isolated fix would remove: it found no failed request, reset
storm, GOAWAY storm, stuck connection or descriptor/goroutine leak. The memory
cost of whole responses to slow readers is the one limit it exposed, and it
was a property of the whole-body send paths and the buffer classes, not a
one-line defect.

That limit was then fixed for HTTP/3, and only for HTTP/3: the QUIC stream now
keeps the handler's body as it is, and its send path builds every packet where
it is sent, so neither the response nor a datagram is copied together. HTTP/1
and HTTP/2 were as they were at that point (HTTP/1's copy was removed later;
see its table above). The fast cases were compared again, this time as an
interleaved A/B of the two revisions, below.

### Fast cases: a clean A/B between the two revisions

The earlier version of this section compared the fast cases between the two
full campaigns, run an hour apart. That comparison cannot separate the code
from the host: HTTP/1's and HTTP/2's code did not change between the
campaigns, yet their request rates moved by 39% and 66% at 16 and 256
connections and by 10% and 14% at 4 x 128, while HTTP/3's moved by 76% at
4 x 128 and 7% at 32 x 16. The two revisions were therefore built once each
from their own trees and run against the same clients, one case at a time,
five counted rounds per side:

- A is `a88c75d2`, the base the earlier comparison used.
- B is main at `f03d3dff`, the revision of this branch.
- A round is A then B on odd rounds and B then A on even ones; every run has
  a fresh server, 30 seconds of measurement after a 3-second warm-up, and the
  same CPU split (server 0-7, clients 8-15). Nothing is rebuilt between
  rounds.
- Before each counted run `ab.sh` sampled `/proc/stat` for one second and
  waited while more than 20% of the host was busy with something else. 72 of
  the 120 fast runs started with the host idle and the rest with up to 20%
  busy, which is the gate's threshold. Sixteen runs that were still busy when
  they finished were discarded and repeated; `loadavg-fast.tsv` has every
  attempt, counted or not. The one-minute load average recorded with each row
  is 2.9-20.4: it includes the campaign's own 33-second run on all 16 CPUs and
  has not decayed between runs, which is why the gate is the busy sample and
  not the average.
- 120 counted fast runs over 12 cases: no request failed, errored or timed
  out, and no server logged a warning.

Medians of the five runs per side, with each side's minimum-maximum range:

| Case | A req/s | A min-max | B req/s | B min-max | A -> B | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| h1-16c | 414,794 | 390,685-415,463 | 408,712 | 399,914-414,979 | -1.5% | noise |
| h1-256c | 712,925 | 660,863-750,313 | 704,682 | 649,985-735,142 | -1.2% | noise |
| h1-pipeline-4x16 | 1,644,221 | 1,606,829-1,685,558 | 1,639,282 | 1,557,372-1,652,933 | -0.3% | noise |
| h2c-4x128 | 1,195,445 | 1,136,841-1,208,638 | 1,197,895 | 1,146,898-1,217,977 | +0.2% | noise |
| h2-4x128 | 1,126,303 | 1,124,792-1,131,276 | 1,128,179 | 1,124,704-1,131,844 | +0.2% | noise |
| h3-4x128 | 949,003 | 930,502-976,227 | 940,551 | 884,371-944,290 | -0.9% | noise |
| h2c-64x8 | 2,855,628 | 2,848,286-2,964,777 | 2,876,195 | 2,849,593-2,974,949 | +0.7% | noise |
| h2-64x8 | 2,548,596 | 2,491,822-2,647,748 | 2,533,972 | 2,482,374-2,642,316 | -0.6% | noise |
| h3-32x16 | 696,093 | 688,773-701,082 | 686,090 | 666,388-697,011 | -1.4% | noise |
| h2c-512x1 | 745,574 | 729,465-763,091 | 748,591 | 745,657-760,357 | +0.4% | noise |
| h2-512x1 | 568,584 | 547,394-569,659 | 567,669 | 565,630-571,005 | -0.2% | noise |
| h3-1x256 | 387,968 | 369,148-398,239 | 382,323 | 370,287-385,478 | -1.5% | noise |

A difference is called real only when the two sides' ranges do not overlap
and the median delta is larger than the wider of the two spreads; that is a
conservative test for five runs on a shared host. By it, all twelve fast
cases are noise: every pair of ranges overlaps, and the median deltas are
between -1.5% and +0.7%, smaller than the wider spread in every case. No
fast-case regression, and no improvement, is large enough to be seen above the
host's spread in the 17 commits between the two revisions: neither the HTTP/3
whole-response change nor the HTTP/1 body-ownership change is visible in
request rate at these shapes.

Mean latency followed the request rate: the largest change in the median mean
was 0.02ms (`h1-256c`), and HTTP/3's p99 medians were 1.36ms against 1.40ms
at 4 x 128, 2.70ms against 2.64ms at 32 x 16, and 1.33ms against 1.34ms on
one connection. RSS after a run and two seconds after it are within 4MiB
between the two sides in eleven of the twelve cases; HTTP/3's idle RSS is
1-7MiB higher on B (30.0 against 36.5MiB at 4 x 128, ranges 28.3-35.3 and
32.1-38.5). Those ranges overlap, so the whole-response change could explain
the difference, but this run does not separate it from the host. The live heap
after each run and once idle is in
`benchmark/loadtest/results/2026-10-26-a88c75d2-main/`.

The three slow cases were measured the same way in a second batch, once the
host had quieted, and they stay where the earlier campaign found them:

| Case | A req/s | A min-max | B req/s | B min-max | A -> B | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| slow-h1 | 415,868 | 413,606-416,599 | 416,437 | 415,634-416,956 | +0.1% | noise |
| slow-h2c | 1,206,099 | 1,104,663-1,214,633 | 1,214,393 | 1,103,869-1,219,205 | +0.7% | noise |
| slow-h3 | 987,300 | 915,355-999,254 | 946,572 | 777,850-953,614 | -4.1% | noise |

`slow-h3` has the widest spread and one low B run; its ranges overlap, so by
the same rule it is noise. What is not noise is what the held responses cost:
the live heap at the after snapshot was 156.2MiB on A against 12.1MiB on B for
`slow-h1` (RSS 320.8 against 39.6MiB), 20.5 against 15.5MiB for `slow-h2c`
(RSS 61.0 against 56.3MiB, ranges overlapping), and 37.1 against 21.1MiB for
`slow-h3` (RSS 96.0 against 62.0MiB). That is the whole-response change, and
it matches the interleaved `stallmem.sh` tables above: the fast load is
unchanged, the memory a stalled reader costs is not.

The two growth cases ran once per side: ten 5-second runs against one server
kept descriptors at 22 on both sides, RSS ended at 36MiB (A h2c) and 37MiB
(B h2c) from 31 and 32MiB, and at 46MiB for both over HTTP/3 from 42 and
43MiB. The same smoke test as before, not an A/B.

The before/now table does not survive this. Its "before" column is below the
clean A side's minimum in all seven of its fast cases (h1-16c 300k against
391-415k, h3-4x128 539k against 931-976k), so that campaign measured the
host's load, not the revision. Its "now" column is not a clean second
measurement either: h1-256c reads 828k against a clean B range of 650-735k,
and h3-1x256 338k against 370-385k; the other fast cases are within 1.5% of a
clean range. The campaign's own medians in the table above stand as a
description of that build; they are not a comparison between revisions. What
the 17 commits do change is what a stalled reader costs, which both the
`stallmem.sh` measurement above and the slow cases' live heap in this run
show.

## Limits of the campaign

- One host, one kernel, loopback: no packet loss, no NAT, no real network
  round trip. The QUIC congestion controller, loss recovery and PMTU
  discovery are therefore barely exercised.
- The load generator shares the host with the server, and the HTTP/3 load
  generator is Go on the same CPUs; the protocols are compared with each
  other, not against an external baseline.
- `h2load` reports no percentiles, so HTTP/1 and HTTP/2 have mean and maximum
  latency only, while HTTP/3 has p50/p90/p99 from `h3load`.
- The stalled HTTP/3 case is cut short by the QUIC idle timeout at 30 seconds;
  a longer hold needs `KeepAlivePeriod` or a reader that consumes and then
  stops.
- Ten 5-second runs are a smoke test for leaks, not a soak. A multi-hour run
  with connection churn, TLS session resumption and varied body sizes is
  still open.
- Five rounds per side are enough to see that a fast-case difference is inside
  the host's spread; they cannot resolve a few percent, and a case whose
  ranges overlap is called noise rather than proven equal. The fast batch
  started every counted run with 0-20% of the host busy and the slow batch
  with 0-15%; sixteen fast attempts were discarded and repeated because the
  host was still busy when they finished, and the slow batch had none. The
  load average and busy percentage of every attempt, counted or not, are in
  `benchmark/loadtest/results/`.
