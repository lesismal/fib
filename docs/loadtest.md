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
| `benchmark/loadtest/run.sh` | the campaign: builds the three, pins the server to the first half of the CPUs and the load generator to the second half, starts a fresh server per case, and summarises each case |

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
round is the low one in the per-run rows.

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
a fresh server, 16 stalled 8MiB responses cost:

| Protocol | Stalled streams | Live heap | RSS |
| --- | --- | --- | --- |
| h1 | 16 | 147MiB | 258MiB |
| h2c | 16 | 45MiB | 158MiB |
| h3 | 1 | 32MiB | 31MiB |
| h3 | 4 | 81MiB | 79MiB |
| h3 | 16 | 275MiB | 260MiB |

The difference is in how each send path holds a whole response:

- **HTTP/2** frames only what the stream and connection windows allow now,
  and holds the rest as a reference to the handler's `Response.Body`. A
  stalled stream therefore costs about its stream window (1MiB) of frame
  buffers, not the body, which is why 16 stalled streams leave only 45MiB of
  live heap. Its 158MiB RSS is allocator high-water from the frame buffers,
  and falls to 39MiB once the streams end.
- **HTTP/1** calls `Conn.SendParts(head, body)`. If the socket takes only
  part of the body, `queueLocked` copies the rest into a buffer from the
  pool and keeps it until the socket takes it. A stalled connection therefore
  holds a copy of the body, rounded to a power-of-two class. The copy is
  released when the peer goes, but the process's RSS stays high until the
  runtime reuses it.
- **HTTP/3** builds the response's HEADERS and its whole body into one frame
  buffer and gives it to the QUIC stream with `WriteFinal`, which keeps it
  until the peer acknowledges it. `bufferpool` classes are powers of two, so
  an 8MiB body takes a 16MiB buffer: 16 stalled streams cost 16 x 16MiB plus
  the one cached 8MiB body, which is the 275MiB of live heap above. There is
  no per-response queue cap on this path.

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
- Lower `MaxConcurrentStreams` on an HTTP/3 server that answers large bodies
  whole. The bound is roughly `MaxConcurrentStreams` times the body size
  rounded up to a power of two, plus the cached bodies.
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

No production code was changed for this campaign. Nothing in it was a
concurrency bug that a small, isolated fix would remove: the campaign found
no failed request, reset storm, GOAWAY storm, stuck connection or
descriptor/goroutine leak. The memory cost of whole responses to slow
readers is the one limit it exposed, and it is a property of the whole-body
send paths and the buffer classes, not a one-line defect; it is documented
above with the mitigations.

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
- The campaign ran on the commit before the branch was rebased onto main,
  with one three-second test from another workstream in the first HTTP/3
  round. A two-round rerun of the six A/B cases after the rebase matched the
  first four within 0-5%; h2c-64x8 read 2.50-2.57M against 2.71-2.92M and
  h3-32x16 625-656k against 661-701k, within the spread those two show on a
  shared host.
