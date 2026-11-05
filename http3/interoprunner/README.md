# fib ⇄ QUIC Interop Runner

A [QUIC Interop Runner](https://github.com/quic-interop/quic-interop-runner)
endpoint for fib's HTTP/3 stack: one Docker image that serves the runner's
files as an HTTP/3 or HTTP/0.9-over-QUIC server, and downloads the runner's
URLs as a client. It is a module of its own, like `../interop`, so none of
this is a dependency of `github.com/lesismal/fib`.

The runner drives almost every testcase over **HTTP/0.9** — a request line
and the raw file on a QUIC stream — and only the `http3` testcase over
HTTP/3. This endpoint therefore speaks both:

- the QUIC-level testcases run on fib's QUIC transport (`http3/quic`) with a
  small file server and client on top, so handshake, transfer, Retry,
  resumption, 0-RTT, key update and NAT rebinding are exercised;
- `http3` runs on fib's full HTTP/3 server and client (`http3`).

## Running it locally

The quickest check, which needs only Docker and openssl, builds the image and
runs seven cases with the image on both sides on a docker network:

```sh
./local-test.sh                    # image fib-interop:local
```

To run the real runner against another implementation:

```sh
# once: docker compose, tshark, and the runner's Python dependencies
git clone --depth 1 https://github.com/quic-interop/quic-interop-runner /tmp/quic-interop-runner
python3 -m pip install -r /tmp/quic-interop-runner/requirements.txt

./run-runner.sh                           # fib vs quic-go, quiche and ngtcp2, both directions
TESTCASES=http3 ./run-runner.sh           # just the HTTP/3 testcase
PEERS=quiche ./run-runner.sh              # one peer; any name in implementations_quic.json
PEERS=quic-go,mvfst ./run-runner.sh       # several
DIRECTION=server TESTCASES=chacha20 ./run-runner.sh
```

`run-runner.sh` clones the runner into `$RUNNER_DIR` (default
`/tmp/quic-interop-runner`) if needed, registers the image as `fib` in the
runner's `implementations_quic.json`, and runs the matrix against each peer
in `PEERS` (default `quic-go,quiche,ngtcp2`; `PEER`, one name, still
works). It goes on after a failed run and exits non-zero if any run had a
failed testcase. Build the image first, from the repository root:

```sh
docker build -f http3/interoprunner/Dockerfile -t fib-interop:local .
```

The runner needs `docker compose`, `tshark` (4.5.0 or newer, as its README
says; older versions cannot follow a migrated connection) and these Python
modules: `pycryptodome`, `random-slugs`, `termcolor`, `prettytable`,
`pyshark`. `pyshark` does not run on Python 3.14; use 3.12, as the runner's
own CI does.

The runner's compose file names the containers' interfaces
(`interface_name`), which needs Docker Engine 28.1 and Compose 2.36 or newer.
On a machine with an older Docker, Docker Desktop on macOS for one, it can
run inside a `docker:29-dind` container instead, with `tshark`, Python 3.12
and the runner's requirements installed in it and the images loaded with
`docker save | docker exec -i … docker load`. Two things are needed there:
`sysctl -w net.bridge.bridge-nf-call-iptables=0
net.bridge.bridge-nf-call-ip6tables=0` inside the dind container, or Docker's
own rules drop the client's packets to the server's address on the
simulator's bridge (they are routed through the simulator, so their
destination is not on the bridge they arrive on), and an `amd64` peer image
on an `arm64` host runs emulated, slowly enough to change some results (see
below).

## The container contract

The runner passes everything through the environment and mounts; the image
reads it and starts the server or client of `TESTCASE`.

| Variable | Meaning |
| --- | --- |
| `ROLE` | `server` or `client`; required |
| `TESTCASE` | the testcase to serve or run; an unsupported one exits 127 |
| `REQUESTS` | space-separated URLs the client downloads (`client`) |
| `DOWNLOADS` | directory the client writes them to, default `/downloads` |
| `SSLKEYLOGFILE` | NSS key log the runner decrypts captures with; written when set |
| `QLOGDIR` | created and otherwise unused: fib does not write qlog |
| `LOGS` | created; the runner copies it out of the container |
| `CERT`, `KEY` | certificate chain and key, default `/certs/cert.pem`, `/certs/priv.key` |
| `WWW` | directory the server serves, default `/www` (an extension; the runner mounts `/www`) |

The runner mounts `/www` (the files to serve), `/downloads` (the client's
output), `/certs` (the generated chain and key) and expects `/logs` to exist.
`run_endpoint.sh` creates the directories it needs and runs the endpoint
base image's `/setup.sh`, which installs the routes through the simulator,
only when a `sim` host is present — so the same image also runs on a plain
docker network, as `local-test.sh` does.

## Testcases

This table is what the endpoint does in each role; the results against
quic-go, quiche and ngtcp2 side by side are in
[Against quiche and ngtcp2](#against-quiche-and-ngtcp2). A "✓" here is a run
that passed against quic-go's published endpoint image
(`martenseemann/quic-go-interop:latest`, commit `d961427`) on Linux with
Docker 29.1.3 and tshark 4.6.6, on 2026-10-26, or on 2026-11-01 where the
row says so. The runner names a testcase to a role through `TestCase.testname`: most of its
matrix reaches the endpoint as `transfer`, `handshake` or `multiconnect`,
which is why those names cover more than their own testcase. In particular
the loss and corruption cases all map to a raw path: `handshakeloss` and
`handshakecorruption` to `multiconnect` (one connection per file), and
`transferloss` and `transfercorruption` to `transfer`; the runner's
simulator supplies the loss or corruption. The endpoint also accepts the
four names directly, as aliases of those paths.

| Testcase | Server | Client | Notes |
| --- | --- | --- | --- |
| `handshake` | ✓ | ✓ | HTTP/0.9 over QUIC |
| `transfer` | ✓ | ✓ | three files, parallel streams; the client's receive windows are small so fib has to grow them |
| `longrtt` | ✓ | ✓ | the transfer succeeds under a 750 ms one-way RTT, and the client's Initial goes again on its probe timeout: with no RTT sample that is RFC 9002's initial PTO, about a second, inside the capture |
| `chacha20` | ✓ | ✓ | the client's ClientHello offers ChaCha20-Poly1305 alone, through fib's own TLS handshake and `tls.QUICConn.SetCipherSuites`: `crypto/tls` picks the TLS 1.3 suites itself and cannot be told to offer one, so the endpoint's client runs fib's handshake for this testcase. As a server fib accepts a client that offers only it |
| `multiplexing` | ✓ | ✓ | ~2000 files over one connection; the server allows 200 streams at once and raises the limit as they finish |
| `retry` | ✓ | ✓ | the server sends a Retry with a token, then accepts it |
| `resumption` | ✓ | ✓ | session ticket, second connection resumes |
| `zerortt` | ✓ | ✓ | the rest of the files in the client's first flight |
| `http3` | ✓ | ✓ | fib's full HTTP/3 server and client |
| `blackhole` | ✓ | ✓ | 10 MiB with a 2 s blackout; the server and client see `transfer` |
| `keyupdate` | ✓ | ✓ | the client updates its 1-RTT keys every 100 ms; fib answers a peer's update too |
| `ecn` | unsupported | unsupported | fib neither marks nor validates ECN |
| `amplificationlimit` | ✓ | ✓ | |
| `handshakeloss` | ✓ | ✓ | 50 connections at 30% loss |
| `transferloss` | ✓ | ✓ | 2 MiB at 2% loss |
| `handshakecorruption` | ✓ | ✓ | 50 connections at 30% corruption; same endpoint path as `handshakeloss` (the runner passes `multiconnect`, and the endpoint accepts the name too); passed on 2026-11-01 |
| `transfercorruption` | ✓ | ✓ | 2 MiB at 2% corruption; same endpoint path as `transferloss` (the runner passes `transfer`, and the endpoint accepts the name too); passed on 2026-11-01 |
| `ipv6` | ✓ | ✓ | the client resolves `server6`; the server listens on both families |
| `v2` | unsupported | unsupported | fib speaks QUIC version 1 only |
| `rebind-port` | ✓ | ✓ | the server follows the NAT rebinding and answers with its own PATH_CHALLENGE |
| `rebind-addr` | ✓ | ✓ | the server follows the rebinding and its PATH_CHALLENGE is in the first packet it sends to the new address, even though the congestion window the old path filled has no room for it |
| `connectionmigration` | unsupported | fails | the server has no `preferred_address` to offer; quic-go's and quiche's servers exit 127 for it too, so those pairs are unsupported. The client's role is `transfer`, and fib cannot perform the active migration the check wants, so it fails against ngtcp2's server, which offers one |
| `versionnegotiation` | unsupported | unsupported | disabled by the runner; fib's client cannot start with a reserved version |

The entries above are what the endpoint's two roles do, not what the
testcase checks in every direction: `keyupdate` is server-side `transfer`,
and the client of `rebind-*` and `connectionmigration` is `transfer`. Both
sides of `ecn`, `v2` and `versionnegotiation` exit 127, which the runner
records as unsupported rather than failed. The loss and corruption rows are
the same endpoint behavior under different simulator scenarios: the
handshake pair uses the `multiconnect` client, one connection per file, and
the transfer pair uses `transfer`.

The server endpoint also sends a `NEW_TOKEN` frame after every handshake
(RFC 9000 section 8.1.3), which no testcase looks for: the runner's clients
make each connection from a new UDP socket, hence a new source port, while a
token is bound to the address it was made for, so it is no token at all to
the server, which falls back to the usual path. `retry` still sees exactly
one Retry, and a client that comes back from the same address skips one.

Outside what the runner checks: fib does not implement the QPACK dynamic
table, server push, DATAGRAM or ECN, and its HTTP/3 stack serves
requests with the static table only. The `http3` testcase therefore checks
one transaction over parallel streams, not those features. `QLOGDIR` is
accepted but fib writes no qlog.

### What ran

Against quic-go, with tshark 4.6.6 (4.5.0 is the runner's minimum; 4.2.2,
Ubuntu 24.04's package, cannot follow the migrated connection and makes
`rebind-*` fail):

- `fib` server, `quic-go` client — passed: `http3`, `handshake`, `transfer`,
  `retry`, `resumption`, `zerortt`, `chacha20`, `multiplexing`, `ipv6`,
  `rebind-port`, `rebind-addr`, `transferloss`, `amplificationlimit`,
  `longrtt`, `blackhole`, `handshakeloss`.
- `fib` client, `quic-go` server — passed: `http3`, `handshake`, `transfer`,
  `retry`, `resumption`, `zerortt`, `chacha20`, `keyupdate`, `multiplexing`,
  `ipv6`, `rebind-port`, `rebind-addr`, `longrtt`, `transferloss`,
  `amplificationlimit`, `blackhole`, `handshakeloss`.
- Both roles exit 127 for `ecn`, `v2` and the server side of
  `connectionmigration`; the runner reports those as unsupported.

`handshakecorruption` and `transfercorruption` were not in these runs; they
passed against all three peers on 2026-11-01 (see below). The runner does
not hand any of the four loss or corruption names to an endpoint: `TestCaseHandshakeLoss.testname` returns
`multiconnect` and `TestCaseTransferLoss.testname` returns `transfer`, for
the loss cases and, inherited, for the corruption cases, while the simulator
drops or corrupts the packets. The `handshakeloss` and `transferloss` runs
above therefore cover the same paths. The endpoint's dispatch now also
accepts both corruption names, and `handshakeloss`/`transferloss`
themselves, as aliases, so starting it with one directly runs the same
server and client.

`chacha20` was rerun on 2026-10-26, once the client could offer
ChaCha20-Poly1305 alone:

```sh
DIRECTION=client TESTCASES=chacha20 ./run-runner.sh   # passed
DIRECTION=server TESTCASES=chacha20 ./run-runner.sh   # passed
```

The client direction is the new one: the runner reads the capture and finds
the ClientHello offering `0x1303` and nothing else. The regression set
`TESTCASES=http3,handshake,transfer,retry,resumption,zerortt` passed in both
directions in the same session, except one fib-server `handshake` run whose
capture the simulator lost (`Expected exactly one version. Got []`; both
containers exited 0 and the transfer itself succeeded) — `handshake` passed on
two clean re-runs.

`rebind-addr` and `longrtt` were the last two that did not pass, and they were
rerun, with the rest of the matrix, on 2026-10-26:

```sh
TESTCASES=rebind-addr DIRECTION=server ./run-runner.sh          # passed
TESTCASES=longrtt DIRECTION=client ./run-runner.sh              # passed
TESTCASES=longrtt,rebind-addr DIRECTION=both ./run-runner.sh    # passed both ways
TESTCASES=http3,handshake,transfer,retry,resumption,zerortt,multiplexing,\
rebind-port,transferloss,blackhole,handshakeloss DIRECTION=both ./run-runner.sh
                                                                # passed both ways
```

What each one needed is below.

- `rebind-addr` failed because the PATH_CHALLENGE waited behind the
  congestion window. A rebinding to another host starts the new path with a
  fresh window while the old path's packets are still counted in flight, so
  `buildPacket` would not put an ack-eliciting frame in a packet; an
  acknowledgement that came due still went out alone, and that ACK-only
  packet was the first one on the new address. The challenge is now armed as
  a probe when validation starts (`http3/quic/path.go`), so it goes out with
  that acknowledgement, past the window, the way a probe timeout's packets
  do. The runner's failure showed the first packet on the new path as a
  short header with 26 bytes of ACK and nothing else.
- `longrtt` was not a probe-timeout bug: the client does send its Initial
  again. With no RTT sample the probe timeout is RFC 9002's initial one, a
  second, and a client that nothing answers sends the whole ClientHello
  again at +999 ms, in a test pair and on the wire alike. What the client
  did not do was wait for the simulator. The runner starts a client as soon
  as its container is up, and the simulator only forwards — and only
  captures — once its scenario is loaded, which is later; the first flight
  was dropped before the capture began and the copy on the timer was the
  first ClientHello in it. Because fib's ClientHello is 1520 bytes (a
  post-quantum key share) it spans two Initial packets, and tshark dissects
  the ClientHello in the second one, so the runner counted one. The
  entrypoint now waits for the simulator's port before a client dials
  (`http3/interoprunner/run_endpoint.sh`), as the reference endpoints do,
  which puts the first flight in the capture; the test then passes on
  repeated runs.

The evidence, from captures taken around the runs: the client's first
Initials (packet numbers 0 and 1) appear on the host's docker bridges at
+2.6 s of the run, while the simulator's capture starts about 0.5 s later,
so the simulator's own trace begins with packet numbers 2 and 3; against
quic-go's client, which waits for the simulator, the first flight is in the
trace and its three repeats are marked as retransmissions.

`local-test.sh` runs `http3`, `handshake`, `transfer`, `retry`,
`resumption`, `zerortt` and `multiconnect` with fib on both sides; all pass.

### Against quiche and ngtcp2

The whole matrix was run on 2026-11-01 against quic-go, quiche and ngtcp2,
each in both roles, with the runner at commit `a224e5e`, tshark 4.6.6 and
Docker 29.9.2 inside a `docker:29-dind` container on an Apple-silicon Mac
(see [Running it locally](#running-it-locally)). The peers are their
published endpoint images: `martenseemann/quic-go-interop:latest`,
`ghcr.io/ngtcp2/ngtcp2-interop:latest` and `cloudflare/quiche-qns:latest`.
quic-go's and ngtcp2's are `arm64` images; quiche's is `amd64` only, so it
ran emulated, several times slower, which matters below.

```sh
TESTCASES=handshake,transfer,longrtt,chacha20,multiplexing,retry,resumption,\
zerortt,http3,amplificationlimit,blackhole,keyupdate,handshakeloss,\
transferloss,handshakecorruption,transfercorruption,ecn,rebind-port,\
rebind-addr,ipv6,connectionmigration,v2 ./run-runner.sh
```

"✓" passed, "✕" failed, "–" one side or both exit 127 (unsupported).
"fib ⇢ X" is fib's server with X's client; "X ⇢ fib" is X's server with
fib's client.

| Testcase | fib ⇢ quic-go | quic-go ⇢ fib | fib ⇢ ngtcp2 | ngtcp2 ⇢ fib | fib ⇢ quiche | quiche ⇢ fib |
| --- | --- | --- | --- | --- | --- | --- |
| `handshake` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `transfer` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `longrtt` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `chacha20` | ✓ | ✓ | ✓ | ✓ | – | ✓ |
| `multiplexing` | ✓ | ✓ | ✓ | ✓ | ✕ (1) | ✓ |
| `retry` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `resumption` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `zerortt` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `http3` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `amplificationlimit` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `blackhole` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `keyupdate` | ✓ | ✓ | ✓ | ✓ | – | ✓ |
| `handshakeloss` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (2) |
| `transferloss` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `handshakecorruption` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `transfercorruption` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `ecn` | – | – | – | – | – | – |
| `rebind-port` | ✓ | ✓ | ✓ | ✓ | ✕ (3) | ✕ (4) |
| `rebind-addr` | ✓ | ✓ | ✓ | ✓ | ✓ (3) | ✕ (4) |
| `ipv6` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `connectionmigration` | – | – | – | ✕ (5) | – | – |
| `v2` | – | – | – | – | – | – |

None of the failures left is fib's to fix, or none that the runner shows:

1. quiche's client wrote a few of its ~2000 requests only in part, without
   the end of the stream, and never sent the rest, so fib's server waited
   for the end of a request line that never came; the client gave up idle.
   The same client fails `multiplexing` against ngtcp2's server the same
   way.
2. Failed once in the full run and passed in three reruns. In the failing
   connection quiche's server sent HANDSHAKE_DONE once, in a packet the
   simulator dropped, and its probes after that were bare PINGs; fib's
   client, its handshake not confirmed, may only probe with Handshake
   packets, which quiche had stopped reading, and the acknowledgements it
   sent of quiche's PINGs were dropped too. Sending its unacknowledged 1-RTT
   data along with those Handshake probes (RFC 9002 section 6.2.4 allows
   it) would give quiche something to acknowledge, and is a possible
   improvement.
3. The emulated quiche client starts late enough that its handshake can
   span the simulator's first rebinding, a second into the run. fib's
   server then follows it to the new port with handshake packets, which
   have no room for a PATH_CHALLENGE, and the runner fails the run because
   the first packet on the new path has none. Both passed in another run in
   which the handshake ended first.
4. quiche's server does not keep up with the rebinding: ngtcp2's client
   fails both against it as fib's does.
5. ngtcp2's server offers a `preferred_address`, and the testcase checks
   that the client moves to it. fib's client does not migrate. Against
   quic-go and quiche the server side is unsupported.

Three fixes in `http3/quic` and one in the endpoint came out of these
runs; the three have tests in `http3/quic`:

- quiche repeats DATA_BLOCKED and STREAM_DATA_BLOCKED in every packet while
  it is blocked, and quic-go raises MAX_STREAMS once per finished stream,
  out of order when several finish together. Both counted as frames that
  ask for nothing, so fib's client closed the connection with "too many
  frames that ask for nothing" (`transfer` and `blackhole` against quiche,
  `multiplexing` against quic-go). A BLOCKED frame that repeats a limit,
  and a MAX_DATA, MAX_STREAM_DATA or MAX_STREAMS that raises none, now count
  only as a PING does: when nothing else in the packet asks for anything.
- A server now follows a client whose address changes during the handshake,
  on an Initial or Handshake packet numbered above the ones before it; it
  used to answer the old address until the handshake timed out.
- A server now sends its Initial data in flight again as soon as the client
  sends an Initial with nothing new in it, a PING or the ClientHello again,
  up to four times a connection (RFC 9002 section 6.2.3), instead of
  waiting for its own backed-off probe timeout; `handshakecorruption`
  against quic-go's client, whose handshake deadline is ten seconds, failed
  without it.
- The endpoint's server allows a 60-second handshake, as its client does:
  under `handshakeloss` ngtcp2's client took more than 30 seconds to get its
  Finished through, and the server's stateless reset ended the connection.

## Layout

- `main.go` — `ROLE`/`TESTCASE` dispatch; unsupported cases exit 127
- `server_quic.go`, `client_quic.go` — HTTP/0.9 over fib's QUIC transport,
  with the UDP datagram dispatch the runner's simulated network needs
- `h3server.go`, `h3client.go` — fib's HTTP/3 server and client for `http3`
- `tlsutil.go` — the runner's certificate, key log and directories
- `Dockerfile`, `run_endpoint.sh` — the image and its entrypoint
- `local-test.sh` — fib against fib on a docker network, no runner needed
- `run-runner.sh` — the real runner, both directions against each peer
- `go.mod` — this module, with `replace ../..` to the fib in this checkout

## CI

`.github/workflows/ci.yml` has two jobs:

- `http3-interop-image` builds the image and runs `local-test.sh` on every
  push and pull request. It is deterministic and needs no peer image
  beyond the endpoint base image. `local-test.sh` retries the build with a
  backoff and then falls back to `mirror.gcr.io` for the Go base image, and
  the job is `continue-on-error`, so a Docker Hub rate limit never blocks a
  pull request.
- `http3-interop-runner` runs the real runner against quic-go, quiche and
  ngtcp2, both directions, on every push and pull request. It is
  `continue-on-error`, because it pulls the simulator and peer images, needs
  `tshark`, and the runner is slower and more brittle than the rest of the
  suite. `run-runner.sh` runs a failed testcase once more and
  reports it only if it fails again: the runner stops the simulator as soon
  as the client exits, and a capture cut short then has the runner count
  fewer handshakes than went over the wire.
