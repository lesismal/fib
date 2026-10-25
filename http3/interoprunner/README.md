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

./run-runner.sh                           # fib vs quic-go, both directions
TESTCASES=http3 ./run-runner.sh           # just the HTTP/3 testcase
PEER=quiche ./run-runner.sh               # target another published endpoint
DIRECTION=server TESTCASES=chacha20 ./run-runner.sh
```

`run-runner.sh` clones the runner into `$RUNNER_DIR` (default
`/tmp/quic-interop-runner`) if needed, registers the image as `fib` in the
runner's `implementations_quic.json`, and runs the matrix. Build the image
first, from the repository root:

```sh
docker build -f http3/interoprunner/Dockerfile -t fib-interop:local .
```

The runner needs `docker compose`, `tshark` (4.5.0 or newer, as its README
says; older versions cannot follow a migrated connection) and these Python
modules: `pycryptodome`, `random-slugs`, `termcolor`, `prettytable`,
`pyshark`.

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

"Supported" means the endpoint implements the testcase; the marks are the
result against quic-go's published endpoint image
(`martenseemann/quic-go-interop:latest`, commit `d961427`) on Linux with
Docker 29.1.3 and tshark 4.6.6, on 2026-10-25. The runner names a testcase
to a role through `TestCase.testname`: most of its matrix reaches the server
as `transfer`, `handshake` or `multiconnect`, which is why those names cover
more than their own testcase.

| Testcase | Server | Client | Notes |
| --- | --- | --- | --- |
| `handshake` | ✓ | ✓ | HTTP/0.9 over QUIC |
| `transfer` | ✓ | ✓ | three files, parallel streams; the client's receive windows are small so fib has to grow them |
| `longrtt` | ✓ | ✗ | the transfer succeeds; the check also wants two ClientHellos under a 750 ms RTT, and fib's PTO does not send the second within the capture (quic-go's client does, which is why the server direction passes) |
| `chacha20` | ✓ | unsupported | TLS 1.3 picks the suite, and `crypto/tls` offers AES-128-GCM before ChaCha20-Poly1305, so fib's ClientHello cannot offer ChaCha20 alone; as a server fib accepts a client that offers only it |
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
| `handshakecorruption` | not run | not run | same endpoint path as `handshakeloss` |
| `transfercorruption` | not run | not run | same endpoint path as `transferloss` |
| `ipv6` | ✓ | ✓ | the client resolves `server6`; the server listens on both families |
| `v2` | unsupported | unsupported | fib speaks QUIC version 1 only |
| `rebind-port` | ✓ | ✓ | the server follows the NAT rebinding and answers with its own PATH_CHALLENGE |
| `rebind-addr` | ✗ | ✓ | the server answers the client's PATH_CHALLENGE with a PATH_RESPONSE but does not send a PATH_CHALLENGE of its own until it migrates; the runner requires one in the first packet on the new path |
| `connectionmigration` | unsupported | not run | the server has no `preferred_address` to offer; quic-go's server exits 127 for it too, so the pair is unsupported. The client's role is `transfer`, and fib cannot perform the active migration the check wants |
| `versionnegotiation` | unsupported | unsupported | disabled by the runner; fib's client cannot start with a reserved version |

The marks above are what the endpoint's two roles do, not what the testcase
checks in every direction: `keyupdate` is server-side `transfer`, and the
client of `rebind-*` and `connectionmigration` is `transfer`. The client
side of `chacha20`, and both sides of `ecn`, `v2` and `versionnegotiation`,
exit 127, which the runner records as unsupported rather than failed.

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
  `rebind-port`, `transferloss`, `amplificationlimit`, `longrtt`,
  `blackhole`, `handshakeloss`. Failed: `rebind-addr`.
- `fib` client, `quic-go` server — passed: `http3`, `handshake`, `transfer`,
  `retry`, `resumption`, `zerortt`, `keyupdate`, `multiplexing`, `ipv6`,
  `rebind-port`, `rebind-addr`, `transferloss`, `amplificationlimit`,
  `blackhole`, `handshakeloss`. Failed: `longrtt`.
- Both roles exit 127 for `ecn`, `v2` and the server side of
  `connectionmigration`; the runner reports those as unsupported.

`local-test.sh` runs `http3`, `handshake`, `transfer`, `retry`,
`resumption`, `zerortt` and `multiconnect` with fib on both sides; all pass.

## Layout

- `main.go` — `ROLE`/`TESTCASE` dispatch; unsupported cases exit 127
- `server_quic.go`, `client_quic.go` — HTTP/0.9 over fib's QUIC transport,
  with the UDP datagram dispatch the runner's simulated network needs
- `h3server.go`, `h3client.go` — fib's HTTP/3 server and client for `http3`
- `tlsutil.go` — the runner's certificate, key log and directories
- `Dockerfile`, `run_endpoint.sh` — the image and its entrypoint
- `local-test.sh` — fib against fib on a docker network, no runner needed
- `run-runner.sh` — the real runner, both directions against a peer
- `go.mod` — this module, with `replace ../..` to the fib in this checkout

## CI

`.github/workflows/ci.yml` has two jobs:

- `http3-interop-image` builds the image and runs `local-test.sh` on every
  push and pull request. It is blocking, deterministic and needs no peer
  image beyond the endpoint base image.
- `http3-interop-runner` runs the real runner against quic-go. It is
  `workflow_dispatch` only and `continue-on-error`, because it pulls the
  simulator and peer images, needs `tshark`, and the runner is slower and
  more brittle than the rest of the suite.
