#!/usr/bin/env bash
# Load-test campaign for fib's HTTP/1, HTTP/2 and HTTP/3 servers.
#
# It builds benchmark/loadtest/{server,h3load,stall} and drives them for a
# fixed time per configuration, recording throughput, latency, errors and the
# server's RSS, goroutines and descriptors before, after and once idle.
#
#   ./benchmark/loadtest/run.sh --mode smoke            # a few seconds
#   ./benchmark/loadtest/run.sh --mode full --rounds 3  # the campaign
#
# Requirements: go, h2load (nghttp2-client), python3, curl, and on Linux
# taskset and flock. On a Debian or Ubuntu host:
#
#   apt-get install -y nghttp2-client
#
# The server and the load generator are pinned to disjoint halves of the
# CPUs where taskset is available, so a Go client and a Go server do not
# steal each other's cores; pass --no-pin to share them.
set -euo pipefail

cd "$(dirname "$0")/../.."

MODE=full
DUR=30
WARMUP=3
ROUNDS=3
OUT=""
CASES=""
PIN=1
BIN_OVERRIDE=""
GROWTH_RUNS=10
GROWTH_DUR=5

usage() {
  sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Options:
  --mode smoke|full   smoke runs one short case per protocol (default full)
  --duration SECONDS  measurement time per case (default 30)
  --warmup SECONDS    warmup before each measurement (default 3)
  --rounds N          interleaved rounds of the six A/B cases (default 3)
  --out DIR           output directory (default /tmp/fib-loadtest/<time>)
  --cases LIST        comma-separated case names to run, others skipped
  --bin DIR           run an already built server, h3load and stall from DIR
                      instead of building them into <out>/bin
  --growth-runs N     repeated short runs per protocol (default 10)
  --no-pin            do not pin the server and the load generator
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE=$2; shift 2;;
    --duration) DUR=$2; shift 2;;
    --warmup) WARMUP=$2; shift 2;;
    --rounds) ROUNDS=$2; shift 2;;
    --out) OUT=$2; shift 2;;
    --cases) CASES=$2; shift 2;;
    --bin) BIN_OVERRIDE=$2; shift 2;;
    --growth-runs) GROWTH_RUNS=$2; shift 2;;
    --no-pin) PIN=0; shift;;
    -h|--help) usage; exit 0;;
    *) echo "run.sh: unknown option $1" >&2; usage >&2; exit 2;;
  esac
done

if [ "$MODE" = smoke ]; then
  DUR=5
  WARMUP=1
  ROUNDS=1
  GROWTH_RUNS=2
  GROWTH_DUR=3
fi

if ! command -v h2load >/dev/null; then
  echo "run.sh: h2load not found; install nghttp2-client (apt-get install -y nghttp2-client)" >&2
  exit 1
fi
if ! command -v python3 >/dev/null; then
  echo "run.sh: python3 not found" >&2
  exit 1
fi

OUT=${OUT:-/tmp/fib-loadtest/$(date +%Y%m%d-%H%M%S)}
BIN=${BIN_OVERRIDE:-$OUT/bin}
RUNDIR=$OUT/cases
mkdir -p "$BIN" "$RUNDIR"

# One campaign at a time on a shared host: a retried ssh must not start a
# second copy.
LOCK=/tmp/fib-loadtest.lock
exec 9>"$LOCK"
if command -v flock >/dev/null && ! flock -n 9; then
  echo "run.sh: another load-test campaign holds $LOCK" >&2
  exit 1
fi

CPU_SERVER=""
CPU_LOAD=""
if [ "$PIN" = 1 ] && command -v taskset >/dev/null; then
  NCPU=$(nproc 2>/dev/null || echo 2)
  if [ "$NCPU" -ge 4 ]; then
    HALF=$((NCPU / 2))
    CPU_SERVER="0-$((HALF - 1))"
    CPU_LOAD="$HALF-$((NCPU - 1))"
  fi
fi

pin_load() { if [ -n "$CPU_LOAD" ]; then taskset -c "$CPU_LOAD" "$@"; else "$@"; fi; }
# stall_cmd is backgrounded as a function: it execs the real command, so the
# PID the caller records is the client's and wait reaches it.
stall_cmd() {
  if [ -n "$CPU_LOAD" ]; then
    exec taskset -c "$CPU_LOAD" "$BIN/stall" "$@"
  fi
  exec "$BIN/stall" "$@"
}

SERVER_PID=""
SERVER_LOG=""
PLAIN=""
TLS=""
H3=""

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "[$(date -u +%H:%M:%S)] load-test campaign: mode=$MODE duration=${DUR}s warmup=${WARMUP}s rounds=$ROUNDS"
echo "out=$OUT server-cpus=${CPU_SERVER:-all} load-cpus=${CPU_LOAD:-all}"

if [ -z "$BIN_OVERRIDE" ]; then
  {
    go build -o "$BIN/server" ./benchmark/loadtest/server
    go build -o "$BIN/h3load" ./benchmark/loadtest/h3load
    go build -o "$BIN/stall" ./benchmark/loadtest/stall
  } || { echo "run.sh: build failed" >&2; exit 1; }
fi

printf '%s\n' \
  'case	proto	conc	reqs	rps	ok	failed	errored	timeout	p50_ms	p99_ms	mean_ms	max_ms	rss_before_mib	rss_after_mib	rss_settle_mib	gor_before	gor_after	gor_peak	gor_settle	fds_after	fds_peak	fds_settle	openconns	pending	paused	stall_bytes	stall_errors	stall_empty	warnings	load_exit' \
  > "$OUT/results.tsv"

want() {
  [ -z "$CASES" ] || [[ ",$CASES," == *",$1,"* ]]
}

start_server() {
  local name=$1
  local log=${2:-"$RUNDIR/$name-server.log"}
  SERVER_LOG=$log
  local addr_file="$RUNDIR/$name-addr.json"
  rm -f "$addr_file"
  # Background the command itself, not a shell function: $! has to be the
  # server's PID for stop_server to reach it.
  if [ -n "$CPU_SERVER" ]; then
    taskset -c "$CPU_SERVER" "$BIN/server" -plain 127.0.0.1:0 -tls 127.0.0.1:0 -h3 127.0.0.1:0 \
      -max-streams 512 -h3-max-streams 512 -addr-file "$addr_file" \
      >"$SERVER_LOG" 2>&1 9>&- &
  else
    "$BIN/server" -plain 127.0.0.1:0 -tls 127.0.0.1:0 -h3 127.0.0.1:0 \
      -max-streams 512 -h3-max-streams 512 -addr-file "$addr_file" \
      >"$SERVER_LOG" 2>&1 9>&- &
  fi
  SERVER_PID=$!
  local i
  for i in $(seq 1 150); do
    [ -s "$addr_file" ] && break
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "run.sh: server exited during startup" >&2
      cat "$SERVER_LOG" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
  if [ ! -s "$addr_file" ]; then
    echo "run.sh: server did not write $addr_file" >&2
    cat "$SERVER_LOG" >&2 || true
    exit 1
  fi
  eval "$(python3 - "$addr_file" <<'PY'
import json, shlex, sys
a = json.load(open(sys.argv[1]))
for key in ("plain", "tls", "h3"):
    print(f"{key.upper()}={shlex.quote(a[key])}")
PY
)"
  for i in $(seq 1 100); do
    if curl -sf --max-time 2 "http://$PLAIN/stats" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "run.sh: server's /stats did not answer" >&2
  cat "$SERVER_LOG" >&2 || true
  exit 1
}

stop_server() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}

stats() {
  local query=${1:-}
  local i
  for i in 1 2 3 4 5; do
    if curl -sf --max-time 10 "http://$PLAIN/stats$query"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

CASE_NAME=""
CASE_DIR=""
CASE_PROTO=""
CASE_CONC=""
CASE_KIND=""
CASE_STALL=0
LOAD_EXIT=0

prep_case() {
  CASE_NAME=$1
  CASE_PROTO=$2
  CASE_CONC=$3
  CASE_KIND=$4
  CASE_STALL=${5:-0}
  CASE_DIR="$RUNDIR/$CASE_NAME"
  rm -rf "$CASE_DIR"
  mkdir -p "$CASE_DIR"
  start_server "$CASE_NAME" "$CASE_DIR/server.log"
  stats "?reset=1&gc=1" >/dev/null 2>&1 || true
  sleep 0.5
  stats "?gc=1" > "$CASE_DIR/before.json" || true
  echo "[$(date -u +%H:%M:%S)] === $CASE_NAME: $CASE_PROTO conc=$CASE_CONC loadavg=$(cut -d' ' -f1-3 /proc/loadavg) ==="
}

finish_case() {
  local kind=$1
  LOAD_EXIT=$2
  stats "?gc=1" > "$CASE_DIR/after.json" || true
  sleep 2
  stats "?gc=1" > "$CASE_DIR/settle.json" || true
  stop_server
  local -a args=(--dir "$CASE_DIR" --name "$CASE_NAME" --proto "$CASE_PROTO"
    --conc "$CASE_CONC" --duration "$DUR" --kind "$kind" --load-exit "$LOAD_EXIT")
  if [ "${CASE_STALL:-0}" = 1 ]; then
    args+=(--stall)
  fi
  python3 benchmark/loadtest/summarize.py "${args[@]}" \
    >> "$OUT/results.tsv" 2> "$CASE_DIR/summary.txt" || true
  cat "$CASE_DIR/summary.txt" >&2 || true
  CASE_STALL=0
}

url_for() {
  case "$1" in
    h1|h2c) echo "http://$PLAIN/plain";;
    h2) echo "https://$TLS/plain";;
    *) echo "https://$H3/plain";;
  esac
}

run_h2load() {
  local proto=$1 conc=$2 m=$3 threads=$4
  shift 4
  local url
  url=$(url_for "$proto")
  local -a args=(-D "${DUR}s" --warm-up-time "${WARMUP}s" -c "$conc" -m "$m" -t "$threads")
  if [ "$proto" = h1 ]; then
    args+=(--h1)
  fi
  args+=("$@")
  pin_load timeout $((DUR + WARMUP + 60)) h2load "${args[@]}" "$url" \
    >"$CASE_DIR/load.out" 2>&1
}

run_h3load() {
  local conns=$1 streams=$2
  shift 2
  pin_load timeout $((DUR + WARMUP + 60)) "$BIN/h3load" \
    -url "https://$H3" -conns "$conns" -streams "$streams" \
    -duration "${DUR}s" -warmup "${WARMUP}s" "$@" \
    >"$CASE_DIR/load.out" 2>&1
}

h2load_case() {
  local name=$1 proto=$2 conc=$3 m=$4 threads=$5
  shift 5
  want "$name" || return 0
  prep_case "$name" "$proto" "$conc" h2load
  local rc=0
  run_h2load "$proto" "$conc" "$m" "$threads" "$@" || rc=$?
  finish_case h2load "$rc"
}

h3_case() {
  local name=$1 conc=$2 streams=$3
  shift 3
  want "$name" || return 0
  prep_case "$name" h3 "$conc" h3load
  local rc=0
  run_h3load "$conc" "$streams" "$@" || rc=$?
  finish_case h3load "$rc"
}

slow_case() {
  local name=$1 fastproto=$2 conc=$3 m=$4 threads=$5 stallproto=$6 streams=$7
  shift 7
  want "$name" || return 0
  prep_case "$name" "$fastproto" "$conc" h2load 1
  local base
  case "$stallproto" in
    h1|h2c) base="http://$PLAIN";;
    h2) base="https://$TLS";;
    h3) base="https://$H3";;
  esac
  local hold=$((DUR + WARMUP + 3))
  local stalled=$((streams / 2))
  local slow=$((streams - stalled))
  stall_cmd -proto "$stallproto" -url "$base" -streams "$stalled" -stall \
    -duration "${hold}s" >"$CASE_DIR/stall-stalled.out" 2>&1 9>&- &
  local stall_pid=$!
  stall_cmd -proto "$stallproto" -url "$base" -streams "$slow" \
    -delay 50ms -duration "${hold}s" >"$CASE_DIR/stall-slow.out" 2>&1 9>&- &
  local slow_pid=$!
  sleep 1
  local rc=0
  local kind=h2load
  if [ "$fastproto" = h3 ]; then
    kind=h3load
    run_h3load "$conc" "$((conc * m))" || rc=$?
  else
    run_h2load "$fastproto" "$conc" "$m" "$threads" "$@" || rc=$?
  fi
  wait "$stall_pid" 2>/dev/null || true
  wait "$slow_pid" 2>/dev/null || true
  finish_case "$kind" "$rc"
}

growth_case() {
  local proto=$1
  want "growth-$proto" || return 0
  local case_dir="$RUNDIR/growth-$proto"
  rm -rf "$case_dir"
  mkdir -p "$case_dir"
  start_server "growth-$proto" "$case_dir/server.log"
  echo "[$(date -u +%H:%M:%S)] === growth-$proto: $GROWTH_RUNS runs of ${GROWTH_DUR}s ==="
  local i
  for i in $(seq 1 "$GROWTH_RUNS"); do
    stats "?reset=1&gc=1" >/dev/null 2>&1 || true
    sleep 0.3
    stats "?gc=1" > "$case_dir/before-$i.json" || true
    local rc=0
    if [ "$proto" = h3 ]; then
      pin_load timeout $((GROWTH_DUR + 90)) "$BIN/h3load" -url "https://$H3" \
        -conns 4 -streams 512 -duration "${GROWTH_DUR}s" -warmup 1s \
        >"$case_dir/load-$i.out" 2>&1 || rc=$?
    else
      pin_load timeout $((GROWTH_DUR + 90)) h2load -D "${GROWTH_DUR}s" \
        --warm-up-time 1s -c 4 -m 128 -t 4 "$(url_for "$proto")" \
        >"$case_dir/load-$i.out" 2>&1 || rc=$?
    fi
    stats "?gc=1" > "$case_dir/after-$i.json" || true
    python3 - "$case_dir/after-$i.json" "$i" "$rc" <<'PY' >> "$OUT/growth.tsv"
import json, sys
d = json.load(open(sys.argv[1]))
e = d.get("engine", {})
print("\t".join(map(str, [sys.argv[2], sys.argv[3], d.get("rss_bytes", 0) >> 20,
      d.get("goroutines", 0), d.get("fds", 0), e.get("OpenConns", 0),
      e.get("PendingBytes", 0)])))
PY
  done
  stats "?gc=1" > "$case_dir/final.json" || true
  stop_server
}

# smoke is the shape of the campaign in a couple of minutes: one short case
# per protocol plus one stalled one.
if [ "$MODE" = smoke ]; then
  printf 'run\tload_exit\trss_mib\tgoroutines\tfds\topenconns\tpending\n' > "$OUT/growth.tsv"
  h2load_case smoke-h1 h1 16 1 4
  h2load_case smoke-h2c h2c 4 128 4
  h2load_case smoke-h2 h2 4 128 4
  h3_case smoke-h3 4 128
  slow_case smoke-slow-h3 h3 4 128 4 h3 8
  exit 0
fi

printf 'run\tload_exit\trss_mib\tgoroutines\tfds\topenconns\tpending\n' > "$OUT/growth.tsv"

# The A/B rounds are interleaved so a change in the host's load, not the
# protocol, moves the three configurations together.
for round in $(seq 1 "$ROUNDS"); do
  echo "[$(date -u +%H:%M:%S)] round $round/$ROUNDS"
  h2load_case h2c-4x128 h2c 4 128 4
  h2load_case h2-4x128 h2 4 128 4
  h3_case h3-4x128 4 512
  h2load_case h2c-64x8 h2c 64 8 8
  h2load_case h2-64x8 h2 64 8 8
  h3_case h3-32x16 32 512
done

# Connection scale under HTTP/1, and many connections under HTTP/2.
h2load_case h1-16c h1 16 1 4
h2load_case h1-256c h1 256 1 8
h2load_case h1-pipeline-4x16 h1 4 16 4
h2load_case h2c-512x1 h2c 512 1 8
h2load_case h2-512x1 h2 512 1 8

# One QUIC connection with many streams.
h3_case h3-1x256 1 256

# Fast load with a subset of clients that stall, and a subset that reads
# slowly, on connections of their own.
slow_case slow-h1 h1 16 1 4 h1 16
slow_case slow-h2c h2c 4 128 4 h2c 16
slow_case slow-h3 h3 4 128 4 h3 16

# Repeated short runs against one process: RSS, goroutines and descriptors
# should come back to where they started, not climb.
growth_case h2c
growth_case h3

column -t -s $'\t' "$OUT/results.tsv" 2>/dev/null || cat "$OUT/results.tsv"
echo "[$(date -u +%H:%M:%S)] results: $OUT/results.tsv"
echo "[$(date -u +%H:%M:%S)] growth:  $OUT/growth.tsv"
