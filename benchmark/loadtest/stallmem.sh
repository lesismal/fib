#!/usr/bin/env bash
# What a stalled reader costs the server: one fresh server, the number of
# responses given whole and never read, and the live heap and RSS read from
# /stats while they are still open. The load campaign's slow-* cases hold the
# same clients beside a fast load for 30 seconds; this measures the cost
# itself, which is what the table in docs/loadtest.md records.
#
#   ./benchmark/loadtest/stallmem.sh --proto h3 --streams 1,4,16 --rounds 3
#
# --bin takes a directory holding server and stall already built, so that a
# run before and a run after a change can be interleaved against the same
# clients without rebuilding either.
set -euo pipefail

cd "$(dirname "$0")/../.."

PROTO=h3
STREAMS=16
ROUNDS=1
OUT=""
BIN=""
BODY=8388608
HOLD=6

usage() {
  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Options:
  --proto h1|h2c|h2|h3  protocol of the stalled clients (default h3)
  --streams LIST        comma-separated counts of stalled responses (default 16)
  --rounds N            how many times each count is measured (default 1)
  --body N              response size asked for (default 8388608)
  --out DIR             output directory
  --bin DIR             directory holding server and stall already built
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --proto) PROTO=$2; shift 2;;
    --streams) STREAMS=$2; shift 2;;
    --rounds) ROUNDS=$2; shift 2;;
    --body) BODY=$2; shift 2;;
    --out) OUT=$2; shift 2;;
    --bin) BIN=$2; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "stallmem.sh: unknown option $1" >&2; usage >&2; exit 2;;
  esac
done

case "$PROTO" in
  h1|h2c|h2|h3) ;;
  *) echo "stallmem.sh: --proto must be h1, h2c, h2 or h3" >&2; exit 2;;
esac

OUT=${OUT:-/tmp/fib-stallmem/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
if [ -z "$BIN" ]; then
  BIN=$OUT/bin
  mkdir -p "$BIN"
  go build -o "$BIN/server" ./benchmark/loadtest/server
  go build -o "$BIN/stall" ./benchmark/loadtest/stall
fi
if [ ! -x "$BIN/server" ] || [ ! -x "$BIN/stall" ]; then
  echo "stallmem.sh: $BIN does not hold server and stall" >&2
  exit 1
fi

CPU_SERVER=""
CPU_LOAD=""
NCPU=$(nproc 2>/dev/null || echo 2)
if command -v taskset >/dev/null && [ "$NCPU" -ge 4 ]; then
  HALF=$((NCPU / 2))
  CPU_SERVER="0-$((HALF - 1))"
  CPU_LOAD="$HALF-$((NCPU - 1))"
fi
pin_server() { if [ -n "$CPU_SERVER" ]; then taskset -c "$CPU_SERVER" "$@"; else "$@"; fi; }
pin_load() { if [ -n "$CPU_LOAD" ]; then taskset -c "$CPU_LOAD" "$@"; else "$@"; fi; }

SERVER_PID=""
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

start_server() {
  local addr_file=$OUT/addr.json
  rm -f "$addr_file"
  pin_server "$BIN/server" -plain 127.0.0.1:0 -tls 127.0.0.1:0 -h3 127.0.0.1:0 \
    -max-streams 512 -h3-max-streams 512 -addr-file "$addr_file" \
    >"$OUT/server.log" 2>&1 &
  SERVER_PID=$!
  local i
  for i in $(seq 1 150); do
    [ -s "$addr_file" ] && break
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "stallmem.sh: the server exited during startup" >&2
      cat "$OUT/server.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
  eval "$(python3 - "$addr_file" <<'PY'
import json, shlex, sys
a = json.load(open(sys.argv[1]))
for key in ("plain", "tls", "h3"):
    print(f"{key.upper()}={shlex.quote(a[key])}")
PY
)"
  for i in $(seq 1 100); do
    if curl -sf --max-time 2 "http://$PLAIN/stats" >/dev/null 2>&1; then
      case "$PROTO" in
        h1|h2c) BASE="http://$PLAIN";;
        h2) BASE="https://$TLS";;
        *) BASE="https://$H3";;
      esac
      return 0
    fi
    sleep 0.1
  done
  echo "stallmem.sh: the server's /stats did not answer" >&2
  cat "$OUT/server.log" >&2 || true
  exit 1
}

stop_server() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}

limit=$(ulimit -n 2>/dev/null || echo 1024)
echo "[$(date -u +%H:%M:%S)] stallmem: proto=$PROTO streams=$STREAMS body=$BODY rounds=$ROUNDS out=$OUT" >&2
printf 'proto\tstreams\tround\theap_mib\trss_mib\topen_streams\n' > "$OUT/stallmem.tsv"

for streams in ${STREAMS//,/ }; do
  if [ "$streams" -ge "$limit" ]; then
    echo "stallmem.sh: $streams streams needs a larger descriptor limit than $limit" >&2
    exit 1
  fi
  for round in $(seq 1 "$ROUNDS"); do
    start_server
    curl -sf "http://$PLAIN/stats?reset=1&gc=1" >/dev/null 2>&1 || true
    # One connection per stalled response, each of which reads its header and
    # then nothing.
    pin_load "$BIN/stall" -proto "$PROTO" -url "$BASE" -path "/big?n=$BODY" \
      -streams "$streams" -stall -duration "${HOLD}s" \
      >"$OUT/stall-${PROTO}-${streams}-${round}.out" 2>&1 &
    stall_pid=$!
    sleep 2
    if ! curl -sf --max-time 10 "http://$PLAIN/stats?gc=1" > "$OUT/stats-${PROTO}-${streams}-${round}.json"; then
      echo "stallmem.sh: /stats did not answer with $streams stalled" >&2
      kill "$stall_pid" 2>/dev/null || true
      wait "$stall_pid" 2>/dev/null || true
      stop_server
      exit 1
    fi
    kill "$stall_pid" 2>/dev/null || true
    wait "$stall_pid" 2>/dev/null || true
    python3 - "$OUT/stats-${PROTO}-${streams}-${round}.json" "$PROTO" "$streams" "$round" \
      >> "$OUT/stallmem.tsv" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
e = d.get("engine", {})
print("\t".join(map(str, [sys.argv[2], sys.argv[3], sys.argv[4],
      d.get("heap_alloc", 0) >> 20, d.get("rss_bytes", 0) >> 20,
      e.get("OpenConns", 0)])))
PY
    stop_server
  done
done

column -t -s $'\t' "$OUT/stallmem.tsv" 2>/dev/null || cat "$OUT/stallmem.tsv"
echo "[$(date -u +%H:%M:%S)] stallmem: $OUT/stallmem.tsv" >&2
