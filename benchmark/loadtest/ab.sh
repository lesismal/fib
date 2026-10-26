#!/usr/bin/env bash
# A clean interleaved A/B on one host: two builds of fib, one case at a time.
#
#   ./benchmark/loadtest/ab.sh --a /tmp/fib-a/bin --b /tmp/fib-b/bin \
#       --out /tmp/fib-ab --rounds 5
#
# Build each side once, from its own tree, into a directory of its own:
#
#   (cd /tmp/fib-a && go build -o bin/server ./benchmark/loadtest/server &&
#     go build -o bin/h3load ./benchmark/loadtest/h3load &&
#     go build -o bin/stall ./benchmark/loadtest/stall)
#
# and the same for /tmp/fib-b. ab.sh does not build. For each case it runs A
# then B, then B then A, and so on, so a change in the host's load moves both
# sides together; `run.sh` starts a fresh server for every run and pins the
# server to CPUs 0..n/2-1 and the clients to n/2..n-1.
#
# The host is shared. Before each run ab.sh samples /proc/stat for one second
# and waits while more than --max-busy percent of the host is busy with
# something else, up to --wait-max seconds in total. The campaign itself pins
# all CPUs for 33 seconds per run, so the kernel's one-minute load average
# sits around 6 between runs even on an otherwise idle host; gating on it
# would make every run wait about 90s. The load average is still read and
# recorded before every run (both in ab.tsv and in loadavg.tsv), and a run
# whose host was busy right after it finished is not counted and is repeated.
#
# ab.tsv is run.sh's results.tsv with side, round, load1_before, busy percent
# and the live heap after and once idle appended; loadavg.tsv has a row for
# every attempt, counted or not. growth-* cases have no results row: their
# growth.tsv is copied to growth/.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
RUN=$here/run.sh

A=""
B=""
OUT=""
ROUNDS=5
DUR=30
WARMUP=3
GROWTH_ROUNDS=1
MAX_BUSY=20
WAIT_MAX=1200
CASES="h2c-4x128,h2-4x128,h3-4x128,h2c-64x8,h2-64x8,h3-32x16,h1-16c,h1-256c,h1-pipeline-4x16,h2c-512x1,h2-512x1,h3-1x256,slow-h1,slow-h2c,slow-h3,growth-h2c,growth-h3"

usage() {
  cat <<'EOF'
Usage: ab.sh --a DIR --b DIR [options]

  --a DIR             the server, h3load and stall built from the first tree
  --b DIR             the same built from the second tree
  --out DIR           output directory (default /tmp/fib-ab/<time>)
  --rounds N          counted rounds per case and side (default 5)
  --duration SECONDS  measurement time per case (default 30)
  --warmup SECONDS    warm-up time per case (default 3)
  --growth-rounds N   counted rounds for growth-* (default 1)
  --cases LIST        comma-separated case names (default: the campaign's)
  --max-busy PERCENT  wait while the host is busier than this (default 20)
  --wait-max SECONDS  give up waiting after this long in total (default 1200)
  -h, --help          this text
EOF
}

die() { echo "ab.sh: $*" >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --a) A=$2; shift 2;;
    --b) B=$2; shift 2;;
    --out) OUT=$2; shift 2;;
    --rounds) ROUNDS=$2; shift 2;;
    --duration) DUR=$2; shift 2;;
    --warmup) WARMUP=$2; shift 2;;
    --growth-rounds) GROWTH_ROUNDS=$2; shift 2;;
    --cases) CASES=$2; shift 2;;
    --max-busy) MAX_BUSY=$2; shift 2;;
    --wait-max) WAIT_MAX=$2; shift 2;;
    -h|--help) usage; exit 0;;
    *) die "unknown option $1 (try --help)";;
  esac
done

[ -n "$A" ] || die "--a is required"
[ -n "$B" ] || die "--b is required"
for dir in "$A" "$B"; do
  for bin in server h3load stall; do
    [ -x "$dir/$bin" ] || die "$dir/$bin is missing; build each side first"
  done
done
[ -x "$RUN" ] || die "run.sh not found at $RUN"

OUT=${OUT:-/tmp/fib-ab/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT/runs" "$OUT/growth"
MERGED=$OUT/ab.tsv
ATTEMPTS=$OUT/loadavg.tsv
printf 'time\tcase\tside\tattempt\tcounted\trc\tload1_before\tbusy_before\tload1_after\tbusy_after\n' > "$ATTEMPTS"

now() { date -u +%H:%M:%S; }
loadavg() { cut -d' ' -f1-3 /proc/loadavg; }
is_growth() { case "$1" in growth-*) return 0;; *) return 1;; esac; }

# sample_cpu sets S_BUSY (percent of all CPUs busy over one second) and
# S_LOAD1 (the one-minute load average at the end of that second).
sample_cpu() {
  local t0 i0 t1 i1 dt di
  read -r t0 i0 <<<"$(awk '/^cpu /{t=0; for(i=2;i<=NF;i++) t+=$i; print t, $5+$6}' /proc/stat)"
  sleep 1
  read -r t1 i1 <<<"$(awk '/^cpu /{t=0; for(i=2;i<=NF;i++) t+=$i; print t, $5+$6}' /proc/stat)"
  dt=$((t1 - t0))
  di=$((i1 - i0))
  if [ "$dt" -gt 0 ]; then S_BUSY=$(((dt - di) * 100 / dt)); else S_BUSY=0; fi
  S_LOAD1=$(cut -d' ' -f1 /proc/loadavg)
}

WAITED=0
gate() {
  while :; do
    sample_cpu
    [ "$S_BUSY" -le "$MAX_BUSY" ] && return 0
    if [ "$WAITED" -ge "$WAIT_MAX" ]; then
      echo "[$(now)] gate: host ${S_BUSY}% busy (load $(loadavg)) after ${WAITED}s; continuing anyway"
      return 0
    fi
    echo "[$(now)] gate: host ${S_BUSY}% busy (load $(loadavg)); waiting"
    sleep 10
    WAITED=$((WAITED + 10))
  done
}

heap_mib() {
  python3 - "$1" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except (OSError, ValueError):
    print("")
else:
    print(f'{d.get("heap_alloc", 0) / (1 << 20):.1f}')
PY
}

append_row() {
  local tsv=$1 side=$2 round=$3 lb=$4 bb=$5 la=$6 ba=$7 ha=$8 hs=$9
  if [ ! -s "$MERGED" ]; then
    {
      printf 'side\tround\tload1_before\tbusy_before\tload1_after\tbusy_after\t'
      head -1 "$tsv" | tr -d '\n'
      printf '\theap_after_mib\theap_settle_mib\n'
    } > "$MERGED"
  fi
  awk -v s="$side" -v r="$round" -v lb="$lb" -v bb="$bb" -v la="$la" -v ba="$ba" \
    -v ha="$ha" -v hs="$hs" \
    'NR==2 { print s "\t" r "\t" lb "\t" bb "\t" la "\t" ba "\t" $0 "\t" ha "\t" hs }' \
    "$tsv" >> "$MERGED"
}

echo "[$(now)] ab: A=$A B=$B out=$OUT rounds=$ROUNDS duration=${DUR}s cases=$CASES"
echo "[$(now)] host: load $(loadavg), $(nproc) CPUs, gate ${MAX_BUSY}% busy"

for case in ${CASES//,/ }; do
  target=$ROUNDS
  case "$case" in growth-*) target=$GROWTH_ROUNDS;; esac
  declare -A done=([A]=0 [B]=0)
  # Give a disturbed or failed run a few chances beyond the counted rounds.
  limit=$((target * 4 + 4))
  attempt=0
  while [ "${done[A]}" -lt "$target" ] || [ "${done[B]}" -lt "$target" ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -gt "$limit" ]; then
      echo "[$(now)] $case: only ${done[A]}/${done[B]} of $target counted after $limit attempts; moving on"
      break
    fi
    if [ $((attempt % 2)) -eq 1 ]; then order="A B"; else order="B A"; fi
    for side in $order; do
      [ "${done[$side]}" -ge "$target" ] && continue
      gate
      lb=$S_LOAD1
      bb=$S_BUSY
      round=$((done[$side] + 1))
      bin=${!side}
      dir="$OUT/runs/$case/$side-$round-a$attempt"
      mkdir -p "$dir"
      echo "[$(now)] $case: $side round $round (load $(loadavg), ${bb}% busy)"
      rc=0
      "$RUN" --bin "$bin" --cases "$case" --rounds 1 \
        --duration "$DUR" --warmup "$WARMUP" --out "$dir" \
        >"$dir/run.log" 2>&1 || rc=$?
      sample_cpu
      la=$S_LOAD1
      ba=$S_BUSY
      counted=0
      if [ "$rc" -ne 0 ]; then
        echo "[$(now)] $case: $side round $round failed rc=$rc; see $dir/run.log"
        sleep 5
      elif [ "$ba" -gt "$MAX_BUSY" ]; then
        echo "[$(now)] $case: $side round $round disturbed (${ba}% busy after); repeating"
      elif ! is_growth "$case" && [ "$(wc -l < "$dir/results.tsv" 2>/dev/null || echo 0)" -lt 2 ]; then
        echo "[$(now)] $case: $side round $round wrote no results row; repeating"
      else
        if is_growth "$case"; then
          cp "$dir/growth.tsv" "$OUT/growth/$case-$side-$round.tsv"
        else
          ha=$(heap_mib "$dir/cases/$case/after.json")
          hs=$(heap_mib "$dir/cases/$case/settle.json")
          append_row "$dir/results.tsv" "$side" "$round" "$lb" "$bb" "$la" "$ba" "$ha" "$hs"
        fi
        done[$side]=$round
        counted=1
      fi
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(date -u +%FT%TZ)" "$case" "$side" "$attempt" "$counted" "$rc" \
        "$lb" "$bb" "$la" "$ba" >> "$ATTEMPTS"
    done
  done
  echo "[$(now)] $case: counted ${done[A]} A and ${done[B]} B"
done

echo "[$(now)] done: $MERGED"
