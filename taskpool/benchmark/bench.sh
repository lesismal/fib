#!/usr/bin/env bash
# Runs BenchmarkPools on Linux or macOS and summarizes the results.
# See README.md, or run ./bench.sh -h.
set -euo pipefail

caller="$(pwd)"
cd "$(dirname "$0")"

SCENARIOS="Handoff,ParallelTiny,LoopCPUShort,LoopCPULong,LoopBlocking,LoopMixed,Bursts"
POOLS="nbio,ants,gopool,fib-adaptive,fib-adaptive-chan,fib-elastic"

scenarios=""
pools=""
count=1
benchtime=1s
cpu=""
out=""
docker=0
docker_cpus=""
docker_image="golang:1.27"

usage() {
	cat <<EOF
Usage: ./bench.sh [options]

  -s LIST     scenarios, comma-separated (default: all)
              $SCENARIOS
  -p LIST     pools, comma-separated (default: all)
              $POOLS
  -c N        runs of each benchmark (default: $count)
  -t TIME     -benchtime, e.g. 1s or 100000x (default: $benchtime)
  -cpu LIST   GOMAXPROCS values, e.g. 4,8 (default: the machine's)
  -o FILE     raw output file (default: results/<os>-<time>.txt)
  -docker     run in a Linux container ($docker_image) instead of here
  -cpus N     with -docker, the CPUs the container gets (default: all)
  -image IMG  with -docker, the image to run (default: $docker_image)
  -l          list the scenarios and pools
  -h          show this help

Examples:
  ./bench.sh                                   # everything, 5 runs
  ./bench.sh -s Handoff,Bursts -p nbio,fib-elastic -c 3
  ./bench.sh -docker -cpus 8 -s LoopBlocking
EOF
}

# alternation turns a comma-separated list into an anchored regex group.
alternation() { echo "^($(echo "$1" | tr ',' '|'))\$"; }

while [ $# -gt 0 ]; do
	case "$1" in
	-s) scenarios="$2"; shift 2 ;;
	-p) pools="$2"; shift 2 ;;
	-c) count="$2"; shift 2 ;;
	-t) benchtime="$2"; shift 2 ;;
	-cpu) cpu="$2"; shift 2 ;;
	-o) out="$2"; case "$out" in /*) ;; *) out="$caller/$out" ;; esac; shift 2 ;;
	-docker) docker=1; shift ;;
	-cpus) docker_cpus="$2"; shift 2 ;;
	-image) docker_image="$2"; shift 2 ;;
	-l) echo "scenarios: $SCENARIOS"; echo "pools:     $POOLS"; exit 0 ;;
	-h|--help) usage; exit 0 ;;
	*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

if [ "$docker" = 1 ]; then
	# Mount the whole repository, since the module replaces fib with ../..,
	# and run this script again inside the container without -docker.
	root="$(cd ../.. && pwd)"
	args=(-c "$count" -t "$benchtime")
	[ -n "$scenarios" ] && args+=(-s "$scenarios")
	[ -n "$pools" ] && args+=(-p "$pools")
	[ -n "$cpu" ] && args+=(-cpu "$cpu")
	if [ -n "$out" ]; then
		case "$out" in
		"$root"/*) args+=(-o "/fib/${out#"$root"/}") ;;
		*) echo "-o must be inside the repository with -docker" >&2; exit 2 ;;
		esac
	fi
	limit=()
	[ -n "$docker_cpus" ] && limit=(--cpus "$docker_cpus")
	exec docker run --rm ${limit[@]+"${limit[@]}"} \
		-v "$root":/fib -v fib-gomod:/go/pkg/mod \
		-w /fib/taskpool/benchmark "$docker_image" \
		bash ./bench.sh "${args[@]}"
fi

filter="BenchmarkPools/$(alternation "${scenarios:-$SCENARIOS}")/$(alternation "${pools:-$POOLS}")"
if [ -z "$out" ]; then
	out="results/$(go env GOOS)-$(go env GOARCH)-$(date +%Y%m%d-%H%M%S).txt"
fi
mkdir -p "$(dirname "$out")"

flags=(-run '^$' -bench "$filter" -count "$count" -benchtime "$benchtime" -timeout 0)
[ -n "$cpu" ] && flags+=(-cpu "$cpu")

echo "go test ${flags[*]}" >&2
echo "raw output: $out" >&2
go test "${flags[@]}" . | tee "$out"
echo
go run ./summarize "$out"
