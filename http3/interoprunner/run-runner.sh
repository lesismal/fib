#!/bin/bash
# Runs the real QUIC Interop Runner against the fib endpoint image.
#
# Needs docker with the compose plugin, tshark, and the runner's Python
# dependencies (`pip3 install -r requirements.txt`). It clones the runner if
# RUNNER_DIR is empty, registers fib's image in its implementations file, and
# runs both directions against quic-go, quiche and ngtcp2 by default.
#
# Usage:
#   ./run-runner.sh                         # the supported matrix, every peer
#   TESTCASES=http3 ./run-runner.sh         # one testcase
#   DIRECTION=server TESTCASES=chacha20 ./run-runner.sh
#   PEERS=quiche IMAGE=fib-interop:latest ./run-runner.sh
#
# PEERS is a comma- or space-separated list of names from the runner's
# implementations_quic.json; PEER, a single name, is still accepted.
# The script exits non-zero if any run had a failed testcase.
#
# Environment: RUNNER_DIR, IMAGE, TESTCASES, PEERS (or PEER), DIRECTION.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
runner=${RUNNER_DIR:-/tmp/quic-interop-runner}
image=${IMAGE:-fib-interop:local}
testcases=${TESTCASES:-http3,handshake,transfer,retry,resumption,zerortt,multiplexing,keyupdate}
peers=${PEERS:-${PEER:-quic-go,quiche,ngtcp2}}
peers=${peers//,/ }
direction=${DIRECTION:-both}

if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is required" >&2
    exit 1
fi
if ! command -v tshark >/dev/null 2>&1; then
    echo "tshark is required (apt-get install tshark)" >&2
    exit 1
fi
if [ ! -f "$runner/run.py" ]; then
    git clone --depth 1 https://github.com/quic-interop/quic-interop-runner "$runner"
fi
if ! python3 -c 'import pyshark, Crypto, termcolor, prettytable, random_slugs' 2>/dev/null; then
    echo "the runner's Python dependencies are missing: pip3 install -r $runner/requirements.txt" >&2
    exit 1
fi

# The runner reads implementations_quic.json from its own directory. Register
# fib's image there, pointing at the image built from this checkout.
python3 - "$runner/implementations_quic.json" "$image" <<'PY'
import json
import sys

path, image = sys.argv[1], sys.argv[2]
data = json.load(open(path))
data["fib"] = {
    "image": image,
    "url": "https://github.com/lesismal/fib",
    "role": "both",
}
json.dump(data, open(path, "w"), indent=2)
PY

cd "$runner"
case "$direction" in
    both | server | client) ;;
    *)
        echo "DIRECTION must be both, server or client" >&2
        exit 1
        ;;
esac

# run <server> <client> runs one direction and keeps going after a failure,
# so one peer's failure still shows the other peers' results.
#
# A testcase that fails is run once more, alone, and counts as failed only if
# it fails again. The runner judges several testcases (resumption, zerortt,
# retry) by the packets in the simulator's captures, and stops every
# container as soon as the client exits; the simulator's capture can then
# end before it has written its last packets, which on 2026-11-05 left the
# server side's trace with the first of resumption's two handshakes only
# (fib_quic-go, "Expected exactly 2 handshake. Got: 1") while the client's
# side showed both. A testcase that fails twice is a real failure.
status=0
run() {
    local results failed
    results=$(mktemp)
    if python3 run.py -s "$1" -c "$2" -t "$testcases" -m -j "$results"; then
        rm -f "$results"
        return
    fi
    failed=$(python3 - "$results" <<'PY'
import json
import sys

try:
    data = json.load(open(sys.argv[1]))
except (OSError, ValueError):
    sys.exit(0)
names = [t["name"] for row in data.get("results", []) for t in row if t.get("result") == "failed"]
print(",".join(dict.fromkeys(names)))
PY
)
    rm -f "$results"
    if [ -z "$failed" ]; then
        # The runner failed without a testcase to blame: nothing to rerun.
        status=1
        return
    fi
    echo "run-runner: $1 (server) vs $2 (client) failed $failed; running it once more" >&2
    python3 run.py -s "$1" -c "$2" -t "$failed" -m || status=1
}
for peer in $peers; do
    if [ "$direction" != client ]; then
        run fib "$peer"
    fi
    if [ "$direction" != server ]; then
        run "$peer" fib
    fi
done
exit "$status"
