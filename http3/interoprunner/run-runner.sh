#!/bin/bash
# Runs the real QUIC Interop Runner against the fib endpoint image.
#
# Needs docker with the compose plugin, tshark, and the runner's Python
# dependencies (`pip3 install -r requirements.txt`). It clones the runner if
# RUNNER_DIR is empty, registers fib's image in its implementations file, and
# runs both directions against quic-go by default.
#
# Usage:
#   ./run-runner.sh                         # the supported matrix, fib vs quic-go
#   TESTCASES=http3 ./run-runner.sh         # one testcase
#   DIRECTION=server TESTCASES=chacha20 ./run-runner.sh
#   PEER=quiche IMAGE=fib-interop:latest ./run-runner.sh
#
# Environment: RUNNER_DIR, IMAGE, TESTCASES, PEER, DIRECTION, RUNNER.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
runner=${RUNNER_DIR:-/tmp/quic-interop-runner}
image=${IMAGE:-fib-interop:local}
testcases=${TESTCASES:-http3,handshake,transfer,retry,resumption,zerortt,multiplexing,keyupdate}
peer=${PEER:-quic-go}
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
    both)
        python3 run.py -s fib -c "$peer" -t "$testcases" -m
        python3 run.py -s "$peer" -c fib -t "$testcases" -m
        ;;
    server)
        python3 run.py -s fib -c "$peer" -t "$testcases" -m
        ;;
    client)
        python3 run.py -s "$peer" -c fib -t "$testcases" -m
        ;;
    *)
        echo "DIRECTION must be both, server or client" >&2
        exit 1
        ;;
esac
