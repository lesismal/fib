#!/bin/bash
# The entrypoint of the fib HTTP/3 QUIC Interop Runner endpoint. The runner
# sets ROLE, TESTCASE and the directories; see README.md.
set -u

# Under the runner the endpoints are on two docker networks with the
# simulator between them; /setup.sh (from the endpoint base image) installs
# the routes that send the peer's address range through it. It also creates
# /logs/qlog. Run the image outside the runner, with no sim host, and there
# is nothing to set up.
if [ -x /setup.sh ] && getent hosts sim >/dev/null 2>&1; then
    /setup.sh || echo "run_endpoint: /setup.sh failed; carrying on" >&2
    # The simulator forwards nothing until it is up, which is later than the
    # endpoints start: a client that dials before then has its first flight
    # dropped before it reaches the capture, and the runner's long RTT
    # testcase, which counts ClientHellos, sees one where it wants two. The
    # other endpoints wait for the simulator's port before a client dials.
    if [ "${ROLE:-}" = client ] && [ -x /wait-for-it.sh ]; then
        /wait-for-it.sh sim:57832 -s -t 30 ||
            echo "run_endpoint: the simulator did not come up within 30s; carrying on" >&2
    fi
fi

for dir in "${DOWNLOADS:-/downloads}" "${LOGS:-/logs}" "${QLOGDIR:-}"; do
    [ -n "$dir" ] && mkdir -p "$dir"
done

exec /usr/local/bin/fib-interop
