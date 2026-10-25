#!/bin/bash
# Runs the interop endpoint against itself, without the Interop Runner: one
# container is the server and another the client of the same image, on a
# docker network, for the testcases the endpoint supports. This is the
# end-to-end check CI runs; it also proves the image serves what the runner
# mounts.
#
# Usage: ./local-test.sh [image]
# Needs docker and openssl.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
image=${1:-fib-interop:local}
network="fib-interop-local-$$"
tmp=$(mktemp -d)
server="fib-interop-server-$$"
logs_server="$tmp/logs-server"
logs_client="$tmp/logs-client"
fail=0

cleanup() {
    docker rm -f "$server" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    rm -rf "$tmp"
}
trap cleanup EXIT

step() { printf '\n================ %s\n' "$*"; }

step "build $image"
docker build -f "$here/Dockerfile" -t "$image" "$repo"

mkdir -p "$tmp/certs" "$tmp/www" "$tmp/downloads" "$logs_server" "$logs_client"
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -keyout "$tmp/certs/priv.key" -out "$tmp/certs/cert.pem" \
    -subj "/CN=server4" \
    -addext "subjectAltName=DNS:server4,DNS:server6,DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
for spec in "tiny 1024" "small 65536" "medium 1048576" "large 2097152"; do
    set -- $spec
    head -c "$2" /dev/urandom > "$tmp/www/$1"
done

docker network create "$network" >/dev/null

# requests names the files of /www, in the order the runner's REQUESTS has
# them, as the URLs the client downloads.
requests() {
    local out=""
    for name in "$@"; do
        out+=" https://server4:443/$name"
    done
    printf '%s' "${out# }"
}

# run_case <name> <server TESTCASE> <client TESTCASE> <files...>
run_case() {
    local name=$1 server_case=$2 client_case=$3
    shift 3
    step "$name"
    rm -rf "$tmp/downloads" && mkdir -p "$tmp/downloads"
    docker run -d --rm --name "$server" --network "$network" \
        --network-alias server4 --network-alias server6 \
        -v "$tmp/certs:/certs:ro" -v "$tmp/www:/www:ro" -v "$logs_server:/logs" \
        -e ROLE=server -e TESTCASE="$server_case" \
        -e SSLKEYLOGFILE=/logs/keys.log -e QLOGDIR=/logs/qlog -e LOGS=/logs \
        "$image" >/dev/null
    local ready=0
    for _ in $(seq 100); do
        if docker logs "$server" 2>&1 | grep -q "listening on :443"; then
            ready=1
            break
        fi
        sleep 0.1
    done
    if [ "$ready" != 1 ]; then
        echo "FAILED: server did not start"
        docker logs "$server" 2>&1 | tail -20
        fail=1
        docker rm -f "$server" >/dev/null 2>&1 || true
        return
    fi
    local urlargs
    urlargs=$(requests "$@")
    if ! docker run --rm --network "$network" \
        -v "$tmp/certs:/certs:ro" -v "$tmp/downloads:/downloads" -v "$logs_client:/logs" \
        -e ROLE=client -e TESTCASE="$client_case" -e REQUESTS="$urlargs" \
        -e SSLKEYLOGFILE=/logs/keys.log -e QLOGDIR=/logs/qlog -e LOGS=/logs \
        "$image"; then
        echo "FAILED: client exited non-zero"
        docker logs "$server" 2>&1 | tail -20
        fail=1
    else
        local bad=0
        for name in "$@"; do
            if ! cmp -s "$tmp/www/$name" "$tmp/downloads/$name"; then
                echo "FAILED: $name differs or is missing"
                ls -l "$tmp/downloads" 2>/dev/null || true
                bad=1
            fi
        done
        if [ "$bad" = 0 ]; then
            echo "PASSED: $name ($*)"
        else
            fail=1
        fi
    fi
    docker rm -f "$server" >/dev/null 2>&1 || true
}

run_case "http3, three files on one connection" http3 http3 tiny small medium
run_case "handshake, one file" handshake handshake tiny
run_case "transfer, three files on one connection" transfer transfer small medium large
run_case "retry, one file after a Retry" retry retry tiny
run_case "resumption, second connection resumes" resumption resumption tiny small medium
run_case "zerortt, the rest in the first flight" zerortt zerortt tiny small medium
run_case "multiconnect, one connection per file" multiconnect multiconnect tiny small medium

echo
if [ "$fail" = 0 ]; then
    echo "ALL LOCAL INTEROP CASES PASSED"
else
    echo "SOME LOCAL INTEROP CASES FAILED"
fi
exit "$fail"
