#!/bin/bash
# Runs the ffmpeg interop checks in a Linux container: golang:1.27, which is
# Debian trixie, with trixie's ffmpeg 7.1 (libx264, libx265, libopus,
# libsrt) installed at start. This is what CI runs. An image that has Go
# and ffmpeg already skips the install.
#
# Usage: ./run.sh [go test flags...], e.g. ./run.sh -run 'TestMatrix/h264-aac'
# Environment: IMAGE (default golang:1.27), CPUS (docker --cpus; unset for
# all of them).
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
image=${IMAGE:-golang:1.27}
cpus=()
if [ -n "${CPUS:-}" ]; then
    cpus=(--cpus "$CPUS")
fi

# Docker Hub rate-limits anonymous pulls, which would fail the run for
# reasons unrelated to the code: retry, then fall back to its mirror on
# mirror.gcr.io.
if ! docker image inspect "$image" >/dev/null 2>&1; then
    pulled=0
    for delay in 5 15 30; do
        if docker pull -q "$image"; then
            pulled=1
            break
        fi
        sleep "$delay"
    done
    if [ "$pulled" = 0 ]; then
        echo "falling back to mirror.gcr.io for $image"
        image=mirror.gcr.io/library/$image
        docker pull -q "$image"
    fi
fi

# The module cache volume is shared with the other container runs.
docker run --rm ${cpus[@]+"${cpus[@]}"} \
    -e GOTOOLCHAIN=auto -e FIB_LOG_SETTINGS=0 -e FIB_MEDIA_INTEROP=1 \
    -v "$repo":/fib -v fibgomod:/go/pkg/mod -w /fib/media/interop \
    "$image" bash -c '
set -e
if ! command -v ffmpeg >/dev/null; then
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends ffmpeg >/dev/null
fi
ffmpeg -hide_banner -version | head -1
exec go test -count=1 -v -timeout 15m "$@" ./...
' run "$@"
