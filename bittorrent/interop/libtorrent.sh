#!/bin/sh
# Checks fib's BitTorrent client against libtorrent-rasterbar (Debian's
# python3-libtorrent, 2.0) both ways, in a container: TestClientLibtorrent
# has each download from the other v1, hybrid and v2 torrents, from .torrent
# files and from magnet links (btih, and btmh for v2, whose piece layers
# come with BEP 52 hash requests), over TCP and over uTP, with RC4
# required. It needs docker; the module cache is kept in a volume.
#
#   ./libtorrent.sh                 # every case
#   ./libtorrent.sh -run 'TestClientLibtorrent/.*v2'
set -eu
cd "$(dirname "$0")/../.."
IMAGE=fib-libtorrent

docker build -q -t "$IMAGE" - <<'EOF' >/dev/null
FROM golang:1.27.3
RUN apt-get update && apt-get install -y --no-install-recommends python3-libtorrent g++ && rm -rf /var/lib/apt/lists/*
EOF

RUN="-run TestClientLibtorrent"
if [ $# -gt 0 ]; then
	RUN="$*"
fi
docker run --rm --cpus 4 -v "$PWD":/fib -v fib-gomod:/go/pkg/mod -w /fib/bittorrent/interop \
	-e FIB_LIBTORRENT=1 -e CGO_ENABLED=1 "$IMAGE" \
	sh -c "python3 -c 'import libtorrent; print(\"libtorrent\", libtorrent.__version__)' && go test -count=1 -timeout 900s $RUN -v ."
