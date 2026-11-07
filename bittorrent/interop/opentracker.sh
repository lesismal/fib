#!/bin/sh
# Checks fib's tracker clients against opentracker, Debian's package of it,
# in a container: TestTrackerFibClientToOpentracker announces to and scrapes
# it over HTTP and UDP. A manual check, not run in CI; it needs docker.
#
#   ./opentracker.sh            # build the image, run it, run the test
#   PORT=16969 ./opentracker.sh
set -eu
cd "$(dirname "$0")"
PORT=${PORT:-16969}
NAME=fib-opentracker-$$

docker build -q -t fib-opentracker - <<'EOF'
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends opentracker && rm -rf /var/lib/apt/lists/*
# Debian's opentracker tracks only the info hashes of its whitelist: those
# of the test. It reads the list once it has chrooted to its rootdir,
# /etc/opentracker, so the list's path is taken from there.
RUN printf 'f1b0000000000000000000000000000000000001\nf1b0000000000000000000000000000000000002\n' \
	>> /etc/opentracker/whitelist.txt && \
	sed -i 's|^access.whitelist .*|access.whitelist /whitelist.txt|' /etc/opentracker/opentracker.conf
EXPOSE 6969/tcp 6969/udp
CMD ["opentracker", "-f", "/etc/opentracker/opentracker.conf"]
EOF

docker run -d --rm --name "$NAME" -p "127.0.0.1:$PORT:6969/tcp" -p "127.0.0.1:$PORT:6969/udp" fib-opentracker >/dev/null
trap 'docker rm -f "$NAME" >/dev/null 2>&1' EXIT INT TERM
sleep 1

FIB_OPENTRACKER=127.0.0.1:$PORT go test -count=1 -run TestTrackerFibClientToOpentracker -v .
