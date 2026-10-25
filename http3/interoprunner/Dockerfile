# The QUIC Interop Runner endpoint for fib's HTTP/3 stack.
#
# Build from the repository root, so that the module's `replace ../..` finds
# fib:
#
#   docker build -f http3/interoprunner/Dockerfile -t fib-interop .
#
# The runtime is the runner's standard endpoint base image: it carries
# /setup.sh, which configures the routes the simulator needs, and the tools
# the runner expects an endpoint to have.
FROM golang:1.27.3 AS build

ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src
COPY . .
WORKDIR /src/http3/interoprunner
RUN go build -trimpath -buildvcs=false -o /out/fib-interop .

FROM martenseemann/quic-network-simulator-endpoint:latest

COPY --from=build /out/fib-interop /usr/local/bin/fib-interop
COPY http3/interoprunner/run_endpoint.sh /run_endpoint.sh
RUN chmod 0755 /run_endpoint.sh

# ROLE, TESTCASE, REQUESTS, DOWNLOADS, SSLKEYLOGFILE, QLOGDIR, LOGS, CERT and
# KEY are read from the environment by run_endpoint.sh and the binary; see
# README.md.
ENTRYPOINT ["/run_endpoint.sh"]
