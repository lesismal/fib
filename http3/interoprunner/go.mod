// The QUIC Interop Runner endpoint. It is a module of its own, like
// ../interop, so that nothing this needs - Docker images, the runner's
// Python, published peer images - can reach fib's go.mod.
module github.com/lesismal/fib/http3/interoprunner

go 1.27.3

require github.com/lesismal/fib v0.0.0

replace github.com/lesismal/fib => ../..
