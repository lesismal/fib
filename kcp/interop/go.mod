// The kcp-go interop checks of package kcp. They are a module of their own
// so that kcp-go is never a dependency of github.com/lesismal/fib.
module github.com/lesismal/fib/kcp/interop

go 1.27.3

require (
	github.com/lesismal/fib v0.0.0
	github.com/xtaci/kcp-go/v5 v5.7.4
	golang.org/x/crypto v0.58.0
)

require (
	github.com/juju/ratelimit v1.0.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.1 // indirect
	github.com/klauspost/reedsolomon v1.14.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pion/transport/v5 v5.1.4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/tjfoc/gmsm v1.4.1 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/lesismal/fib => ../..
