// The HTTP/3 conformance tests' peer. It is a module of its own so that
// quic-go is not a dependency of github.com/lesismal/fib/go.
module github.com/lesismal/fib/go/http3/interop

go 1.27

require github.com/quic-go/quic-go v0.62.0

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)
