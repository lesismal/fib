// Package interop checks the WebRTC packages against pion over loopback:
// fib's PeerConnection against pion's, offering and answering, with a
// data channel, a track each way and TURN through fib's turnserver, and
// fib's DTLS against pion's. The checks are the tests of this package;
// they need nothing but Go, so `go test ./...` here runs them.
package interop
