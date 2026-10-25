//go:build !linux

package udpbatch

// EnableGRO reports false: only Linux coalesces a run of a socket's datagrams
// into one read, and reports the size of each in its control message; the
// batch reads one datagram at a time elsewhere.
func EnableGRO(int) bool { return false }
