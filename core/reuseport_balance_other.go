//go:build !(linux && (amd64 || arm64))

package core

// reusePortBalancer is empty where balancing is unsupported.
type reusePortBalancer struct{}

// newReusePortBalancer reports that balancing is unsupported, so the caller
// keeps plain SO_REUSEPORT.
func newReusePortBalancer([]int) (*reusePortBalancer, error) {
	return nil, errReusePortBalanceUnsupported
}

// openPinnedReusePortBalancer reports that balancing is unsupported.
func openPinnedReusePortBalancer(string, int, int, []int) (*reusePortBalancer, error) {
	return nil, errReusePortBalanceUnsupported
}

func (b *reusePortBalancer) Close() error { return nil }
