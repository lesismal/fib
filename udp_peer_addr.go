package fib

import (
	"errors"
	"fmt"
	"net"
	"strconv"
)

// errUDPPeerAddr is what OpenUDPPeer reports for an address the socket
// cannot reach.
var errUDPPeerAddr = errors.New("fib: UDP peer address does not fit the socket")

// errPeerClosed is what OpenUDPPeer reports once the engine or the binding
// opens no new peers.
var errPeerClosed = fmt.Errorf("fib: UDP binding opens no new peers: %w", net.ErrClosed)

// zoneIndex is the interface index an IPv6 zone names, by number or by name.
func zoneIndex(zone string) (uint32, error) {
	if zone == "" {
		return 0, nil
	}
	if n, err := strconv.ParseUint(zone, 10, 32); err == nil {
		return uint32(n), nil
	}
	ifi, err := net.InterfaceByName(zone)
	if err != nil {
		return 0, err
	}
	return uint32(ifi.Index), nil
}
