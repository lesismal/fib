package ice

import (
	"net"
	"net/netip"
)

// HostAddrs returns the addresses of the interfaces that are up, for host
// candidates of a socket bound to a wildcard address. It leaves out
// loopback addresses, which no peer elsewhere reaches, and link-local ones,
// which browsers leave out too: they need a zone to be used and are almost
// never the only way between two hosts.
func HostAddrs() ([]netip.Addr, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var out []netip.Addr
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipn, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(ipn.IP)
			if !ok {
				continue
			}
			ip = ip.Unmap()
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsUnspecified() {
				continue
			}
			out = append(out, ip)
		}
	}
	return out, nil
}
