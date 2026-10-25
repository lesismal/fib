package sys

import "syscall"

// SetDontFragment has the datagrams a UDP socket sends go with the Don't
// Fragment bit over IPv4 and unfragmented over IPv6, with IP_PMTUDISC_PROBE:
// the path MTU the kernel learns from ICMP is ignored, so that path MTU
// discovery above the socket probes for sizes the kernel would otherwise
// cut down or refuse. A datagram larger than the interface's MTU is still
// refused, with EMSGSIZE. A dual-stack IPv6 socket takes both options, one
// of IPv4 only the first.
func SetDontFragment(fd int) error {
	err4 := syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_PROBE)
	err6 := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_MTU_DISCOVER, syscall.IPV6_PMTUDISC_PROBE)
	if err4 == nil || err6 == nil {
		return nil
	}
	return err4
}
