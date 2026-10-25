package sys

import "syscall"

const (
	ipDontFrag   = syscall.IP_DONTFRAG
	ipv6DontFrag = syscall.IPV6_DONTFRAG
)
