package sys

// IP_DONTFRAG and IPV6_DONTFRAG of <netinet/in.h> and <netinet6/in6.h>,
// which the syscall package leaves out.
const (
	ipDontFrag   = 28
	ipv6DontFrag = 62
)
