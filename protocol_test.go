package fib

import "testing"

func TestProtocolString(t *testing.T) {
	for p, want := range map[Protocol]string{
		ProtocolTCP:  "tcp",
		ProtocolUDP:  "udp",
		ProtocolUnix: "unix",
		0:            "unknown",
	} {
		if got := p.String(); got != want {
			t.Errorf("Protocol(%d).String() = %q, want %q", p, got, want)
		}
	}
}
