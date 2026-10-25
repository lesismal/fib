//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"time"

	"github.com/lesismal/fib/http3/quic"
)

// TransportConfig is how the QUIC connections under a server or a client
// behave: their flow control, the streams the peer may open, when they
// acknowledge, keep alive and update their keys, and how much a peer may
// make them do without asking for anything. Zero values take the defaults,
// which are package quic's.
type TransportConfig struct {
	// KeepAlivePeriod, when set, has a connection that has sent nothing for
	// this long send a PING, so that it neither idles out under
	// MaxIdleTimeout nor loses the NAT binding its path goes through. Zero,
	// the default, pings never: a connection that carries nothing idles out.
	KeepAlivePeriod time.Duration
	// StreamReceiveWindow and ConnReceiveWindow are how much the peer may
	// send ahead of what has been consumed, on one stream and on the
	// connection, given back as half of it is: they bound the memory a
	// connection's bodies can take waiting to be read, and over a long, fast
	// path, how fast one stream can go. Defaults 1 MiB and 16 MiB.
	StreamReceiveWindow uint64
	ConnReceiveWindow   uint64
	// MaxIncomingUniStreams is how many unidirectional streams the peer may
	// have open at once: its control and QPACK streams, and those of the
	// extensions it may try. Default 16; less than 3 leaves a peer that opens
	// all three unable to.
	MaxIncomingUniStreams uint64
	// MaxAckDelay is the longest a connection holds back an acknowledgement,
	// which it tells the peer. Default 25 milliseconds.
	MaxAckDelay time.Duration
	// StreamResetBurst and StreamResetRate bound the request streams a
	// server's client may reset while they are still open, or have the
	// server reset for an error it made on them, before the connection
	// closes with H3_EXCESSIVE_LOAD: the Rapid Reset attack (CVE-2023-44487)
	// over HTTP/3. ControlFrameBurst and ControlFrameRate bound in the same
	// way the frames a peer, server or client, may send that ask for work
	// and carry nothing, such as PING after PING. Each is a bucket of the
	// burst, refilled at the rate a second; zero means quic's defaults, 1000
	// and 100, and either of a pair below zero no limit. A client counts no
	// resets. A server in front of gRPC clients, which cancel calls by
	// resetting their streams, may need a higher reset rate.
	StreamResetBurst  int
	StreamResetRate   int
	ControlFrameBurst int
	ControlFrameRate  int
	// KeyUpdateInterval, when set, updates the keys a connection sends with
	// once they have been in use this long, as well as once they have
	// protected as many packets as the cipher allows.
	KeyUpdateInterval time.Duration
	// ActiveConnectionIDs is how many connection IDs a connection gives its
	// peer to send to at once, as many as the peer allows. Default 4.
	ActiveConnectionIDs int
}

// apply sets what the TransportConfig sets in a QUIC config.
func (t TransportConfig) apply(c *quic.Config) {
	c.KeepAlivePeriod = t.KeepAlivePeriod
	c.StreamReceiveWindow = t.StreamReceiveWindow
	c.ConnReceiveWindow = t.ConnReceiveWindow
	c.MaxIncomingUniStreams = t.MaxIncomingUniStreams
	c.MaxAckDelay = t.MaxAckDelay
	c.StreamResetBurst, c.StreamResetRate = t.StreamResetBurst, t.StreamResetRate
	c.ControlFrameBurst, c.ControlFrameRate = t.ControlFrameBurst, t.ControlFrameRate
	c.KeyUpdateInterval = t.KeyUpdateInterval
	c.ActiveConnectionIDs = t.ActiveConnectionIDs
}
