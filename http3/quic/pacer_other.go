//go:build !windows

package quic

import "time"

// minPacingDelay is the shortest wait for the pacing budget: a timer set any
// closer would fire late anyway.
const minPacingDelay = time.Millisecond

// socketFull is wouldBlock's platform part, which EAGAIN and ENOBUFS cover
// here.
func socketFull(error) bool { return false }
