//go:build !linux && !darwin && !windows

package http

import fib "github.com/lesismal/fib"

// serverState is empty here: the server, and the Context it hands a request
// to, are built only on the platforms that have an engine to serve on.
type serverState struct{}

func (p *Parser) resetServerState() {}

// blockServerState is empty here too, for the same reason.
type blockServerState struct{}

func (b *requestBlock) recycleServerState() {}

// runOnStreams runs fn here. Only a server's Context gives a body the
// callback it would hand on, and there is no server here.
func runOnStreams(_ *fib.Connection, fn func()) { fn() }
