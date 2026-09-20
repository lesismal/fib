//go:build !linux && !darwin && !windows

package http

// serverState is empty here: the server, and the Context it hands a request
// to, are built only on the platforms that have an engine to serve on.
type serverState struct{}

func (p *Parser) resetServerState() {}
