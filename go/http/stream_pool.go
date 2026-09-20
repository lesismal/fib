package http

import (
	fib "github.com/lesismal/fib/go"
)

// A multiplexed protocol — HTTP/2 here, HTTP/3 in package http3 — carries
// many requests on one connection, and the goroutine that reads the
// connection is the one that frames them. Serving a request on that
// goroutine, as the HTTP/1 server does, means the connection reads nothing
// more until the handler returns, so requests a client sent together are
// served one after another however little each has to do with the rest: the
// head-of-line blocking multiplexing exists to remove, moved up into the
// application.
//
// A StreamPool runs those handlers on a pool of its own instead. The reader
// frames a request, hands it over and goes back to the socket, so one
// connection's requests are served concurrently and a slow handler holds up
// only itself.

// StreamPoolConfig describes the pool a multiplexed protocol runs its
// request handlers on, and how much of one connection it may run at once.
// Its zero value is the default: a pool shared by every server that asks for
// the same sizing, sized by fib.DefaultStreamPoolSizing, with no limit of
// its own on how much of a connection runs at once.
type StreamPoolConfig struct {
	// Disable serves every request on the goroutine that reads its
	// connection, one at a time, the way the HTTP/1 server does. It is
	// MaxConcurrentHandlers of 1 by another name, and builds no pool at all.
	Disable bool
	// MaxConcurrentHandlers is how many of one connection's requests may be
	// served at once. Zero, the default, and any value below it do not limit
	// them: every request goes to the pool as it is framed, and what bounds
	// them is how many streams the client may have open, which
	// Config.MaxConcurrentStreams sets.
	//
	// A positive N holds one connection to N handlers at once, and the Nth
	// of them runs on the connection's own reader rather than on the pool:
	// the connection then reads nothing more until that handler returns, so
	// the limit is paid for by the peer's flow control rather than by a
	// queue of requests here. N of 1 therefore runs every handler on the
	// reader, as Disable does.
	//
	// The limit is per connection, not per server. A server bounds what all
	// of its connections run at once through MaxWorkers instead.
	MaxConcurrentHandlers int
	// MaxWorkers is the ceiling the pool grows to under load, MinWorkers the
	// resident floor it retires back down to, and QueueSize how many
	// requests may wait for a worker. Zero means what
	// fib.DefaultStreamPoolSizing reports, except for MinWorkers, whose zero
	// takes the pool's own floor of twenty workers per P.
	//
	// The pool is shared by every server asking for the same three numbers,
	// so a process serving several ports pays for one pool. It is never
	// stopped: a handler has no close of its own, so there is no moment at
	// which the last server using a pool is known to be done with it. A
	// caller that wants a pool it can stop supplies one through TaskPool.
	MaxWorkers int
	MinWorkers int
	QueueSize  int
	// TaskPool, when set, runs the handlers instead of the pool the fields
	// above describe. The caller owns it: it may be the engine's own pool,
	// or one shared with other work, and it is the caller that stops it.
	TaskPool fib.TaskPool
}
