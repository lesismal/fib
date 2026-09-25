package http

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

// StreamPoolConfig describes whether a multiplexed protocol runs its request
// handlers on the stream pool, and how much of one connection it may run
// there at once. Its zero value is the default: every request goes to the
// pool, with no limit of its own on how much of a connection runs at once.
//
// There is one stream pool for each engine name, "<Name>-streams", shared by
// every HTTP/2 and HTTP/3 server whose connections come from engines of that
// name, and it is never the pool of an engine: an engine worker that framed a
// request and found an engine's own queue full of handlers would wait for
// room that only another engine worker, just as stuck, could make. Its
// ceiling is twice the widest engine pool of that name running, or twice
// what fib.DefaultPoolSizing reports while none is, and its floor zero, so an
// idle pool keeps no worker; neither is configured per server. It stops once
// the last engine of its name has closed, without waiting for the handlers
// still running, and a request that comes after is served by its reader.
type StreamPoolConfig struct {
	// Disable serves every request on the goroutine that reads its
	// connection, one at a time, the way the HTTP/1 server does. It is
	// MaxConcurrentHandlers of 1 by another name, and uses no pool at all.
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
	// The limit is per connection, not per server. What bounds all the
	// connections of every server at once is the pool's ceiling.
	MaxConcurrentHandlers int
}
