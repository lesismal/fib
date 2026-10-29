// Package outputwait lets the packages of this module wait for a fib
// connection's queued output to drain, which package fib does not offer
// outside the module.
package outputwait

// Wait waits while more than limit bytes of what c, a *fib.Connection, was
// sent are still queued for its socket, until no more than half of limit
// are, and reports an error instead once the connection has closed or a
// write to it has failed. Package fib sets it when it is initialized; it is
// nil on a platform fib serves through the portable backend.
var Wait func(c any, limit int64) error

// Pending reports how many bytes of what c, a *fib.Connection, was sent are
// still queued for its socket, without waiting. It is set and nil where Wait
// is.
var Pending func(c any) int64

// Queued is Pending with what a cork holds counted too, as Wait counts it,
// without taking the connection's lock: a writer that paces what it queues
// by it sees what it queued a moment ago even before the cork comes off. It
// is set and nil where Wait is.
var Queued func(c any) int64

// Unsent is Pending read under the connection's lock, for a caller that
// wants the exact figure rather than one to pace by: it never counts bytes a
// write in progress has already handed the socket, which the peer may have
// read, and counts those a read round has queued without counting them yet.
// It is set and nil where Wait is.
var Unsent func(c any) int64

// Notify is Wait for a caller that may not wait: when more than limit bytes
// of what c was sent are queued for its socket it arranges for fn to run, on
// another goroutine, once no more than half of limit are, or with an error
// once the connection has closed, and reports true; when no more than limit
// are, it reports false and fn never runs. It is set and nil where Wait is.
var Notify func(c any, limit int64, fn func(error)) bool
