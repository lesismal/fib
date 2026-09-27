// Package fib implements edge-triggered event loops that wait for their
// connections' events and schedule the connections dynamically onto a pool of
// logical workers, which run every round of a connection, its reads and its
// callbacks; the loops never run one. The one thing a loop reads itself is a
// UDP socket its peers share, whose datagrams it sorts onto their
// connections for the workers. By default, Config.IOPollers spreads the
// connections over several loops; without it, a single loop waits for all of
// them.
package fib
