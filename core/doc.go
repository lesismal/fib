// Package core implements edge-triggered event loops that wait for their
// connections' events and schedule the connections dynamically onto a pool of
// logical workers, which run every round of a connection, its reads and its
// callbacks; the loops never run one. The one thing a loop reads itself is a
// UDP socket its peers share, whose datagrams it sorts onto their
// connections for the workers. By default, Config.IOPollers spreads the
// connections over several loops; without it, a single loop waits for all of
// them.
//
// Engine.Dial and Engine.DialWith, which may bind a chosen local address and
// port range, open outgoing connections on the same loops and workers, and
// Engine.OpenUDPPeer lets a UDP binding speak first to an address that has
// sent it nothing.
//
// Package core is the implementation of package fib: github.com/lesismal/fib
// re-exports every exported declaration here as an alias or a thin wrapper,
// and is the package to import.
package core
