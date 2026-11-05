// Package fib implements edge-triggered event loops that wait for their
// connections' events and schedule the connections dynamically onto a pool of
// logical workers, which run every round of a connection, its reads and its
// callbacks; the loops never run one. The one thing a loop reads itself is a
// UDP socket its peers share, whose datagrams it sorts onto their connections
// for the workers. By default, Config.IOPollers spreads the connections over
// several loops; without it, a single loop waits for all of them.
//
// Engine.Dial and Engine.DialWith, which may bind a chosen local address and
// port range, open outgoing connections on the same loops and workers, and
// Engine.OpenUDPPeer lets a UDP binding speak first to an address that has
// sent it nothing.
//
// The implementation lives in package core. Every declaration package core
// exports is re-exported here, as a type alias, a constant, a variable or a
// thin wrapper, so that importing github.com/lesismal/fib keeps the API it had
// when the implementation lived in this package; the types are the same types,
// with no indirection on the data path. See package core for the code and the
// documentation of each declaration and method.
package fib
