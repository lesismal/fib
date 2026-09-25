// Package fib implements a single edge-triggered epoll event loop whose
// connections are dynamically scheduled onto a pool of logical workers.
// Config.IOPollers spreads them over several loops instead.
package fib
