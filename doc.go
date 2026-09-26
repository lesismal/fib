// Package fib implements edge-triggered event loops that serve connections
// either on the loops themselves or on a pool of logical workers. By default,
// Config.IOPollers spreads the connections over several loops; without it, a
// single loop schedules them dynamically onto the workers.
package fib
