package core

import "errors"

// ErrPoolBusy is what TryHand reports when the pool it was to hand work to
// is full: its queue holds all it may, or functions it turned away already
// wait for room. A caller fails what it was asked to do with it.
var ErrPoolBusy = errors.New("fib: pool is busy")
