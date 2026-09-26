//go:build darwin

package fib

import "syscall"

const soReusePort = syscall.SO_REUSEPORT

// reusePortSpreads is false: Darwin lets sockets share an address through
// SO_REUSEPORT, but hands a TCP connection to one of them rather than
// spreading connections over all of them, so the pollers cannot each accept
// their own; see Config.ReusePort.
const reusePortSpreads = false
