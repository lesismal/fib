package fib

import "os"

// preforkChildEnv names the variable package prefork starts its children
// with; see preforkChild.
const preforkChildEnv = "FIB_PREFORK_CHILD"

// preforkChild reports whether this process is one of the children package
// prefork starts, which all serve the same addresses: every engine it binds
// listens with SO_REUSEPORT, as Config.ReusePort asks, so that the kernel
// spreads the connections, and the datagrams, over the children rather than
// refusing the address to all of them but the first.
var preforkChild = os.Getenv(preforkChildEnv) != ""
