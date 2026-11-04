//go:build darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

// spliceSupported says the system has splice(2), which only Linux has; see
// Connection.SpliceTo.
const spliceSupported = false

func openSplicePipe() (splicePipe, error)  { return splicePipe{}, ErrSpliceUnsupported }
func closeSplicePipe(splicePipe)           {}
func spliceIn(int, int, int) (int, error)  { return 0, ErrSpliceUnsupported }
func spliceOut(int, int, int) (int, error) { return 0, ErrSpliceUnsupported }
