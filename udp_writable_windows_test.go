//go:build windows

package fib

// udpListenerWoken is how many times the loop found room on l's socket,
// which on Windows it never looks for.
func udpListenerWoken(*udpListener) int { return 0 }
