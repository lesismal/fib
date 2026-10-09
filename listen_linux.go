package fib

import "syscall"

// stopListeningFD stops the listening socket fd from taking connections
// while keeping its descriptor: a listening socket that is shut down for
// reading stops listening, and accept on it fails.
func stopListeningFD(fd int) { _ = syscall.Shutdown(fd, syscall.SHUT_RD) }
