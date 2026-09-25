//go:build linux && !amd64 && !386

package fib

import "syscall"

const sysSendmmsg = syscall.SYS_SENDMMSG
