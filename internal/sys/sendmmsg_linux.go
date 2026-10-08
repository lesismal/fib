//go:build linux && !amd64 && !386

package sys

import "syscall"

const SysSendmmsg = syscall.SYS_SENDMMSG
