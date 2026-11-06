//go:build darwin || freebsd || ios || netbsd

package filewatch

import "syscall"

func changeTime(st *syscall.Stat_t) int64 { return st.Ctimespec.Nano() }
