//go:build aix || android || dragonfly || illumos || linux || openbsd || solaris

package filewatch

import "syscall"

func changeTime(st *syscall.Stat_t) int64 { return st.Ctim.Nano() }
