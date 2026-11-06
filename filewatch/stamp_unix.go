//go:build unix

package filewatch

import (
	"io/fs"
	"syscall"
)

func sysStamp(s *Stamp, info fs.FileInfo) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	s.Change = changeTime(st)
	s.Inode = uint64(st.Ino)
	s.Device = uint64(st.Dev)
}
