//go:build windows

package filewatch

import (
	"io/fs"
	"syscall"
)

func sysStamp(s *Stamp, info fs.FileInfo) {
	if d, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
		s.Change = d.CreationTime.Nanoseconds()
	}
}
