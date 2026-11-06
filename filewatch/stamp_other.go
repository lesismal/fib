//go:build !unix && !windows

package filewatch

import "io/fs"

func sysStamp(*Stamp, fs.FileInfo) {}
