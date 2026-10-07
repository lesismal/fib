//go:build linux || darwin

package http

import (
	"errors"
	"os"
	"syscall"
)

var errIsDir = errors.New("http: is a directory")

// statStamp stats full: its stamp and true, false for a file that does not
// exist, or an error, a directory's among them.
func statStamp(full string) (fileStamp, bool, error) {
	info, err := os.Stat(full)
	if errors.Is(err, os.ErrNotExist) {
		return fileStamp{}, false, nil
	}
	if err != nil {
		return fileStamp{}, false, err
	}
	if info.IsDir() {
		return fileStamp{}, false, errIsDir
	}
	stamp := fileStamp{size: info.Size(), modTime: info.ModTime()}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		stamp.inode = uint64(st.Ino)
	}
	return stamp, true, nil
}
