//go:build windows

package http

import (
	"errors"
	"os"
)

var errIsDir = errors.New("http: is a directory")

// statStamp stats full: its stamp and true, false for a file that does not
// exist, or an error, a directory's among them. Windows reports no inode;
// size and modification time stand for the file.
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
	return fileStamp{size: info.Size(), modTime: info.ModTime()}, true, nil
}
