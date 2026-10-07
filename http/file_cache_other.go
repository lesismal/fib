//go:build darwin || windows

package http

// fileWatch reports changes to the files under a FileCache's directories;
// only Linux has one, through inotify, and elsewhere the cache checks each
// file as it serves it.
type fileWatch struct{}

func newFileWatch(string, func(string)) *fileWatch { return nil }

func (*fileWatch) add(string, string) bool { return false }

func (*fileWatch) close() {}
