// Package filewatch tells a cache of files under a directory when what it
// holds may have gone stale, and whether a file it holds is still the one it
// read.
//
// A Watcher reports changes to the entries of the directories under a root
// that its user asks it to watch, through inotify on Linux; elsewhere New
// returns ErrUnsupported, and a cache has only the Stamp of each file to go
// by, checking it as often as it can afford to.
//
// A Stamp is what a stat tells of a file's identity and contents: its size,
// modification time, status change time and inode. A file replaced by another
// of the same size and modification time, as rsync -t, cp -p or tar leave
// it, still has another inode, and one written over in place and given its
// old modification time back still has a newer status change time, which no
// one can set back.
package filewatch

import (
	"errors"
	"io/fs"
	"os"
)

// ErrUnsupported is what New returns where the system offers no watch.
var ErrUnsupported = errors.New("filewatch: not supported on this system")

// Event is a change a Watcher saw.
type Event struct {
	// Name is the slash-separated path, relative to the root, of the entry
	// that changed: created, written, its metadata changed, removed, or
	// moved in or out. It is "" when All is set.
	Name string
	// Dir says the entry is, or was, a directory, or a symbolic link to
	// one: whatever was learned of paths under Name is stale too.
	Dir bool
	// All says anything under the root may have changed: events were lost
	// as the kernel's queue overflowed, the root went, or a symbolic link on
	// the way to it changed.
	All bool
}

// Stamp is what a stat tells of a file's identity and contents. Two stamps
// of a file are equal while it has neither been replaced nor written.
type Stamp struct {
	Size int64
	// ModTime is the modification time, in nanoseconds since the Unix
	// epoch.
	ModTime int64
	// Change is the status change time, in nanoseconds since the Unix
	// epoch, which every write, rename onto the name and chmod moves on and
	// nothing sets back; on Windows, which has none, the creation time,
	// which a file put in another's place mostly brings along, but NTFS
	// tunneling may hand it the replaced file's.
	Change int64
	// Inode and Device name the file on its file system; Windows and some
	// other systems leave them 0.
	Inode, Device uint64
}

// StampOf is the stamp of the file info describes, which os.Stat or
// os.Lstat returned.
func StampOf(info fs.FileInfo) Stamp {
	s := Stamp{Size: info.Size(), ModTime: info.ModTime().UnixNano()}
	sysStamp(&s, info)
	return s
}

// Stat stats the file at name, following symbolic links, and returns its
// info and stamp.
func Stat(name string) (fs.FileInfo, Stamp, error) {
	info, err := os.Stat(name)
	if err != nil {
		return nil, Stamp{}, err
	}
	return info, StampOf(info), nil
}
