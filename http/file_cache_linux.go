//go:build linux

package http

import (
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"
)

// watchMask is every change to a directory's entries, or to a file's
// contents or metadata, that may make what the cache holds of it stale, and
// the directory going away.
const watchMask = syscall.IN_CREATE | syscall.IN_DELETE | syscall.IN_MODIFY | syscall.IN_CLOSE_WRITE |
	syscall.IN_MOVED_FROM | syscall.IN_MOVED_TO | syscall.IN_ATTRIB | syscall.IN_DELETE_SELF | syscall.IN_MOVE_SELF

// fileWatch reports changes to the files under a FileCache's directories
// through inotify: each directory a cached file is in is watched, and every
// change to one of its entries is reported by its path relative to the root,
// on a goroutine the watch runs, within moments of the change.
type fileWatch struct {
	file     *os.File
	onChange func(name string)

	mu   sync.Mutex
	dirs map[string]int32
	wds  map[int32]string
}

// newFileWatch returns a watch reporting to onChange, or nil where inotify
// cannot be had.
func newFileWatch(_ string, onChange func(string)) *fileWatch {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		return nil
	}
	// Non-blocking, so that the runtime's poller waits for its events
	// rather than a thread.
	w := &fileWatch{file: os.NewFile(uintptr(fd), "inotify"), onChange: onChange,
		dirs: make(map[string]int32), wds: make(map[int32]string)}
	go w.run()
	return w
}

// add watches dir, relative to root, if it is not watched yet, and reports
// whether it is.
func (w *fileWatch) add(root, dir string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.dirs[dir]; ok {
		return true
	}
	var wd int
	err := w.control(func(fd int) error {
		var err error
		wd, err = syscall.InotifyAddWatch(fd, filepath.Join(root, filepath.FromSlash(dir)), watchMask)
		return err
	})
	if err != nil {
		return false
	}
	w.dirs[dir], w.wds[int32(wd)] = int32(wd), dir
	return true
}

// control runs f on the inotify descriptor.
func (w *fileWatch) control(f func(fd int) error) error {
	raw, err := w.file.SyscallConn()
	if err != nil {
		return err
	}
	var ferr error
	if err := raw.Control(func(fd uintptr) { ferr = f(int(fd)) }); err != nil {
		return err
	}
	return ferr
}

func (w *fileWatch) close() { _ = w.file.Close() }

// run reads the watch's events until it is closed, and reports each.
func (w *fileWatch) run() {
	buf := make([]byte, 64<<10)
	for {
		n, err := w.file.Read(buf)
		if err != nil {
			return
		}
		for at := 0; at+syscall.SizeofInotifyEvent <= n; {
			ev := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[at]))
			nameBytes := buf[at+syscall.SizeofInotifyEvent : at+syscall.SizeofInotifyEvent+int(ev.Len)]
			at += syscall.SizeofInotifyEvent + int(ev.Len)
			name := string(nameBytes)
			for len(name) > 0 && name[len(name)-1] == 0 {
				name = name[:len(name)-1]
			}
			w.mu.Lock()
			dir, known := w.wds[ev.Wd]
			if ev.Mask&(syscall.IN_IGNORED|syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF) != 0 && known {
				delete(w.wds, ev.Wd)
				delete(w.dirs, dir)
			}
			w.mu.Unlock()
			switch {
			case ev.Mask&syscall.IN_Q_OVERFLOW != 0, !known,
				ev.Mask&(syscall.IN_IGNORED|syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF) != 0:
				// Changes were lost, or a directory went: nothing cached
				// can be trusted.
				w.onChange("")
			case name != "":
				w.onChange(path.Join(dir, name))
			}
		}
	}
}
