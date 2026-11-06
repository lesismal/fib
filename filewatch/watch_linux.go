//go:build linux

package filewatch

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

// mask is every change to a directory's entries, or to a file's contents or
// metadata, that may make what a cache holds of it stale, and the directory
// itself going. IN_ONLYDIR makes a watch of a path that is no directory fail.
const mask = syscall.IN_CREATE | syscall.IN_DELETE | syscall.IN_MODIFY | syscall.IN_CLOSE_WRITE |
	syscall.IN_MOVED_FROM | syscall.IN_MOVED_TO | syscall.IN_ATTRIB | syscall.IN_DELETE_SELF |
	syscall.IN_MOVE_SELF | syscall.IN_ONLYDIR

// maxLinks bounds the symbolic links followed on the way to the root, as
// the kernel's own bound on a path's links does.
const maxLinks = 255

// Watcher reports changes to the entries of directories under a root,
// through inotify: a directory is watched once Watch is asked for it, and
// stays watched until it goes or the Watcher is closed. Each change is
// reported as an Event to the function New was given, on a goroutine of the
// Watcher's own, within moments of the change; the function must not call
// Close. A Watcher is safe for concurrent use, and a nil *Watcher watches
// nothing.
//
// Watching a directory watches every directory on its way from the root, so
// that one of them renamed or removed is reported, as a Dir event, as well
// as its entries changing. The directories that symbolic links on the way to
// the root are in are watched too, so that a link swapped for another, as a
// deploy that points a "current" link at a new release does, is reported as
// All. A symbolic link under the root to a file elsewhere is followed by the
// system, but a change to the file it leads to is not seen; nor is a change
// another system makes on a network file system.
type Watcher struct {
	root    string
	file    *os.File
	onEvent func(Event)
	done    chan struct{}

	mu     sync.Mutex
	closed bool
	// dirs are the watched directories under the root, by slash-separated
	// path relative to it, with their watch descriptors.
	dirs  map[string]int32
	nodes map[int32]*node
}

// node is what one watch descriptor stands for: directories under the root,
// several when links lead to one by more than one path, and the names in it
// of symbolic links on the way to the root.
type node struct {
	rels  []string
	links map[string]bool
}

// New returns a Watcher of the directories under root, reporting each change
// to onEvent. It watches nothing under root until Watch is called.
func New(root string, onEvent func(Event)) (*Watcher, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		return nil, os.NewSyscallError("inotify_init1", err)
	}
	// Non-blocking, so that the runtime's poller waits for its events
	// rather than a thread.
	w := &Watcher{root: abs, file: os.NewFile(uintptr(fd), "inotify"), onEvent: onEvent,
		done: make(chan struct{}), dirs: make(map[string]int32), nodes: make(map[int32]*node)}
	if err := w.watchLinksLocked(abs); err != nil {
		_ = w.file.Close()
		return nil, err
	}
	go w.run()
	return w, nil
}

// Watch watches dir, a slash-separated path relative to the root, "" or "."
// for the root itself, and every directory on its way from the root. It
// reports whether a change to dir's entries, or to what dir is, will be
// reported: true too when dir, or a directory on its way, does not exist or
// is no directory, since its coming is reported by the directory above it;
// false when a watch could not be had, the kernel's limit reached, or the
// Watcher is closed.
func (w *Watcher) Watch(dir string) bool {
	if w == nil {
		return false
	}
	dir = path.Clean("/" + dir)[1:]
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	if _, ok := w.dirs[dir]; ok {
		return true
	}
	rel, rest := "", dir
	for {
		if _, ok := w.dirs[rel]; !ok {
			wd, err := w.addLocked(filepath.Join(w.root, filepath.FromSlash(rel)))
			if err != nil {
				return rel != "" && (errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR))
			}
			w.dirs[rel] = wd
			w.nodes[wd].rels = append(w.nodes[wd].rels, rel)
		}
		if rest == "" {
			return true
		}
		var segment string
		segment, rest, _ = strings.Cut(rest, "/")
		rel = path.Join(rel, segment)
	}
}

// Close stops the watch and waits until no event is being reported. It must
// not be called from the function New was given.
func (w *Watcher) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		<-w.done
		return nil
	}
	w.closed = true
	err := w.file.Close()
	w.mu.Unlock()
	<-w.done
	return err
}

// addLocked watches the directory at full, and returns its node's
// descriptor.
func (w *Watcher) addLocked(full string) (int32, error) {
	var wd int
	err := w.control(func(fd int) error {
		var err error
		wd, err = syscall.InotifyAddWatch(fd, full, mask)
		return err
	})
	if err != nil {
		return 0, err
	}
	if w.nodes[int32(wd)] == nil {
		w.nodes[int32(wd)] = &node{}
	}
	return int32(wd), nil
}

// watchLinksLocked watches the directory of each symbolic link met on the
// way to full, as the system follows them.
func (w *Watcher) watchLinksLocked(full string) error {
	done, rest := "/", strings.TrimPrefix(full, "/")
	for hops := 0; rest != ""; {
		var segment string
		segment, rest, _ = strings.Cut(rest, "/")
		switch segment {
		case "", ".":
			continue
		case "..":
			done = filepath.Dir(done)
			continue
		}
		next := filepath.Join(done, segment)
		info, err := os.Lstat(next)
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			done = next
			continue
		}
		if hops++; hops > maxLinks {
			return &os.PathError{Op: "watch", Path: full, Err: syscall.ELOOP}
		}
		wd, err := w.addLocked(done)
		if err != nil {
			return err
		}
		n := w.nodes[wd]
		if n.links == nil {
			n.links = make(map[string]bool)
		}
		n.links[segment] = true
		target, err := os.Readlink(next)
		if err != nil {
			return err
		}
		if strings.HasPrefix(target, "/") {
			done = "/"
		}
		rest = target + "/" + rest
	}
	return nil
}

// dropLocked stops watching rel and the directories under it, which are not
// where they were, or not there at all.
func (w *Watcher) dropLocked(rel string) {
	for dir, wd := range w.dirs {
		if dir != rel && rel != "" && !strings.HasPrefix(dir, rel+"/") {
			continue
		}
		delete(w.dirs, dir)
		n := w.nodes[wd]
		if n == nil {
			continue
		}
		n.rels = slices.DeleteFunc(n.rels, func(r string) bool { return r == dir })
		if len(n.rels) == 0 && len(n.links) == 0 {
			delete(w.nodes, wd)
			_ = w.control(func(fd int) error {
				_, err := syscall.InotifyRmWatch(fd, uint32(wd))
				return err
			})
		}
	}
}

// control runs f on the inotify descriptor.
func (w *Watcher) control(f func(fd int) error) error {
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

// run reads the watch's events until it is closed, and reports each.
func (w *Watcher) run() {
	defer close(w.done)
	buf := make([]byte, 64<<10)
	var events []Event
	for {
		n, err := w.file.Read(buf)
		if err != nil {
			return
		}
		events = w.parse(buf[:n], events[:0])
		for _, ev := range events {
			w.onEvent(ev)
		}
	}
}

// selfMask is the events that say a watched directory itself went: removed,
// moved, its file system unmounted, or its watch dropped.
const selfMask = syscall.IN_DELETE_SELF | syscall.IN_MOVE_SELF | syscall.IN_UNMOUNT | syscall.IN_IGNORED

// parse turns the inotify events in buf into Events, appended to events,
// and keeps the watches as they say.
func (w *Watcher) parse(buf []byte, events []Event) []Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	all := func() {
		if len(events) == 0 || !events[len(events)-1].All {
			events = append(events, Event{All: true})
		}
	}
	for at := 0; at+syscall.SizeofInotifyEvent <= len(buf); {
		ev := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[at]))
		end := at + syscall.SizeofInotifyEvent + int(ev.Len)
		if end > len(buf) {
			break
		}
		name := strings.TrimRight(string(buf[at+syscall.SizeofInotifyEvent:end]), "\x00")
		at = end
		if ev.Mask&syscall.IN_Q_OVERFLOW != 0 {
			all()
			continue
		}
		n := w.nodes[ev.Wd]
		if n == nil {
			// A watch already dropped.
			continue
		}
		if ev.Mask&selfMask != 0 || name == "" {
			// The directory itself: gone, or its metadata changed.
			gone := ev.Mask&selfMask != 0
			relink := gone && len(n.links) > 0
			for _, rel := range slices.Clone(n.rels) {
				if rel == "" {
					all()
				} else {
					events = append(events, Event{Name: rel, Dir: true})
				}
				if gone {
					w.dropLocked(rel)
				}
			}
			if ev.Mask&syscall.IN_IGNORED != 0 {
				delete(w.nodes, ev.Wd)
			}
			if relink {
				// The directory of a link on the way to the root went:
				// the root may be another tree, its links elsewhere.
				all()
				n.links = nil
				w.dropLocked("")
				_ = w.watchLinksLocked(w.root)
			}
			continue
		}
		if n.links[name] {
			// A link on the way to the root: what is under the root may
			// be another tree now.
			all()
			w.dropLocked("")
			_ = w.watchLinksLocked(w.root)
		}
		for _, rel := range slices.Clone(n.rels) {
			full := path.Join(rel, name)
			_, watched := w.dirs[full]
			dir := ev.Mask&syscall.IN_ISDIR != 0 || watched
			if dir {
				w.dropLocked(full)
			}
			events = append(events, Event{Name: full, Dir: dir})
		}
	}
	return events
}
