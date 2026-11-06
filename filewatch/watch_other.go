//go:build !linux

package filewatch

// Watcher reports changes under a root; only Linux has one, and elsewhere
// New returns ErrUnsupported. A nil *Watcher watches nothing.
type Watcher struct{}

// New returns ErrUnsupported: this system offers no watch.
func New(string, func(Event)) (*Watcher, error) { return nil, ErrUnsupported }

// Watch reports false: nothing is watched here.
func (*Watcher) Watch(string) bool { return false }

// Close does nothing.
func (*Watcher) Close() error { return nil }
