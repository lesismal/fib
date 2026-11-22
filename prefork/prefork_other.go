//go:build !linux

package prefork

import (
	"errors"
	"syscall"
)

// supported is false: elsewhere SO_REUSEPORT does not spread connections
// between processes, or there is no fork, and Run serves in this process.
const supported = false

func sysProcAttr() *syscall.SysProcAttr { return nil }

// planPlacement and placeThread are never called here, where Run serves in
// this process and starts no children.
func planPlacement(Placement, int) ([]slot, error) { return nil, errors.ErrUnsupported }

func placeThread(slot, bool) (func(), error) { return nil, errors.ErrUnsupported }
