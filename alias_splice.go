//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// This file re-exports the public API of package core, where the implementation
// lives. alias_test.go fails when the two disagree: add a re-export here
// whenever core's public API changes.

package fib

import "github.com/lesismal/fib/core"

// Splice is what Connection.SpliceTo moves, and who it tells how that went.
type Splice = core.Splice

// ErrSpliceAborted is what a splice whose Begin gave it up reports.
var ErrSpliceAborted = core.ErrSpliceAborted

// ErrSpliceBusy is what SpliceTo reports when either connection is in a
// splice already, or the engine has as many pipes open as it allows.
var ErrSpliceBusy = core.ErrSpliceBusy

// ErrSpliceUnsupported is what SpliceTo reports for a pair of connections
// it cannot splice between: on a system without splice(2), or where
// either side has a layer, such as TLS, that its bytes have to pass
// through, or is UDP.
var ErrSpliceUnsupported = core.ErrSpliceUnsupported
