// Package sidepool holds the pools that run what goes on beside an engine's
// connections rather than in them: the work of the clients built on an
// engine — resolving the host a dial names, and handing a request, a dial or
// a WebSocket handshake its outcome — and TLS handshakes. None of it may run
// on the goroutine that has it to do, which is an event loop or a worker
// holding a lock more often than not, and none of it gets a goroutine of its
// own.
//
// Each pool is its own, apart from every engine's, so that neither ever
// waits on the other's queue: a worker of an engine may hand a handshake to
// the handshake pool, and a handshake is fed by the engine's loop, never by
// a worker waiting in a submission to the handshake pool. The pools are built
// the first time they are used, shared by every engine in the process, and
// never stopped; with no floor, one that is idle keeps no worker.
package sidepool

import (
	"sync"

	"github.com/lesismal/fib/taskpool"
)

const (
	// clientWorkers is the client pool's ceiling. What it runs is short —
	// a callback that hands a result on — or waits on DNS, which a burst of
	// dials to named hosts can have hundreds of at once.
	clientWorkers = 4096
	// handshakeWorkers is the handshake pool's ceiling. crypto/tls runs a
	// handshake as one blocking call, so a worker waits on its peer for as
	// long as the handshake takes, a round trip or two, or until
	// HandshakeTimeout for a peer that stalls. The ceiling is set high enough
	// that stalled peers do not keep new ones waiting, as they could not
	// when each handshake had a goroutine of its own; the pool retires the
	// workers again once the handshakes are over.
	handshakeWorkers = 1 << 16
	// queueSize bounds what waits for a worker in either pool, once it is at
	// its ceiling. A submission that finds the queue full waits for room.
	queueSize = 10000
)

var client, handshake lazyPool

// Client returns the pool the clients built on an engine run their
// asynchronous work on: DNS lookups for a dial, and the callbacks that hand a
// dial, a request or a WebSocket handshake its outcome.
func Client() *taskpool.TaskPool { return client.get("fib-client", clientWorkers) }

// Handshake returns the pool TLS handshakes run on.
func Handshake() *taskpool.TaskPool { return handshake.get("fib-tls-handshake", handshakeWorkers) }

// Go runs fn on the client pool.
func Go(fn func()) { Client().Go(fn) }

type lazyPool struct {
	once sync.Once
	pool *taskpool.TaskPool
}

func (l *lazyPool) get(name string, workers int) *taskpool.TaskPool {
	l.once.Do(func() {
		l.pool = taskpool.NewAdaptive(taskpool.AdaptiveConfig{
			Name: name, MaxWorkers: workers, QueueSize: queueSize,
		})
	})
	return l.pool
}
