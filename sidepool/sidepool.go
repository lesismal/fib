// Package sidepool holds the pools that run what goes on beside an engine's
// connections rather than in them: the work of the clients built on an
// engine — resolving the host a dial names, and handing a request, a dial or
// a WebSocket handshake its outcome — TLS handshakes, and the disk reads of
// a file a response sends without waiting. None of it may run
// on the goroutine that has it to do, which is an event loop or a worker
// holding a lock more often than not, and none of it gets a goroutine of its
// own, short of a notification Post finds no room for (see Post).
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

	"github.com/lesismal/fib/internal/poolsize"
	"github.com/lesismal/fib/taskpool"
)

// What each pool's work calls for, before the machine's memory has its say;
// see DefaultSizing.
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
	// fileWorkers is the file pool's ceiling. What it runs reads a piece of
	// a file from the disk and hands it to a response, so it bounds how many
	// reads wait on the disk at once, which more workers would not make
	// faster.
	fileWorkers = 256
	// flushWorkers is the flush pool's ceiling. What it runs writes what a
	// connection has queued, one non-blocking write, and returns.
	flushWorkers = 4096
	// queueSize bounds what waits for a worker in each pool but the notify
	// pool (see notifyQueue), once it is at its ceiling. A submission to the
	// client pool that finds the queue full waits for room; package tls
	// submits to the handshake pool with TryGo, and closes the connection
	// instead, since it submits from an event loop.
	queueSize = 10000
)

// PoolSize is a pool's worker ceiling and queue size. A zero field is fib's
// default for it.
type PoolSize struct {
	Workers int
	Queue   int
}

// Sizing is the size of every pool of this package. A zero field is fib's
// default for it; see DefaultSizing.
type Sizing struct {
	Client    PoolSize
	Handshake PoolSize
	File      PoolSize
	Flush     PoolSize
	Notify    PoolSize
}

// DefaultSizing reports the sizes fib gives the pools on this machine: what
// each pool's work calls for, held under what its share of the process's
// memory pays for at an estimated cost a worker (see internal/poolsize), on
// a machine whose memory can be read. Prefork children share the memory.
func DefaultSizing() Sizing {
	m := poolsize.Detect()
	return Sizing{
		Client:    PoolSize{poolsize.Client.Ceiling(m, clientWorkers), queueSize},
		Handshake: PoolSize{poolsize.Handshake.Ceiling(m, handshakeWorkers), queueSize},
		File:      PoolSize{poolsize.File.Ceiling(m, fileWorkers), queueSize},
		Flush:     PoolSize{poolsize.Flush.Ceiling(m, flushWorkers), queueSize},
		Notify:    PoolSize{poolsize.Notify.Ceiling(m, notifyWorkers), notifyQueue},
	}
}

// configured is what Configure set, and its lock, which building a pool
// takes too.
var (
	configMu   sync.Mutex
	configured Sizing
)

// Configure sets the sizes of the pools not built yet; a zero field leaves
// fib's default. The pools are the process's, shared by every engine, and
// built the first time they are used, so the sizes apply to the pools built
// from now on: it reports the names of those already built with other sizes,
// which keep theirs. fib.New calls it with Config.SidePools.
func Configure(s Sizing) (kept []string) {
	configMu.Lock()
	defer configMu.Unlock()
	for _, p := range pools {
		want := p.of(&s)
		if want.Workers == 0 && want.Queue == 0 {
			continue
		}
		cur := p.of(&configured)
		if want.Workers != 0 {
			cur.Workers = want.Workers
		}
		if want.Queue != 0 {
			cur.Queue = want.Queue
		}
		if p.lazy.built != (PoolSize{}) && p.lazy.built != p.sizeLocked() {
			kept = append(kept, p.name)
		}
	}
	return kept
}

// Current reports the size of each pool: what it was built with, or what it
// will be built with.
func Current() Sizing {
	configMu.Lock()
	defer configMu.Unlock()
	var s Sizing
	for _, p := range pools {
		size := p.lazy.built
		if size == (PoolSize{}) {
			size = p.sizeLocked()
		}
		*p.of(&s) = size
	}
	return s
}

// NamedSize is one pool's size with the pool's name.
type NamedSize struct {
	Name string
	PoolSize
}

// Pools lists s pool by pool, in the order the pools are declared.
func (s Sizing) Pools() []NamedSize {
	out := make([]NamedSize, len(pools))
	for i, p := range pools {
		out[i] = NamedSize{p.name, *p.of(&s)}
	}
	return out
}

// Custom reports which pools' sizes differ from DefaultSizing, by name.
func Custom() []string {
	cur, def := Current(), DefaultSizing()
	var names []string
	for _, p := range pools {
		if *p.of(&cur) != *p.of(&def) {
			names = append(names, p.name)
		}
	}
	return names
}

// pool is one of the package's pools: its name, where its size sits in a
// Sizing, and the lazily built pool itself.
type pool struct {
	name string
	of   func(*Sizing) *PoolSize
	lazy *lazyPool
}

var client, handshake, file, flush lazyPool

var pools = []pool{
	{"fib-client", func(s *Sizing) *PoolSize { return &s.Client }, &client},
	{"fib-tls-handshake", func(s *Sizing) *PoolSize { return &s.Handshake }, &handshake},
	{"fib-file", func(s *Sizing) *PoolSize { return &s.File }, &file},
	{"fib-flush", func(s *Sizing) *PoolSize { return &s.Flush }, &flush},
	{"fib-notify", func(s *Sizing) *PoolSize { return &s.Notify }, &notify},
}

// sizeLocked is the size p is built with now: what Configure set, fib's
// default where it set nothing.
func (p pool) sizeLocked() PoolSize {
	size := *p.of(&configured)
	def := DefaultSizing()
	if size.Workers == 0 {
		size.Workers = p.of(&def).Workers
	}
	if size.Queue == 0 {
		size.Queue = p.of(&def).Queue
	}
	return size
}

// Client returns the pool the clients built on an engine run their
// asynchronous work on: DNS lookups for a dial, and the callbacks that hand a
// dial, a request or a WebSocket handshake its outcome.
func Client() *taskpool.TaskPool { return pools[0].get() }

// Handshake returns the pool TLS handshakes run on.
func Handshake() *taskpool.TaskPool { return pools[1].get() }

// File returns the pool the pieces of a file a response sends without
// waiting are read from the disk on; see http.Context.SendFile.
func File() *taskpool.TaskPool { return pools[2].get() }

// Flush returns the pool that writes out what a connection has queued, for
// a protocol that corks what it sends and has it written later, such as
// HTTP/2 serving its responses. What it runs only writes to a socket, and
// submits nothing to any pool, so any goroutine may wait to submit to it,
// a worker of any other pool included.
func Flush() *taskpool.TaskPool { return pools[3].get() }

// Go runs fn on the client pool. It waits for room when the pool is full, so
// it must not be called from a task of the client pool itself, nor from
// anything the client pool may wait on; such a caller uses
// fib.Engine.Hand or fib.Connection.Hand instead.
func Go(fn func()) { Client().Go(fn) }

type lazyPool struct {
	once sync.Once
	pool *taskpool.TaskPool
	// built is the size the pool was built with, under configMu; zero until
	// it is.
	built PoolSize
}

// get returns p's pool, building it the first time.
func (p pool) get() *taskpool.TaskPool {
	l := p.lazy
	l.once.Do(func() {
		configMu.Lock()
		size := p.sizeLocked()
		l.built = size
		configMu.Unlock()
		l.pool = taskpool.NewAdaptive(taskpool.AdaptiveConfig{
			Name: p.name, MaxWorkers: size.Workers, QueueSize: size.Queue,
		})
	})
	return l.pool
}
