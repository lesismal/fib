// Package poolsize sizes fib's worker pools from the machine they run on:
// the CPUs the process may use, the memory it may use, and how many
// processes share that memory.
//
// A pool's ceiling is what it may grow to under a load that blocks its
// workers, such as handlers waiting on a database or handshakes waiting on
// their peers. Every pool grows only as its load asks and retires idle
// workers, so a ceiling costs nothing until it is reached; what it bounds is
// how much memory a pool can come to hold. Each pool's ceiling is the
// smaller of two: what its work calls for, mostly a number per CPU, and what
// its share of the process's memory budget pays for at an estimated cost per
// worker. A machine whose memory cannot be read has no second bound.
package poolsize

import (
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/lesismal/fib/internal/sys"
)

// Machine is what the pools are sized against.
type Machine struct {
	// CPUs is min(runtime.NumCPU, runtime.GOMAXPROCS): a cgroup's CPU quota
	// counts, since GOMAXPROCS follows it, and so does a prefork child's
	// share.
	CPUs int
	// Memory is the machine's physical memory, or the memory limit of the
	// cgroup the process runs in where that is lower; zero when neither can
	// be read.
	Memory int64
	// Processes is how many processes share Memory: the children of fib's
	// prefork, which all run in the one cgroup, or 1.
	Processes int
}

// preforkChildrenEnv is the variable fib's prefork sets in each child to the
// number of children; see package prefork.
const preforkChildrenEnv = "FIB_PREFORK_CHILDREN"

// Detect reports the machine this process runs on. It is read once.
func Detect() Machine { return detect() }

var detect = sync.OnceValue(func() Machine {
	m := Machine{
		CPUs:      max(1, min(runtime.NumCPU(), runtime.GOMAXPROCS(0))),
		Memory:    max(0, sys.MemoryLimit()),
		Processes: 1,
	}
	if strconv.IntSize == 32 && m.Memory > 2<<30 {
		// A 32-bit process cannot address much more than this.
		m.Memory = 2 << 30
	}
	if n, err := strconv.Atoi(os.Getenv(preforkChildrenEnv)); err == nil && n > 1 {
		m.Processes = n
	}
	return m
})

// Budget is the memory every pool of one process together may come to hold
// at their ceilings: half of the machine's memory, divided among the
// processes sharing it. Zero when the memory is unknown.
func (m Machine) Budget() int64 {
	if m.Memory <= 0 {
		return 0
	}
	return m.Memory / 2 / int64(max(1, m.Processes))
}

// Pool is one kind of pool: its share of the budget, in percent, what one
// of its workers is estimated to hold while it works, and the fewest workers
// per CPU it keeps whatever the memory says. The costs are estimates, to be
// replaced by measurements: a goroutine's stack and the buffers and state a
// task of that kind holds while it waits.
type Pool struct {
	Name     string
	Share    int
	Cost     int64
	FloorCPU int
}

// The pools fib sizes. Their shares add up to 100.
var (
	// Engine is the pool an engine reads its connections on, and runs
	// handlers that answer in place.
	Engine = Pool{Name: "engine", Share: 30, Cost: 64 << 10, FloorCPU: 64}
	// Streams is the pool HTTP/2 and HTTP/3 run their handlers on.
	Streams = Pool{Name: "streams", Share: 30, Cost: 64 << 10, FloorCPU: 64}
	// Handshake is fib-tls-handshake: a crypto/tls handshake holds its
	// connection's state and record buffers while it waits on the peer.
	Handshake = Pool{Name: "fib-tls-handshake", Share: 15, Cost: 64 << 10, FloorCPU: 64}
	// Client is fib-client: DNS lookups and the callbacks of clients.
	Client = Pool{Name: "fib-client", Share: 10, Cost: 32 << 10, FloorCPU: 32}
	// File is fib-file: each read holds a piece of a file.
	File = Pool{Name: "fib-file", Share: 5, Cost: 256 << 10, FloorCPU: 4}
	// Flush is fib-flush: a write of what a connection queued.
	Flush = Pool{Name: "fib-flush", Share: 5, Cost: 16 << 10, FloorCPU: 16}
	// Notify is fib-notify: the callbacks of sidepool.Post.
	Notify = Pool{Name: "fib-notify", Share: 5, Cost: 16 << 10, FloorCPU: 16}
)

// Ceiling is the ceiling p takes on m when its work calls for want workers:
// want, held under what p's share of the budget pays for, but not below
// FloorCPU workers per CPU, nor above want.
func (p Pool) Ceiling(m Machine, want int) int {
	if want <= 0 {
		return want
	}
	budget := m.Budget() * int64(p.Share) / 100
	if budget <= 0 || p.Cost <= 0 {
		return want
	}
	afford := budget / p.Cost
	if afford >= int64(want) {
		return want
	}
	return min(want, max(int(afford), p.FloorCPU*max(1, m.CPUs)))
}
