package fib

import (
	"strconv"
	"sync"

	"github.com/lesismal/fib/internal/sys"
)

// Machine is the hardware the defaults are sized against: the cores the
// process may run on, the memory it may use, and the descriptors it may hold
// open. A field that could not be read is zero, and the sizing falls back to
// its conservative constants rather than guessing.
type Machine struct {
	// CPUs is min(runtime.NumCPU, runtime.GOMAXPROCS), so a cgroup's CPU
	// quota counts and a prefork child's share does too.
	CPUs int
	// Memory is the machine's physical memory, or the memory limit of the
	// cgroup the process runs in where that is lower. It is what the
	// server-wide budget and the per-connection watermark are sized from.
	Memory int64
	// FDs is the soft RLIMIT_NOFILE, which bounds the connections a process
	// can hold; zero on a platform that has none.
	FDs int
}

// DetectMachine reports the machine this process runs on. It is read once:
// the cgroup limit and the descriptors' limit do not move while a server
// runs.
func DetectMachine() Machine { return detectMachine() }

var detectMachine = sync.OnceValue(func() Machine {
	m := Machine{CPUs: defaultCPUs(), Memory: sys.MemoryLimit(), FDs: sys.FDLimit()}
	if m.CPUs < 1 {
		m.CPUs = 1
	}
	if m.Memory < 0 {
		m.Memory = 0
	}
	if m.FDs < 0 {
		m.FDs = 0
	}
	// A 32-bit process cannot address much more than this, whatever the
	// machine has.
	if strconv.IntSize == 32 && m.Memory > 2<<30 {
		m.Memory = 2 << 30
	}
	return m
})

// Sizing is the part of Config the machine sizes: the per-connection write
// watermark, and, when a server-wide budget is chosen, the bytes that budget
// may hold and the hard cap on one connection's queue.
//
// The read buffer and the protocol windows are not sized here. The read
// buffer's memory is O(event loops), not O(connections), and it is a
// throughput choice; the windows are a network path's, not the machine's.
type Sizing struct {
	// WriteWatermark is Config.WriteBufferHighWatermark.
	WriteWatermark int
	// MaxPendingBytes is Config.MaxPendingBytes: the budget passed in, or
	// zero when none was. A zero budget is not turned on by sizing.
	MaxPendingBytes int64
	// MaxWriteQueueBytes is Config.MaxWriteQueueBytes: a per-connection cap
	// derived from the budget, or zero when there is no budget. fib leaves it
	// off by default; a deployment that wants a peer held to its own share of
	// the budget applies it.
	MaxWriteQueueBytes int64
}

// Sizing reports the sizing for this machine. readBuffer is the
// Config.ReadBufferSize the engine will use; pendingBytes is the
// Config.MaxPendingBytes chosen, or zero for none; maxConns is the
// Config.MaxConns chosen, or zero for no cap. Those two feed the watermark:
// it is held to a quarter of the reference connections being fully
// backlogged before the budget is reached.
func (m Machine) Sizing(readBuffer int, pendingBytes int64, maxConns int) Sizing {
	if readBuffer <= 0 {
		readBuffer = defaultReadBufferSize
	}
	s := Sizing{}
	if pendingBytes > 0 {
		s.MaxPendingBytes = pendingBytes
	}
	if m.Memory <= 0 && s.MaxPendingBytes <= 0 {
		// Nothing says how much memory there is. Keep the conservative
		// watermark rather than inventing a budget.
		s.WriteWatermark = sizingWatermarkMin
		return s
	}

	budget := s.MaxPendingBytes
	if budget <= 0 {
		budget = m.Memory * sizingMemoryShareNum / sizingMemoryShareDen
	}

	// How many connections the budget can hold at the per-connection baseline,
	// and how many descriptors allow. The smaller is the reference: fewer
	// connections can afford a larger watermark each.
	cmem := int64(sizingRefConns)
	if budget > 0 {
		cmem = budget / sizingConnBaseline
	}
	cfd := int64(sizingRefConns)
	if m.FDs > 0 {
		cfd = int64(m.FDs) * 4 / 5
	}
	cref := min(cmem, cfd)
	switch {
	case maxConns > 0:
		cref = min(cref, int64(maxConns))
	case cref > sizingRefConns:
		// With no cap of its own, fib sizes against a reference rather than
		// an unbounded count. A deployment with more connections sets
		// Config.MaxConns, and the watermark follows it down.
		cref = sizingRefConns
	}
	if cref < 100 {
		// A tiny or unreadable count must not make the divide below wild.
		cref = 100
	}

	w := int64(readBuffer) * 4
	if budget > 0 {
		// A quarter of the reference connections fully backlogged is the
		// budget: 4*budget/cref, since 1/0.25 is 4.
		w = min(w, 4*budget/cref)
	}
	w = clampInt64(w, sizingWatermarkMin, sizingWatermarkMax)
	w = floorInt64(w, sizingWatermarkStep)
	s.WriteWatermark = int(w)

	if budget > 0 {
		q := budget / 8
		q = clampInt64(q, sizingQueueMin, sizingQueueMax)
		// One connection may not hold more than half of a small budget.
		q = min(q, budget/2)
		s.MaxWriteQueueBytes = floorInt64(q, 1<<20)
	}
	return s
}

// DefaultSizing reports the sizing for the machine this process runs on; see
// Machine.Sizing.
func DefaultSizing(readBuffer int, pendingBytes int64, maxConns int) Sizing {
	return DetectMachine().Sizing(readBuffer, pendingBytes, maxConns)
}

// DefaultWriteWatermark reports the per-connection write watermark for
// readBuffer and the deployment's budget and connection cap. It is what
// Config.WriteBufferHighWatermark is when that is left at zero.
func DefaultWriteWatermark(readBuffer int, pendingBytes int64, maxConns int) int {
	return DefaultSizing(readBuffer, pendingBytes, maxConns).WriteWatermark
}

const (
	// sizingMemoryShare is the share of the machine's memory the sizing
	// treats as the output budget when the deployment does not set one.
	// Config.MaxPendingBytes still defaults to off; this is the envelope the
	// watermark is sized against.
	sizingMemoryShareNum = 1
	sizingMemoryShareDen = 2
	// sizingConnBaseline is the per-connection share of that budget used to
	// estimate how many connections it can hold. It is deliberately small:
	// idle connections cost far less, and under-estimating the count makes
	// the watermark conservative.
	sizingConnBaseline = 16 << 10
	// sizingRefConns is the connection count the watermark is sized against
	// when Config.MaxConns leaves it unset. It matches the defaults a gateway
	// commonly runs with; a deployment that sets MaxConns gets its own count.
	sizingRefConns = 100_000
	// sizingWatermarkMin and sizingWatermarkMax bound the watermark. The
	// minimum is fib's long-standing default; the maximum is the round size a
	// long read flushes at, past which a larger watermark buys no larger
	// writes.
	sizingWatermarkMin  = 64 << 10
	sizingWatermarkMax  = 256 << 10
	sizingWatermarkStep = 16 << 10
	// sizingQueueMin and sizingQueueMax bound the per-connection hard cap.
	// The maximum leaves room for HTTP/2's 250 streams at 64 KiB each, and the
	// minimum keeps the cap useful on a small budget.
	sizingQueueMin = 8 << 20
	sizingQueueMax = 32 << 20
)

func clampInt64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func floorInt64(v, step int64) int64 {
	if step <= 0 {
		return v
	}
	return v / step * step
}
