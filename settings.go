package fib

import (
	"fmt"
	"strings"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// Settings is a snapshot of an engine's effective configuration: the values
// the engine actually runs with, after the defaults and the machine sizing
// have filled in what the caller left out. It is what the engine's startup log
// prints with Config.LogStatus and what Engine.Settings returns, so that a
// deployment can be read back from its log rather than inferred from its
// config file.
type Settings struct {
	Name                     string
	Network                  string
	Backlog                  int
	WorkerCount              int
	MaxEvents                int
	PollBatchSize            int
	ReadBufferSize           int
	ReadBufferStartSize      int
	WriteBufferHighWatermark int
	MaxPendingBytes          int64
	MaxWriteQueueBytes       int64
	MaxConns                 int
	UseWritev                bool
	CoalesceWrites           bool
	SocketSyscalls           bool
	TaskPool                 string
	TaskPoolMode             taskpool.Mode
	MinWorkerCount           int
	SharedTaskPool           bool
	IOPollers                bool
	IOPollerCount            int
	Pollers                  int
	EngineLoop               bool
	ReusePort                bool
	BalanceReusePort         bool
	AcceptPlacement          string
	UDPIdleTimeout           time.Duration
	LogStatus                bool
	LogSettings              bool

	// custom records which fields the caller set rather than leaving for
	// fib's defaults. Parameters reports it.
	custom map[string]bool
}

// Settings returns the configuration the engine runs with. It is a snapshot:
// the engine's sizing is fixed when it is made.
func (e *Engine) Settings() Settings {
	c := e.bindConfig
	network := c.Network
	if network == "" {
		network = "tcp"
	}
	pool := fmt.Sprintf("%T", e.taskPool)
	if named, ok := e.taskPool.(interface{ Name() string }); ok {
		pool = named.Name()
	}
	return Settings{
		Name:                     e.name,
		Network:                  network,
		Backlog:                  c.Backlog,
		WorkerCount:              c.WorkerCount,
		MaxEvents:                c.MaxEvents,
		PollBatchSize:            c.PollBatchSize,
		ReadBufferSize:           c.ReadBufferSize,
		ReadBufferStartSize:      c.ReadBufferStartSize,
		WriteBufferHighWatermark: c.WriteBufferHighWatermark,
		MaxPendingBytes:          c.MaxPendingBytes,
		MaxWriteQueueBytes:       c.MaxWriteQueueBytes,
		MaxConns:                 c.MaxConns,
		UseWritev:                c.UseWritev,
		CoalesceWrites:           c.CoalesceWrites,
		SocketSyscalls:           c.SocketSyscalls,
		TaskPool:                 pool,
		TaskPoolMode:             c.TaskPoolMode,
		MinWorkerCount:           c.MinWorkerCount,
		SharedTaskPool:           c.SharedTaskPool,
		IOPollers:                c.IOPollers,
		IOPollerCount:            c.IOPollerCount,
		Pollers:                  len(e.pollers),
		EngineLoop:               c.EngineLoop,
		ReusePort:                c.ReusePort,
		BalanceReusePort:         c.BalanceReusePort,
		AcceptPlacement:          c.AcceptPlacement.String(),
		UDPIdleTimeout:           udpIdleTimeout(c.UDPIdleTimeout),
		LogStatus:                c.LogStatus,
		LogSettings:              c.LogSettings,
		custom:                   e.configCustom,
	}
}

// String formats the settings as one line of key=value pairs, in the order
// the fields are declared, so that a log line is both readable and easy to
// grep. Byte counts are decimal integers and durations use Go's spelling.
func (s Settings) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "name=%s network=%s backlog=%d workers=%d maxEvents=%d pollBatch=%d ",
		s.Name, s.Network, s.Backlog, s.WorkerCount, s.MaxEvents, s.PollBatchSize)
	fmt.Fprintf(&b, "readBuffer=%d readBufferStart=%d writeWatermark=%d writeLowWatermark=%d maxPendingBytes=%d maxWriteQueueBytes=%d maxConns=%d ",
		s.ReadBufferSize, s.ReadBufferStartSize, s.WriteBufferHighWatermark, s.WriteBufferHighWatermark/4, s.MaxPendingBytes, s.MaxWriteQueueBytes, s.MaxConns)
	fmt.Fprintf(&b, "taskPool=%s taskPoolMode=%s minWorkers=%d sharedTaskPool=%t ",
		s.TaskPool, s.TaskPoolMode, s.MinWorkerCount, s.SharedTaskPool)
	fmt.Fprintf(&b, "ioPollers=%t ioPollerCount=%d pollers=%d engineLoop=%t reusePort=%t balanceReusePort=%t ",
		s.IOPollers, s.IOPollerCount, s.Pollers, s.EngineLoop, s.ReusePort, s.BalanceReusePort)
	fmt.Fprintf(&b, "accept=%s coalesceWrites=%t useWritev=%t socketSyscalls=%t udpIdleTimeout=%s logStatus=%t logSettings=%t",
		s.AcceptPlacement, s.CoalesceWrites, s.UseWritev, s.SocketSyscalls, s.UDPIdleTimeout, s.LogStatus, s.LogSettings)
	return b.String()
}

// Parameter is one setting as the startup log reports it: its name, its
// effective value, whether the caller set it or fib's default produced it,
// and what it does.
type Parameter struct {
	Name   string
	Value  string
	Source string // "default" or "custom"
	About  string
}

// Parameters reports every basic setting the engine runs with, in the order
// the startup log prints them. A setting is custom when the caller gave it a
// value that differs from what fib would have used, either explicitly or
// through a helper such as DefaultConfigFor; a value fib sized from the
// machine is default.
func (s Settings) Parameters() []Parameter {
	source := func(name string) string {
		if s.custom[name] {
			return "custom"
		}
		return "default"
	}
	low := "0"
	if s.WriteBufferHighWatermark > 0 {
		low = byteSize(int64(s.WriteBufferHighWatermark) / 4)
	}
	return []Parameter{
		{"name", s.Name, source("name"), "engine name; names its worker and stream pools"},
		{"network", s.Network, source("network"), "network a binding that names none uses: tcp, udp or unix"},
		{"backlog", intValue(int64(s.Backlog)), source("backlog"), "listen queue depth; zero means the kernel's somaxconn"},
		{"workers", intValue(int64(s.WorkerCount)), source("workers"), "task pool worker ceiling; follows GOMAXPROCS and the cores"},
		{"maxEvents", intValue(int64(s.MaxEvents)), source("maxEvents"), "tasks the pool may hold waiting for a worker"},
		{"pollBatch", intValue(int64(s.PollBatchSize)), source("pollBatch"), "most events one wait of an event loop collects"},
		{"readBuffer", byteSize(int64(s.ReadBufferSize)), source("readBuffer"), "the most bytes one read of a connection's socket takes"},
		{"readBufferStart", byteSize(int64(s.ReadBufferStartSize)), source("readBufferStart"), "bytes a connection's read round starts with; a round that fills it moves the connection to readBuffer"},
		{"writeWatermark", offBytes(int64(s.WriteBufferHighWatermark)), source("writeWatermark"), "bytes a connection may have waiting before its reads pause; zero is sized from readBuffer and the machine"},
		{"writeLowWatermark", low, "default", "backlog at which paused reads resume; a quarter of writeWatermark"},
		{"maxPendingBytes", offBytes(s.MaxPendingBytes), source("maxPendingBytes"), "bytes all connections together may have waiting; zero means no budget"},
		{"maxWriteQueueBytes", offBytes(s.MaxWriteQueueBytes), source("maxWriteQueueBytes"), "bytes one connection may have queued before it is closed; zero means no cap"},
		{"maxConns", intValue(int64(s.MaxConns)), source("maxConns"), "connections accepted and open at once; zero means no cap"},
		{"useWritev", boolValue(s.UseWritev), source("useWritev"), "send several buffers in one writev where the platform has it"},
		{"coalesceWrites", boolValue(s.CoalesceWrites), source("coalesceWrites"), "cork a read round's replies and write them together"},
		{"socketSyscalls", boolValue(s.SocketSyscalls), source("socketSyscalls"), "read and write with recvfrom and sendto rather than read and write"},
		{"taskPool", s.TaskPool, source("taskPool"), "the pool connections run on; built from the settings unless supplied"},
		{"taskPoolMode", s.TaskPoolMode.String(), source("taskPoolMode"), "how the pool grows and shrinks: elastic, adaptive or inline"},
		{"minWorkers", intValue(int64(s.MinWorkerCount)), source("minWorkers"), "resident workers an adaptive pool retires down to; zero retires all"},
		{"sharedTaskPool", boolValue(s.SharedTaskPool), source("sharedTaskPool"), "engines of one name share one task pool"},
		{"ioPollers", boolValue(s.IOPollers), source("ioPollers"), "split the engine across event loops that wait for readiness"},
		{"ioPollerCount", intValue(int64(s.IOPollerCount)), source("ioPollerCount"), "loops to create; zero follows the CPUs"},
		{"pollers", intValue(int64(s.Pollers)), "default", "loops actually created"},
		{"engineLoop", boolValue(s.EngineLoop), source("engineLoop"), "give the engine a loop of its own to accept and read UDP"},
		{"reusePort", boolValue(s.ReusePort), source("reusePort"), "bind listeners with SO_REUSEPORT so processes may share an address"},
		{"balanceReusePort", boolValue(s.BalanceReusePort), source("balanceReusePort"), "hand reuseport connections out round-robin with eBPF"},
		{"accept", s.AcceptPlacement, source("accept"), "which poller serves a connection accepted on a poller's own socket"},
		{"udpIdleTimeout", s.UDPIdleTimeout.String(), source("udpIdleTimeout"), "close a UDP peer silent this long; zero means the default"},
		{"logStatus", boolValue(s.LogStatus), source("logStatus"), "log the engine started line when it starts"},
		{"logSettings", boolValue(s.LogSettings), source("logSettings"), "log this setting list when the engine starts"},
	}
}

// customConfig records the settings c set rather than left for fib's
// defaults. A value equal to the default counts as default even when the
// caller wrote it, since the log cannot tell the two apart and they run the
// same way.
func customConfig(c Config) map[string]bool {
	base := DefaultConfig()
	autoWatermark := DefaultWriteWatermark(c.ReadBufferSize, c.MaxPendingBytes, c.MaxConns)
	custom := make(map[string]bool, 32)
	set := func(name string, yes bool) {
		if yes {
			custom[name] = true
		}
	}
	set("name", c.Name != "" && c.Name != base.Name)
	set("network", c.Network != "" && c.Network != base.Network)
	set("backlog", c.Backlog != 0 && c.Backlog != base.Backlog)
	set("workers", c.WorkerCount != 0 && c.WorkerCount != base.WorkerCount)
	set("maxEvents", c.MaxEvents != 0 && c.MaxEvents != base.MaxEvents)
	set("pollBatch", c.PollBatchSize != 0 && c.PollBatchSize != base.PollBatchSize)
	set("readBuffer", c.ReadBufferSize != 0 && c.ReadBufferSize != base.ReadBufferSize)
	set("readBufferStart", c.ReadBufferStartSize != 0 && c.ReadBufferStartSize != readBufferStart(0, c.ReadBufferSize))
	set("writeWatermark", c.WriteBufferHighWatermark != 0 && c.WriteBufferHighWatermark != autoWatermark)
	set("maxPendingBytes", c.MaxPendingBytes != 0)
	set("maxWriteQueueBytes", c.MaxWriteQueueBytes != 0)
	set("maxConns", c.MaxConns != 0)
	set("useWritev", c.UseWritev != base.UseWritev)
	set("coalesceWrites", c.CoalesceWrites != base.CoalesceWrites)
	set("socketSyscalls", c.SocketSyscalls != base.SocketSyscalls)
	set("taskPool", c.TaskPool != nil)
	set("taskPoolMode", c.TaskPoolMode != 0 && c.TaskPoolMode != base.TaskPoolMode)
	set("minWorkers", c.MinWorkerCount != 0)
	set("sharedTaskPool", c.SharedTaskPool != base.SharedTaskPool)
	set("ioPollers", c.IOPollers != base.IOPollers)
	set("ioPollerCount", c.IOPollerCount != 0)
	set("engineLoop", c.EngineLoop != base.EngineLoop)
	set("reusePort", c.ReusePort != base.ReusePort)
	set("balanceReusePort", c.BalanceReusePort != base.BalanceReusePort)
	set("accept", c.AcceptPlacement != 0 && c.AcceptPlacement != base.AcceptPlacement)
	set("udpIdleTimeout", c.UDPIdleTimeout != 0)
	set("logStatus", c.LogStatus != base.LogStatus)
	set("logSettings", c.LogSettings != base.LogSettings)
	return custom
}

func intValue(n int64) string { return fmt.Sprintf("%d", n) }

func boolValue(b bool) string { return fmt.Sprintf("%t", b) }

// byteSize spells a byte count both exactly and in its largest whole unit,
// as in "65536 (64 KiB)".
func byteSize(n int64) string {
	switch {
	case n == 0:
		return "0"
	case n%(1<<30) == 0:
		return fmt.Sprintf("%d (%d GiB)", n, n>>30)
	case n%(1<<20) == 0:
		return fmt.Sprintf("%d (%d MiB)", n, n>>20)
	case n%(1<<10) == 0:
		return fmt.Sprintf("%d (%d KiB)", n, n>>10)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// offBytes is byteSize for a setting whose zero or negative value turns it
// off: a watermark, a budget or a queue cap.
func offBytes(n int64) string {
	if n <= 0 {
		return fmt.Sprintf("%d (off)", n)
	}
	return byteSize(n)
}
