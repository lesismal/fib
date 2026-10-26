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
	}
}

// String formats the settings as one line of key=value pairs, in the order
// the fields are declared, so that a log line is both readable and easy to
// grep. Byte counts are decimal integers and durations use Go's spelling.
func (s Settings) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "name=%s network=%s backlog=%d workers=%d maxEvents=%d pollBatch=%d ",
		s.Name, s.Network, s.Backlog, s.WorkerCount, s.MaxEvents, s.PollBatchSize)
	fmt.Fprintf(&b, "readBuffer=%d writeWatermark=%d writeLowWatermark=%d maxPendingBytes=%d maxWriteQueueBytes=%d maxConns=%d ",
		s.ReadBufferSize, s.WriteBufferHighWatermark, s.WriteBufferHighWatermark/4, s.MaxPendingBytes, s.MaxWriteQueueBytes, s.MaxConns)
	fmt.Fprintf(&b, "taskPool=%s taskPoolMode=%s minWorkers=%d sharedTaskPool=%t ",
		s.TaskPool, s.TaskPoolMode, s.MinWorkerCount, s.SharedTaskPool)
	fmt.Fprintf(&b, "ioPollers=%t ioPollerCount=%d pollers=%d engineLoop=%t reusePort=%t balanceReusePort=%t ",
		s.IOPollers, s.IOPollerCount, s.Pollers, s.EngineLoop, s.ReusePort, s.BalanceReusePort)
	fmt.Fprintf(&b, "accept=%s coalesceWrites=%t useWritev=%t socketSyscalls=%t udpIdleTimeout=%s",
		s.AcceptPlacement, s.CoalesceWrites, s.UseWritev, s.SocketSyscalls, s.UDPIdleTimeout)
	return b.String()
}
