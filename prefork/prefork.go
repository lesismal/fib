// Package prefork serves one program from several processes, each with a P or
// a few, a heap and a collector of its own, as fasthttp's prefork does: a busy
// server that allocates spends far less of each request on garbage
// collection in many small processes than in one with a P for every CPU,
// where every collection's mark phase shares its work buffers, and every span
// its heap lock, between all the Ps. On 64 CPUs a single process spent 66µs
// of user time on each request of HttpArena's json-tls profile, against the
// 46µs of a child of one or two Ps, and served 0.56M requests a second
// against 0.87M to 0.99M; latency-1m's 99th percentile fell from 1.6ms to
// 124µs, on a quarter fewer cores.
//
// The process Run is called in becomes the master. It starts the children,
// each running the same program with the same arguments, and serves nothing
// itself: it waits for them, hands them SIGINT and SIGTERM, and returns once
// they have all exited. Each child's Run calls serve, which binds the
// program's engines as it would without prefork; every engine a child binds
// listens with SO_REUSEPORT, so that the children share the program's
// addresses and the kernel spreads connections, and datagrams, over them by
// a hash of their addresses.
//
// Whatever the program does before calling Run, every child does as well, and
// so does the master: work only the children need, loading data or opening
// pools, belongs in serve. The children share nothing but their listening
// addresses, so state a program keeps in memory is kept once per child.
//
// Prefork needs Linux, where SO_REUSEPORT spreads connections between
// processes; elsewhere Run calls serve in this process, as a lone child would.
package prefork

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

// childEnv is the variable a child is started with, set to its index; fib's
// engines read it too, to listen with SO_REUSEPORT. childrenEnv carries how
// many children there are.
const (
	childEnv    = "FIB_PREFORK_CHILD"
	childrenEnv = "FIB_PREFORK_CHILDREN"
)

// DefaultProcsPerChild is the GOMAXPROCS a child runs with when
// Config.ProcsPerChild leaves it to the package: two. A child of one P has
// nothing to run anything else on while its event loop, or a handler,
// waits in a system call, and each wait for an event costs it a trip
// through the runtime's poller as well as its own; see fib's epollWaiter.
// On 64 CPUs, against 64 children of one P, 32 of two served HttpArena's
// async profile, 32000 connections each waiting 10ms in its handler, at
// 1.94M requests a second against 1.73M, and latency-1m on 25 cores against
// 33, with a 99th percentile of 124us against 174us; they served baseline
// at 3.51M against 3.72M and json-tls at 0.87M against 0.99M. A single
// process served 1.61M, 2.79M and 0.56M, with a 99th percentile of 1.6ms.
const DefaultProcsPerChild = 2

// DefaultShutdownTimeout is how long the master waits for its children to
// exit, once it has asked them to, when Config.ShutdownTimeout leaves it to
// the package.
const DefaultShutdownTimeout = 10 * time.Second

// Config says how many children serve and how large each is.
type Config struct {
	// Children is how many processes serve. Zero or less means the master's
	// GOMAXPROCS divided by ProcsPerChild, rounded up, so that the children
	// together have the Ps the program would have had alone.
	Children int
	// ProcsPerChild is the GOMAXPROCS each child runs with. Zero or less
	// means DefaultProcsPerChild. A child with fewer Ps than fib's defaults
	// give pollers to serves its connections on its engine's own loop; see
	// fib.Config.IOPollers.
	ProcsPerChild int
	// ShutdownTimeout bounds how long the master waits for the children to
	// exit once it has sent them SIGTERM, after which it kills them. Zero or
	// less means DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
}

// IsChild reports whether this process is a child Run started.
func IsChild() bool { return Child() > 0 }

// Child reports which child this process is, numbered from 1, or 0 for a
// process Run did not start.
func Child() int {
	n, err := strconv.Atoi(os.Getenv(childEnv))
	if err != nil || n < 1 {
		return 0
	}
	return n
}

// Children reports, in a child, how many children the master started, this
// one among them, and 1 in a process Run did not start: the number of
// processes sharing whatever the program has once per machine or container,
// a database's connection limit, for one, which each child's pool should
// take its share of rather than the whole.
func Children() int {
	n, err := strconv.Atoi(os.Getenv(childrenEnv))
	if err != nil || n < 1 || !IsChild() {
		return 1
	}
	return n
}

// Run is RunContext with a context that SIGINT and SIGTERM cancel.
func Run(config Config, serve func(ctx context.Context) error) error {
	return RunContext(context.Background(), config, serve)
}

// RunContext makes this process the master, starting the children and
// waiting for them, or, in a child, serves.
//
// In a child it calls serve with a context that is done once the child is
// asked to stop: by SIGINT or SIGTERM, which the master sends it when ctx is
// done or another child has failed, or by the master's exiting. serve should
// stop its engines then and return, and RunContext returns what it returned.
//
// In the master it returns once every child has exited: nil when they were
// asked to stop, by ctx or by SIGINT or SIGTERM sent to the master, and
// returned nil; otherwise an error naming the first child that failed, or
// that exited before it was asked to, at which point the master asks the
// others to stop. A child that has not exited ShutdownTimeout after being
// asked is killed.
func RunContext(ctx context.Context, config Config, serve func(ctx context.Context) error) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if IsChild() || !supported {
		return serve(ctx)
	}
	return master(ctx, config)
}

// child is one running child. stopped records that the master has asked it
// to stop, after which SIGTERM ending it is what was asked for.
type child struct {
	index   int
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	stopped bool
}

func master(ctx context.Context, config Config) error {
	// A child is sent SIGTERM when the thread that started it exits, not
	// the process (see sysProcAttr), so every child is started from this
	// goroutine's thread, which is kept for as long as they run.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	procs := config.ProcsPerChild
	if procs <= 0 {
		procs = DefaultProcsPerChild
	}
	n := config.Children
	if n <= 0 {
		n = max(1, (runtime.GOMAXPROCS(0)+procs-1)/procs)
	}
	timeout := config.ShutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}
	path, err := os.Executable()
	if err != nil {
		return fmt.Errorf("prefork: %w", err)
	}

	children := make([]*child, 0, n)
	exited := make(chan *child, n)
	var failure error
	for i := 1; i <= n; i++ {
		c, err := start(path, i, n, procs)
		if err != nil {
			failure = err
			break
		}
		children = append(children, c)
		go func() {
			c.err = c.cmd.Wait()
			close(c.done)
			exited <- c
		}()
	}

	// Wait for a reason to stop: ctx, or a child exiting on its own.
	running := len(children)
	if failure == nil {
		select {
		case <-ctx.Done():
		case c := <-exited:
			running--
			failure = exitError(c)
			if failure == nil {
				failure = fmt.Errorf("prefork: child %d exited before it was asked to stop", c.index)
			}
		}
	}

	for _, c := range children {
		c.stopped = true
		signalChild(c, syscall.SIGTERM)
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for running > 0 {
		select {
		case c := <-exited:
			running--
			if err := exitError(c); err != nil && failure == nil {
				failure = err
			}
		case <-deadline.C:
			for _, c := range children {
				signalChild(c, syscall.SIGKILL)
			}
			if failure == nil {
				failure = errors.New("prefork: children still running after ShutdownTimeout were killed")
			}
			deadline.Reset(time.Hour)
		}
	}
	return failure
}

// start starts child index of the program at path, one of n children, with
// procs Ps.
func start(path string, index, n, procs int) (*child, error) {
	cmd := exec.Command(path, os.Args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), childEnv+"="+strconv.Itoa(index), childrenEnv+"="+strconv.Itoa(n),
		"GOMAXPROCS="+strconv.Itoa(procs))
	cmd.SysProcAttr = sysProcAttr()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("prefork: start child %d: %w", index, err)
	}
	return &child{index: index, cmd: cmd, done: make(chan struct{})}, nil
}

// signalChild sends sig to c unless it has exited.
func signalChild(c *child, sig os.Signal) {
	select {
	case <-c.done:
	default:
		_ = c.cmd.Process.Signal(sig)
	}
}

// exitError describes how c ended, or is nil if it exited with status 0, or
// was ended by the SIGTERM the master sent it to stop.
func exitError(c *child) error {
	if c.err == nil {
		return nil
	}
	var exit *exec.ExitError
	if c.stopped && errors.As(c.err, &exit) {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGTERM {
			return nil
		}
	}
	return fmt.Errorf("prefork: child %d: %w", c.index, c.err)
}
