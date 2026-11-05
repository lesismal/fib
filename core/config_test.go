//go:build linux

package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/udpbatch"
	"github.com/lesismal/fib/taskpool"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	// The exact sizing is DefaultPoolSizing's business and is tuned by
	// measurement; restating its arithmetic here would only pin the test to
	// whatever the constants happen to be. What DefaultConfig owes is that it
	// reports that sizing rather than one of its own.
	want := DefaultPoolSizing(config.TaskPoolMode)
	if config.WorkerCount != want.WorkerCount {
		t.Fatalf("WorkerCount = %d, want %d", config.WorkerCount, want.WorkerCount)
	}
	if config.MaxEvents != want.MaxEvents {
		t.Fatalf("MaxEvents = %d, want %d", config.MaxEvents, want.MaxEvents)
	}
	// Workers are deliberately oversubscribed relative to cores: each one runs
	// its connection's syscalls inline, so a pool the size of GOMAXPROCS leaves
	// cores idle whenever workers are in the kernel.
	if config.WorkerCount <= runtime.GOMAXPROCS(0) {
		t.Fatalf("WorkerCount = %d, want more than GOMAXPROCS %d", config.WorkerCount, runtime.GOMAXPROCS(0))
	}
	// The event batch sizes the task queue, which has to be able to hold a
	// round's worth of runnable connections; a queue narrower than the pool
	// would make the event loop wait on workers it has already woken. Past
	// maxMaxEvents it is capped rather than grown with the pool, which it is
	// on 100 CPUs and more.
	if config.MaxEvents < config.WorkerCount && config.MaxEvents != maxMaxEvents {
		t.Fatalf("MaxEvents = %d, want at least WorkerCount %d", config.MaxEvents, config.WorkerCount)
	}
	if !config.UseWritev {
		t.Fatal("UseWritev = false, want adaptive writev enabled by default")
	}
	if config.WriteBufferHighWatermark != defaultWriteHighWatermark {
		t.Fatalf("WriteBufferHighWatermark = %d, want %d", config.WriteBufferHighWatermark, defaultWriteHighWatermark)
	}
	// The server-wide budget is off by default: it pauses every connection once
	// the sum reaches it, so only the per-connection watermark applies unless a
	// deployment opts in.
	if config.MaxPendingBytes != 0 {
		t.Fatalf("MaxPendingBytes = %d, want 0 (no server-wide budget)", config.MaxPendingBytes)
	}
	if config.TaskPoolMode != taskpool.ModeAdaptive {
		t.Fatalf("TaskPoolMode = %v, want adaptive", config.TaskPoolMode)
	}
	if !config.SharedTaskPool {
		t.Fatal("SharedTaskPool = false, want shared workers by default")
	}
}

// The pool a multiplexed protocol runs its handlers on is fed by the engine's
// own, and a handler waits on the application where an engine worker only
// waits on the kernel, so it has to be the wider of the two.
func TestDefaultStreamPoolSizingExceedsTheEnginePool(t *testing.T) {
	for _, mode := range []taskpool.Mode{taskpool.ModeElastic, taskpool.ModeAdaptive, taskpool.ModeAdaptiveChan} {
		engine := DefaultPoolSizing(mode)
		streams := DefaultStreamPoolSizing(mode)
		if streams.WorkerCount <= engine.WorkerCount {
			t.Fatalf("%v: stream pool WorkerCount = %d, want more than the engine's %d",
				mode, streams.WorkerCount, engine.WorkerCount)
		}
		if streams.MaxEvents < streams.WorkerCount && streams.MaxEvents != maxMaxEvents {
			t.Fatalf("%v: stream pool MaxEvents = %d, want at least WorkerCount %d",
				mode, streams.MaxEvents, streams.WorkerCount)
		}
	}
}

// Every mode's worker count is a ceiling, so they share one default, and its
// queue stays within the bounds poolSizing keeps it to.
func TestDefaultPoolSizingIsSharedByModes(t *testing.T) {
	want := DefaultPoolSizing(taskpool.ModeAdaptive)
	if want.WorkerCount <= runtime.GOMAXPROCS(0) {
		t.Fatalf("WorkerCount = %d, want more than GOMAXPROCS %d", want.WorkerCount, runtime.GOMAXPROCS(0))
	}
	if want.MaxEvents < minMaxEvents || want.MaxEvents > maxMaxEvents {
		t.Fatalf("MaxEvents = %d, want within [%d, %d]", want.MaxEvents, minMaxEvents, maxMaxEvents)
	}
	for _, mode := range []taskpool.Mode{taskpool.ModeElastic, taskpool.ModeAdaptiveChan} {
		if got := DefaultPoolSizing(mode); got != want {
			t.Fatalf("%v sizing = %+v, want adaptive's %+v", mode, got, want)
		}
	}
}

// The elastic floor follows the cores, so a GOMAXPROCS lowered below them
// still leaves each core its share of workers, and a small machine is not
// handed a floor sized for a large one.
func TestElasticFloorFollowsCores(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	floor := runtime.NumCPU() * elasticMinWorkersPerCPU
	want := max(elasticWorkersPerCPU, floor)
	for _, mode := range []taskpool.Mode{taskpool.ModeElastic, taskpool.ModeAdaptive, taskpool.ModeAdaptiveChan} {
		if got := DefaultPoolSizing(mode).WorkerCount; got != want {
			t.Fatalf("%v WorkerCount at GOMAXPROCS 1 = %d, want %d", mode, got, want)
		}
	}
}

// Switching the mode has to carry the sizing with it, or a config keeps numbers
// that no mode's default gave it.
func TestSetTaskPoolModeMovesSizing(t *testing.T) {
	config := DefaultConfig()
	config.WorkerCount, config.MaxEvents = 7, 70
	config.SetTaskPoolMode(taskpool.ModeElastic)
	want := DefaultPoolSizing(taskpool.ModeElastic)
	if config.TaskPoolMode != taskpool.ModeElastic {
		t.Fatalf("TaskPoolMode = %v, want elastic", config.TaskPoolMode)
	}
	if config.WorkerCount != want.WorkerCount || config.MaxEvents != want.MaxEvents {
		t.Fatalf("sizing = %d/%d, want elastic's %d/%d",
			config.WorkerCount, config.MaxEvents, want.WorkerCount, want.MaxEvents)
	}
}

// Sizing the caller chose is theirs to keep, whichever order the two setters
// are called in.
func TestSetPoolSizingSurvivesModeSwitch(t *testing.T) {
	const workers, events = 512, 4096
	for _, order := range []string{"sizing first", "mode first"} {
		t.Run(order, func(t *testing.T) {
			config := DefaultConfig()
			if order == "sizing first" {
				config.SetPoolSizing(workers, events)
				config.SetTaskPoolMode(taskpool.ModeAdaptiveChan)
			} else {
				config.SetTaskPoolMode(taskpool.ModeAdaptiveChan)
				config.SetPoolSizing(workers, events)
			}
			if config.WorkerCount != workers || config.MaxEvents != events {
				t.Fatalf("sizing = %d/%d, want the pinned %d/%d",
					config.WorkerCount, config.MaxEvents, workers, events)
			}
			// And it still holds across a further switch.
			config.SetTaskPoolMode(taskpool.ModeElastic)
			if config.WorkerCount != workers || config.MaxEvents != events {
				t.Fatalf("after switching modes sizing = %d/%d, want the pinned %d/%d",
					config.WorkerCount, config.MaxEvents, workers, events)
			}
		})
	}
}

// A field left at zero means "leave it", so that one of the two can be tuned
// without having to restate the other.
func TestSetPoolSizingKeepsFieldsLeftUnset(t *testing.T) {
	config := DefaultConfig()
	events := config.MaxEvents
	config.SetPoolSizing(64, 0)
	if config.WorkerCount != 64 {
		t.Fatalf("WorkerCount = %d, want 64", config.WorkerCount)
	}
	if config.MaxEvents != events {
		t.Fatalf("MaxEvents = %d, want it left at %d", config.MaxEvents, events)
	}
}

// DefaultConfig serves on pollers, with no loop of the engine's own, binds
// its listeners with SO_REUSEPORT, and places accepted connections by
// descriptor, as the zero Config does.
func TestDefaultIOPollers(t *testing.T) {
	config := DefaultConfig()
	if !config.IOPollers || config.EngineLoop || !config.ReusePort || config.AcceptPlacement != AcceptFDModulo {
		t.Errorf("IOPollers %v, EngineLoop %v, ReusePort %v, AcceptPlacement %v, want true, false, true, fd-modulo",
			config.IOPollers, config.EngineLoop, config.ReusePort, config.AcceptPlacement)
	}
	if (Config{}).AcceptPlacement != AcceptFDModulo {
		t.Error("the zero Config does not place accepted connections by descriptor")
	}
}

// The defaults go by GOMAXPROCS where it is below the cores, as in a child of
// package prefork: a process held to four Ps gets one poller, and one held to
// eight gets two, however many cores the machine has.
func TestDefaultsFollowGOMAXPROCS(t *testing.T) {
	if runtime.NumCPU() < 8 {
		t.Skip("needs 8 CPUs")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	if config := DefaultConfig(); pollerCount(config) != 1 {
		t.Errorf("GOMAXPROCS 4: %d pollers, want 1", pollerCount(config))
	}
	runtime.GOMAXPROCS(8)
	config := DefaultConfig()
	if got := pollerCount(config); !config.IOPollers || got != 2 {
		t.Errorf("GOMAXPROCS 8: IOPollers %v with %d pollers, want on with 2", config.IOPollers, got)
	}
}

// The engine gives itself one poller on four CPUs or fewer, two on up to
// sixteen, and one for every eight CPUs, rounded down, on more.
func TestDefaultPollerCount(t *testing.T) {
	for cpus, want := range map[int]int{1: 1, 4: 1, 5: 2, 8: 2, 16: 2, 17: 2, 23: 2, 24: 3, 32: 4, 64: 8, 128: 16} {
		if got := defaultPollerCount(cpus); got != want {
			t.Errorf("%d CPUs: %d pollers, want %d", cpus, got, want)
		}
	}
}

// acceptsConnections reports whether fd is a listening socket.
func acceptsConnections(t *testing.T, fd int) bool {
	t.Helper()
	listening, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN)
	if err != nil {
		t.Fatal(err)
	}
	return listening != 0
}

// Each poller listens on a TCP socket of its own, bound where the engine's
// is, and accepts and serves its connections itself, with ReusePort or
// without it; the engine's socket only holds the address and the port the
// kernel chose, and takes none.
func TestPollersAcceptWithReusePort(t *testing.T) {
	for _, reusePort := range []bool{true, false} {
		t.Run("ReusePort="+strconv.FormatBool(reusePort), func(t *testing.T) {
			testPollersAccept(t, reusePort)
		})
	}
}

func testPollersAccept(t *testing.T, reusePort bool) {
	const pollers, conns = 3, 30
	config := pollerConfig("pollers-reuseport", pollers)
	config.ReusePort = reusePort
	opened := make(chan *Connection, conns)
	server, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) { opened <- c },
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})
	if !server.pollersListen || len(server.listeners) != 1 {
		t.Fatalf("engine has pollersListen=%v and %d listeners", server.pollersListen, len(server.listeners))
	}
	if acceptsConnections(t, server.listeners[0].fd) {
		t.Fatal("the engine's own socket listens beside its pollers'")
	}
	bound, err := syscall.Getsockname(server.listeners[0].fd)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range server.pollers {
		if len(p.listeners) != 1 {
			t.Fatalf("poller %d has %d listeners, want 1", i, len(p.listeners))
		}
		if !acceptsConnections(t, p.listeners[0].fd) {
			t.Fatalf("poller %d's socket does not listen", i)
		}
		if got, err := syscall.Getsockname(p.listeners[0].fd); err != nil || netaddr.ToAddr(got).String() != netaddr.ToAddr(bound).String() {
			t.Fatalf("poller %d is bound to %v (%v), want %v", i, netaddr.ToAddr(got), err, netaddr.ToAddr(bound))
		}
	}

	payload := []byte("accepted by a poller")
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(payload); err != nil {
				t.Error(err)
				return
			}
			reply := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, reply); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(reply, payload) {
				t.Errorf("echo mismatch: %q", reply)
			}
		}()
	}
	wg.Wait()
	used := map[*Engine]bool{}
	for i := 0; i < conns; i++ {
		c := <-opened
		if c.engine == server || c.engine.parent != server {
			t.Fatalf("connection is served by %p, want one of the engine's pollers", c.engine)
		}
		if c.Engine() != server {
			t.Fatal("Engine() does not report the engine the connection came from")
		}
		used[c.engine] = true
	}
	if len(used) < 2 {
		t.Fatalf("%d connections landed on %d poller(s)", conns, len(used))
	}
}

// Without ReusePort the pollers still listen with SO_REUSEPORT, but an
// address another socket holds is refused as it would be without pollers,
// whether a plain listener holds it or another engine.
func TestPollersRefuseAddressInUse(t *testing.T) {
	held, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	config := pollerConfig("pollers-in-use", 2)
	config.ReusePort = false
	if e, err := New(config, Binding{Addr: held.Addr().String(), Handler: HandlerFuncs{}}); err == nil {
		e.Close()
		t.Fatalf("bound %s, which a listener holds", held.Addr())
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("bind error = %v, want EADDRINUSE", err)
	}

	first, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	addr, err := first.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	if e, err := New(config, Binding{Addr: addr.String(), Handler: HandlerFuncs{}}); err == nil {
		e.Close()
		t.Fatalf("a second engine bound %s", addr)
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("second bind error = %v, want EADDRINUSE", err)
	}
	// With ReusePort both ask to share it, and do.
	config.ReusePort = true
	a, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	addr, _ = a.LocalAddr()
	b, err := New(config, Binding{Addr: addr.String(), Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatalf("ReusePort engines could not share %s: %v", addr, err)
	}
	b.Close()
}

// A Unix socket has no SO_REUSEPORT to share, so an engine with a loop of
// its own keeps accepting for its pollers whatever ReusePort says.
func TestReusePortLeavesUnixListenersOnEngine(t *testing.T) {
	config := pollerConfig("pollers-reuseport-unix", 2)
	config.ReusePort = true
	config.EngineLoop = true
	config.Network = "unix"
	server, err := New(config, Binding{Addr: filepath.Join(t.TempDir(), "fib.sock"), Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if server.pollersListen || !acceptsConnections(t, server.listeners[0].fd) {
		t.Fatal("a Unix listener was left to the pollers")
	}
	for i, p := range server.pollers {
		if len(p.listeners) != 0 {
			t.Fatalf("poller %d listens on %d sockets, want none", i, len(p.listeners))
		}
	}
}

// With ReusePort on a UDP address, each poller reads a socket of its own
// bound there, the first holding the one the engine bound, and the engine
// keeps none, since a bound UDP socket takes a share of the datagrams. Every
// datagram of a peer reaches the one poller its hash picked, so a peer opens
// one connection, and the pollers sweep their own idle peers.
func TestPollersReadUDPWithReusePort(t *testing.T) {
	const pollers, peers, rounds = 4, 32, 3
	config := pollerConfig("pollers-udp-reuseport", pollers)
	config.ReusePort = true
	config.UDPIdleTimeout = 200 * time.Millisecond
	var mu sync.Mutex
	opened := map[string]*Connection{}
	reopened := 0
	closed := make(chan error, peers)
	server, addr := startUDPServer(t, config, HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			if opened[c.RemoteAddr().String()] != nil {
				reopened++
			}
			opened[c.RemoteAddr().String()] = c
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if c.engine.parent != c.Engine() {
				t.Error("a UDP peer is served off the pollers")
			}
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
		Close: func(_ *Connection, err error) { closed <- err },
	})
	if !server.pollersListen || len(server.udpListeners) != 0 {
		t.Fatalf("engine has pollersListen=%v and %d UDP sockets of its own", server.pollersListen, len(server.udpListeners))
	}
	fds := map[int]bool{}
	for i, p := range server.pollers {
		if len(p.udpListeners) != 1 {
			t.Fatalf("poller %d has %d UDP sockets, want 1", i, len(p.udpListeners))
		}
		fd := p.udpListeners[0].fd
		fds[fd] = true
		got, err := syscall.Getsockname(fd)
		if err != nil || sockaddrToUDPAddr(got).String() != addr.String() {
			t.Fatalf("poller %d is bound to %v (%v), want %v", i, sockaddrToUDPAddr(got), err, addr)
		}
	}
	if len(fds) != pollers {
		t.Fatalf("%d pollers share %d sockets", pollers, len(fds))
	}

	conns := make([]*net.UDPConn, peers)
	for i := range conns {
		conn, err := net.DialUDP("udp4", nil, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns[i] = conn
	}
	reply := make([]byte, 64)
	for round := 0; round < rounds; round++ {
		for i, conn := range conns {
			payload := []byte("peer " + strconv.Itoa(i) + " round " + strconv.Itoa(round))
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
			n, err := conn.Read(reply)
			if err != nil || !bytes.Equal(reply[:n], payload) {
				t.Fatalf("peer %d read %q, %v", i, reply[:n], err)
			}
		}
	}
	mu.Lock()
	used := map[*Engine]bool{}
	for _, c := range opened {
		used[c.engine] = true
	}
	count, again := len(opened), reopened
	mu.Unlock()
	if count != peers || again != 0 {
		t.Fatalf("%d peers opened %d connections, %d of them again", peers, count, again)
	}
	if len(used) < 2 {
		t.Fatalf("%d peers landed on %d poller(s)", peers, len(used))
	}
	for i := 0; i < peers; i++ {
		select {
		case err := <-closed:
			if !errors.Is(err, ErrUDPIdleTimeout) {
				t.Fatalf("closed with %v, want ErrUDPIdleTimeout", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d idle peers were closed", i, peers)
		}
	}
}

// Engines that listen on ports of the kernel's choosing at once, as the
// tests of many packages do, are each given a port of their own: none is
// given one another engine's pollers listen on, which would split its
// connections between them.
func TestPollersOnChosenPortsShareNone(t *testing.T) {
	const engines = 32
	var wg sync.WaitGroup
	addrs := make([]string, engines)
	for i := range engines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			config := DefaultConfig()
			config.IOPollers, config.IOPollerCount = true, 8
			reply := []byte(strconv.Itoa(i))
			engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{Data: func(c *Connection, _ []byte) { _ = c.Send(reply) }}})
			if err != nil {
				t.Error(err)
				return
			}
			done := make(chan error, 1)
			go func() { done <- engine.Run() }()
			t.Cleanup(func() {
				engine.Stop()
				<-done
				_ = engine.Close()
			})
			addr, err := engine.LocalAddr()
			if err != nil {
				t.Error(err)
				return
			}
			addrs[i] = addr.String()
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for i, addr := range addrs {
		for range 16 {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, _ = conn.Write([]byte("hi"))
			buf := make([]byte, 8)
			n, err := conn.Read(buf)
			conn.Close()
			if err != nil {
				t.Fatal(err)
			}
			if got := string(buf[:n]); got != strconv.Itoa(i) {
				t.Fatalf("engine %d's address %s was answered by engine %s", i, addr, got)
			}
		}
	}
}

// Close stops every poller's listener before it closes a connection, so that
// a peer that reconnects the moment its connection is closed is refused, not
// let into a listener of a poller still waiting its turn to close.
func TestCloseStopsListeningFirst(t *testing.T) {
	config := DefaultConfig()
	config.IOPollers, config.IOPollerCount = true, 32
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	var conns []net.Conn
	for range 8 {
		conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns = append(conns, conn)
	}
	// Each client dials again as soon as its connection is closed.
	redialed := make(chan error, len(conns))
	for _, conn := range conns {
		go func() {
			_, _ = conn.Read(make([]byte, 1))
			again, err := net.DialTimeout("tcp", addr.String(), time.Second)
			if err == nil {
				again.Close()
			}
			redialed <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	engine.Stop()
	<-done
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	for range conns {
		if err := <-redialed; err == nil {
			t.Fatal("a client reconnected to a closed engine")
		}
	}
}

// TestDefaultBacklogMatchesKernelLimit keeps the accept queue as deep as the
// one net.Listen asks for, which is what every framework built on it gets. An
// overflowing accept queue does not refuse connections: the kernel drops the
// client's ACK, so a connection burst shows up only as clients sitting on
// SYN-ACK retransmission timers. Measured against a 3000-connection burst, the
// historical SOMAXCONN of 128 cost 1399 upgrades per second and a median of
// 1.06s, against 63064 per second and 34ms at the kernel's own limit.
func TestDefaultBacklogMatchesKernelLimit(t *testing.T) {
	want := syscall.SOMAXCONN
	if data, err := os.ReadFile("/proc/sys/net/core/somaxconn"); err == nil {
		if limit, parseErr := strconv.Atoi(strings.TrimSpace(string(data))); parseErr == nil && limit > 0 {
			want = limit
			if want > 1<<16-1 {
				want = 1<<16 - 1
			}
		}
	}
	if got := DefaultConfig().Backlog; got != want {
		t.Fatalf("default backlog = %d, want the kernel limit %d", got, want)
	}
}

// SendBatch sends runs of datagrams of one size as one segmented message,
// and the peer receives each datagram whole and in order all the same: runs
// that end in a shorter one, runs broken by a larger one, a run longer than
// a message takes, and datagrams of their own size between them. With GSO
// refused, as by a kernel without it, the same batch goes a message each.
func TestUDPSendBatchSegments(t *testing.T) {
	var sizes []int
	for range 13 {
		sizes = append(sizes, 1200)
	}
	sizes = append(sizes, 500, 800, 800, 800, 300, 1200, 100)
	for range 40 {
		sizes = append(sizes, 1000)
	}
	batch := make([][]byte, len(sizes))
	for i, n := range sizes {
		batch[i] = bytes.Repeat([]byte{byte('A' + i%26)}, n)
		copy(batch[i], fmt.Sprintf("%03d", i))
	}
	for _, off := range []bool{false, true} {
		t.Run(fmt.Sprintf("gsoOff=%v", off), func(t *testing.T) {
			defer udpbatch.SetGSOOff(udpbatch.SetGSOOff(off))
			sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			_ = sink.SetReadBuffer(4 << 20)
			engine, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
			dialed := make(chan *Connection, 1)
			if err := engine.Dial("udp", sink.LocalAddr().String(), 5*time.Second, func(c *Connection, err error) {
				if err != nil {
					t.Error(err)
				}
				dialed <- c
			}); err != nil {
				t.Fatal(err)
			}
			client := <-dialed
			if client == nil {
				t.FailNow()
			}
			defer client.Close()
			if err := client.SendBatch(batch); err != nil {
				t.Fatal(err)
			}
			if !off && udpbatch.GSOOff() {
				t.Fatal("the kernel refused UDP_SEGMENT, and the batch went a message each")
			}
			buf := make([]byte, 65536)
			_ = sink.SetReadDeadline(time.Now().Add(5 * time.Second))
			for i := range batch {
				n, _, err := sink.ReadFromUDP(buf)
				if err != nil {
					t.Fatalf("datagram %d: %v", i, err)
				}
				if !bytes.Equal(buf[:n], batch[i]) {
					t.Fatalf("datagram %d of %d bytes arrived as %d bytes starting %q", i, len(batch[i]), n, buf[:min(n, 8)])
				}
			}
		})
	}
}
