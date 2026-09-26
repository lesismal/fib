//go:build linux || darwin

package fib

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/streampool"
	"github.com/lesismal/fib/taskpool"
)

func pollerConfig(name string, pollers int) Config {
	config := DefaultConfig()
	config.Name = name
	config.IOPollers = true
	config.IOPollerCount = pollers
	return config
}

// checkPoller fails unless c, opened on descriptor fd, is served by the
// poller that descriptor picks, while still reporting the engine that
// accepted or dialed it as its own.
func checkPoller(t *testing.T, e *Engine, c *Connection, fd int) {
	t.Helper()
	if fd < 0 {
		t.Errorf("connection has no descriptor")
	} else if want := e.pollers[fd%len(e.pollers)]; c.engine != want {
		t.Errorf("descriptor %d is served by %p, want poller %p", fd, c.engine, want)
	}
	if c.Engine() != e {
		t.Error("Engine() does not report the engine the connection came from")
	}
}

// Accepted connections go to the poller their descriptor picks, which runs
// their rounds itself, and the stream pool is sized as it is without
// pollers.
func TestPollersServeAcceptedConnections(t *testing.T) {
	const pollers, conns = 3, 24
	config := pollerConfig("pollers-accept", pollers)
	type opened struct {
		c  *Connection
		fd int
	}
	accepted := make(chan opened, conns)
	server, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) { accepted <- opened{c, c.FD()} },
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})
	if len(server.pollers) != pollers {
		t.Fatalf("engine has %d pollers, want %d", len(server.pollers), pollers)
	}
	if pool, ok := server.taskPool.(*taskpool.TaskPool); !ok || pool.Mode() != taskpool.ModeInline {
		t.Fatalf("engine runs on %T, want an inline pool", server.taskPool)
	}
	if got, want := streampool.Ceiling(server.Name()), config.WorkerCount*streamPoolFactor; got != want {
		t.Fatalf("stream pool ceiling = %d, want %d as without pollers", got, want)
	}

	payload := []byte("served on a poller")
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
			for round := 0; round < 10; round++ {
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
					return
				}
			}
		}()
	}
	wg.Wait()
	used := map[*Engine]bool{}
	for i := 0; i < conns; i++ {
		o := <-accepted
		checkPoller(t, server, o.c, o.fd)
		used[o.c.engine] = true
	}
	if len(used) < 2 {
		t.Fatalf("%d connections landed on %d poller(s)", conns, len(used))
	}
}

// Dialed connections, over TCP and over UDP, go to the pollers as accepted
// ones do.
func TestPollersServeDialedConnections(t *testing.T) {
	_, tcpAddr := startEchoServer(t, DefaultConfig(), echoHandler())
	_, udpAddr := startUDPServer(t, DefaultConfig(), echoHandler())

	replies := make(chan *Connection, 16)
	var client *Engine
	client, err := NewEngine(pollerConfig("pollers-dial", 4), HandlerFuncs{Data: func(c *Connection, b []byte) {
		if string(b) == "dialed" {
			replies <- c
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run() }()
	defer func() {
		client.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()

	const perNetwork = 4
	dialed := make(chan error, 2*perNetwork)
	for _, target := range []struct{ network, addr string }{{"tcp4", tcpAddr}, {"udp4", udpAddr.String()}} {
		for i := 0; i < perNetwork; i++ {
			err := client.Dial(target.network, target.addr, 5*time.Second, func(c *Connection, err error) {
				if err == nil {
					checkPoller(t, client, c, c.FD())
					err = c.Send([]byte("dialed"))
				}
				dialed <- err
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	timeout := time.After(10 * time.Second)
	for i := 0; i < 2*perNetwork; i++ {
		select {
		case err := <-dialed:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatal("dials did not finish")
		}
	}
	for i := 0; i < 2*perNetwork; i++ {
		select {
		case c := <-replies:
			checkPoller(t, client, c, c.FD())
		case <-timeout:
			t.Fatalf("only %d of %d dialed connections were echoed", i, 2*perNetwork)
		}
	}
}

// A UDP listener's peers share its socket, so they stay on the engine's own
// loop.
func TestPollersLeaveUDPPeersOnEngine(t *testing.T) {
	server, addr := startUDPServer(t, pollerConfig("pollers-udp", 2), HandlerFuncs{Data: func(c *Connection, b []byte) {
		if c.engine.parent != nil {
			t.Error("a UDP peer was handed to a poller")
		}
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if len(server.pollers) != 2 {
		t.Fatalf("engine has %d pollers, want 2 for the connections it dials", len(server.pollers))
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("peer")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 16)
	n, err := conn.Read(reply)
	if err != nil || string(reply[:n]) != "peer" {
		t.Fatalf("read %q, %v", reply[:n], err)
	}
}

// The inline pool recovers a handler that panics: its connection is closed,
// and the poller goes on serving the others.
func TestPollerSurvivesHandlerPanic(t *testing.T) {
	_, addr := startEchoServer(t, pollerConfig("pollers-panic", 1), HandlerFuncs{Data: func(c *Connection, b []byte) {
		if string(b) == "panic" {
			panic("handler panic")
		}
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	doomed, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer doomed.Close()
	_ = doomed.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := doomed.Write([]byte("panic")); err != nil {
		t.Fatal(err)
	}
	if _, err := doomed.Read(make([]byte, 1)); err == nil {
		t.Fatal("the panicking connection was left open")
	}

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("still here")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len("still here"))
	if _, err := io.ReadFull(conn, reply); err != nil || string(reply) != "still here" {
		t.Fatalf("read %q, %v", reply, err)
	}
}

// Draining the budget below its resume level wakes exactly the loops that
// hold connections paused on it, whichever loop drained it.
func TestBudgetDrainWakesWaitingPollers(t *testing.T) {
	config := pollerConfig("pollers-budget-wake", 3)
	config.Addr = "127.0.0.1:0"
	config.MaxPendingBytes = 1000
	server, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	waiting := server.pollers[2]
	waiting.budgetWaiting.Store(true)
	server.pendingTotal.Store(900)
	// Still above the resume level of a quarter of the budget.
	server.pollers[0].releaseBudget(500)
	if waiting.wakePending.Load() {
		t.Fatal("a release that left the budget above its resume level woke a loop")
	}
	server.pollers[0].releaseBudget(200)
	if !waiting.wakePending.Load() {
		t.Fatal("the loop holding budget-paused connections was not woken")
	}
	for _, e := range []*Engine{server, server.pollers[0], server.pollers[1]} {
		if e.wakePending.Load() {
			t.Fatal("a loop with nothing paused on the budget was woken")
		}
	}
}

// A connection one poller paused on the server-wide budget resumes when a
// connection on another poller drains it, though its own poller sees no
// event of its own.
func TestBudgetPausedConnectionResumesAcrossPollers(t *testing.T) {
	const backlog = 16 << 20
	// Three pollers rather than two: the test's own client descriptors come
	// from the same process, so accepted ones step by two.
	config := pollerConfig("pollers-budget", 3)
	config.MaxPendingBytes = 64 << 10
	accepted := make(chan *Connection, 8)
	big := bytes.Repeat([]byte{'b'}, backlog)
	server, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) { accepted <- c },
		Data: func(c *Connection, b []byte) {
			reply := b
			if string(b) == "big" {
				reply = big
			}
			if err := c.Send(reply); err != nil {
				c.Close()
			}
		},
	})

	// Dial until two connections land on different pollers.
	var clients []net.Conn
	defer func() {
		for _, conn := range clients {
			conn.Close()
		}
	}()
	var holder, paused net.Conn
	var first *Connection
	for holder == nil || paused == nil {
		if len(clients) == 8 {
			t.Fatal("no two connections landed on different pollers")
		}
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, conn)
		c := <-accepted
		switch {
		case first == nil:
			first, holder = c, conn
		case c.engine != first.engine:
			paused = conn
		}
	}
	_ = holder.SetDeadline(time.Now().Add(20 * time.Second))
	_ = paused.SetDeadline(time.Now().Add(20 * time.Second))

	// The holder's peer does not read, so its reply holds the budget.
	if _, err := holder.Write([]byte("big")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return server.Stats().PendingBytes >= config.MaxPendingBytes }) {
		t.Fatal("the holder never exhausted the budget")
	}
	// The other connection's first round finds the budget exhausted and is
	// paused on its account, so its second request waits.
	echo := func(request string) {
		t.Helper()
		if _, err := paused.Write([]byte(request)); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, len(request))
		if _, err := io.ReadFull(paused, reply); err != nil || string(reply) != request {
			t.Fatalf("read %q, %v", reply, err)
		}
	}
	echo("first")
	if !waitFor(func() bool { return server.Stats().ReadsPausedByBudget > 0 }) {
		t.Fatal("no connection was paused on the budget")
	}
	drained := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(holder, make([]byte, backlog))
		drained <- err
	}()
	echo("second")
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}

// A connection that asks for workers runs its rounds on them though the
// engine runs rounds on its pollers, so a handler of it that blocks leaves
// the other connections on its loop running. The pool of workers is built
// when the first connection asks for it.
func TestRunOnWorkersUnderPollers(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	server, addr := startEchoServer(t, pollerConfig("pollers-workers", 1), HandlerFuncs{
		Open: func(c *Connection) { c.SetRunOnWorkers(true) },
		Data: func(c *Connection, b []byte) {
			if string(b) == "block" {
				entered <- struct{}{}
				<-release
			}
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})
	if server.workers.ready.Load() != nil {
		t.Fatal("the pool of workers was built before any connection asked for it")
	}
	blocked, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Close()
	_ = blocked.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := blocked.Write([]byte("block")); err != nil {
		t.Fatal(err)
	}
	<-entered
	other, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	_ = other.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := other.Write([]byte("fast")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(other, reply); err != nil || string(reply) != "fast" {
		close(release)
		t.Fatalf("the other connection read %q, %v while one on its loop was blocked", reply, err)
	}
	close(release)
	reply = make([]byte, 5)
	if _, err := io.ReadFull(blocked, reply); err != nil || string(reply) != "block" {
		t.Fatalf("read %q, %v", reply, err)
	}
	built := server.workers.ready.Load()
	if built == nil {
		t.Fatal("no pool of workers was built")
	}
	pool, ok := built.pool.(*taskpool.TaskPool)
	if !ok || pool.Mode() == taskpool.ModeInline || pool.Name() != "pollers-workers-workers" {
		t.Fatalf("connections ran on %v, want the engine's pool of workers", built.pool)
	}
	if name := server.taskPool.(*taskpool.TaskPool).Name(); name != "pollers-workers-inline" {
		t.Fatalf("engine's own pool is %q", name)
	}
}

// The default configuration spreads connections over one poller per CPU,
// each running its connections' rounds inline, and still describes a pool of
// workers for the connections that ask for one: ModeInline stays opt-in.
func TestDefaultConfigHasPollersAndWorkerPool(t *testing.T) {
	config := DefaultConfig()
	if !config.IOPollers || config.TaskPoolMode == taskpool.ModeInline {
		t.Fatalf("DefaultConfig has IOPollers=%v, TaskPoolMode=%v", config.IOPollers, config.TaskPoolMode)
	}
	config.Name = "default-config"
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if len(server.pollers) != runtime.NumCPU() {
		t.Fatalf("default engine has %d pollers, want %d", len(server.pollers), runtime.NumCPU())
	}
	pool, ok := server.taskPool.(*taskpool.TaskPool)
	if !ok || pool.Mode() != taskpool.ModeInline || !server.inlineTasks {
		t.Fatalf("default engine runs on %v, want its inline pool", server.taskPool)
	}
}
