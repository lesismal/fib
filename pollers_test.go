//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/udpbatch"
	"github.com/lesismal/fib/streampool"
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
// poller that descriptor picks, or, accepted by a poller on a TCP socket of
// its own, by one of the engine's pollers, while still reporting the engine
// that accepted or dialed it as its own.
func checkPoller(t *testing.T, e *Engine, c *Connection, fd int) {
	t.Helper()
	if fd < 0 {
		t.Errorf("connection has no descriptor")
	} else if keptByAcceptor(e, c) {
		if c.engine.parent != e {
			t.Errorf("descriptor %d is served by %p, want one of the engine's pollers", fd, c.engine)
		}
	} else if want := e.pollers[fd%len(e.pollers)]; c.engine != want {
		t.Errorf("descriptor %d is served by %p, want poller %p", fd, c.engine, want)
	}
	if c.Engine() != e {
		t.Error("Engine() does not report the engine the connection came from")
	}
}

// keptByAcceptor reports whether c was accepted by a poller on a TCP socket
// of its own under AcceptKernelHash, which keeps the connection rather than
// handing it on.
func keptByAcceptor(e *Engine, c *Connection) bool {
	if c.dialed || e.acceptPlacement != AcceptKernelHash {
		return false
	}
	if c.flags.has(flagUnix) {
		return false
	}
	for _, l := range e.listeners {
		if l.spread {
			return true
		}
	}
	return false
}

// Accepted connections go to the poller their descriptor picks, which hands
// their rounds to the engine's workers, and the stream pool is sized as it
// is without pollers.
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
	if pool, ok := server.taskPool.(*taskpool.TaskPool); !ok || pool.Name() != "pollers-accept-workers" {
		t.Fatalf("engine runs on %v, want its pool of workers", server.taskPool)
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

// PollBatchSize sizes every loop's wait apart from the task queue, and a batch
// smaller than what is ready leaves the rest for the next wait rather than
// losing it: with a batch of one, connections whose data arrives together
// are still all served.
func TestPollBatchSizeIsItsOwnSetting(t *testing.T) {
	const pollers, conns = 2, 16
	config := pollerConfig("poll-batch", pollers)
	config.PollBatchSize = 1
	server, addr := startEchoServer(t, config, HandlerFuncs{
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})
	for _, e := range append([]*Engine{server}, server.pollers...) {
		if e.pollBatchSize != 1 {
			t.Fatalf("a loop waits for %d events, want 1", e.pollBatchSize)
		}
		if e.maxEvents != config.MaxEvents {
			t.Fatalf("maxEvents %d, want MaxEvents %d left alone", e.maxEvents, config.MaxEvents)
		}
	}

	payload := []byte("one event per wait")
	start := make(chan struct{})
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
			<-start
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
	close(start)
	wg.Wait()
}

// Dialed connections, over TCP and over UDP, go to the pollers as accepted
// ones do.
func TestPollersServeDialedConnections(t *testing.T) {
	_, tcpAddr := startEchoServer(t, DefaultConfig(), echoHandler())
	_, udpAddr := startUDPServer(t, DefaultConfig(), echoHandler())

	replies := make(chan *Connection, 16)
	var client *Engine
	client, err := New(pollerConfig("pollers-dial", 4))
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

	handler := HandlerFuncs{Data: func(c *Connection, b []byte) {
		if string(b) == "dialed" {
			replies <- c
		}
	}}
	const perNetwork = 4
	dialed := make(chan error, 2*perNetwork)
	for _, target := range []struct{ network, addr string }{{"tcp4", tcpAddr}, {"udp4", udpAddr.String()}} {
		for i := 0; i < perNetwork; i++ {
			err := client.DialWithHandler(target.network, target.addr, 5*time.Second, handler, func(c *Connection, err error) {
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

// Without ReusePort a UDP listener's peers share its one socket, so they
// stay on one loop: the engine's own with EngineLoop, and otherwise the
// first poller's.
func TestPollersLeaveUDPPeersOnOneLoop(t *testing.T) {
	for _, engineLoop := range []bool{true, false} {
		t.Run(fmt.Sprintf("EngineLoop=%v", engineLoop), func(t *testing.T) {
			testPollersLeaveUDPPeersOnOneLoop(t, engineLoop)
		})
	}
}

func testPollersLeaveUDPPeersOnOneLoop(t *testing.T, engineLoop bool) {
	config := pollerConfig("pollers-udp", 2)
	config.ReusePort = false
	config.EngineLoop = engineLoop
	var server *Engine
	var ready sync.WaitGroup
	ready.Add(1)
	server, addr := startUDPServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		ready.Wait()
		want := server
		if !engineLoop {
			want = server.pollers[0]
		}
		if c.engine != want {
			t.Errorf("a UDP peer is served by %p, want %p", c.engine, want)
		}
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	ready.Done()
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

// A worker recovers a handler that panics: its connection is closed, and the
// poller goes on serving the others.
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
	config.MaxPendingBytes = 1000
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
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
	loops := []*Engine{server.pollers[0], server.pollers[1]}
	if !server.noLoop {
		// An engine without a loop of its own is marked awake for good.
		loops = append(loops, server)
	}
	for _, e := range loops {
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
	// The engine's loop hands connections out by descriptor, where pollers
	// sharing one listening socket might all take theirs on one poller.
	config.EngineLoop = true
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

// A poller only waits for events and hands its connections' rounds to the
// engine's workers, so a handler that blocks leaves the other connections on
// its loop running.
func TestPollersRunRoundsOnWorkers(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	server, addr := startEchoServer(t, pollerConfig("pollers-workers", 1), HandlerFuncs{
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
	pool, ok := server.taskPool.(*taskpool.TaskPool)
	if !ok || pool.Mode() == taskpool.ModeInline || pool.Name() != "pollers-workers-workers" {
		t.Fatalf("connections ran on %v, want the engine's pool of workers", server.taskPool)
	}
}

// The default configuration spreads connections over pollers on any number
// of CPUs, and hands their rounds to the engine's pool of workers.
func TestDefaultConfigHasPollersAndWorkerPool(t *testing.T) {
	config := DefaultConfig()
	if !config.IOPollers || config.TaskPoolMode == taskpool.ModeInline {
		t.Fatalf("DefaultConfig on %d CPUs has IOPollers=%v, TaskPoolMode=%v",
			defaultCPUs(), config.IOPollers, config.TaskPoolMode)
	}
	config.Name = "default-config"
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	want := 0
	if pollersSupported {
		want = defaultPollerCount(defaultCPUs())
	}
	if len(server.pollers) != want {
		t.Fatalf("default engine has %d pollers, want %d", len(server.pollers), want)
	}
	pool, ok := server.taskPool.(*taskpool.TaskPool)
	if !ok || pool.Mode() != config.TaskPoolMode || pool.Name() != "default-config-workers" {
		t.Fatalf("default engine runs on %v, want its pool of workers", server.taskPool)
	}
}

// A batch reads what the socket holds in one receive, each datagram with its
// sender, and says the socket is empty when it comes back short; read one at
// a time, as when the kernel refuses a batch, it cannot tell.
func TestUDPBatchReceive(t *testing.T) {
	for _, single := range []bool{false, true} {
		t.Run(fmt.Sprintf("single=%v", single), func(t *testing.T) {
			server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			raw, err := server.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			peers := make([]*net.UDPConn, 3)
			for i := range peers {
				if peers[i], err = net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
				defer peers[i].Close()
				if _, err := peers[i].Write([]byte(fmt.Sprintf("datagram %d", i))); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(50 * time.Millisecond)
			b := udpbatch.New()
			b.SetSingle(single)
			var got []string
			var empty bool
			_ = raw.Read(func(fd uintptr) bool {
				for len(got) < len(peers) {
					n, e, err := b.Recv(int(fd), udpbatch.Size, false)
					if err != nil {
						t.Fatal(err)
					}
					empty = e
					for i := 0; i < n; i++ {
						key, _, ok := rawSockaddrKey(b.Addr(i))
						if !ok {
							t.Fatal("no sender address")
						}
						got = append(got, fmt.Sprintf("%s from %v", b.Datagram(i), key))
					}
				}
				return true
			})
			for i, peer := range peers {
				if want := fmt.Sprintf("datagram %d from %v", i, peer.LocalAddr()); got[i] != want {
					t.Fatalf("datagram %d read as %q, want %q", i, got[i], want)
				}
			}
			if empty == single {
				t.Fatalf("empty %v after the last datagram, reading one at a time %v", empty, single)
			}
			if _, _, err := b.Recv(int(fdOf(t, raw)), udpbatch.Size, false); err != syscall.EAGAIN {
				t.Fatalf("a receive from the empty socket: %v", err)
			}
		})
	}
}

func fdOf(t *testing.T, raw syscall.RawConn) uintptr {
	var fd uintptr
	if err := raw.Control(func(f uintptr) { fd = f }); err != nil {
		t.Fatal(err)
	}
	return fd
}

// Datagrams that pile up from many peers faster than a round reads them,
// several batches of them, all reach their peers' connections, and each
// reply its peer. The burst stays well inside the smallest socket buffer a
// kernel gives by default, 208KB on Linux, so that none is dropped on the
// way in.
func TestUDPBurstFromManyPeers(t *testing.T) {
	_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{
		Data: func(c *Connection, b []byte) { _ = c.Send(append([]byte(c.RemoteAddr().String()+" "), b...)) },
	})
	const peers, each = 4, 40
	conns := make([]*net.UDPConn, peers)
	for i := range conns {
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetReadBuffer(1 << 20)
		conns[i] = conn
	}
	for j := 0; j < each; j++ {
		for _, conn := range conns {
			if _, err := conn.Write([]byte(fmt.Sprintf("%d", j))); err != nil {
				t.Fatal(err)
			}
		}
	}
	buf := make([]byte, 128)
	for _, conn := range conns {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		for j := 0; j < each; j++ {
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("reply %d of %d: %v", j, each, err)
			}
			if want := fmt.Sprintf("%s %d", conn.LocalAddr(), j); string(buf[:n]) != want {
				t.Fatalf("reply %q, want %q", buf[:n], want)
			}
		}
	}
}

// SendBatch sends each datagram whole, in order, to the peer's address from
// a listener and on a dialed socket alike, batched or, where the kernel
// refuses batches, one at a time; SendDatagrams does too, and says it sent
// them all.
func TestUDPSendBatch(t *testing.T) {
	for _, single := range []bool{false, true} {
		t.Run(fmt.Sprintf("single=%v", single), func(t *testing.T) {
			defer udpbatch.SetSingleSends(udpbatch.SetSingleSends(single))
			batch := make([][]byte, udpbatch.Size+5)
			for i := range batch {
				batch[i] = []byte(fmt.Sprintf("datagram %d", i))
			}
			// A listener answers each peer's first datagram with a batch.
			_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{
				Data: func(c *Connection, b []byte) {
					if err := c.SendBatch(batch); err != nil {
						t.Error(err)
					}
				},
			})
			peer, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			_ = peer.SetReadBuffer(1 << 20)
			if _, err := peer.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 64)
			_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
			for i := range batch {
				n, err := peer.Read(buf)
				if err != nil {
					t.Fatal(err)
				}
				if string(buf[:n]) != string(batch[i]) {
					t.Fatalf("datagram %d arrived as %q", i, buf[:n])
				}
			}
			// A dialed connection sends its batch to the socket it dialed.
			sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			_ = sink.SetReadBuffer(1 << 20)
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
			if n, err := client.SendDatagrams(batch); err != nil || n != len(batch) {
				t.Fatalf("SendDatagrams sent %d of %d: %v", n, len(batch), err)
			}
			_ = sink.SetReadDeadline(time.Now().Add(5 * time.Second))
			for i := range batch {
				n, _, err := sink.ReadFromUDP(buf)
				if err != nil {
					t.Fatal(err)
				}
				if string(buf[:n]) != string(batch[i]) {
					t.Fatalf("dialed datagram %d arrived as %q", i, buf[:n])
				}
			}
		})
	}
}

// Descriptors are recycled by the kernel, and the connection table is indexed
// by descriptor. A new connection landing on a closed one's descriptor must be
// reached by its own events, and must not inherit anything from its predecessor.
func TestRecycledDescriptorGetsFreshConnection(t *testing.T) {
	var mu sync.Mutex
	tokensByFD := map[int][]uint64{}
	config := DefaultConfig()
	_, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			tokensByFD[c.FD()] = append(tokensByFD[c.FD()], c.token())
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})

	// Serially open, use and close connections. Closing each before opening the
	// next makes the kernel hand the same descriptor back repeatedly.
	payload := []byte("recycled")
	reply := make([]byte, len(payload))
	for i := 0; i < 24; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(reply, payload) {
			t.Fatalf("round %d: echo mismatch", i)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		// Give the loop a moment to process the close before the next dial, so
		// the descriptor is actually free to be reused.
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	reused := false
	for fd, tokens := range tokensByFD {
		if len(tokens) > 1 {
			reused = true
		}
		seen := map[uint64]bool{}
		for _, token := range tokens {
			if seen[token] {
				t.Fatalf("descriptor %d handed out token %d twice", fd, token)
			}
			seen[token] = true
			if int(uint32(token)) != fd {
				t.Fatalf("token %d does not encode descriptor %d", token, fd)
			}
		}
	}
	if !reused {
		t.Skip("kernel never recycled a descriptor; nothing was exercised")
	}
}

// A stale token must not resolve, even to a live connection on the same
// descriptor. This is what the generation half of the token buys.
//
// The lookups run inside OnOpen because the connection table belongs to the
// event loop; checking it from the test goroutine would be the race it is
// meant to rule out.
func TestConnectionForRejectsStaleToken(t *testing.T) {
	type failure struct{ msg string }
	checked := make(chan failure, 1)
	config := DefaultConfig()
	_, addr := startEchoServer(t, config, HandlerFuncs{Open: func(c *Connection) {
		// Reach the server through the connection rather than through a
		// variable the test goroutine is still assigning.
		server := c.engine
		report := func(msg string) {
			select {
			case checked <- failure{msg}:
			default:
			}
		}
		switch {
		case server.connectionFor(c.token()) != c:
			report("connectionFor(live token) did not return the live connection")
		// Same descriptor, different generation.
		case server.connectionFor(c.token()^(1<<32)) != nil:
			report("connectionFor(stale token) resolved to a connection")
		// A descriptor past the end of the table must not panic.
		case server.connectionFor(uint64(uint32(len(server.connections)+100))) != nil:
			report("connectionFor(out-of-range descriptor) resolved to a connection")
		default:
			report("")
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case got := <-checked:
		if got.msg != "" {
			t.Fatal(got.msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the connection")
	}
}

func TestPriorityDataUsesDedicatedCallback(t *testing.T) {
	regular := make(chan []byte, 1)
	priority := make(chan []byte, 1)
	config := DefaultConfig()
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
		Data:         func(_ *Connection, data []byte) { regular <- append([]byte(nil), data...) },
		PriorityData: func(_ *Connection, data []byte) { priority <- append([]byte(nil), data...) },
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.DialTCP("tcp4", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var sendErr error
	if err := raw.Control(func(fd uintptr) {
		_, sendErr = syscall.SendmsgN(int(fd), []byte("!"), nil, nil, syscall.MSG_OOB)
	}); err != nil {
		t.Fatal(err)
	}
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	if _, err := conn.Write([]byte("normal")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-priority:
		if !bytes.Equal(data, []byte("!")) {
			t.Fatalf("priority data = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for priority data")
	}
	select {
	case data := <-regular:
		if !bytes.Equal(data, []byte("normal")) {
			t.Fatalf("regular data = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for regular data")
	}
	_ = conn.Close()
	server.Stop()
	if err := <-runDone; err != nil {
		t.Error(err)
	}
	if err := server.Close(); err != nil {
		t.Error(err)
	}
}

// newOfflineConnection builds a connection that is not attached to a socket,
// for exercising queue bookkeeping directly.
func newOfflineConnection(e *Engine) *Connection {
	c := &Connection{engine: e}
	c.fd.Store(-1)
	return c
}

// A round that finds output queued leaves its read for later, and a write edge
// is not always coming to pick it up: another goroutine's Flush, between two
// writes with the lock let go, keeps the round from flushing for itself, and
// then writes everything without the socket ever filling. Here that Flush is
// held at that point while the peer's next bytes arrive; the drain that
// follows has to hand the read back.
func TestReadDeferredBehindAnotherGoroutinesFlush(t *testing.T) {
	sentB := make(chan struct{})
	parked := make(chan *Connection, 1)
	handler := HandlerFuncs{
		Data: func(c *Connection, data []byte) {
			switch string(data) {
			case "a":
				c.mu.Lock()
				c.flushing = true
				c.mu.Unlock()
				// Output the peer does not wait for, queued behind the flush.
				_ = c.Send([]byte("x"))
				<-sentB
				if !waitFor(func() bool {
					c.mu.Lock()
					defer c.mu.Unlock()
					return uint32(c.pendingEvents)&evIn != 0
				}) {
					t.Error("the event loop never noted the next read")
				}
				parked <- c
			case "b":
				_ = c.Send([]byte("r"))
			}
		},
	}
	// The handler waits for the loop to note the next read, which a loop
	// running the round inline never would.
	config := DefaultConfig()
	config.IOPollers = false
	_, addr := startEchoServer(t, config, handler)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = conn.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	// Let the round deliver "a" alone before "b" follows.
	time.Sleep(50 * time.Millisecond)
	if _, err = conn.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	close(sentB)
	c := <-parked
	if !waitFor(func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return !c.scheduled
	}) {
		t.Fatal("the round never ended")
	}
	// The Flush takes the lock again and writes "x" whole.
	c.mu.Lock()
	c.flushing = false
	c.mu.Unlock()
	if err = c.flushOutput(); err != nil {
		t.Fatal(err)
	}
	var got []byte
	buf := make([]byte, 16)
	for !bytes.Contains(got, []byte("r")) {
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("got %q, then %v: the read left for the drain was never done", got, err)
		}
		got = append(got, buf[:n]...)
	}
}

// waitFor polls done for up to five seconds and reports whether it came true.
func waitFor(done func() bool) bool {
	for deadline := time.Now().Add(5 * time.Second); !done(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			return false
		}
	}
	return true
}

// TCP connections, accepted or dialed, must have Nagle off from the start.
func TestTCPConnectionsSetNoDelay(t *testing.T) {
	for _, pollers := range []bool{false, true} {
		config := DefaultConfig()
		config.IOPollers = pollers
		accepted := make(chan int, 1)
		_, addr := startEchoServer(t, config, HandlerFuncs{Open: func(c *Connection) {
			v, _ := syscall.GetsockoptInt(c.FD(), syscall.IPPROTO_TCP, syscall.TCP_NODELAY)
			accepted <- v
		}})
		client, _ := startEchoServer(t, config, HandlerFuncs{})
		dialed := make(chan int, 1)
		if err := client.Dial("tcp4", addr, 5*time.Second, func(c *Connection, err error) {
			if err != nil {
				t.Error(err)
				dialed <- 0
				return
			}
			v, _ := syscall.GetsockoptInt(c.FD(), syscall.IPPROTO_TCP, syscall.TCP_NODELAY)
			dialed <- v
		}); err != nil {
			t.Fatal(err)
		}
		if v := <-dialed; v == 0 {
			t.Errorf("IOPollers=%v: dialed connection has TCP_NODELAY off", pollers)
		}
		if v := <-accepted; v == 0 {
			t.Errorf("IOPollers=%v: accepted connection has TCP_NODELAY off", pollers)
		}
	}
}

func sockoptInt(t *testing.T, c *Connection, level, opt int) int {
	t.Helper()
	v, err := syscall.GetsockoptInt(c.FD(), level, opt)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// The TCP options land on the socket, whichever way they point.
func TestTCPOptions(t *testing.T) {
	c, _ := acceptOne(t, "tcp", HandlerFuncs{})
	checkProtocol(t, c, ProtocolTCP)
	checkDialed(t, c, false)
	if err := c.SetNoDelay(false); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, syscall.TCP_NODELAY); v != 0 {
		t.Fatalf("TCP_NODELAY = %d after SetNoDelay(false)", v)
	}
	if err := c.SetNoDelay(true); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, syscall.TCP_NODELAY); v == 0 {
		t.Fatal("TCP_NODELAY off after SetNoDelay(true)")
	}

	if err := c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: 30 * time.Second,
		Interval: 4500 * time.Millisecond, Count: 4}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name       string
		level, opt int
		value      int
	}{
		{"SO_KEEPALIVE", syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1},
		{"idle", syscall.IPPROTO_TCP, tcpKeepIdle, 30},
		{"interval", syscall.IPPROTO_TCP, tcpKeepInterval, 5},
		{"count", syscall.IPPROTO_TCP, tcpKeepCount, 4},
	} {
		if v := sockoptInt(t, c, want.level, want.opt); (v != 0) != (want.value != 0) || want.value > 1 && v != want.value {
			t.Errorf("%s = %d, want %d", want.name, v, want.value)
		}
	}
	// A negative time leaves the setting alone, and zero takes the default.
	if err := c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: -1, Interval: 0, Count: -1}); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepIdle); v != 30 {
		t.Errorf("idle = %d after a negative Idle, want 30", v)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepInterval); v != 15 {
		t.Errorf("interval = %d after a zero Interval, want 15", v)
	}
	if err := c.SetKeepAlivePeriod(time.Minute); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.IPPROTO_TCP, tcpKeepIdle); v != 60 {
		t.Errorf("idle = %d after SetKeepAlivePeriod(time.Minute)", v)
	}
	if err := c.SetKeepAlive(false); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE); v != 0 {
		t.Error("SO_KEEPALIVE on after SetKeepAlive(false)")
	}

	for _, sec := range []int{-1, 0, 5} {
		if err := c.SetLinger(sec); err != nil {
			t.Fatalf("SetLinger(%d): %v", sec, err)
		}
	}
	if err := c.SetReadBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_RCVBUF); v < 64<<10 {
		t.Errorf("SO_RCVBUF = %d", v)
	}
	if err := c.SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_SNDBUF); v < 64<<10 {
		t.Errorf("SO_SNDBUF = %d", v)
	}
	if mptcp, err := c.MultipathTCP(); err != nil || mptcp {
		t.Errorf("MultipathTCP() = %v, %v", mptcp, err)
	}
}

// File is a duplicate: closing it leaves the connection working.
func TestTCPFileAndSyscallConn(t *testing.T) {
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	f, err := c.File()
	if err != nil {
		t.Fatal(err)
	}
	if int(f.Fd()) == c.FD() {
		t.Fatal("File returned the connection's own descriptor")
	}
	f.Close()

	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd uintptr
	if err := raw.Control(func(s uintptr) { fd = s }); err != nil || int(fd) != c.FD() {
		t.Fatalf("Control saw %d, %v; want %d", fd, err, c.FD())
	}
	if err := raw.Write(func(s uintptr) bool {
		_, err := syscall.Write(int(s), []byte("raw"))
		return err == nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("sent")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 7)
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil || string(got) != "rawsent" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

// TCP's own options do nothing on a Unix socket, which still takes the
// options every socket has.
func TestTCPOptionsOnUnixSocket(t *testing.T) {
	c, _ := acceptOne(t, "unix", HandlerFuncs{})
	checkProtocol(t, c, ProtocolUnix)
	checkDialed(t, c, false)
	for name, err := range map[string]error{
		"SetNoDelay":         c.SetNoDelay(false),
		"SetKeepAlive":       c.SetKeepAlive(true),
		"SetKeepAlivePeriod": c.SetKeepAlivePeriod(time.Second),
		"SetKeepAliveConfig": c.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true}),
		"SetLinger":          c.SetLinger(0),
		"SetReadBuffer":      c.SetReadBuffer(32 << 10),
		"SetWriteBuffer":     c.SetWriteBuffer(32 << 10),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if mptcp, err := c.MultipathTCP(); err != nil || mptcp {
		t.Errorf("MultipathTCP() = %v, %v", mptcp, err)
	}
	if v := sockoptInt(t, c, syscall.SOL_SOCKET, syscall.SO_SNDBUF); v < 32<<10 {
		t.Errorf("SO_SNDBUF = %d", v)
	}
}

// A UDP listener's peer has no socket of its own: the options and half-closes
// do nothing, and the socket cannot be handed out.
func TestTCPOptionsOnUDPPeer(t *testing.T) {
	opened := make(chan *Connection, 1)
	_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{Open: func(c *Connection) { opened <- c }})
	client, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	var c *Connection
	select {
	case c = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("no peer opened")
	}
	checkProtocol(t, c, ProtocolUDP)
	checkDialed(t, c, false)
	for name, err := range map[string]error{
		"SetNoDelay":     c.SetNoDelay(false),
		"SetKeepAlive":   c.SetKeepAlive(true),
		"SetLinger":      c.SetLinger(0),
		"SetReadBuffer":  c.SetReadBuffer(1 << 20),
		"SetWriteBuffer": c.SetWriteBuffer(1 << 20),
		"CloseRead":      c.CloseRead(),
		"CloseWrite":     c.CloseWrite(),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := c.File(); err == nil {
		t.Error("File on a UDP peer succeeded")
	}
	if _, err := c.SyscallConn(); err == nil {
		t.Error("SyscallConn on a UDP peer succeeded")
	}
}

func TestTCPOptionsOnClosedConnection(t *testing.T) {
	closed := make(chan struct{})
	c, _ := acceptOne(t, "tcp", HandlerFuncs{Close: func(*Connection, error) { close(closed) }})
	c.Close()
	<-closed
	if err := c.SetNoDelay(true); !errors.Is(err, net.ErrClosed) {
		t.Errorf("SetNoDelay on a closed connection: %v", err)
	}
	if err := c.CloseWrite(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("CloseWrite on a closed connection: %v", err)
	}
	if _, err := c.File(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("File on a closed connection: %v", err)
	}
}

// CloseWrite waits for the queue: the peer reads everything sent before it,
// then the end of the stream, while the connection goes on reading.
func TestCloseWriteAfterQueuedOutput(t *testing.T) {
	received := make(chan []byte, 16)
	c, client := acceptOne(t, "tcp", HandlerFuncs{Data: func(_ *Connection, b []byte) {
		received <- append([]byte(nil), b...)
	}})
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4MB, more than the socket takes
	if err := c.Send(payload); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("late")); !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("Send after CloseWrite: %v", err)
	}
	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("client read %d bytes, want %d", len(got), len(payload))
	}
	if _, err := client.Write([]byte("still reading")); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		if string(b) != "still reading" {
			t.Fatalf("server read %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server stopped reading after CloseWrite")
	}
}

// CloseAfterSendLinger, with the peer still sending, gets the peer everything
// sent and then the end of the stream, where a plain close with input unread
// would reset the connection; the connection closes once the linger runs out,
// with no error, though the peer never stops.
//
// The linger is 2 seconds, not the instant the peer's end needs, because the
// 4 MiB have to go out inside it: under the coverage job's instrumentation the
// same window is several times slower, and with 300ms a run of this test read
// a third of the payload before the connection closed as its linger said.
func TestCloseAfterSendLinger(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4MB, more than the socket takes
	closed := make(chan error, 1)
	var once sync.Once
	_, client := acceptOne(t, "tcp", HandlerFuncs{
		Data: func(c *Connection, _ []byte) {
			once.Do(func() {
				if err := c.Send(payload); err != nil {
					t.Error(err)
				}
				c.CloseAfterSendLinger(2 * time.Second)
			})
		},
		Close: func(_ *Connection, err error) { closed <- err },
	})
	go func() {
		// The peer goes on sending until the connection is gone.
		chunk := make([]byte, 64<<10)
		for {
			if _, err := client.Write(chunk); err != nil {
				return
			}
		}
	}()
	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("client read %d bytes, want %d", len(got), len(payload))
	}
	start := time.Now()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("OnClose: %v", err)
		}
		if waited := time.Since(start); waited > 5*time.Second {
			t.Fatalf("closed %v after the end of the stream", waited)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lingering connection never closed")
	}
}

// A lingering close that runs out of time closes even while the peer keeps
// the socket full: the round that drops what arrives ends with the linger,
// rather than once a read comes up short, which a peer sending faster than
// the round drops may never let happen.
func TestCloseAfterSendLingerFlood(t *testing.T) {
	const linger = 300 * time.Millisecond
	closed := make(chan error, 1)
	_, client := acceptOne(t, "tcp", HandlerFuncs{
		Data: func(c *Connection, _ []byte) {
			if err := c.Send([]byte("bye")); err != nil {
				t.Error(err)
			}
			c.CloseAfterSendLinger(linger)
		},
		Close: func(_ *Connection, err error) { closed <- err },
	})
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	if got, err := io.ReadAll(client); err != nil || string(got) != "bye" {
		t.Fatalf("client read %q, %v", got, err)
	}
	// The linger is under way: the peer now sends faster than it drops.
	start := time.Now()
	for i := 0; i < 8; i++ {
		go func() {
			chunk := make([]byte, 256<<10)
			for {
				if _, err := client.Write(chunk); err != nil {
					return
				}
			}
		}()
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("OnClose: %v", err)
		}
		if waited := time.Since(start); waited > linger+2*time.Second {
			t.Fatalf("closed %v after the end of the stream", waited)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lingering connection never closed")
	}
}

// A reply made in a read round reaches the peer while the peer keeps the
// socket full, and so does the end of output a CloseAfterSendLinger behind it
// makes: the round flushes as it reads on, rather than only once a read comes
// up short, which a peer sending faster than the round reads may never let
// happen.
func TestRoundFlushesWhilePeerFloods(t *testing.T) {
	var waited time.Duration
	for i := 0; i < 3; i++ {
		var once sync.Once
		_, client := acceptOne(t, "tcp", HandlerFuncs{
			Data: func(c *Connection, _ []byte) {
				once.Do(func() {
					if err := c.Send([]byte("bye")); err != nil {
						t.Error(err)
					}
					c.CloseAfterSendLinger(300 * time.Millisecond)
				})
			},
		})
		start := time.Now()
		for j := 0; j < 8; j++ {
			go func() {
				chunk := make([]byte, 256<<10)
				for {
					if _, err := client.Write(chunk); err != nil {
						return
					}
				}
			}()
		}
		client.SetReadDeadline(time.Now().Add(10 * time.Second))
		if got, err := io.ReadAll(client); err != nil || string(got) != "bye" {
			t.Fatalf("client read %q, %v", got, err)
		}
		waited += time.Since(start)
	}
	if waited > 2*time.Second {
		t.Fatalf("the replies and their ends took %v in all", waited)
	}
}

// CloseRead stops OnData, and the end of input that follows, from the peer
// half-closing, does not close the connection, which goes on sending.
func TestCloseRead(t *testing.T) {
	received := make(chan []byte, 16)
	closed := make(chan error, 1)
	c, client := acceptOne(t, "tcp", HandlerFuncs{
		Data:  func(_ *Connection, b []byte) { received <- append([]byte(nil), b...) },
		Close: func(_ *Connection, err error) { closed <- err },
	})
	if err := c.CloseRead(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" {
		// Darwin resets a connection that is sent data after shutting its
		// reading side, as it would a net.TCPConn's.
		if _, err := client.Write([]byte("ignored")); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		t.Fatalf("OnData after CloseRead: %q", b)
	case err := <-closed:
		t.Fatalf("the connection closed after CloseRead: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := c.Send([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil || string(got) != "reply" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

// A CloseWrite made in OnData, behind a reply that is still corked, shuts the
// side only once the round has flushed the reply.
func TestCloseWriteFromOnData(t *testing.T) {
	_, client := acceptOne(t, "tcp", HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(append([]byte("echo:"), b...)); err != nil {
			t.Error(err)
		}
		if err := c.CloseWrite(); err != nil {
			t.Error(err)
		}
	}})
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil || string(got) != "echo:ping" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

// checkProtocol checks that c reports p, and that exactly the matching Is
// method agrees.
func checkProtocol(t *testing.T, c *Connection, p Protocol) {
	t.Helper()
	if got := c.Protocol(); got != p {
		t.Errorf("Protocol() = %v, want %v", got, p)
	}
	if c.IsTCP() != (p == ProtocolTCP) || c.IsUDP() != (p == ProtocolUDP) || c.IsUnix() != (p == ProtocolUnix) {
		t.Errorf("%v connection: IsTCP=%v IsUDP=%v IsUnix=%v", p, c.IsTCP(), c.IsUDP(), c.IsUnix())
	}
}

// checkDialed checks that c reports whether it was dialed or accepted.
func checkDialed(t *testing.T, c *Connection, dialed bool) {
	t.Helper()
	if c.IsDialed() != dialed || c.IsAccepted() == dialed {
		t.Errorf("%v connection: IsDialed=%v IsAccepted=%v, want dialed=%v",
			c.Protocol(), c.IsDialed(), c.IsAccepted(), dialed)
	}
}

// Dialed connections report their protocol, and that they were dialed, and
// keep both once closed.
func TestDialedConnectionProtocol(t *testing.T) {
	_, tcpAddr := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	_, udpAddr := startUDPServer(t, DefaultConfig(), HandlerFuncs{})
	unixPath := filepath.Join(t.TempDir(), "s")
	unixConfig := DefaultConfig()
	unixConfig.Network = "unix"
	unixServer, err := New(unixConfig, Binding{Addr: unixPath, Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- unixServer.Run() }()
	t.Cleanup(func() {
		unixServer.Stop()
		<-runDone
		unixServer.Close()
	})
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	for _, d := range []struct {
		network, addr string
		want          Protocol
	}{
		{"tcp4", tcpAddr, ProtocolTCP},
		{"udp4", udpAddr.String(), ProtocolUDP},
		{"unix", unixPath, ProtocolUnix},
	} {
		dialed := make(chan *Connection, 1)
		if err := client.Dial(d.network, d.addr, 5*time.Second, func(c *Connection, err error) {
			if err != nil {
				t.Errorf("dial %s: %v", d.network, err)
			}
			dialed <- c
		}); err != nil {
			t.Fatal(err)
		}
		c := <-dialed
		if c == nil {
			continue
		}
		checkProtocol(t, c, d.want)
		checkDialed(t, c, true)
		c.Close()
		checkProtocol(t, c, d.want)
		checkDialed(t, c, true)
	}
}

// hangupHandler is a HangupHandler whose OnData waits, as a handler waiting
// on a database or another server does, until OnHangup tells it the peer has
// gone or a while has passed.
type hangupHandler struct {
	HandlerFuncs
	waiting chan struct{}
	hangups atomic.Int32
	hangup  chan struct{}
	waited  chan bool
	closed  chan error
}

func newHangupHandler() *hangupHandler {
	h := &hangupHandler{
		waiting: make(chan struct{}, 1),
		hangup:  make(chan struct{}),
		waited:  make(chan bool, 1),
		closed:  make(chan error, 1),
	}
	h.Data = func(*Connection, []byte) {
		h.waiting <- struct{}{}
		select {
		case <-h.hangup:
			h.waited <- true
		case <-time.After(5 * time.Second):
			h.waited <- false
		}
	}
	h.Close = func(_ *Connection, err error) { h.closed <- err }
	return h
}

func (h *hangupHandler) OnHangup(*Connection) {
	if h.hangups.Add(1) == 1 {
		close(h.hangup)
	}
}

// TestOnHangupReachesARunningRound checks that a peer that goes while its
// connection's round is running is reported to the handler then, rather than
// once the round reads it, and once; that the connection still closes, with
// OnClose, once the round is done; and that a peer going while no round is
// running is left to the round it starts.
func TestOnHangupReachesARunningRound(t *testing.T) {
	for _, pollers := range []bool{false, true} {
		t.Run(fmt.Sprintf("pollers=%v", pollers), func(t *testing.T) {
			serve := func(h Handler) string {
				config := DefaultConfig()
				config.IOPollers = pollers
				server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: h})
				if err != nil {
					t.Fatal(err)
				}
				addr, err := server.LocalAddr()
				if err != nil {
					t.Fatal(err)
				}
				runDone := make(chan error, 1)
				go func() { runDone <- server.Run() }()
				t.Cleanup(func() {
					server.Stop()
					<-runDone
					_ = server.Close()
				})
				return addr.String()
			}

			h := newHangupHandler()
			conn, err := net.DialTimeout("tcp", serve(h), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conn.Write([]byte("wait")); err != nil {
				t.Fatal(err)
			}
			<-h.waiting
			conn.Close()
			select {
			case waited := <-h.waited:
				if !waited {
					t.Fatal("OnData was never told its peer had gone")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("OnData never returned")
			}
			select {
			case err := <-h.closed:
				if err != io.EOF {
					t.Fatalf("OnClose was given %v, want io.EOF", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the connection never closed")
			}
			if n := h.hangups.Load(); n != 1 {
				t.Fatalf("OnHangup ran %d times", n)
			}

			// A peer that goes while its connection is idle is read by the
			// round its hangup starts.
			idle := newHangupHandler()
			idle.Data = nil
			conn, err = net.DialTimeout("tcp", serve(idle), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
			select {
			case <-idle.closed:
			case <-time.After(5 * time.Second):
				t.Fatal("the idle connection never closed")
			}
			if n := idle.hangups.Load(); n != 0 {
				t.Fatalf("OnHangup ran %d times for an idle connection", n)
			}
		})
	}
}
