//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"errors"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/taskpool"
)

// Sizes left at zero take their defaults, rather than giving an engine that
// reads nothing or pauses at once.
func TestBindFillsInZeroSizes(t *testing.T) {
	config := DefaultConfig()
	config.MaxEvents = 0
	config.ReadBufferSize = 0
	config.WriteBufferHighWatermark = 0
	config.Backlog = 0
	e, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.maxEvents != 256 || e.readBufferSize != 16<<10 || e.writeHighWatermark != 4<<10 {
		t.Fatalf("maxEvents %d, readBufferSize %d, writeHighWatermark %d", e.maxEvents, e.readBufferSize, e.writeHighWatermark)
	}
	if e.pollBatchSize != defaultPollBatchSize {
		t.Fatalf("pollBatchSize %d, want %d", e.pollBatchSize, defaultPollBatchSize)
	}
	if _, ok := e.Handler().(HandlerFuncs); !ok {
		t.Fatalf("a nil handler became %T", e.Handler())
	}
}

// A built-in pool with no workers, or of a mode that does not exist, is
// refused when the engine is made.
func TestBindRefusesAnUnusablePool(t *testing.T) {
	config := DefaultConfig()
	config.WorkerCount = 0
	if e, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}}); err == nil {
		e.Close()
		t.Error("an engine with no workers was made")
	}
	config = DefaultConfig()
	config.TaskPoolMode = taskpool.Mode(99)
	if e, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}}); err == nil {
		e.Close()
		t.Error("an engine with an invalid pool mode was made")
	}
}

// The queue that goes with a worker count stays within its bounds, however
// few or many workers there are.
func TestPoolSizingClampsMaxEvents(t *testing.T) {
	if got := poolSizing(1).MaxEvents; got != minMaxEvents {
		t.Errorf("one worker: MaxEvents %d, want %d", got, minMaxEvents)
	}
	if got := poolSizing(1 << 20).MaxEvents; got != maxMaxEvents {
		t.Errorf("a million workers: MaxEvents %d, want %d", got, maxMaxEvents)
	}
	if got := udpIdleTimeout(-time.Second); got != 0 {
		t.Errorf("a negative UDP idle timeout = %v, want it off", got)
	}
}

// An adaptive-chan pool is what that mode builds, and an engine serves on it.
func TestEngineOnAnAdaptiveChanPool(t *testing.T) {
	config := DefaultConfig()
	config.SharedTaskPool = false
	config.SetTaskPoolMode(taskpool.ModeAdaptiveChan)
	pool := newTaskPool(config)
	defer pool.Stop()
	if pool.Mode() != taskpool.ModeAdaptiveChan {
		t.Fatalf("newTaskPool built a %v pool", pool.Mode())
	}
	_, addr := startEchoServer(t, config, echoHandler())
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if got := readAllFrom(t, conn, 2); string(got) != "hi" {
		t.Fatalf("echo %q", got)
	}
}

// syncBuffer is a bytes.Buffer safe to log to from several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// With LogStatus an engine logs, once it runs, its name, where it listens,
// its pool and its pollers; a client engine listens nowhere.
func TestLogStatusLogsTheEngine(t *testing.T) {
	var logs syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)
	config := DefaultConfig()
	config.Name = "logged"
	config.LogStatus = true
	server, addr := startEchoServer(t, config, echoHandler())
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- client.Run() }()
	if !waitFor(func() bool { return strings.Count(logs.String(), "engine started") == 2 }) {
		t.Fatalf("logged %q", logs.String())
	}
	client.Stop()
	<-done
	client.Close()
	out := logs.String()
	if !strings.Contains(out, "tcp://"+addr) || !strings.Contains(out, "listen=none") || !strings.Contains(out, "engine=logged") {
		t.Fatalf("logged %q", out)
	}
	_ = server
}

// rejectingPool turns every task away, as a pool that has been stopped does.
type rejectingPool struct{}

func (rejectingPool) GoTask(taskpool.Task) bool   { return false }
func (rejectingPool) GoTasks([]taskpool.Task) int { return 0 }

// A connection the pool will not run is closed rather than left scheduled
// with nothing ever to run it, and the close that follows, whose round the
// pool will not run either, still finishes.
func TestConnectionsThePoolRefusesAreClosed(t *testing.T) {
	config := DefaultConfig()
	config.SetTaskPool(rejectingPool{})
	_, addr := startEchoServer(t, config, echoHandler())
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := conn.Read(make([]byte, 8)); err == nil {
		t.Fatalf("read %d bytes from a connection nothing runs", n)
	} else if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("the connection nothing runs was left open")
	}
}

// A Unix listener is not shared between pollers, so the engine accepts on it
// and hands each connection to a poller, which serves it as its own.
func TestUnixListenerHandsConnectionsToPollers(t *testing.T) {
	config := DefaultConfig()
	config.Network = "unix"
	config.IOPollers = true
	config.IOPollerCount = 2
	path := unixSocketPath(t)
	server, err := New(config, Binding{Addr: path, Handler: echoHandler()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Run() }()
	defer func() {
		server.Stop()
		<-done
		server.Close()
	}()
	for i := range 4 {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		msg := []byte{'a' + byte(i)}
		if _, err := conn.Write(msg); err != nil {
			t.Fatal(err)
		}
		if got := readAllFrom(t, conn, 1); !bytes.Equal(got, msg) {
			t.Fatalf("echo %q, want %q", got, msg)
		}
		conn.Close()
	}
}

// A UDP engine lists its UDP address among its listen addresses, and a TCP
// one has no UDP address to report.
func TestListenAddrsCoverUDP(t *testing.T) {
	server, addr := startUDPServer(t, DefaultConfig(), echoHandler())
	addrs, err := server.ListenAddrs()
	if err != nil || len(addrs) != 1 || addrs[0].String() != addr.String() {
		t.Fatalf("ListenAddrs = %v, %v; want [%v]", addrs, err, addr)
	}
	tcp := newOfflineServer(t)
	if _, err := tcp.LocalUDPAddr(); err != errNoListener {
		t.Fatalf("LocalUDPAddr of a TCP engine = %v, want errNoListener", err)
	}
	// An engine whose idle timeout is off never sweeps.
	tcp.udpIdleTimeout = 0
	tcp.sweepUDP()
}

// A UDP connection reports its own and its peer's addresses, on the server's
// side as on the dialer's, and its sends take any shape of datagram: empty
// sends nothing, parts go as one datagram, and a batch holding an empty
// datagram goes one Send at a time.
func TestUDPConnectionAddressesAndSends(t *testing.T) {
	type seen struct {
		local  net.Addr
		remote string
	}
	peers := make(chan seen, 8)
	_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{Data: func(c *Connection, b []byte) {
		peers <- seen{c.LocalAddr(), c.RemoteAddrPort().String()}
		_ = c.Send(b)
	}})
	got := make(chan string, 16)
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	handler := HandlerFuncs{Data: func(_ *Connection, b []byte) { got <- string(b) }}
	dialed := make(chan *Connection, 1)
	if err := client.DialWithHandler("udp", addr.String(), 5*time.Second, handler, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
		}
		dialed <- c
	}); err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if c == nil {
		t.FailNow()
	}
	if c.RemoteAddrPort().String() != addr.String() {
		t.Errorf("dialed RemoteAddrPort %v, want %v", c.RemoteAddrPort(), addr)
	}
	if local, ok := c.LocalAddr().(*net.UDPAddr); !ok || local.Port == 0 {
		t.Errorf("dialed LocalAddr %v", c.LocalAddr())
	}
	if err := c.Send(nil); err != nil {
		t.Fatalf("an empty Send = %v", err)
	}
	if _, err := c.Read(make([]byte, 8)); err != errUDPRead {
		t.Fatalf("Read on a UDP connection = %v, want errUDPRead", err)
	}
	sends := []func() error{
		func() error { return c.SendParts([]byte("first"), nil) },
		func() error { return c.SendParts(nil, []byte("second")) },
		func() error { return c.SendRawPooled(append(bufferpool.Get(8)[:0], "pooled"...)) },
		func() error { return c.SendBatch([][]byte{[]byte("one"), nil, []byte("two")}) },
	}
	want := []string{"first", "second", "pooled", "one", "two"}
	for _, send := range sends {
		if err := send(); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range want {
		select {
		case g := <-got:
			if g != w {
				t.Fatalf("echo %q, want %q", g, w)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no echo of %q", w)
		}
	}
	p := <-peers
	if local, ok := p.local.(*net.UDPAddr); !ok || local.Port != addr.Port {
		t.Errorf("the server's peer has LocalAddr %v, want port %d", p.local, addr.Port)
	}
	if p.remote != c.LocalAddr().String() {
		t.Errorf("the server's peer has RemoteAddrPort %s, want %v", p.remote, c.LocalAddr())
	}
}

// A dial with no network is a TCP dial, and one whose port cannot be looked
// up fails through done, from the resolver, rather than from Dial.
func TestDialNetworkDefaultsAndResolverFailure(t *testing.T) {
	_, addr := startEchoServer(t, DefaultConfig(), echoHandler())
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	results := make(chan error, 2)
	done := func(c *Connection, err error) {
		if c != nil {
			if !c.IsTCP() {
				err = errors.New("not TCP")
			}
			c.Close()
		}
		results <- err
	}
	if err := client.Dial("", addr, 5*time.Second, done); err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatalf("a dial with no network: %v", err)
	}
	if err := client.Dial("tcp", "localhost:no-such-port", 5*time.Second, done); err != nil {
		t.Fatalf("Dial = %v, want the failure reported through done", err)
	}
	select {
	case err := <-results:
		if err == nil {
			t.Fatal("a dial to a port that does not exist succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the failed dial was never reported")
	}
}

// A UDP dial the kernel refuses to connect, as Linux refuses the broadcast
// address to a socket without SO_BROADCAST, fails through done.
func TestUDPDialConnectFailure(t *testing.T) {
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	results := make(chan error, 1)
	if err := client.Dial("udp4", "255.255.255.255:9", 5*time.Second, func(c *Connection, err error) {
		if c != nil {
			c.Close()
		}
		results <- err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-results:
		if err == nil && runtime.GOOS == "linux" {
			t.Fatal("a UDP dial to the broadcast address connected")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dial was never reported")
	}
}

// A connected UDP socket learns from the ICMP its peer's host sends back that
// nothing listens there, and the batched send that follows reports it.
func TestUDPBatchReportsARefusedPeer(t *testing.T) {
	probe, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.LocalAddr().String()
	probe.Close()
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{})
	dialed := make(chan *Connection, 1)
	if err := client.Dial("udp4", addr, 5*time.Second, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
		}
		dialed <- c
	}); err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if c == nil {
		t.FailNow()
	}
	batch := [][]byte{[]byte("one"), []byte("two")}
	if !waitFor(func() bool { return c.SendBatch(batch) != nil }) {
		t.Skip("no ICMP port unreachable came back")
	}
}
