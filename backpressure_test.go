//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/taskpool"
)

// startEchoServer brings up a server on a loopback port and returns its address
// plus a shutdown function that fails the test if the loop errored.
func startEchoServer(t *testing.T, config Config, handler Handler) (*Engine, string) {
	t.Helper()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, handler)
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
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return server, addr.String()
}

// The server-wide budget is the bound that actually holds at high connection
// counts, where the per-connection watermark alone would admit its own limit
// times the connection count. Peers that stop reading must not be able to push
// the server past it.
func TestEngineWideBudgetBoundsQueuedBytes(t *testing.T) {
	const (
		budget      = 256 << 10
		connections = 8
		payload     = 32 << 10
	)
	config := DefaultConfig()
	// Give each connection room to buffer far more than the budget allows in
	// aggregate, so only the server-wide bound can hold the total down.
	config.WriteBufferHighWatermark = budget
	config.MaxPendingBytes = budget
	server, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		// Reply with far more than arrived, so a peer that never reads builds
		// a backlog quickly.
		for i := 0; i < 8; i++ {
			if err := c.Send(b); err != nil {
				c.Close()
				return
			}
		}
	}})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < connections; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Never read: the replies pile up in the server's queue and the
			// kernel's buffers until backpressure stops them.
			data := bytes.Repeat([]byte{'x'}, payload)
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := conn.Write(data); err != nil {
					return
				}
			}
		}()
	}

	// Sample the budget while the peers hammer it.
	deadline := time.Now().Add(3 * time.Second)
	var peak int64
	for time.Now().Before(deadline) {
		if pending := server.pendingTotal.Load(); pending > peak {
			peak = pending
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if peak == 0 {
		t.Fatal("no outbound backlog built up; the test never exercised the budget")
	}
	// The bound is checked before a round queues its replies, so one round's
	// worth of output may land on top of it. Allow that overshoot, but nothing
	// like the connections*watermark a per-connection bound alone would admit.
	limit := int64(budget) * 4
	if peak > limit {
		t.Fatalf("peak pending = %d bytes, want at most %d (budget %d)", peak, limit, budget)
	}
}

// A connection stopped only by the server-wide budget may have nothing of its
// own left to flush, so nothing of its own would ever re-evaluate it. It has to
// be resumed once the budget recovers, or it stalls forever.
func TestBudgetPausedConnectionResumes(t *testing.T) {
	config := DefaultConfig()
	config.WriteBufferHighWatermark = 64 << 10
	// A budget this small is exhausted by the first reply, so the reading
	// connection below is certain to be paused on the budget's account rather
	// than its own.
	config.MaxPendingBytes = 1
	_, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	// Each round trip has to survive the pause: the reply exhausts the budget,
	// the connection is parked, and only the resume path can bring it back for
	// the next request.
	request := bytes.Repeat([]byte{'p'}, 512)
	reply := make([]byte, len(request))
	for i := 0; i < 20; i++ {
		if _, err := conn.Write(request); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(reply, request) {
			t.Fatalf("round %d: echo mismatch", i)
		}
	}
}

// Chunks queued behind an undrained item must pack into that item's buffer
// rather than each taking one of their own. Before this, a connection under
// backpressure allocated a fresh array per reply, which is what made a
// 100k-connection rate test hold gigabytes. Packing stops at the buffer's
// capacity rather than growing past it, so what a connection holds tracks what
// it has actually queued instead of a full round's worth apiece.
func TestQueuedChunksPackIntoPooledBuffers(t *testing.T) {
	server := newOfflineServer(t)
	c := newOfflineConnection(server)

	chunk := bytes.Repeat([]byte{'z'}, 1024)
	const chunks = 16
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < chunks; i++ {
		c.queueLocked(chunk, nil)
	}

	// Every item must carry a pooled buffer, hold no more than it, and be
	// filled before the next one starts: that is what makes the footprint
	// proportional to the backlog.
	queued := 0
	for i := range c.sends {
		item := &c.sends[i]
		if !item.pooled {
			t.Fatalf("item %d has no pooled buffer to return", i)
		}
		if i < len(c.sends)-1 && len(item.data)+len(chunk) <= cap(item.data) {
			t.Fatalf("item %d holds %d of %d bytes; another chunk still fit",
				i, len(item.data), cap(item.data))
		}
		queued += len(item.data)
	}
	if want := chunks * len(chunk); queued != want {
		t.Fatalf("queue holds %d bytes, want %d", queued, want)
	}
	// The whole round still reaches the socket in one writev.
	if len(c.sends) > maxWritevItems {
		t.Fatalf("queue holds %d items, more than one writev takes (%d)", len(c.sends), maxWritevItems)
	}

	// Once the socket has consumed part of the trailing item, packing into it
	// would disturb the write in progress, so the next chunk starts a new one.
	before := len(c.sends)
	c.sends[before-1].offset = 1
	c.queueLocked(chunk, nil)
	if len(c.sends) != before+1 {
		t.Fatalf("queue holds %d items, want %d: a partially written item must not be packed into",
			len(c.sends), before+1)
	}
}

// Queued bytes, not buffer capacity, are what bounds this server's outbound
// memory. Sizing buffers to the backlog was tried and measured twice without
// moving the resident peak, so what the pool owes is simply that every buffer
// it hands out can hold a full round and comes back reusable.
func TestPooledSendBuffersHoldAFullRound(t *testing.T) {
	server := newOfflineServer(t)
	buf := server.acquireSendBuffer()
	if cap(buf) < server.sendBufferSize {
		t.Fatalf("pooled buffer holds %d bytes, want a full round of %d",
			cap(buf), server.sendBufferSize)
	}
	if len(buf) != 0 {
		t.Fatalf("pooled buffer came back holding %d bytes", len(buf))
	}

	// A chunk larger than a round still lands in one item, so a big message is
	// never split across buffers.
	c := newOfflineConnection(server)
	big := bytes.Repeat([]byte{'y'}, server.sendBufferSize*2)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueLocked(big, nil)
	if len(c.sends) != 1 {
		t.Fatalf("one large chunk produced %d items, want 1", len(c.sends))
	}
	if got := cap(c.sends[0].data); got < len(big) {
		t.Fatalf("large chunk took a buffer of %d, want at least %d", got, len(big))
	}
}

// Draining an item has to hand its buffer back to the pool, or the merge above
// only postpones the allocation to the next round.
func TestDrainedItemReturnsBufferToPool(t *testing.T) {
	server := newOfflineServer(t)
	c := newOfflineConnection(server)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueLocked(bytes.Repeat([]byte{'z'}, 1024), nil)
	if !c.sends[0].pooled {
		t.Fatal("queued item has no pooled buffer")
	}
	c.releaseItemLocked(&c.sends[0])
	if c.sends[0].pooled || c.sends[0].data != nil {
		t.Fatal("released item still references its buffer")
	}
	// The pool is a sync.Pool, which may drop entries at any GC, so the only
	// guarantee worth asserting is that what comes back is still a buffer a
	// whole round fits in.
	if got := server.acquireSendBuffer(); len(got) != 0 || cap(got) < server.sendBufferSize {
		t.Fatalf("pooled buffer came back holding %d of %d bytes", len(got), cap(got))
	}
}

// newOfflineServer builds a server for exercising connection bookkeeping
// directly, without running its event loop.
func newOfflineServer(t *testing.T) *Engine {
	t.Helper()
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server
}

// An outsized buffer goes back to the pool like any other, but to the class
// its capacity belongs to: a round's replies must never be handed a buffer one
// large message inflated, or every connection would hold that much.
func TestOutsizedSendBufferGoesBackToItsOwnClass(t *testing.T) {
	server := newOfflineServer(t)
	c := newOfflineConnection(server)
	oversized := bufferpool.Get(server.sendBufferSize * 2)[:0]
	item := sendItem{data: oversized, pooled: true}
	c.mu.Lock()
	c.releaseItemLocked(&item)
	c.mu.Unlock()
	if item.pooled || item.data != nil {
		t.Fatal("released item still references its buffer")
	}
	// A round's buffer comes from the class a round asks for, whatever the
	// oversized one was returned to.
	for range 8 {
		if got := server.acquireSendBuffer(); cap(got) != server.sendBufferSize {
			t.Fatalf("a round was given a buffer of cap %d, want %d", cap(got), server.sendBufferSize)
		}
	}
}

// Backpressure must not corrupt the stream: a peer that reads slowly still has
// to receive every byte, in order, regardless of how often reads were paused.
func TestPausedReadsPreserveStreamIntegrity(t *testing.T) {
	const payload = 512 << 10
	config := DefaultConfig()
	config.WriteBufferHighWatermark = 8 << 10
	config.MaxPendingBytes = 32 << 10
	_, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	// A recognisable pattern so a reorder or a gap shows up as a mismatch.
	sent := make([]byte, payload)
	for i := range sent {
		sent[i] = byte(i % 251)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(sent)
		writeErr <- err
	}()

	got := make([]byte, payload)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-writeErr; err != nil && err != syscall.EPIPE {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, sent) {
		for i := range got {
			if got[i] != sent[i] {
				t.Fatalf("echo diverges at byte %d: got %d, want %d", i, got[i], sent[i])
			}
		}
	}
}

// A strict ping-pong peer holds one reply at a time, so a watermark set above
// one reply must not pause its reads however long it runs. This is the shape of
// the echo benchmark, and the property is easy to lose: the reply is queued
// rather than written while the read round is corked, so it passes through the
// pending counter that the watermark is compared against even when the socket
// could have taken it immediately.
func TestPingPongUnderWatermarkNeverPausesReads(t *testing.T) {
	const (
		watermark = 8 << 10
		message   = 1 << 10
		header    = 6
		rounds    = 200
	)
	for _, mode := range []struct {
		name                      string
		useWritev, socketSyscalls bool
	}{{"writev", true, false}, {"write", false, false}, {"sendmsg", true, true}, {"sendto", false, true}} {
		t.Run(mode.name, func(t *testing.T) {
			config := DefaultConfig()
			config.WriteBufferHighWatermark = watermark
			// Leave the server-wide budget off: one connection cannot exhaust
			// it, and it would pause reads for reasons this test is not about.
			config.MaxPendingBytes = 0
			config.UseWritev = mode.useWritev
			config.SocketSyscalls = mode.socketSyscalls
			server, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
				// Reply in two parts, as a framed protocol does.
				if err := c.SendParts(bytes.Repeat([]byte{'h'}, header), b); err != nil {
					c.Close()
				}
			}})

			conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			payload := bytes.Repeat([]byte{'x'}, message)
			reply := make([]byte, header+message)
			for round := 0; round < rounds; round++ {
				if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Write(payload); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
				if _, err := io.ReadFull(conn, reply); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
			}

			stats := server.Stats()
			if stats.ReadsPausedByWatermark != 0 || stats.ReadsPausedByBudget != 0 {
				t.Fatalf("reads were paused %d times by the watermark and %d by the budget over %d ping-pong rounds "+
					"of %d-byte replies under a %d-byte watermark, want none",
					stats.ReadsPausedByWatermark, stats.ReadsPausedByBudget, rounds, header+message, watermark)
			}
			// Nothing may be left owing either, or the counter would creep up
			// across rounds and trip the watermark on a later one. A reply is
			// discounted just after the write that hands it to the socket, so
			// the client can be reading the last one while the server has yet
			// to run those few instructions: wait for the counter to settle
			// instead of reading it the moment the last byte lands. A leak
			// never settles, so this still catches one.
			deadline := time.Now().Add(5 * time.Second)
			pending := server.Stats().PendingBytes
			for pending != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				pending = server.Stats().PendingBytes
			}
			if pending != 0 {
				t.Fatalf("pending = %d bytes after %d drained rounds, want 0", pending, rounds)
			}
		})
	}
}

// The other half of the same story: a connection far below its own watermark is
// still stopped when the server-wide budget is gone, because that budget is
// shared. Stats has to attribute the pause to the budget rather than the
// watermark, since that is the only way to tell the two apart from outside.
func TestStatsAttributesBudgetPausesToTheBudget(t *testing.T) {
	const (
		watermark = 1 << 20 // far above anything one connection here will hold
		budget    = 128 << 10
		peers     = 8
		payload   = 32 << 10
	)
	config := DefaultConfig()
	config.WriteBufferHighWatermark = watermark
	config.MaxPendingBytes = budget
	server, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < peers; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Never read, so the replies pile into the shared budget.
			data := bytes.Repeat([]byte{'x'}, payload)
			// Short enough that a writer blocked by the pause this test is
			// waiting for gives up promptly once stop is closed.
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := conn.Write(data); err != nil {
					return
				}
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for server.Stats().ReadsPausedByBudget == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	stats := server.Stats()
	if stats.ReadsPausedByBudget == 0 {
		t.Fatalf("no pause was attributed to the budget; stats = %+v", stats)
	}
}

// What is sent on a corked connection away from its rounds waits for Flush,
// and reaches the peer in one piece once it comes.
func TestCorkHoldsSendsUntilFlush(t *testing.T) {
	opened := make(chan *Connection, 1)
	addr := startServer(t, "tcp", "127.0.0.1:0", HandlerFuncs{Open: func(c *Connection) { opened <- c }})
	peer, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	c := <-opened
	c.Cork()
	for _, part := range []string{"one ", "two ", "three"} {
		if err := c.Send([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 64)
	_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := peer.Read(buf); n > 0 || err == nil {
		t.Fatalf("read %q before Flush", buf[:n])
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(peer, buf[:len("one two three")]); err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:len("one two three")]); got != "one two three" {
		t.Fatalf("read %q", got)
	}
	// Uncorked again, a send goes out on its own.
	if err := c.Send([]byte("four")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(peer, buf[:4]); err != nil || string(buf[:4]) != "four" {
		t.Fatalf("read %q, %v", buf[:4], err)
	}
}

func TestEngineServesOnAdaptivePool(t *testing.T) {
	config := DefaultConfig()
	config.SetTaskPoolMode(taskpool.ModeAdaptive)
	config.MinWorkerCount = 2
	config.SharedTaskPool = false
	_, addr := startEchoServer(t, config, echoHandler())
	payload := bytes.Repeat([]byte("adaptive"), 1024)
	for i := 0; i < 8; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reply, payload) {
			t.Fatal("echo mismatch")
		}
		conn.Close()
	}
}

func TestBindRejectsAdaptiveFloorAboveCeiling(t *testing.T) {
	for _, floor := range []int{-1, 65} {
		config := DefaultConfig()
		config.Addr = "127.0.0.1:0"
		config.SetTaskPoolMode(taskpool.ModeAdaptive)
		config.SetPoolSizing(64, 0)
		config.MinWorkerCount = floor
		if server, err := Bind(config, nil); err == nil {
			server.Close()
			t.Fatalf("Bind accepted MinWorkerCount %d with WorkerCount 64", floor)
		}
	}
}

// TestHoldReadsStopsAndResumesDelivery checks the read-side counterpart of the
// write watermarks: a handler that holds reads stops hearing from its peer
// however much the peer sends, and releasing the hold delivers what the socket
// kept without the peer having to send anything more.
func TestHoldReadsStopsAndResumesDelivery(t *testing.T) {
	const payload = 512 << 10
	var (
		mu       sync.Mutex
		received []byte
		held     atomic.Bool
		holder   atomic.Pointer[Connection]
	)
	delivered := make(chan int, 64)
	handler := HandlerFuncs{
		Data: func(c *Connection, data []byte) {
			mu.Lock()
			received = append(received, data...)
			total := len(received)
			mu.Unlock()
			if held.CompareAndSwap(false, true) {
				// Stop after the first read, with the rest still in flight.
				holder.Store(c)
				c.HoldReads(true)
			}
			delivered <- total
		},
	}
	_, addr := startEchoServer(t, DefaultConfig(), handler)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	body := bytes.Repeat([]byte("hold"), payload/4)
	writeDone := make(chan error, 1)
	go func() { _, err := conn.Write(body); writeDone <- err }()

	// Wait for the hold, then check that nothing more is delivered under it.
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("no data was delivered")
	}
	var before int
	deadline := time.After(300 * time.Millisecond)
	quiet := false
	for !quiet {
		select {
		case before = <-delivered:
		case <-deadline:
			quiet = true
		}
	}
	mu.Lock()
	stalled := len(received)
	mu.Unlock()
	if stalled == len(body) {
		t.Skip("the whole payload fitted in one read round; nothing was left to hold back")
	}
	if before > stalled {
		t.Fatalf("delivery reported %d bytes with only %d received", before, stalled)
	}

	// Releasing the hold must deliver what the socket kept, although the peer
	// has sent nothing since.
	holder.Load().HoldReads(false)
	for {
		mu.Lock()
		total := len(received)
		mu.Unlock()
		if total == len(body) {
			break
		}
		select {
		case <-delivered:
		case <-time.After(10 * time.Second):
			t.Fatalf("after the hold was released %d of %d bytes arrived", total, len(body))
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(received, body) {
		t.Fatal("the bytes delivered around the hold do not match what was sent")
	}
}

// Large echoes over TCP and Unix sockets, which queue replies, write them in
// pieces and read in several calls, come back intact whichever set of system
// calls Config.SocketSyscalls picks, and with writev on or off.
func TestSocketSyscallsEcho(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, mode := range []struct {
			name                      string
			useWritev, socketSyscalls bool
		}{{"writev", true, false}, {"write", false, false}, {"sendmsg", true, true}, {"sendto", false, true}} {
			t.Run(network+"/"+mode.name, func(t *testing.T) {
				config := DefaultConfig()
				config.Network = network
				config.Addr = "127.0.0.1:0"
				if network == "unix" {
					config.Addr = unixSocketPath(t)
				}
				config.UseWritev = mode.useWritev
				config.SocketSyscalls = mode.socketSyscalls
				server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
					if c.Send(b) != nil {
						c.Close()
					}
				}})
				if err != nil {
					t.Fatal(err)
				}
				runDone := make(chan error, 1)
				go func() { runDone <- server.Run() }()
				t.Cleanup(func() {
					server.Stop()
					if err := <-runDone; err != nil {
						t.Error(err)
					}
					_ = server.Close()
				})
				addrs, err := server.ListenAddrs()
				if err != nil {
					t.Fatal(err)
				}

				var wg sync.WaitGroup
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func(value byte) {
						defer wg.Done()
						conn, err := net.DialTimeout(network, addrs[0].String(), 5*time.Second)
						if err != nil {
							t.Error(err)
							return
						}
						defer conn.Close()
						_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
						payload := bytes.Repeat([]byte{value}, 1<<20)
						go func() { _, _ = conn.Write(payload) }()
						received := make([]byte, len(payload))
						if _, err := io.ReadFull(conn, received); err != nil {
							t.Error(err)
							return
						}
						if !bytes.Equal(received, payload) {
							t.Error("echo mismatch")
						}
					}(byte(i + 1))
				}
				wg.Wait()
			})
		}
	}
}

// writeTestFile writes size pseudo-random bytes to a new file and returns its
// path with its contents.
func writeTestFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	path := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func startServer(t *testing.T, network, addr string, handler Handler) string {
	t.Helper()
	config := DefaultConfig()
	config.Network = network
	config.Addr = addr
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.ListenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = server.Close()
	})
	return addrs[0].String()
}

// sendFileHandler answers any byte with head, the file's range and tail, from
// the handler itself (queued behind the corked round) or from a goroutine of
// its own (straight to the socket). The caller's file is closed as soon as
// SendFile returns, which the connection's own descriptor has to survive.
func sendFileHandler(t *testing.T, path string, offset, count int64, fromGoroutine bool) Handler {
	reply := func(c *Connection) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			c.Close()
			return
		}
		_ = c.Send([]byte("head"))
		if err := c.SendFile(f, offset, count); err != nil {
			t.Error(err)
		}
		_ = f.Close()
		_ = c.Send([]byte("tail"))
	}
	return HandlerFuncs{Data: func(c *Connection, _ []byte) {
		if fromGoroutine {
			go reply(c)
		} else {
			reply(c)
		}
	}}
}

func TestSendFile(t *testing.T) {
	const size = 8 << 20
	path, data := writeTestFile(t, size)
	cases := []struct {
		name          string
		offset, count int64
	}{
		{"whole", 0, size},
		{"middle", 12345, size / 2},
		{"small", 7, 100},
	}
	networks := []struct{ network, addr string }{{"tcp", "127.0.0.1:0"}, {"unix", ""}}
	for _, nw := range networks {
		for _, fromGoroutine := range []bool{false, true} {
			for _, tc := range cases {
				name := nw.network + "/" + map[bool]string{false: "handler", true: "goroutine"}[fromGoroutine] + "/" + tc.name
				t.Run(name, func(t *testing.T) {
					addr := nw.addr
					if nw.network == "unix" {
						addr = unixSocketPath(t)
					}
					addr = startServer(t, nw.network, addr, sendFileHandler(t, path, tc.offset, tc.count, fromGoroutine))
					conn, err := net.DialTimeout(nw.network, addr, 5*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
					if _, err = conn.Write([]byte{1}); err != nil {
						t.Fatal(err)
					}
					// Let the socket fill up before reading, so that the file
					// has to wait for the peer part way.
					time.Sleep(50 * time.Millisecond)
					want := append(append([]byte("head"), data[tc.offset:tc.offset+tc.count]...), "tail"...)
					got := make([]byte, len(want))
					if _, err = io.ReadFull(conn, got); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatal("received bytes differ from the file")
					}
				})
			}
		}
	}
}

// A file shorter than the range cannot deliver what was promised, so the
// connection is closed after what the file did hold.
func TestSendFileShortFileClosesConnection(t *testing.T) {
	path, data := writeTestFile(t, 1000)
	addr := startServer(t, "tcp", "127.0.0.1:0", sendFileHandler(t, path, 0, 5000, false))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err = conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(conn)
	if err != nil && !isConnReset(err) {
		t.Fatal(err)
	}
	want := append([]byte("head"), data...)
	if len(got) > len(want) || !bytes.Equal(got, want[:len(got)]) {
		t.Fatalf("received %d bytes, not a prefix of the file", len(got))
	}
	if bytes.HasSuffix(got, []byte("tail")) {
		t.Fatal("bytes after the short file were sent")
	}
}

func isConnReset(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func TestSendFileRejectsBadRangeAndDatagrams(t *testing.T) {
	path, _ := writeTestFile(t, 10)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c := &Connection{}
	if err := c.SendFile(f, -1, 1); err == nil {
		t.Fatal("negative offset accepted")
	}
	c.udp = &udpState{}
	if err := c.SendFile(f, 0, 1); err != ErrSendFileDatagram {
		t.Fatalf("UDP SendFile = %v", err)
	}
}

// startUDPServer runs an engine on a UDP socket and returns it with its
// address.
func startUDPServer(t *testing.T, config Config, handler Handler) (*Engine, *net.UDPAddr) {
	t.Helper()
	config.Network = "udp"
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return server, addr
}

// Every datagram reaches OnData whole and alone, and every Send leaves as one
// datagram, up to the largest macOS sends by default (its UDP send buffer is 9216 bytes).
func TestUDPServerEchoKeepsDatagramBoundaries(t *testing.T) {
	_, addr := startUDPServer(t, DefaultConfig(), echoHandler())
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, maxDatagramSize)
	for _, size := range []int{1, 100, 1400, 9000} {
		payload := bytes.Repeat([]byte{byte(size)}, size)
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:n], payload) {
			t.Fatalf("%d-byte datagram came back as %d bytes", size, n)
		}
	}
}

// Each peer address is a connection of its own, which knows the peer's
// address and outlives any single datagram.
func TestUDPPeersAreSeparateConnections(t *testing.T) {
	var mu sync.Mutex
	opened := map[*Connection]string{}
	_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			opened[c] = c.RemoteAddr().String()
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if !c.IsUDP() {
				t.Error("UDP peer does not report IsUDP")
			}
			_ = c.Send(append([]byte(c.RemoteAddr().String()+" "), b...))
		},
	})
	const peers = 8
	for i := 0; i < peers; i++ {
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		for round := 0; round < 3; round++ {
			if _, err := conn.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 128)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatal(err)
			}
			if want := conn.LocalAddr().String() + " ping"; string(buf[:n]) != want {
				t.Fatalf("reply %q, want %q", buf[:n], want)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(opened) != peers {
		t.Fatalf("%d connections opened for %d peers", len(opened), peers)
	}
}

// A peer over IPv6 is told apart by its address and port as an IPv4 one is,
// and its replies go back to the address its datagrams came from.
func TestUDPPeersOverIPv6(t *testing.T) {
	config := DefaultConfig()
	config.Network = "udp6"
	config.Addr = "[::1]:0"
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		_ = c.Send(append([]byte(c.RemoteAddr().String()+" "), b...))
	}})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	addr, err := server.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = server.Close()
	}()
	for i := 0; i < 3; i++ {
		conn, err := net.DialUDP("udp6", nil, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 128)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if want := conn.LocalAddr().String() + " ping"; string(buf[:n]) != want {
			t.Fatalf("reply %q, want %q", buf[:n], want)
		}
	}
}

// A peer that goes quiet is closed after the idle timeout, and one that sends
// again afterwards gets a fresh connection.
func TestUDPIdlePeerTimesOut(t *testing.T) {
	config := DefaultConfig()
	config.UDPIdleTimeout = 100 * time.Millisecond
	opened := make(chan *Connection, 4)
	closed := make(chan error, 4)
	_, addr := startUDPServer(t, config, HandlerFuncs{
		Open:  func(c *Connection) { opened <- c },
		Close: func(_ *Connection, err error) { closed <- err },
	})
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for round := 0; round < 2; round++ {
		if _, err := conn.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-opened:
		case <-time.After(5 * time.Second):
			t.Fatal("no connection opened")
		}
		select {
		case err := <-closed:
			if !errors.Is(err, ErrUDPIdleTimeout) {
				t.Fatalf("closed with %v, want ErrUDPIdleTimeout", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("idle peer was not closed")
		}
	}
}

// A dialed UDP connection talks to its one peer, and SendParts joins its
// parts into a single datagram.
func TestDialUDP(t *testing.T) {
	_, addr := startUDPServer(t, DefaultConfig(), echoHandler())
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	received := make(chan []byte, 8)
	handler := HandlerFuncs{Data: func(_ *Connection, b []byte) { received <- append([]byte(nil), b...) }}
	dialed := make(chan *Connection, 1)
	err := client.DialWithHandler("udp", addr.String(), time.Second, handler, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		dialed <- c
	})
	if err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if !c.IsUDP() || c.RemoteAddr().String() != addr.String() {
		t.Fatalf("dialed connection: udp %v, remote %v", c.IsUDP(), c.RemoteAddr())
	}
	for _, msg := range []string{"one", "two", "three"} {
		if err := c.SendParts([]byte(msg), []byte("!")); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-received:
			if string(got) != msg+"!" {
				t.Fatalf("echo %q, want %q", got, msg+"!")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no echo")
		}
	}
	c.Close()
}

// burstHandler is a DatagramsHandler that records the bursts it is given.
// The first burst waits on gate, so that what arrives meanwhile piles up.
type burstHandler struct {
	HandlerFuncs
	gate   chan struct{}
	mu     sync.Mutex
	bursts [][]string
}

func (h *burstHandler) OnDatagrams(_ *Connection, datagrams [][]byte) {
	burst := make([]string, len(datagrams))
	for i, d := range datagrams {
		burst[i] = string(d)
	}
	h.mu.Lock()
	first := len(h.bursts) == 0
	h.bursts = append(h.bursts, burst)
	h.mu.Unlock()
	if first {
		<-h.gate
	}
}

func (h *burstHandler) received() [][]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]string(nil), h.bursts...)
}

// A DatagramsHandler gets what piled up for a connection in one call, in
// the order it arrived, and OnData gets nothing.
func TestUDPDatagramsHandlerTakesBursts(t *testing.T) {
	h := &burstHandler{gate: make(chan struct{})}
	h.Data = func(*Connection, []byte) { t.Error("OnData called for a DatagramsHandler") }
	_, addr := startUDPServer(t, DefaultConfig(), h)
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor := func(what string, done func([][]string) bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !done(h.received()) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s; got %v", what, h.received())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if _, err := conn.Write([]byte("0")); err != nil {
		t.Fatal(err)
	}
	waitFor("the first burst", func(b [][]string) bool { return len(b) == 1 })
	want := []string{"0"}
	for i := 1; i <= 5; i++ {
		d := string(rune('0' + i))
		want = append(want, d)
		if _, err := conn.Write([]byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	// Give the loop time to queue them all before the first burst returns.
	time.Sleep(100 * time.Millisecond)
	close(h.gate)
	count := func(b [][]string) int {
		n := 0
		for _, burst := range b {
			n += len(burst)
		}
		return n
	}
	waitFor("every datagram", func(b [][]string) bool { return count(b) == len(want) })
	bursts := h.received()
	var got []string
	for _, burst := range bursts {
		got = append(got, burst...)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("datagrams arrived as %v, want %v in order", bursts, want)
	}
	if len(bursts) != 2 {
		t.Fatalf("datagrams arrived in bursts %v, want the five that piled up in one", bursts)
	}
}
