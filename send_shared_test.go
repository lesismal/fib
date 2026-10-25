//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// SendShared hands one array to several connections whose peers are not
// reading. What they cannot take has to wait in each queue as that array
// itself, never pooled and never written into, so that a broadcast to slow
// peers holds one copy rather than one each; and once the peers read, each
// has to get the whole of it, intact.
func TestSendSharedQueuesTheCallersArray(t *testing.T) {
	opened := make(chan *Connection, 2)
	_, addr := startEchoServer(t, DefaultConfig(), HandlerFuncs{Open: func(c *Connection) { opened <- c }})

	var peers []net.Conn
	var conns []*Connection
	for i := 0; i < 2; i++ {
		peer, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		peers = append(peers, peer)
		select {
		case c := <-opened:
			conns = append(conns, c)
		case <-time.After(5 * time.Second):
			t.Fatal("OnOpen not called")
		}
	}

	// Far more than socket buffers hold, so most of it has to queue.
	shared := make([]byte, 8<<20)
	for i := range shared {
		shared[i] = byte(i * 7)
	}
	want := bytes.Clone(shared)
	tail := []byte("tail")
	for _, c := range conns {
		// What the socket takes at once would never be queued, so the send
		// buffer is kept far below the array: a host tuned for throughput
		// can otherwise swallow the whole of it in the kernel and the
		// queueing this test is about never happens.
		if err := c.SetWriteBuffer(1 << 20); err != nil {
			t.Fatal(err)
		}
		if err := c.SendShared(shared); err != nil {
			t.Fatal(err)
		}
		// A copying send behind it must start an item of its own rather
		// than pack into the shared array.
		if err := c.Send(tail); err != nil {
			t.Fatal(err)
		}
	}

	for i, c := range conns {
		c.mu.Lock()
		found := false
		for j := c.sendHead; j < len(c.sends); j++ {
			item := &c.sends[j]
			if len(item.data) == 0 || &item.data[len(item.data)-1] != &shared[len(shared)-1] {
				continue
			}
			found = true
			if item.pooled {
				t.Errorf("connection %d: the shared array is queued as a pooled buffer", i)
			}
		}
		c.mu.Unlock()
		if !found {
			if runtime.GOOS == "windows" {
				// An overlapped send can take the whole array at once and
				// complete, leaving nothing of it queued to look at.
				t.Skipf("connection %d: the kernel accepted the whole shared array; none of it is queued", i)
			}
			t.Fatalf("connection %d: the shared array is not queued; it was copied or written at once", i)
		}
	}

	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = peer.SetDeadline(time.Now().Add(15 * time.Second))
			got := make([]byte, len(want)+len(tail))
			if _, err := io.ReadFull(peer, got); err != nil {
				t.Errorf("peer %d: %v", i, err)
				return
			}
			if !bytes.Equal(got[:len(want)], want) || !bytes.Equal(got[len(want):], tail) {
				t.Errorf("peer %d: what arrived differs from what was sent", i)
			}
		}()
	}
	wg.Wait()
	if !bytes.Equal(shared, want) {
		t.Fatal("the shared array was written into")
	}
}
