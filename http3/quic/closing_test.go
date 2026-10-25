package quic

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// dropWhile has end drop what it sends for as long as the returned stop has
// not been called.
func dropWhile(end *pipeEnd) (stop func()) {
	var dropping atomic.Bool
	dropping.Store(true)
	end.mu.Lock()
	end.drop = func([]byte) bool { return dropping.Load() }
	end.mu.Unlock()
	return func() { dropping.Store(false) }
}

// countSends has end count what it sends, losing none of it.
func countSends(end *pipeEnd) *atomic.Int32 {
	var n atomic.Int32
	end.mu.Lock()
	end.drop = func([]byte) bool { n.Add(1); return false }
	end.mu.Unlock()
	return &n
}

// waitPeerClose waits for h to hear its connection close with the application
// error code the peer closed it with.
func waitPeerClose(t *testing.T, h *testHandler, code uint64) {
	t.Helper()
	select {
	case err := <-h.closed:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != code || !appErr.Remote {
			t.Fatalf("closed with %v, want the peer's close with code %#x", err, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never heard of the close")
	}
}

// A connection that closed answers what still arrives with its
// CONNECTION_CLOSE again (RFC 9000 section 10.2.1), so that a peer whose
// first one was lost hears of the close when it next sends, rather than
// only once it idles out; whichever side it was that closed.
func TestClosingAnswersAfterALostClose(t *testing.T) {
	for _, closer := range []string{"client", "server"} {
		t.Run(closer+" closes", func(t *testing.T) {
			p := newTestPair(t, 0, func(client, server *Config) {
				// Long enough that only the close answered can end the peer's
				// side within the test.
				client.MaxIdleTimeout, server.MaxIdleTimeout = time.Minute, time.Minute
			})
			<-p.clientH.handshake
			closing, closingEnd, peer, peerH := p.client, p.clientEnd, p.serverConn(t), p.serverH
			if closer == "server" {
				closing, closingEnd, peer, peerH = p.serverConn(t), p.serverEnd, p.client, p.clientH
			}
			stop := dropWhile(closingEnd)
			closing.Close(0x42, "bye")
			stop()
			// The peer sends, as it would go on doing, and is answered: what it
			// was sending already may have been answered first.
			if s, err := peer.OpenStream(); err == nil {
				_ = s.Write([]byte("still there?"), false)
			}
			waitPeerClose(t, peerH, 0x42)
		})
	}
}

// The close goes again for the 1st, 2nd, 4th, 8th and so on of the datagrams
// that arrive while closing, not for every one, and the path closes once the
// closing period is over.
func TestClosingAnswersSparinglyThenEnds(t *testing.T) {
	p := newTestPair(t, 0, func(client, server *Config) {
		// The closing period lasts no longer than the idle timeout.
		client.MaxIdleTimeout, server.MaxIdleTimeout = 300*time.Millisecond, 300*time.Millisecond
	})
	<-p.clientH.handshake
	stop := dropWhile(p.clientEnd)
	p.client.Close(0x42, "bye")
	stop()
	// The server, quiet from here, may have had packets on their way, which
	// the client counted already.
	stopServer := dropWhile(p.serverEnd)
	defer stopServer()
	time.Sleep(50 * time.Millisecond)
	sent := countSends(p.clientEnd)
	p.client.mu.Lock()
	seen := p.client.closingSeen
	p.client.mu.Unlock()
	want := int32(0)
	for n := seen + 1; n <= seen+10; n++ {
		if n&(n-1) == 0 {
			want++
		}
	}
	datagram := make([]byte, 64)
	for range 10 {
		p.client.HandleDatagram(datagram)
	}
	if n := sent.Load(); n != want {
		t.Fatalf("answered datagrams %d to %d with %d closes, want %d", seen+1, seen+10, n, want)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.clientEnd.mu.Lock()
		closed := p.clientEnd.closed
		p.clientEnd.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the path stayed open past the closing period")
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.client.mu.Lock()
	kept := p.client.closePacket != nil
	p.client.mu.Unlock()
	if kept {
		t.Fatal("the close is still kept to answer with after the closing period")
	}
}

// A connection whose peer closed it drains: it sends nothing more, and keeps
// its path for the draining period rather than letting it go at once.
func TestDrainingAfterPeerClose(t *testing.T) {
	p := newTestPair(t, 0, nil)
	sends := watchSends(p.clientEnd)
	p.serverConn(t).Close(0x33, "bye")
	select {
	case err := <-p.clientH.closed:
		var ae *ApplicationError
		if !errors.As(err, &ae) || ae.Code != 0x33 || !ae.Remote {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client not closed")
	}
	before := len(sends())
	closedAt := time.Now()
	// What the server still has in flight arrives at a draining client,
	// which answers none of it.
	for i := 0; i < 5; i++ {
		p.client.HandleDatagram(append([]byte{0x40}, p.client.scid...))
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(sends()); n != before {
		t.Fatalf("a draining client sent %d datagrams", n-before)
	}
	p.clientEnd.mu.Lock()
	early := p.clientEnd.closed
	p.clientEnd.mu.Unlock()
	if early {
		t.Fatal("path closed at once")
	}
	waitFor(t, "path closed", func() bool {
		p.clientEnd.mu.Lock()
		defer p.clientEnd.mu.Unlock()
		return p.clientEnd.closed
	})
	if d := time.Since(closedAt); d < minClosingPeriod/2 {
		t.Fatalf("path closed after %v", d)
	}
}
