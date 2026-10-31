//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// holdingServer answers each request with cvCliOK once release is closed,
// over keep-alive connections, and counts the connections it accepted.
func holdingServer(t *testing.T, release <-chan struct{}) (string, *atomic.Int32) {
	var conns atomic.Int32
	u := rawServer(t, func(conn net.Conn, _ int) {
		conns.Add(1)
		r := bufio.NewReader(conn)
		for {
			if _, err := stdhttp.ReadRequest(r); err != nil {
				return
			}
			<-release
			if _, err := io.WriteString(conn, cvCliOK); err != nil {
				return
			}
		}
	})
	return u, &conns
}

// sendAll sends n requests to u at once and returns their outcomes, which
// come once release is closed for those let in, and at once for those
// refused.
func sendAll(t *testing.T, client *Client, u string, n int) chan error {
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		client.Do(mustRequest(t, "GET", u, nil), func(_ *ClientResponse, err error) { results <- err })
	}
	return results
}

// A burst to a cold host within MaxConnsPerHost is let in whole, however
// small MaxPendingPerHost: the requests the client is dialing connections
// for are not waiting beyond them.
func TestMaxPendingPerHostColdBurst(t *testing.T) {
	release := make(chan struct{})
	u, conns := holdingServer(t, release)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 256
	config.MaxPendingPerHost = 8
	client := newTestClient(t, config)
	results := sendAll(t, client, u, 256)
	deadline := time.Now().Add(10 * time.Second)
	for conns.Load() < 256 && time.Now().Before(deadline) && len(results) == 0 {
		time.Sleep(time.Millisecond)
	}
	if n := conns.Load(); n != 256 {
		t.Errorf("%d connections opened, want 256", n)
	}
	close(release)
	for i := 0; i < 256; i++ {
		if err := cvCliWait(t, results, "a request of the burst"); err != nil {
			t.Fatalf("a request of the burst got %v", err)
		}
	}
}

// With MaxConnsPerHost 1 and MaxPendingPerHost 2, a cold host takes the
// request its connection is dialed for and two more, and turns the fourth
// away: the queue is bounded at the two together.
func TestMaxPendingPerHostWhileDialing(t *testing.T) {
	release := make(chan struct{})
	u, _ := holdingServer(t, release)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	config.MaxPendingPerHost = 2
	client := newTestClient(t, config)
	results := sendAll(t, client, u, 4)
	if err := cvCliWait(t, results, "the request over the cap"); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("the first outcome is %v, want %v", err, ErrTooManyPending)
	}
	close(release)
	for i := 0; i < 3; i++ {
		if err := cvCliWait(t, results, "the requests let in"); err != nil {
			t.Fatalf("a request let in got %v", err)
		}
	}
}

// For a host that may speak HTTP/2, which the client dials once for until it
// knows, a dial that hangs leaves room for the connections MaxConnsPerHost
// allows and MaxPendingPerHost more, and no more.
func TestMaxPendingPerHostMaybeHTTP2(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// The TLS handshake never gets an answer.
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 4
	config.MaxPendingPerHost = 2
	client := newTestClient(t, config)
	if !client.mayMultiplex(hostTarget{secure: true}) {
		t.Fatal("the client does not offer HTTP/2")
	}
	results := sendAll(t, client, "https://"+listener.Addr().String()+"/", 7)
	if err := cvCliWait(t, results, "the request over the cap"); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("the first outcome is %v, want %v", err, ErrTooManyPending)
	}
	select {
	case err := <-results:
		t.Fatalf("a second request ended with %v while the dial hangs", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// hostQueue reports how many requests wait in the client's queue for the
// host of u, and how many of them ended while they waited.
func hostQueue(c *Client, u string) (waiting, ended int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for target, h := range c.hosts {
		if strings.HasSuffix(u, target.addr) {
			return len(h.waiting), int(h.ended.Load())
		}
	}
	return 0, 0
}

// Requests that ended while they waited are counted as they end, not found
// by walking the queue, and make room for as many more: with three of four
// cancelled, three more are let in and the next is turned away. Once every
// request is over, the count is back to none.
func TestMaxPendingPerHostEndedWhileWaiting(t *testing.T) {
	release := make(chan struct{})
	u, _ := holdingServer(t, release)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	config.MaxPendingPerHost = 4
	client := newTestClient(t, config)
	results := make(chan error, 16)
	send := func(ctx context.Context) {
		client.Do(mustRequest(t, "GET", u, nil).WithContext(ctx), func(_ *ClientResponse, err error) { results <- err })
	}
	send(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 3; i++ {
		send(ctx)
	}
	send(context.Background())
	cancel()
	for i := 0; i < 3; i++ {
		if err := cvCliWait(t, results, "a cancelled request"); !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled request got %v", err)
		}
	}
	if _, ended := hostQueue(client, u); ended != 3 {
		t.Fatalf("%d requests counted as ended while they waited, want 3", ended)
	}
	for i := 0; i < 4; i++ {
		send(context.Background())
	}
	if err := cvCliWait(t, results, "the request over the cap"); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("the first outcome after the cancels is %v, want %v", err, ErrTooManyPending)
	}
	close(release)
	for i := 0; i < 5; i++ {
		if err := cvCliWait(t, results, "the requests let in"); err != nil {
			t.Fatalf("a request let in got %v", err)
		}
	}
	if waiting, ended := hostQueue(client, u); waiting != 0 || ended != 0 {
		t.Fatalf("%d requests waiting and %d counted as ended once all are over, want none", waiting, ended)
	}
}

// With MaxConnsPerHost 1 and MaxPendingPerHost 1, a burst that arrives as the
// host's first connection is dialed takes the request the connection was
// dialed for and one more, and turns one away. A dial that had just succeeded
// stopped counting as dialing before the connection took its request, under
// a second hold of the client's lock, so that a request arriving between the
// two found the one the dial was for waiting with nothing to cover it, and
// was turned away with the next.
//
// The test holds the connection's own lock across that moment, once the dial
// has settled under it: dialed then stops where it asks whether the
// connection is in flight, and the burst arrives there.
func TestMaxPendingPerHostAsTheDialEnds(t *testing.T) {
	release := make(chan struct{})
	u, _ := holdingServer(t, release)
	dialed, settle := make(chan *clientConn, 1), make(chan struct{})
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	config.MaxPendingPerHost = 1
	config.Dial = func(engine *fib.Engine, network, addr string, timeout time.Duration, handler fib.Handler, done func(*fib.Connection, error)) error {
		return engine.DialWithHandler(network, addr, timeout, handler, func(c *fib.Connection, err error) {
			dialed <- handler.(*clientConn)
			<-settle
			done(c, err)
		})
	}
	client := newTestClient(t, config)
	results := sendAll(t, client, u, 1)
	cc := <-dialed
	h := hostPoolOf(t, client, u)
	// The client's lock keeps dialed from going past settling the dial, which
	// it does under the connection's, until the test holds that one.
	client.mu.Lock()
	close(settle)
	for {
		cc.mu.Lock()
		if cc.settled {
			break
		}
		cc.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	client.mu.Unlock()
	// dialed now stops at the connection's lock, either having let the
	// client's go there or holding it, which TryLock tells.
	for deadline := time.Now().Add(200 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if client.mu.TryLock() {
			dialing := h.dialing
			client.mu.Unlock()
			if dialing == 0 {
				break
			}
		}
	}
	more := make(chan error, 2)
	go func() {
		for i := 0; i < 2; i++ {
			client.Do(mustRequest(t, "GET", u, nil), func(_ *ClientResponse, err error) { more <- err })
		}
	}()
	// Nothing the burst asks for can be answered while the server holds the
	// response, so the first thing to arrive is a refusal. The server lets
	// go only once it has: a slow runner whose burst came in after the first
	// request had finished would find room, and none of it be turned away.
	// The burst may wait for the connection's lock, which the dial takes
	// under the client's, so the test lets go of it first, once the burst
	// has had time to come in where the dial's gap was.
	time.Sleep(50 * time.Millisecond)
	refused := 0
	countBurst := func(err error) {
		if errors.Is(err, ErrTooManyPending) {
			refused++
		} else if err != nil {
			t.Fatalf("a request of the burst got %v", err)
		}
	}
	cc.mu.Unlock()
	countBurst(cvCliWait(t, more, "the burst's refusal"))
	close(release)
	countBurst(cvCliWait(t, more, "a request of the burst"))
	if err := cvCliWait(t, results, "the request the dial was for"); err != nil {
		t.Fatalf("the request the dial was for got %v", err)
	}
	if refused != 1 {
		t.Fatalf("%d of the burst turned away, want 1", refused)
	}
}

// hostPoolOf finds the pool the client keeps for u's host.
func hostPoolOf(t *testing.T, c *Client, u string) *hostPool {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for target, h := range c.hosts {
		if strings.HasSuffix(u, target.addr) || strings.Contains(u, target.addr+"/") {
			return h
		}
	}
	t.Fatal("no host for " + u)
	return nil
}
