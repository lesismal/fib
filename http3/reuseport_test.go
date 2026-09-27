//go:build linux || darwin

package http3

import (
	"fmt"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

// With ReusePort, the engine's pollers read its UDP address themselves, and
// every datagram of a client reaches the poller its addresses hash to, so a
// QUIC connection is served whole by one poller. Each client here dials from
// an engine, and a port, of its own.
func TestRequestsOverReusePortPollers(t *testing.T) {
	const clients, requests = 8, 5
	fc := fib.DefaultConfig()
	fc.Name = "http3-reuseport"
	fc.IOPollers = true
	fc.IOPollerCount = 4
	fc.ReusePort = true
	url := startServerOn(t, fc, Config{}, echo)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		client := newClient(t, nil)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < requests; j++ {
				body := fmt.Sprintf("client %d request %d", i, j)
				resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, url+"/r", strings.NewReader(body))).Wait()
				if err != nil {
					t.Error(err)
					return
				}
				if got, want := readBody(t, resp), "POST /r "+body; got != want {
					t.Errorf("got %q, want %q", got, want)
				}
			}
		}(i)
	}
	wg.Wait()
}

// rebindingProxy stands between a client and a server as a NAT does: the
// server sees the client at the proxy's upstream socket, and rebind moves it
// to a new one, as a NAT whose mapping expired would.
type rebindingProxy struct {
	front  *net.UDPConn
	server *net.UDPAddr
	mu     sync.Mutex
	client *net.UDPAddr
	back   *net.UDPConn
	backs  []*net.UDPConn
}

func newRebindingProxy(t *testing.T, server *net.UDPAddr) *rebindingProxy {
	t.Helper()
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	p := &rebindingProxy{front: front, server: server}
	p.rebind(t)
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, from, err := front.ReadFromUDP(buf)
			if err != nil {
				return
			}
			p.mu.Lock()
			p.client = from
			back := p.back
			p.mu.Unlock()
			_, _ = back.WriteToUDP(buf[:n], server)
		}
	}()
	t.Cleanup(func() {
		_ = front.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, b := range p.backs {
			_ = b.Close()
		}
	})
	return p
}

// rebind has the client's datagrams leave from a new port. What the server
// sends to the old one still reaches the client, as it would until the old
// mapping went.
func (p *rebindingProxy) rebind(t *testing.T) *net.UDPAddr {
	t.Helper()
	back, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := back.Read(buf)
			if err != nil {
				return
			}
			p.mu.Lock()
			client := p.client
			p.mu.Unlock()
			if client != nil {
				_, _ = p.front.WriteToUDP(buf[:n], client)
			}
		}
	}()
	p.mu.Lock()
	p.back = back
	p.backs = append(p.backs, back)
	p.mu.Unlock()
	return back.LocalAddr().(*net.UDPAddr)
}

// A client whose address changes mid-connection, behind a NAT, keeps its
// connection: its next requests arrive from the new address, which the
// server follows, wherever its loops hand the new address.
func TestConnectionFollowsRebindingClient(t *testing.T) {
	fc := fib.DefaultConfig()
	fc.Name = "http3-rebinding"
	fc.IOPollers = true
	fc.IOPollerCount = 4
	fc.ReusePort = true
	url := startServerOn(t, fc, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.RemoteAddr))
	})
	port, err := strconv.Atoi(url[strings.LastIndexByte(url, ':')+1:])
	if err != nil {
		t.Fatal(err)
	}
	proxy := newRebindingProxy(t, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	proxied := fmt.Sprintf("https://localhost:%d", proxy.front.LocalAddr().(*net.UDPAddr).Port)
	client := newClient(t, nil)
	get := func() string {
		t.Helper()
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, proxied+"/", nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		return readBody(t, resp)
	}
	first := get()
	for round := 0; round < 3; round++ {
		addr := proxy.rebind(t)
		for i := 0; i < 3; i++ {
			if got := get(); got != addr.String() {
				t.Fatalf("round %d: the server saw the client at %s, want %s (first %s)", round, got, addr, first)
			}
		}
	}
	client.mu.Lock()
	conns := len(client.conns)
	client.mu.Unlock()
	if conns != 1 {
		t.Fatalf("%d connections, want the one that moved", conns)
	}
}
