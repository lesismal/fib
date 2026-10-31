//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package turnserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/stun"
	"github.com/lesismal/fib/webrtc/turn"
)

// streamClient drives a turn.Client over a TCP or TLS connection, framing
// what the server sends with turn.FrameLen.
type streamClient struct {
	mu        sync.Mutex
	c         *turn.Client
	conn      net.Conn
	got       chan string
	allocated chan struct{}
	failed    chan error
	closed    chan struct{}
}

func newStreamClient(t *testing.T, conn net.Conn, server netip.AddrPort, bind bool) *streamClient {
	u := &streamClient{conn: conn, got: make(chan string, 16), allocated: make(chan struct{}),
		failed: make(chan error, 1), closed: make(chan struct{})}
	c, err := turn.NewClient(turn.ClientConfig{
		Server: server, Username: user, Password: pass, BindChannels: bind, Stream: true,
		Send:        func(_ netip.AddrPort, b []byte) { _, _ = conn.Write(b) },
		OnAllocated: func(_, _ netip.AddrPort) { close(u.allocated) },
		OnFailed:    func(err error) { u.failed <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	u.c = c
	go func() {
		defer close(u.closed)
		var buf []byte
		chunk := make([]byte, 4096)
		for {
			n, err := conn.Read(chunk)
			if err != nil {
				return
			}
			buf = append(buf, chunk[:n]...)
			for {
				n, err := turn.FrameLen(buf)
				if err != nil {
					t.Errorf("framing the server's stream: %v", err)
					return
				}
				if n == 0 || n > len(buf) {
					break
				}
				u.mu.Lock()
				_, data, _ := c.HandlePacket(server, buf[:n], time.Now())
				if data != nil {
					u.got <- string(data)
				}
				u.mu.Unlock()
				buf = buf[n:]
			}
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return u
}

func (u *streamClient) do(f func(c *turn.Client)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	f(u.c)
}

func selfSigned(t *testing.T) stdtls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "turn.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"turn.test"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func listenStream(t *testing.T, s *Server, e *fib.Engine, tls bool) netip.AddrPort {
	t.Helper()
	var i int
	var err error
	if tls {
		i, err = s.ListenTLS("127.0.0.1:0", &stdtls.Config{Certificates: []stdtls.Certificate{selfSigned(t)}})
	} else {
		i, err = s.ListenTCP("127.0.0.1:0")
	}
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := e.BindingAddrs(i)
	if err != nil {
		t.Fatal(err)
	}
	return addrs[0].(*net.TCPAddr).AddrPort()
}

func dialStream(t *testing.T, addr netip.AddrPort, tls bool) net.Conn {
	t.Helper()
	if tls {
		c, err := stdtls.Dial("tcp", addr.String(), &stdtls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TURN over TCP and TLS: a client allocates, relays UDP to a peer and back,
// with and without a channel, and its allocation ends with its connection.
func TestStreamRelay(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tls, bind bool
	}{
		{"tcp", false, false},
		{"tcp channels", false, true},
		{"tls channels", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, e := startServer(t, fib.DefaultConfig(), Config{})
			addr := listenStream(t, s, e, tc.tls)
			peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			peerAddr := peer.LocalAddr().(*net.UDPAddr).AddrPort()
			conn := dialStream(t, addr, tc.tls)
			u := newStreamClient(t, conn, addr, tc.bind)
			u.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
			select {
			case <-u.allocated:
			case err := <-u.failed:
				t.Fatal(err)
			case <-time.After(10 * time.Second):
				t.Fatal("no allocation")
			}
			var relayed netip.AddrPort
			u.do(func(c *turn.Client) { relayed, _, _ = c.Relayed() })
			buf := make([]byte, 2048)
			// Payloads of every length mod 4 exercise the padding.
			for i, msg := range []string{"one", "four", "fives", "sixsix", "seven77"} {
				var from netip.AddrPort
				var n int
				for range 50 {
					u.do(func(c *turn.Client) { _ = c.SendTo(peerAddr, []byte(msg)) })
					_ = peer.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
					if n, from, err = peer.ReadFromUDPAddrPort(buf); err == nil {
						break
					}
				}
				if err != nil || string(buf[:n]) != msg || from != relayed {
					t.Fatalf("%d: peer read %q from %v: %v", i, buf[:n], from, err)
				}
				if _, err := peer.WriteToUDPAddrPort([]byte("echo:"+msg), relayed); err != nil {
					t.Fatal(err)
				}
				if got := wait(t, u.got, "the echo"); got != "echo:"+msg {
					t.Fatalf("client got %q", got)
				}
				time.Sleep(20 * time.Millisecond)
			}
			conn.Close()
			deadline := time.Now().Add(10 * time.Second)
			for s.Allocations() != 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if s.Allocations() != 0 || !freed(relayed) {
				t.Fatal("the allocation outlived its connection")
			}
		})
	}
}

// A stream connection is closed when it frames as neither STUN nor
// ChannelData, when it holds too much of one message, when it stays
// without an allocation too long, and when the server closes.
func TestStreamBounds(t *testing.T) {
	cfg := Config{StreamIdleTimeout: 100 * time.Millisecond, MaxStreamMessage: 1024}
	cfg.SweepInterval = 20 * time.Millisecond
	s, _, e := startServer(t, fib.DefaultConfig(), cfg)
	addr := listenStream(t, s, e, false)
	closedSoon := func(c net.Conn, what string) {
		t.Helper()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Read(make([]byte, 64)); err == nil || isTimeout(err) {
			t.Fatalf("%s: connection not closed: %v", what, err)
		}
	}
	// Bytes that are not TURN.
	c := dialStream(t, addr, false)
	_, _ = c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	closedSoon(c, "HTTP")
	// A STUN message, then the start of one larger than the buffer may
	// hold, sent in pieces.
	c = dialStream(t, addr, false)
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	_, _ = c.Write(m.Raw[:7])
	time.Sleep(20 * time.Millisecond)
	_, _ = c.Write(m.Raw[7:])
	reply := make([]byte, 128)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := c.Read(reply); err != nil || n < stun.HeaderSize {
		t.Fatalf("a Binding over TCP in pieces: %d, %v", n, err)
	}
	// A ChannelData message longer than the bound.
	_, _ = c.Write([]byte{0x40, 0x00, 0xff, 0xfc, 1, 2, 3})
	closedSoon(c, "an oversized message")
	// Idle without an allocation.
	c = dialStream(t, addr, false)
	closedSoon(c, "idle")
	// Closed with the server.
	c = dialStream(t, addr, false)
	deadline := time.Now().Add(5 * time.Second)
	for countStreams(s) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	s.Close()
	closedSoon(c, "server closed")
	// A server not started has nowhere to listen.
	cfg2 := Config{RelayIP: loopback}
	cfg2.Realm, cfg2.Auth = realm, turn.StaticAuth(realm, nil)
	s2, err := New(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.ListenTCP("127.0.0.1:0"); err != errNotStarted {
		t.Fatalf("ListenTCP before Start: %v", err)
	}
	// Data before Start, or on a connection with no state, is ignored;
	// a stream client gone is not sent to.
	h := s2.StreamHandler()
	h.OnData(&fib.Connection{}, []byte{1})
	h.OnClose(&fib.Connection{}, nil)
	h.OnPriorityData(nil, nil)
	s2.sendToClient(turn.FiveTuple{Transport: turn.TransportTCP}, []byte("x"))
	if s2.Unsent() != 1 {
		t.Fatal("a datagram to a gone stream client not counted")
	}
}

func countStreams(s *Server) (n int) {
	s.streams.Range(func(any, any) bool { n++; return true })
	return n
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}
