//go:build cgo

// Package interop checks fib's BitTorrent packages against other
// implementations. utp_test.go checks bittorrent/utp against libutp, the
// reference uTP of BitTorrent Inc. that libtorrent's predecessors,
// Transmission and anacrolix/torrent (through go-libutp, a cgo wrapper of
// it) use, in both roles, directly and through a UDP relay that drops and
// reorders datagrams.
package interop

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	libutp "github.com/anacrolix/go-libutp"
	"github.com/lesismal/fib"
	fibutp "github.com/lesismal/fib/bittorrent/utp"
)

func startEngine(t *testing.T) *fib.Engine {
	t.Helper()
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	e, err := fib.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	t.Cleanup(func() {
		e.Stop()
		<-done
		_ = e.Close()
	})
	return e
}

// relay forwards datagrams between its clients and target, as a NAT,
// and returns its address. With lossy set it drops one datagram in 20
// each way, and holds back one in 7 until the one after it has gone, or
// for 3ms when none follows: at random, from a fixed seed, for a drop at
// a fixed period locks onto a sender's pattern and drops the same
// retransmission again and again.
func relay(t *testing.T, target string, lossy bool) string {
	t.Helper()
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ta, _ := net.ResolveUDPAddr("udp4", target)
	t.Cleanup(func() { front.Close() })
	// pipe forwards what read reads with write, dropping and holding
	// back as relay says; a datagram is written with the address it came
	// from.
	pipe := func(conn *net.UDPConn, read func([]byte) (int, netip.AddrPort, error), write func([]byte, netip.AddrPort)) {
		buf := make([]byte, 65536)
		var held []byte
		var heldFrom netip.AddrPort
		rng := rand.New(rand.NewPCG(uint64(conn.LocalAddr().(*net.UDPAddr).Port), 5))
		for {
			if held != nil {
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Millisecond))
			} else {
				_ = conn.SetReadDeadline(time.Time{})
			}
			m, from, err := read(buf)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				write(held, heldFrom)
				held = nil
				continue
			}
			if err != nil {
				return
			}
			d := buf[:m]
			switch r := rng.IntN(140); {
			case lossy && r < 7:
				continue
			case lossy && r < 27 && held == nil:
				held, heldFrom = append([]byte(nil), d...), from
				continue
			}
			write(d, from)
			if held != nil {
				write(held, heldFrom)
				held = nil
			}
		}
	}
	// Each client address gets a socket of its own toward target, as a NAT
	// maps it, and the replies on it go back to that address only.
	var mu sync.Mutex
	backs := map[netip.AddrPort]*net.UDPConn{}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range backs {
			b.Close()
		}
	})
	go pipe(front, front.ReadFromUDPAddrPort, func(d []byte, client netip.AddrPort) {
		mu.Lock()
		back := backs[client]
		if back == nil {
			var err error
			if back, err = net.DialUDP("udp4", nil, ta); err != nil {
				mu.Unlock()
				return
			}
			backs[client] = back
			go pipe(back, func(b []byte) (int, netip.AddrPort, error) {
				n, err := back.Read(b)
				return n, netip.AddrPort{}, err
			}, func(d []byte, _ netip.AddrPort) { _, _ = front.WriteToUDPAddrPort(d, client) })
		}
		mu.Unlock()
		_, _ = back.Write(d)
	})
	return front.LocalAddr().String()
}

func payload(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, 1))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// both writes out while it reads len(in) bytes, and checks them.
func both(c net.Conn, out, in []byte) error {
	errc := make(chan error, 1)
	go func() {
		_, err := c.Write(out)
		errc <- err
	}()
	got := make([]byte, len(in))
	if _, err := io.ReadFull(c, got); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if err := <-errc; err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(in) {
		return fmt.Errorf("the %d bytes read differ", len(in))
	}
	return nil
}

// toEOF reads c to its end and checks what it read.
func toEOF(c net.Conn, want []byte) error {
	got, err := io.ReadAll(c)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(want) {
		return fmt.Errorf("read %d bytes to EOF, want %d, or they differ", len(got), len(want))
	}
	return nil
}

// session runs on pairs of connections, a dialer's and an acceptor's:
// size bytes each way at once on the first, then on two more one side
// writes size/2 bytes and closes while the other reads to EOF, each way.
type session struct {
	size   int
	dial   func(ctx context.Context) (net.Conn, error)
	accept func() (net.Conn, error)
}

func (s session) run(t *testing.T) {
	pair := func() (d, a net.Conn) {
		t.Helper()
		ac := make(chan net.Conn, 1)
		go func() {
			c, err := s.accept()
			if err != nil {
				c = nil
			}
			ac <- c
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		d, err := s.dial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// libutp's acceptor takes data only once the initiator's first
		// arrive: uTP's initiators speak first.
		if _, err := d.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		if a = <-ac; a == nil {
			t.Fatal("accept failed")
		}
		if _, err := io.ReadFull(a, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		return d, a
	}

	d, a := pair()
	start := time.Now()
	fromD, fromA := payload(1, s.size), payload(2, s.size)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); checkErr(t, "dialer", both(d, fromD, fromA)) }()
	go func() { defer wg.Done(); checkErr(t, "acceptor", both(a, fromA, fromD)) }()
	wg.Wait()
	t.Logf("%d bytes each way in %v", s.size, time.Since(start))
	for _, c := range []net.Conn{d, a} {
		if fc, ok := c.(*fibutp.Conn); ok {
			t.Logf("fib's end: %+v, RTT %v", fc.Stats(), fc.RTT())
		}
	}
	d.Close()
	a.Close()

	for _, writerIsDialer := range []bool{true, false} {
		d, a := pair()
		w, r := a, d
		if writerIsDialer {
			w, r = d, a
		}
		data := payload(3, s.size/2)
		go func() {
			if _, err := w.Write(data); err != nil {
				t.Error(err)
			}
			w.Close()
		}()
		checkErr(t, fmt.Sprintf("reader to EOF (dialer writes: %v)", writerIsDialer), toEOF(r, data))
		r.Close()
	}
}

func checkErr(t *testing.T, who string, err error) {
	if err != nil {
		t.Errorf("%s: %v", who, err)
	}
}

// size is what a session sends each way: several MB on a clean path, less
// through the lossy relay, where libutp recovers slowly. libutp starts a
// connection it dials with a random sequence number but leaves
// fast_resend_seq_nr at 1 (utp_connect after utp_create_socket), so for
// half the numbers it never sends a packet again on a selective
// acknowledgement, only on its retransmission timeout of a second or more.
func size(lossy bool) int {
	if lossy {
		return 1 << 20
	}
	return 4 << 20
}

func TestUTPFibDialsLibutp(t *testing.T) {
	for _, lossy := range []bool{false, true} {
		t.Run(fmt.Sprintf("lossy=%v", lossy), func(t *testing.T) {
			e := startEngine(t)
			ls, err := libutp.NewSocket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ls.Close()
			target := relay(t, ls.Addr().String(), lossy)
			session{
				size: size(lossy),
				dial: func(ctx context.Context) (net.Conn, error) {
					return fibutp.DialContext(ctx, e, target, fibutp.Config{})
				},
				accept: ls.Accept,
			}.run(t)
		})
	}
}

func TestUTPLibutpDialsFib(t *testing.T) {
	for _, lossy := range []bool{false, true} {
		t.Run(fmt.Sprintf("lossy=%v", lossy), func(t *testing.T) {
			e := startEngine(t)
			fs, err := fibutp.Listen(e, "udp4", "127.0.0.1:0", fibutp.Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer fs.Close()
			ls, err := libutp.NewSocket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ls.Close()
			target := relay(t, fs.Addr().String(), lossy)
			session{
				size: size(lossy),
				dial: func(ctx context.Context) (net.Conn, error) {
					return ls.DialContext(ctx, "udp4", target)
				},
				accept: fs.Accept,
			}.run(t)
		})
	}
}
