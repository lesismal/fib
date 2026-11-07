// Package interop checks fib's KCP against github.com/xtaci/kcp-go, in
// both roles, over a UDP relay that drops datagrams: every crypt the two
// share, with and without forward error correction, so that the parity
// each sends is the parity the other recovers from.
package interop

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib"
	fibkcp "github.com/lesismal/fib/kcp"
	kcpgo "github.com/xtaci/kcp-go/v5"
	"golang.org/x/crypto/blowfish"
	"golang.org/x/crypto/cast5"
	"golang.org/x/crypto/tea"
	"golang.org/x/crypto/twofish"
	"golang.org/x/crypto/xtea"
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

// relay forwards datagrams between one client and target, dropping every
// dropEvery-th each way (none when 0), and returns its address.
func relay(t *testing.T, target string, dropEvery int) string {
	t.Helper()
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ta, _ := net.ResolveUDPAddr("udp", target)
	back, err := net.DialUDP("udp", nil, ta)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { front.Close(); back.Close() })
	var client atomic.Pointer[net.UDPAddr]
	drop := func(n int) bool { return dropEvery > 0 && n%dropEvery == dropEvery/2 }
	go func() {
		buf := make([]byte, 65536)
		for n := 0; ; n++ {
			m, from, err := front.ReadFromUDP(buf)
			if err != nil {
				return
			}
			client.Store(from)
			if !drop(n) {
				_, _ = back.Write(buf[:m])
			}
		}
	}()
	go func() {
		buf := make([]byte, 65536)
		for n := 0; ; n++ {
			m, err := back.Read(buf)
			if err != nil {
				return
			}
			if c := client.Load(); c != nil && !drop(n) {
				_, _ = front.WriteToUDP(buf[:m], c)
			}
		}
	}()
	return front.LocalAddr().String()
}

type crypts struct {
	name string
	fib  func() (*fibkcp.Crypt, error)
	go_  func() (kcpgo.BlockCrypt, error)
}

var key = func() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i*29 + 1)
	}
	return k
}()

func block(nb func([]byte) (cipher.Block, error), k []byte) func() (*fibkcp.Crypt, error) {
	return func() (*fibkcp.Crypt, error) {
		b, err := nb(k)
		if err != nil {
			return nil, err
		}
		return fibkcp.NewBlockCrypt(b)
	}
}

func allCrypts() []crypts {
	named := func(name string, n int, g func([]byte) (kcpgo.BlockCrypt, error)) crypts {
		return crypts{name,
			func() (*fibkcp.Crypt, error) { return fibkcp.NewCrypt(name, key) },
			func() (kcpgo.BlockCrypt, error) { return g(key[:n]) }}
	}
	return []crypts{
		{"null", func() (*fibkcp.Crypt, error) { return nil, nil }, func() (kcpgo.BlockCrypt, error) { return nil, nil }},
		named("none", 32, kcpgo.NewNoneBlockCrypt),
		named("xor", 32, kcpgo.NewSimpleXORBlockCrypt),
		named("aes", 32, kcpgo.NewAESBlockCrypt),
		named("aes-128", 16, kcpgo.NewAESBlockCrypt),
		named("aes-192", 24, kcpgo.NewAESBlockCrypt),
		named("3des", 24, kcpgo.NewTripleDESBlockCrypt),
		named("aes-128-gcm", 16, kcpgo.NewAESGCMCrypt),
		{"blowfish", block(func(k []byte) (cipher.Block, error) { return blowfish.NewCipher(k) }, key),
			func() (kcpgo.BlockCrypt, error) { return kcpgo.NewBlowfishBlockCrypt(key) }},
		{"cast5", block(func(k []byte) (cipher.Block, error) { return cast5.NewCipher(k) }, key[:16]),
			func() (kcpgo.BlockCrypt, error) { return kcpgo.NewCast5BlockCrypt(key[:16]) }},
		{"twofish", block(func(k []byte) (cipher.Block, error) { return twofish.NewCipher(k) }, key),
			func() (kcpgo.BlockCrypt, error) { return kcpgo.NewTwofishBlockCrypt(key) }},
		{"tea", block(func(k []byte) (cipher.Block, error) { return tea.NewCipherWithRounds(k, 16) }, key[:16]),
			func() (kcpgo.BlockCrypt, error) { return kcpgo.NewTEABlockCrypt(key[:16]) }},
		{"xtea", block(func(k []byte) (cipher.Block, error) { return xtea.NewCipher(k) }, key[:16]),
			func() (kcpgo.BlockCrypt, error) { return kcpgo.NewXTEABlockCrypt(key[:16]) }},
	}
}

var (
	_ = aes.BlockSize
	_ = des.BlockSize
)

const payload = 1 << 20

func data() []byte {
	b := make([]byte, payload)
	for i := range b {
		b[i] = byte(i*7 + i>>11)
	}
	return b
}

// echo writes data through c and reads it back.
func echo(t *testing.T, c net.Conn) {
	t.Helper()
	d := data()
	errc := make(chan error, 1)
	go func() {
		for b := d; len(b) > 0; {
			n := min(len(b), 32<<10)
			if _, err := c.Write(b[:n]); err != nil {
				errc <- err
				return
			}
			b = b[n:]
		}
		errc <- nil
	}()
	_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
	got := make([]byte, len(d))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, d) {
		t.Fatal("echo differs")
	}
}

var fast3 = fibkcp.Config{NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true}

var shardings = [][2]int{{0, 0}, {10, 3}}

// mu serializes the cases: kcp-go's FEC counters are global.
var mu sync.Mutex

// TestFibServerKcpGoClient runs fib's Server, echoing, for kcp-go clients.
func TestFibServerKcpGoClient(t *testing.T) {
	for _, cr := range allCrypts() {
		for _, sh := range shardings {
			t.Run(fmt.Sprintf("%s-fec%d.%d", cr.name, sh[0], sh[1]), func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				fc, err := cr.fib()
				if err != nil {
					t.Fatal(err)
				}
				gc, err := cr.go_()
				if err != nil {
					t.Fatal(err)
				}
				e := startEngine(t)
				cfg := fast3
				cfg.Crypt, cfg.DataShards, cfg.ParityShards = fc, sh[0], sh[1]
				var sess atomic.Pointer[fibkcp.Session]
				srv, err := fibkcp.Listen(e, "127.0.0.1:0", fibkcp.ServerConfig{Config: cfg})
				if err != nil {
					t.Fatal(err)
				}
				defer srv.Close()
				go func() {
					c, err := srv.Accept()
					if err != nil {
						return
					}
					sess.Store(c.(*fibkcp.Session))
					_, _ = io.Copy(c, c)
				}()
				addr := relay(t, srv.Addr().String(), 17)
				before := kcpgo.DefaultSnmp.Copy().FECRecovered
				c, err := kcpgo.DialWithOptions(addr, gc, sh[0], sh[1])
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				c.SetNoDelay(1, 10, 2, 1)
				c.SetWindowSize(128, 128)
				echo(t, c)
				goRecovered := kcpgo.DefaultSnmp.Copy().FECRecovered - before
				fibRecovered := sess.Load().Stats().FECRecovered
				t.Logf("recovered: fib %d, kcp-go %d", fibRecovered, goRecovered)
				if sh[0] > 0 && (fibRecovered == 0 || goRecovered == 0) {
					t.Fatalf("FEC recovered nothing: fib %d, kcp-go %d", fibRecovered, goRecovered)
				}
			})
		}
	}
}

// TestKcpGoServerFibClient runs kcp-go's Listener, echoing, for fib
// clients.
func TestKcpGoServerFibClient(t *testing.T) {
	for _, cr := range allCrypts() {
		for _, sh := range shardings {
			t.Run(fmt.Sprintf("%s-fec%d.%d", cr.name, sh[0], sh[1]), func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				fc, err := cr.fib()
				if err != nil {
					t.Fatal(err)
				}
				gc, err := cr.go_()
				if err != nil {
					t.Fatal(err)
				}
				l, err := kcpgo.ListenWithOptions("127.0.0.1:0", gc, sh[0], sh[1])
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				go func() {
					c, err := l.AcceptKCP()
					if err != nil {
						return
					}
					c.SetNoDelay(1, 10, 2, 1)
					c.SetWindowSize(128, 128)
					_, _ = io.Copy(c, c)
				}()
				addr := relay(t, l.Addr().String(), 17)
				e := startEngine(t)
				cfg := fast3
				cfg.Crypt, cfg.DataShards, cfg.ParityShards = fc, sh[0], sh[1]
				before := kcpgo.DefaultSnmp.Copy().FECRecovered
				c, err := fibkcp.Dial(e, addr, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				echo(t, c)
				goRecovered := kcpgo.DefaultSnmp.Copy().FECRecovered - before
				fibRecovered := c.Stats().FECRecovered
				t.Logf("recovered: fib %d, kcp-go %d", fibRecovered, goRecovered)
				if sh[0] > 0 && (fibRecovered == 0 || goRecovered == 0) {
					t.Fatalf("FEC recovered nothing: fib %d, kcp-go %d", fibRecovered, goRecovered)
				}
			})
		}
	}
}

// TestRawKCP drives fib's bare KCP against kcp-go's over memory, no
// session on either side, in message mode with fragments.
func TestRawKCP(t *testing.T) {
	var a *fibkcp.KCP
	var b *kcpgo.KCP
	var toA, toB [][]byte
	a = fibkcp.New(42, func(p []byte) { toB = append(toB, append([]byte(nil), p...)) })
	b = kcpgo.NewKCP(42, func(p []byte, n int) { toA = append(toA, append([]byte(nil), p[:n]...)) })
	a.NoDelay(1, 10, 2, true)
	b.NoDelay(1, 10, 2, 1)
	b.WndSize(128, 128)
	var msgs [][]byte
	for i := range 50 {
		m := bytes.Repeat([]byte{byte(i)}, 1+i*1777%(30*a.MSS()))
		msgs = append(msgs, m)
		if err := a.Send(m); err != nil {
			t.Fatal(err)
		}
		if b.Send(m) != 0 {
			t.Fatal("kcp-go send")
		}
	}
	start := time.Now()
	now := func() uint32 { return uint32(time.Since(start) / time.Millisecond) }
	gotA, gotB := 0, 0
	buf := make([]byte, 255*1400)
	for deadline := time.Now().Add(20 * time.Second); gotA < len(msgs) || gotB < len(msgs); {
		if time.Now().After(deadline) {
			t.Fatalf("stuck: fib got %d, kcp-go got %d", gotA, gotB)
		}
		a.Flush(now())
		b.Update()
		for _, p := range toB {
			b.Input(p, kcpgo.IKCP_PACKET_REGULAR, true)
		}
		toB = toB[:0]
		for _, p := range toA {
			if err := a.Input(p, now()); err != nil {
				t.Fatal(err)
			}
		}
		toA = toA[:0]
		for {
			n, err := a.Recv(buf)
			if err != nil {
				break
			}
			if !bytes.Equal(buf[:n], msgs[gotA]) {
				t.Fatalf("fib message %d differs", gotA)
			}
			gotA++
		}
		for {
			n := b.Recv(buf)
			if n < 0 {
				break
			}
			if !bytes.Equal(buf[:n], msgs[gotB]) {
				t.Fatalf("kcp-go message %d differs", gotB)
			}
			gotB++
		}
		time.Sleep(time.Millisecond)
	}
}
