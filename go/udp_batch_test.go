//go:build linux || darwin

package fib

import (
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
)

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
			b := newUDPBatch()
			b.single = single
			var got []string
			var empty bool
			_ = raw.Read(func(fd uintptr) bool {
				for len(got) < len(peers) {
					n, e, err := b.recv(int(fd), udpBatchSize, false)
					if err != nil {
						t.Fatal(err)
					}
					empty = e
					for i := 0; i < n; i++ {
						key, _, ok := rawSockaddrKey(&b.names[i])
						if !ok {
							t.Fatal("no sender address")
						}
						got = append(got, fmt.Sprintf("%s from %v", b.datagram(i), key))
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
			if _, _, err := b.recv(int(fdOf(t, raw)), udpBatchSize, false); err != syscall.EAGAIN {
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
// refuses batches, one at a time.
func TestUDPSendBatch(t *testing.T) {
	for _, single := range []bool{false, true} {
		t.Run(fmt.Sprintf("single=%v", single), func(t *testing.T) {
			singleSends.Store(single)
			defer singleSends.Store(false)
			batch := make([][]byte, udpBatchSize+5)
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
			engine, _ := startEchoServer(t, DefaultConfig(), nil)
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
			if err := client.SendBatch(batch); err != nil {
				t.Fatal(err)
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

// A batched send describes its datagrams to the kernel without allocating.
func TestUDPSendBatchAllocs(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	to := &syscall.SockaddrInet4{Port: sink.LocalAddr().(*net.UDPAddr).Port, Addr: [4]byte{127, 0, 0, 1}}
	batch := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	if _, err := sendBatch(fd, to, batch); err != nil {
		t.Fatal(err)
	}
	if singleSends.Load() {
		t.Skip("the kernel sends one datagram at a time")
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = sendBatchSys(fd, to, batch) }); allocs != 0 {
		t.Fatalf("a batched send allocated %v times", allocs)
	}
}
