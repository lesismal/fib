//go:build linux

package fib

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"
)

// SendBatch sends runs of datagrams of one size as one segmented message,
// and the peer receives each datagram whole and in order all the same: runs
// that end in a shorter one, runs broken by a larger one, a run longer than
// a message takes, and datagrams of their own size between them. With GSO
// refused, as by a kernel without it, the same batch goes a message each.
func TestUDPSendBatchSegments(t *testing.T) {
	var sizes []int
	for range 13 {
		sizes = append(sizes, 1200)
	}
	sizes = append(sizes, 500, 800, 800, 800, 300, 1200, 100)
	for range 40 {
		sizes = append(sizes, 1000)
	}
	batch := make([][]byte, len(sizes))
	for i, n := range sizes {
		batch[i] = bytes.Repeat([]byte{byte('A' + i%26)}, n)
		copy(batch[i], fmt.Sprintf("%03d", i))
	}
	for _, off := range []bool{false, true} {
		t.Run(fmt.Sprintf("gsoOff=%v", off), func(t *testing.T) {
			was := gsoOff.Load()
			gsoOff.Store(off)
			defer gsoOff.Store(was)
			sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			_ = sink.SetReadBuffer(4 << 20)
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
			if !off && gsoOff.Load() {
				t.Fatal("the kernel refused UDP_SEGMENT, and the batch went a message each")
			}
			buf := make([]byte, 65536)
			_ = sink.SetReadDeadline(time.Now().Add(5 * time.Second))
			for i := range batch {
				n, _, err := sink.ReadFromUDP(buf)
				if err != nil {
					t.Fatalf("datagram %d: %v", i, err)
				}
				if !bytes.Equal(buf[:n], batch[i]) {
					t.Fatalf("datagram %d of %d bytes arrived as %d bytes starting %q", i, len(batch[i]), n, buf[:min(n, 8)])
				}
			}
		})
	}
}
