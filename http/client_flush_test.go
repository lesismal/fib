//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// A request sent from a response's callback goes on the connection that
// response came on, which the client has taken back while the round that
// read it is still going on: the round holds what is sent until it ends,
// after the callback, unless the request asks for Framing.Flush, which has
// it written at once.
func TestClientFlushWritesInsideTheRound(t *testing.T) {
	for _, flush := range []bool{false, true} {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		got := make(chan struct{}, 4)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			var pending []byte
			buf := make([]byte, 4096)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					return
				}
				pending = append(pending, buf[:n]...)
				for {
					end := bytes.Index(pending, []byte("\r\n\r\n"))
					if end < 0 {
						break
					}
					pending = pending[end+4:]
					got <- struct{}{}
					if _, err := conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")); err != nil {
						return
					}
				}
			}
		}()
		client := newTestClient(t, DefaultClientConfig())
		url := "http://" + listener.Addr().String() + "/"
		arrived := make(chan bool, 1)
		second := make(chan error, 1)
		client.Do(mustRequest(t, "GET", url, nil), func(resp *ClientResponse, err error) {
			if err != nil {
				t.Error(err)
				arrived <- false
				return
			}
			<-got
			resp.Close()
			client.StartFramed(mustRequest(t, "GET", url, nil), Framing{Flush: flush}, func(resp *ClientResponse, err error) {
				if resp != nil {
					resp.Close()
				}
				second <- err
			})
			// The round is still going on, and holds what it corked for as
			// long as this callback runs.
			wait := 200 * time.Millisecond
			if flush {
				wait = 5 * time.Second
			}
			select {
			case <-got:
				arrived <- true
			case <-time.After(wait):
				arrived <- false
			}
		})
		if a := <-arrived; a != flush {
			t.Fatalf("flush %v: the second request arrived during the first's callback: %v", flush, a)
		}
		select {
		case err := <-second:
			if err != nil {
				t.Fatalf("flush %v: %v", flush, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("flush %v: the second request was not answered", flush)
		}
	}
}
