//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// healthConfig checks connections that are quiet for 100ms and gives the
// server 200ms to answer.
func healthConfig(c *ClientConfig) {
	c.ReadIdleTimeout = 100 * time.Millisecond
	c.PingTimeout = 200 * time.Millisecond
}

// silentPeer reads a request, reports whether the health check's PING came,
// and then says nothing more, as a server whose connection died without a
// word would, until the client closes the connection.
func silentPeer(p *cvH2cPeer, pinged chan<- bool) {
	p.settings()
	if _, _, ok := p.request(); !ok {
		return
	}
	got := false
	for {
		f, ok := p.next()
		if !ok {
			break
		}
		if f.typ == h2FramePing && !f.has(h2FlagAck) && bytes.Equal(f.payload, healthPing[:]) {
			got = true
		}
	}
	pinged <- got
}

// A connection whose server stops answering is taken for dead once a PING
// goes unanswered for PingTimeout, and a request on it that is not safe to
// repeat fails with ErrPingTimeout, long before its Timeout.
func TestClientHTTP2HealthCheckFindsDeadConnection(t *testing.T) {
	pinged := make(chan bool, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) { silentPeer(p, pinged) })
	client := cvH2cClient(t, healthConfig)
	start := time.Now()
	_, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
	if !errors.Is(err, ErrPingTimeout) {
		t.Fatalf("err = %v, want ErrPingTimeout", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %v", d)
	}
	if !<-pinged {
		t.Fatal("the server saw no PING")
	}
}

// A request the dead connection carried without any answer, and that is safe
// to repeat, goes again on a new connection.
func TestClientHTTP2HealthCheckRetriesOnNewConnection(t *testing.T) {
	pinged := make(chan bool, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, i int) {
		if i == 0 {
			silentPeer(p, pinged)
			return
		}
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, false, ":status", "200")
		p.frame(h2FrameData, h2FlagEndStream, id, []byte("second")...)
		p.drain()
	})
	client := cvH2cClient(t, healthConfig)
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "second" {
		t.Fatalf("got %q", got)
	}
	if !<-pinged {
		t.Fatal("the server saw no PING")
	}
}

// A server that answers the PINGs keeps its connection however long the
// response takes, and so does one that keeps sending without answering
// them, for 1s, well past ReadIdleTimeout and PingTimeout.
func TestClientHTTP2HealthCheckKeepsLiveConnection(t *testing.T) {
	for _, mode := range []string{"ack", "trickle"} {
		t.Run(mode, func(t *testing.T) {
			pings := make(chan int, 1)
			url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
				p.settings()
				id, _, ok := p.request()
				if !ok {
					return
				}
				p.headers(id, false, ":status", "200")
				n := 0
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					if mode == "trickle" {
						p.frame(h2FrameData, 0, id, 'x')
						time.Sleep(50 * time.Millisecond)
						continue
					}
					_ = p.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
					f, ok := p.next()
					if ok && f.typ == h2FramePing && !f.has(h2FlagAck) {
						n++
						p.frame(h2FramePing, h2FlagAck, 0, f.payload...)
					}
				}
				_ = p.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
				p.frame(h2FrameData, h2FlagEndStream, id, []byte("done")...)
				pings <- n
				p.drain()
			})
			client := cvH2cClient(t, healthConfig)
			resp, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); !bytes.HasSuffix([]byte(got), []byte("done")) {
				t.Fatalf("got %q", got)
			}
			n := <-pings
			if mode == "ack" && n == 0 {
				t.Fatal("no PING was sent while the server was quiet")
			}
		})
	}
}

// Without ReadIdleTimeout nothing is checked: a quiet connection is left to
// the request's Timeout.
func TestClientHTTP2HealthCheckOff(t *testing.T) {
	pinged := make(chan bool, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) { silentPeer(p, pinged) })
	client := cvH2cClient(t, func(c *ClientConfig) { c.Timeout = 600 * time.Millisecond })
	_, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
	if errors.Is(err, ErrPingTimeout) || err == nil {
		t.Fatalf("err = %v, want the request's timeout", err)
	}
	client.Close()
}
