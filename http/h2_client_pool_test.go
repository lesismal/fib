//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"sync"
	"testing"
	"time"
)

func TestLeastLoadedPicksMostRoom(t *testing.T) {
	conn := func(streams, maxStreams int) *clientConn {
		return &clientConn{streams: streams, maxStreams: maxStreams}
	}
	full, busy, light, other := conn(2, 2), conn(5, 10), conn(1, 10), conn(9, 10)
	small, large := conn(1, 3), conn(50, 100)
	tie1, tie2 := conn(3, 10), conn(3, 10)
	cases := []struct {
		name  string
		conns []*clientConn
		want  *clientConn
	}{
		{"none", nil, nil},
		{"all full", []*clientConn{full, conn(100, 100)}, nil},
		{"lone", []*clientConn{busy}, busy},
		{"skips the full first", []*clientConn{full, busy}, busy},
		{"most room, not first", []*clientConn{other, busy, light}, light},
		{"room by each server's limit", []*clientConn{small, large}, large},
		{"tie goes to the earlier", []*clientConn{tie1, tie2}, tie1},
	}
	for _, tc := range cases {
		if got := leastLoadedLocked(tc.conns); got != tc.want {
			t.Errorf("%s: picked %p, want %p", tc.name, got, tc.want)
		}
	}
}

// A request goes to the HTTP/2 connection with the most streams to spare, not
// the first that has any: once a second connection is open, a freed stream on
// the first does not draw every new request back to it.
func TestClientHTTP2PrefersLeastLoadedConnection(t *testing.T) {
	type seen struct {
		conn int
		path string
	}
	arrived := make(chan seen, 16)
	release := make(chan struct{})
	url := cvH2cServe(t, func(p *cvH2cPeer, i int) {
		limit := uint32(2)
		if i > 0 {
			limit = 10
		}
		p.settings([2]uint32{uint32(h2SettingMaxConcurrentStreams), limit})
		var mu sync.Mutex
		answer := func(id uint32) {
			mu.Lock()
			defer mu.Unlock()
			p.headers(id, true, ":status", "200")
		}
		var held []uint32
		for {
			id, fields, ok := p.request()
			if !ok {
				return
			}
			arrived <- seen{i, fields[":path"]}
			switch fields[":path"] {
			case "/hold":
				held = append(held, id)
				if i == 0 && len(held) == 2 {
					// The first of the two held here is answered on demand.
					first := held[0]
					go func() {
						<-release
						answer(first)
					}()
				}
			default:
				answer(id)
			}
		}
	})
	client := cvH2cClient(t, nil)
	next := func(t *testing.T) seen {
		t.Helper()
		select {
		case s := <-arrived:
			return s
		case <-time.After(10 * time.Second):
			t.Fatal("no request arrived")
		}
		return seen{}
	}
	// The first answer brings the server's SETTINGS, and with them the
	// connection's limit of two streams.
	if _, err := client.Go(mustRequest(t, "GET", url+"/warm", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	next(t)
	holds := make([]*Future, 3)
	for i := range holds {
		holds[i] = client.Go(mustRequest(t, "GET", url+"/hold", nil))
		if s := next(t); s.conn != min(i/2, 1) {
			t.Fatalf("held request %d went to connection %d", i, s.conn)
		}
	}
	close(release)
	if _, err := holds[0].Wait(); err != nil {
		t.Fatal(err)
	}
	// The first connection has one stream of two to spare, the second nine
	// of ten.
	if _, err := client.Go(mustRequest(t, "GET", url+"/quick", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	if s := next(t); s.conn != 1 {
		t.Fatalf("request went to connection %d, want the less loaded 1", s.conn)
	}
}
