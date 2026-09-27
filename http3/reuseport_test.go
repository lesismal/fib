//go:build linux || darwin

package http3

import (
	"fmt"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"

	fib "github.com/lesismal/fib"
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

