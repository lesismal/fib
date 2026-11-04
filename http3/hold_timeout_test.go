//go:build linux || darwin || windows

package http3

import (
	"fmt"
	stdhttp "net/http"
	"os"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// answerFailure holds the body after its first piece when hold is set, and
// answers with the error that ends it.
func answerFailure(hold bool) func(*fibhttp.Context) {
	return func(c *fibhttp.Context) {
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil || fin {
				_ = c.Respond(stdhttp.StatusGatewayTimeout, "text/plain", fmt.Appendf(nil, "%v", err))
				return
			}
			if hold {
				c.HoldBody(true)
			}
		})
	}
}

// A body held past MaxBodyHold, and one that stops arriving for its read
// timeouts, fail on their own stream, which the handler answers.
func TestHoldAndReadTimeouts(t *testing.T) {
	client := uploadClient(t)
	for _, tc := range []struct {
		name   string
		config Config
		hold   bool
		want   string
	}{
		{"hold", Config{StreamRequestBody: true, MaxBodyHold: 100 * time.Millisecond}, true, fibhttp.ErrBodyHeldTooLong.Error()},
		{"body", Config{StreamRequestBody: true, ReadBodyTimeout: 150 * time.Millisecond}, false, os.ErrDeadlineExceeded.Error()},
		{"request", Config{StreamRequestBody: true, ReadTimeout: 150 * time.Millisecond, ReadBodyTimeout: time.Hour}, false, os.ErrDeadlineExceeded.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := startServer(t, tc.config, answerFailure(tc.hold))
			body := fibhttp.NewBodyWriter(0)
			req := mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
			req.Body = body
			future := client.Go(req)
			_, _ = body.Write(make([]byte, 8<<10))
			resp, err := future.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); resp.StatusCode != stdhttp.StatusGatewayTimeout || got != tc.want {
				t.Fatalf("status %d, body %q, want %q", resp.StatusCode, got, tc.want)
			}
		})
	}
}

func TestBodyDeadline(t *testing.T) {
	start := time.Unix(1000, 0)
	if !bodyDeadline(&Config{}, start).IsZero() {
		t.Fatal("a deadline without timeouts")
	}
	if got := bodyDeadline(&Config{ReadTimeout: 2 * time.Second, ReadBodyTimeout: time.Second}, start); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("got %v", got)
	}
}

// A body still being collected before its handler runs has its stream reset
// once the read timeouts pass, on a server that streams bodies past a
// threshold the body never reaches and on one that does not stream them; one
// that arrives in time is served, its timer stopped.
func TestReadTimeoutBeforeHandler(t *testing.T) {
	client := uploadClient(t)
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			config := Config{StreamRequestBody: stream, StreamRequestBodyThreshold: 1 << 20, ReadBodyTimeout: 200 * time.Millisecond}
			url := startServer(t, config, uploadSink)
			body := fibhttp.NewBodyWriter(0)
			req := mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
			req.Body = body
			future := client.Go(req)
			_, _ = body.Write(make([]byte, 8<<10))
			if _, err := future.Wait(); err == nil {
				t.Fatal("a stalled body was answered")
			}
			body = fibhttp.NewBodyWriter(0)
			req = mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
			req.Body = body
			future = client.Go(req)
			_, _ = body.Write([]byte("quick"))
			_ = body.Close()
			resp, err := future.Wait()
			if err != nil || resp.StatusCode != stdhttp.StatusOK {
				t.Fatalf("err %v, status %v", err, resp)
			}
		})
	}
}

// A body that streams after being collected keeps the deadline it had from
// its header: it fails a pause short of the timeout after it begins to
// stream, 300ms, and not a whole timeout, 800ms, which it would were the
// deadline started again. The two are a pause apart, and the check sits
// halfway, so that a loaded runner's timers have 250ms either way: with a
// 200ms pause one waited 302ms against 300ms (macOS CI, run 41838563945). The timeout runs from when the header reaches
// the server, so the connection is dialed first: on a fresh one the
// header waits for the QUIC handshake, which on a loaded runner took
// long enough to push the failure past the old 80ms margin (CI run 203).
func TestReadTimeoutCarriesIntoStream(t *testing.T) {
	const timeout, pause = 800 * time.Millisecond, 500 * time.Millisecond
	client := uploadClient(t)
	url := startServer(t, Config{StreamRequestBody: true, StreamRequestBodyThreshold: 4 << 10, ReadBodyTimeout: timeout}, answerFailure(false))
	if resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/dial", nil)).Wait(); err != nil {
		t.Fatalf("dialing: %v", err)
	} else {
		readBody(t, resp)
	}
	body := fibhttp.NewBodyWriter(0)
	req := mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
	req.Body = body
	future := client.Go(req)
	_, _ = body.Write(make([]byte, 2<<10))
	time.Sleep(pause)
	_, _ = body.Write(make([]byte, 8<<10))
	start := time.Now()
	resp, err := future.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != os.ErrDeadlineExceeded.Error() {
		t.Fatalf("body %q", got)
	}
	if waited := time.Since(start); waited > timeout-pause/2 {
		t.Fatalf("the deadline started again when the body began to stream: %v more", waited)
	}
}
