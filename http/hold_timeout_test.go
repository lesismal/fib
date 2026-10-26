//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// holdForever holds the body after its first piece and never lets it go,
// answering with the error its callback is told.
func holdForever(c *Context) {
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil || fin {
			_ = c.Respond(stdhttp.StatusGatewayTimeout, "text/plain", fmt.Appendf(nil, "%v", err))
			return
		}
		c.HoldBody(true)
	})
}

// A body held longer than MaxBodyHold fails with ErrBodyHeldTooLong, over
// HTTP/1 and h2c, which its callback answers.
func TestMaxBodyHold(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.MaxBodyHold = 100 * time.Millisecond
	url := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(holdForever)))
	h1 := newTestClient(t, uploadClientConfig())
	h2config := uploadClientConfig()
	h2config.UnencryptedHTTP2 = true
	h2c := newTestClient(t, h2config)
	for _, tc := range []struct {
		name   string
		client *Client
	}{{"http1", h1}, {"h2c", h2c}} {
		t.Run(tc.name, func(t *testing.T) {
			req, body := pushRequest(t, url, -1, false)
			future := tc.client.Go(req)
			_, _ = body.Write(make([]byte, 8<<10))
			start := time.Now()
			resp, err := future.Wait()
			if err != nil {
				t.Fatal(err)
			}
			got := readBody(t, resp)
			if resp.StatusCode != stdhttp.StatusGatewayTimeout || got != ErrBodyHeldTooLong.Error() {
				t.Fatalf("status %d, body %q", resp.StatusCode, got)
			}
			// The hold may have begun before start, which is taken after
			// the upload: a hundred milliseconds from here is the right
			// answer only if nothing was spent before it. Half of
			// MaxBodyHold still tells an immediate failure apart from the
			// hold running its course, and a coarse Windows clock cannot
			// move it.
			if waited := time.Since(start); waited < 50*time.Millisecond {
				t.Fatalf("failed after %v, before MaxBodyHold", waited)
			}
		})
	}
}

// Holds that each end before MaxBodyHold do not add up to it, and a negative
// MaxBodyHold holds as long as asked.
func TestMaxBodyHoldPerHold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		max   time.Duration
		hold  time.Duration
		holds int
	}{{"short holds", 150 * time.Millisecond, 50 * time.Millisecond, 5}, {"unbounded", -1, 250 * time.Millisecond, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			config := immediateStreamingConfig()
			config.MaxBodyHold = tc.max
			var holds atomic.Int32
			addr := serveStreamingServer(t, config, func(c *Context) {
				var total int
				c.OnBody(func(data []byte, fin bool, err error) {
					total += len(data)
					if err != nil || fin {
						_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %v", total, err))
						return
					}
					if int(holds.Add(1)) <= tc.holds {
						c.HoldBody(true)
						time.AfterFunc(tc.hold, func() { c.HoldBody(false) })
					}
				})
			})
			conn := dialRaw(t, addr)
			const size = 64 << 10
			conn.send(fmt.Sprintf("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", size))
			go func() {
				for i := 0; i < size; i += 1 << 10 {
					if _, err := conn.Write(bytes.Repeat([]byte("p"), 1<<10)); err != nil {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
			}()
			if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d <nil>", size) {
				t.Fatalf("body = %q", body)
			}
		})
	}
}

// A timer that fires for a hold that has ended, or that ended and began
// again since, leaves the body alone.
func TestHoldExpiredStale(t *testing.T) {
	s := &BodyStream{maxHold: time.Hour}
	s.holdExpired()
	s.sinkHeld, s.heldSince = true, time.Now()
	s.holdExpired()
	if s.err != nil {
		t.Fatalf("a stale timer failed the body: %v", s.err)
	}
	// A body already over is not failed again.
	ended := &BodyStream{ended: true}
	ended.failElsewhere(ErrBodyHeldTooLong)
	if ended.err != nil {
		t.Fatalf("an ended body failed: %v", ended.err)
	}
	if maxBodyHold(0) != DefaultMaxBodyHold || maxBodyHold(-1) != 0 || maxBodyHold(time.Second) != time.Second {
		t.Fatal("maxBodyHold")
	}
}

// ReadBodyTimeout closes an HTTP/1 connection whose body stops arriving, even
// once its unread body has stopped the connection's reads, where the peer
// going away would not be heard: the case a handler that holds its body, or
// does not read it, would otherwise leave open for good. ReadTimeout does the
// same for the whole request.
func TestReadBodyTimeoutWithReadsHeld(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*Config)
	}{
		{"body", func(c *Config) { c.ReadBodyTimeout = 200 * time.Millisecond }},
		{"request", func(c *Config) { c.ReadTimeout = 200 * time.Millisecond }},
		{"both", func(c *Config) { c.ReadTimeout, c.ReadBodyTimeout = time.Hour, 200*time.Millisecond }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := immediateStreamingConfig()
			config.StreamRequestBodyBuffer = 16 << 10
			tc.configure(&config)
			canceled := make(chan error, 1)
			held := make(chan bool, 1)
			addr := serveStreamingServer(t, config, func(c *Context) {
				// Never reads the body, and never answers.
				c.Retain()
				c.OnCancel(func(err error) {
					canceled <- err
					c.Release()
				})
				go func() {
					deadline := time.Now().Add(5 * time.Second)
					for !c.Conn.ReadsHeld() && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					held <- c.Conn.ReadsHeld()
				}()
			})
			conn := dialRaw(t, addr)
			conn.send(fmt.Sprintf("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 8<<20))
			go func() { _, _ = conn.Write(make([]byte, 1<<20)) }()
			if !<-held {
				t.Fatal("the connection never held its reads")
			}
			select {
			case <-canceled:
			case <-time.After(10 * time.Second):
				t.Fatal("the request outlived its read timeout with its reads held")
			}
			// The server closed the connection, which the client may hear as a
			// reset, with the body it sent unread.
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, _ = io.Copy(io.Discard, conn)
		})
	}
}

// On h2c the read timeouts fail the stalled body alone, with
// os.ErrDeadlineExceeded, and the connection goes on serving.
func TestReadBodyTimeoutH2(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*Config)
	}{
		{"body", func(c *Config) { c.ReadBodyTimeout = 200 * time.Millisecond }},
		{"request", func(c *Config) { c.ReadTimeout = 200 * time.Millisecond }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultConfig()
			config.StreamRequestBody = true
			tc.configure(&config)
			url := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(fibUploadSink)))
			h2config := uploadClientConfig()
			h2config.UnencryptedHTTP2 = true
			client := newTestClient(t, h2config)
			req, body := pushRequest(t, url, -1, false)
			future := client.Go(req)
			_, _ = body.Write(make([]byte, 8<<10))
			resp, err := future.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); resp.StatusCode != stdhttp.StatusBadRequest || got != os.ErrDeadlineExceeded.Error() {
				t.Fatalf("status %d, body %q", resp.StatusCode, got)
			}
			// The same connection serves a body that arrives in time.
			req, body = pushRequest(t, url, -1, false)
			future = client.Go(req)
			_, _ = body.Write([]byte("quick"))
			_ = body.Close()
			resp, err = future.Wait()
			if err != nil || resp.StatusCode != stdhttp.StatusOK {
				t.Fatalf("err %v, status %v", err, resp)
			}
		})
	}
}

func TestStreamBodyDeadline(t *testing.T) {
	start := time.Unix(1000, 0)
	for _, tc := range []struct {
		read, body time.Duration
		want       time.Time
	}{
		{0, 0, time.Time{}},
		{time.Second, 0, start.Add(time.Second)},
		{0, time.Second, start.Add(time.Second)},
		{time.Second, 2 * time.Second, start.Add(time.Second)},
		{2 * time.Second, time.Second, start.Add(time.Second)},
	} {
		config := Config{ReadTimeout: tc.read, ReadBodyTimeout: tc.body}
		if got := streamBodyDeadline(&config, start); !got.Equal(tc.want) {
			t.Errorf("ReadTimeout %v, ReadBodyTimeout %v: %v, want %v", tc.read, tc.body, got, tc.want)
		}
	}
}

// On h2c a body still being collected before its handler runs, under the
// streaming threshold or on a server that does not stream bodies, has its
// stream reset once the read timeouts pass; a body that arrives in time is
// served on the same connection, its timer stopped.
func TestReadBodyTimeoutH2BeforeHandler(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			config := DefaultConfig()
			config.StreamRequestBody = stream
			config.StreamRequestBodyThreshold = 1 << 20
			config.ReadBodyTimeout = 200 * time.Millisecond
			var ran atomic.Bool
			url := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
				ran.Store(true)
				fibUploadSink(c)
			})))
			h2config := uploadClientConfig()
			h2config.UnencryptedHTTP2 = true
			client := newTestClient(t, h2config)
			req, body := pushRequest(t, url, -1, false)
			future := client.Go(req)
			_, _ = body.Write(make([]byte, 8<<10))
			if _, err := future.Wait(); err == nil {
				t.Fatal("a stalled body was answered")
			}
			if ran.Load() {
				t.Fatal("the handler ran for a body that never arrived")
			}
			req, body = pushRequest(t, url, -1, false)
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

// DefaultConfig bounds every read a client could stall, and a Config built
// without it bounds none.
func TestDefaultConfigReadTimeouts(t *testing.T) {
	config := DefaultConfig()
	if config.ReadHeaderTimeout != DefaultReadHeaderTimeout || config.ReadBodyTimeout != DefaultReadBodyTimeout ||
		config.IdleTimeout != DefaultIdleTimeout || config.ReadTimeout != 0 {
		t.Fatalf("DefaultConfig timeouts: %+v", config)
	}
	if h := NewHandlerWithConfig(config, nil); !h.timed {
		t.Fatal("a server from DefaultConfig is not timed")
	}
	if h := NewHandlerWithConfig(Config{}, nil); h.timed {
		t.Fatal("a zero Config is timed")
	}
}
