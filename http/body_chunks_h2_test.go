//go:build linux || darwin || windows

package http

import (
	"fmt"
	stdhttp "net/http"
	"testing"
	"time"
)

// A body held while it arrives over h2c, until all of it has, reaches its
// callback whole and in order, a chunk at a time.
func TestH2HeldBodyArrivesInChunks(t *testing.T) {
	config := immediateStreamingConfig()
	url := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		var got []byte
		largest, held := 0, false
		c.OnBody(func(data []byte, fin bool, err error) {
			got = append(got, data...)
			largest = max(largest, len(data))
			if err != nil || fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %x %d %v", len(got), crc(got), largest, err))
				return
			}
			if !held {
				held = true
				c.HoldBody(true)
				time.AfterFunc(100*time.Millisecond, func() { c.HoldBody(false) })
			}
		})
	})))
	clientConfig := uploadClientConfig()
	clientConfig.UnencryptedHTTP2 = true
	client := newTestClient(t, clientConfig)
	body := chunkPattern(600 << 10)
	req, w := pushRequest(t, url, int64(len(body)), false)
	future := client.Go(req)
	go func() {
		for rest := body; len(rest) > 0; {
			n := min(len(rest), 40<<10)
			if _, err := w.Write(rest[:n]); err != nil {
				return
			}
			rest = rest[n:]
		}
		_ = w.Close()
	}()
	resp, err := future.Wait()
	if err != nil {
		t.Fatal(err)
	}
	var n, largest int
	var sum uint32
	var cbErr string
	if _, err := fmt.Sscanf(readBody(t, resp), "%d %x %d %s", &n, &sum, &largest, &cbErr); err != nil {
		t.Fatal(err)
	}
	if n != len(body) || sum != crc(body) || cbErr != "<nil>" {
		t.Fatalf("handler saw %d bytes, crc %x, err %s; sent %d, crc %x", n, sum, cbErr, len(body), crc(body))
	}
	if largest > streamChunk {
		t.Fatalf("a piece of %d bytes, more than a chunk", largest)
	}
}
