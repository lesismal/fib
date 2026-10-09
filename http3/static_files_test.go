//go:build linux || darwin || windows

package http3

import (
	"io"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/statictest"
)

// TestStaticFiles serves files over HTTP/3: whole and ranged, small and big,
// from the file cache, from net/http's helpers and with io.Copy from a file.
func TestStaticFiles(t *testing.T) {
	dir, files := statictest.Setup(t)
	url := startServer(t, DefaultConfig(), statictest.Handler(t, dir))
	client := newClient(t, func(c *ClientConfig) { c.Timeout = 30 * time.Second })
	statictest.Run(t, func(method, path string, header map[string]string) (int, stdhttp.Header, []byte) {
		req, err := stdhttp.NewRequest(method, url+path, nil)
		if err != nil {
			t.Errorf("%s %s: %v", method, path, err)
			return 0, nil, nil
		}
		for k, v := range header {
			req.Header.Set(k, v)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Errorf("%s %s: %v", method, path, err)
			return 0, nil, nil
		}
		defer resp.Body.Close()
		if resp.ProtoMajor != 3 {
			t.Errorf("%s %s: served over %s", method, path, resp.Proto)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("%s %s: reading the body: %v", method, path, err)
			return 0, nil, nil
		}
		return resp.StatusCode, resp.Header, body
	}, files)
}
