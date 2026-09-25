//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// hookClient is a client that reaches a server over proto.
type hookClient struct {
	proto string
	*stdhttp.Client
}

// hookClients reach a server over HTTP/1.1 and, without TLS, over HTTP/2.
func hookClients(t *testing.T) []hookClient {
	var h1, h2 stdhttp.Protocols
	h1.SetHTTP1(true)
	h2.SetUnencryptedHTTP2(true)
	clients := []hookClient{
		{"HTTP/1.1", &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: &h1}}},
		{"HTTP/2.0", &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: &h2}}},
	}
	for _, c := range clients {
		t.Cleanup(c.CloseIdleConnections)
	}
	return clients
}

// TestResponseHooks has a response written each way a handler writes one —
// Respond, WriteResponse, and the ResponseWriter methods, with a body short
// enough to be held back and one long enough to be streamed — and checks
// that the hooks see and change it the same way over HTTP/1 and HTTP/2.
func TestResponseHooks(t *testing.T) {
	long := strings.Repeat("x", 3*writerBufferSize)
	shared := stdhttp.Header{"Content-Type": {"text/plain"}}
	write := map[string]func(c *Context){
		"respond": func(c *Context) { _ = c.Respond(stdhttp.StatusAccepted, "text/plain", []byte("body")) },
		"response": func(c *Context) {
			_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusAccepted, Header: shared, Body: []byte("body")})
		},
		"writer": func(c *Context) {
			c.Header().Set("Content-Type", "text/plain")
			c.WriteHeader(stdhttp.StatusAccepted)
			_, _ = io.WriteString(c, "bo")
			_, _ = io.WriteString(c, "dy")
		},
		"streamed": func(c *Context) {
			c.WriteHeader(stdhttp.StatusAccepted)
			for i := 0; i < len(long); i += 1000 {
				_, _ = io.WriteString(c, long[i:min(i+1000, len(long))])
				c.Flush()
			}
		},
	}
	type finish struct {
		status int
		size   int64
	}
	var mu sync.Mutex
	finished := map[string]finish{}
	for _, withBody := range []bool{false, true} {
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
			var order []string
			// Registered first, so run last: it sees what the others did.
			c.OnHeader(func(status int, h stdhttp.Header) {
				order = append(order, "outer")
				h.Set("X-Order", strings.Join(order, ","))
				h.Set("X-Status", fmt.Sprint(status))
			})
			c.OnFinish(func(status int, h stdhttp.Header, size int64) {
				mu.Lock()
				finished[r.URL.Path+" "+r.Proto] = finish{status, size}
				mu.Unlock()
			})
			if withBody {
				c.OnResponse(func(response *Response) {
					order = append(order, "body")
					response.StatusCode = stdhttp.StatusOK
					response.Body = bytes.ToUpper(response.Body)
				})
			}
			c.OnHeader(func(_ int, h stdhttp.Header) {
				order = append(order, "inner")
				h.Set("X-Inner", "1")
			})
			write[strings.TrimPrefix(r.URL.Path, "/")](c)
		})))
		for _, client := range hookClients(t) {
			for name := range write {
				key := "/" + name + " " + client.proto
				t.Run(fmt.Sprintf("%s/%s/body=%v", client.proto, name, withBody), func(t *testing.T) {
					mu.Lock()
					delete(finished, key)
					mu.Unlock()
					resp, err := client.Get("http://" + addr + "/" + name)
					if err != nil {
						t.Fatal(err)
					}
					data, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					want, wantStatus, wantOrder := "body", stdhttp.StatusAccepted, "inner,outer"
					if name == "streamed" {
						want = long
					}
					if withBody {
						want, wantStatus, wantOrder = strings.ToUpper(want), stdhttp.StatusOK, "inner,body,outer"
					}
					if string(data) != want {
						t.Errorf("body = %.20q (%d bytes), want %.20q", data, len(data), want)
					}
					if resp.StatusCode != wantStatus {
						t.Errorf("status = %d, want %d", resp.StatusCode, wantStatus)
					}
					if got := resp.Header.Get("X-Order"); got != wantOrder {
						t.Errorf("hooks ran %q, want %q", got, wantOrder)
					}
					if got := resp.Header.Get("X-Status"); got != fmt.Sprint(wantStatus) {
						t.Errorf("OnHeader saw status %s, want %d", got, wantStatus)
					}
					if resp.Header.Get("X-Inner") != "1" {
						t.Error("inner OnHeader's field is missing")
					}
					// OnFinish runs once the response is handed to the
					// connection, which may be after the client has it.
					var got finish
					for deadline := time.Now().Add(time.Second); ; time.Sleep(time.Millisecond) {
						mu.Lock()
						got = finished[key]
						mu.Unlock()
						if got.status != 0 || time.Now().After(deadline) {
							break
						}
					}
					if got != (finish{wantStatus, int64(len(want))}) {
						t.Errorf("OnFinish saw %+v, want status %d and %d bytes", got, wantStatus, len(want))
					}
				})
			}
		}
	}
	if len(shared) != 1 {
		t.Errorf("hooks changed the header the handler passed to WriteResponse: %v", shared)
	}
}

// TestResponseHookAfterBegin checks that a hook registered once the response
// has been begun is not run, and does not stop it streaming.
func TestResponseHookAfterBegin(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		c.WriteHeader(stdhttp.StatusOK)
		c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Late", "1") })
		c.OnResponse(func(response *Response) { response.Body = nil })
		_, _ = io.WriteString(c, "body")
	})))
	for _, client := range hookClients(t) {
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(data) != "body" || resp.Header.Get("X-Late") != "" {
			t.Errorf("%s: late hooks ran: body %q, header %v", client.proto, data, resp.Header)
		}
	}
}
