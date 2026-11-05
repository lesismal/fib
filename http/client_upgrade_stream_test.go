//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// upgradeRequest is a request asking to switch to "echo".
func upgradeRequest(t *testing.T, url string) *stdhttp.Request {
	t.Helper()
	req := mustRequest(t, "GET", url, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "echo")
	return req
}

// startRefusedUpgrade sends req with StartUpgrade and returns the response
// its callback got, which must be a refusal: no error and no tunnel.
func startRefusedUpgrade(t *testing.T, client *Client, req *stdhttp.Request, handler *upgradeClientTunnel,
	inCallback func(*ClientResponse)) *ClientResponse {
	t.Helper()
	answered := make(chan *ClientResponse, 1)
	client.StartUpgrade(req, handler, func(cr *ClientResponse, err error) {
		if err != nil {
			t.Errorf("refused upgrade: %v", err)
			answered <- nil
			return
		}
		if inCallback != nil {
			inCallback(cr)
		}
		answered <- cr
	})
	var cr *ClientResponse
	select {
	case cr = <-answered:
	case <-time.After(10 * time.Second):
		t.Fatal("no answer to the upgrade")
	}
	if cr == nil {
		t.FailNow()
	}
	if cr.Tunnel() != nil {
		t.Fatal("a refusal has a tunnel")
	}
	return cr
}

// TestClientStartUpgradeRefusalStreams checks that a server refusing a
// switch with a large body has it streamed like any response, its reads held
// back for a reader that waits, rather than buffered whole first.
func TestClientStartUpgradeRefusalStreams(t *testing.T) {
	const size = 32 << 20
	body := pattern(size)
	var written atomic.Int64
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(size))
		w.WriteHeader(stdhttp.StatusForbidden)
		for off := 0; off < size; off += 64 << 10 {
			n, err := w.Write(body[off : off+64<<10])
			written.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))
	defer server.Close()
	config := streamConfig(64 << 10)
	config.StreamResponseBodyBuffer = 64 << 10
	client := newTestClient(t, config)

	handler := newUpgradeClientTunnel()
	cr := startRefusedUpgrade(t, client, upgradeRequest(t, server.URL), handler, func(cr *ClientResponse) {
		// Mark the body as being read without taking anything yet.
		_, _ = cr.Body.Read(nil)
	})
	if cr.StatusCode != stdhttp.StatusForbidden || cr.BodyComplete() || cr.s == nil {
		t.Fatalf("status %d, complete %v: the refusal did not stream", cr.StatusCode, cr.BodyComplete())
	}
	time.Sleep(300 * time.Millisecond)
	cr.s.mu.Lock()
	buffered, held := cr.s.buffered, cr.s.held
	cr.s.mu.Unlock()
	if !held || buffered > 1<<20 {
		t.Fatalf("buffered %d bytes, held %v: reads were not held back", buffered, held)
	}
	if n := written.Load(); n >= size {
		t.Fatalf("the server wrote all %d bytes to a reader that took none", n)
	}
	var data []byte
	buf := make([]byte, 64<<10)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		n, err := cr.Body.Read(buf)
		data = append(data, buf[:n]...)
		if errors.Is(err, ErrWouldBlock) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(data, body) {
		t.Fatalf("read %d of %d bytes, or they differ", len(data), size)
	}
	select {
	case <-handler.opened:
		t.Fatal("the handler opened without a switch")
	default:
	}
}

// TestClientStartUpgradeRefusalWhole checks that a small refusal still
// arrives whole, below the threshold, and that a client that does not stream
// buffers every refusal whole as before.
func TestClientStartUpgradeRefusalWhole(t *testing.T) {
	small := pattern(1 << 10)
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusUpgradeRequired)
		if r.URL.Path == "/large" {
			_, _ = w.Write(pattern(256 << 10))
			return
		}
		_, _ = w.Write(small)
	}))
	defer server.Close()

	for _, tc := range []struct {
		name   string
		config ClientConfig
		path   string
		size   int
	}{
		{"below the threshold", streamConfig(64 << 10), "/", len(small)},
		{"not streaming", DefaultClientConfig(), "/large", 256 << 10},
	} {
		client := newTestClient(t, tc.config)
		handler := newUpgradeClientTunnel()
		cr := startRefusedUpgrade(t, client, upgradeRequest(t, server.URL+tc.path), handler, nil)
		if cr.StatusCode != stdhttp.StatusUpgradeRequired || !cr.BodyComplete() {
			t.Fatalf("%s: status %d, complete %v", tc.name, cr.StatusCode, cr.BodyComplete())
		}
		if got := readBody(t, cr.Response); len(got) != tc.size || got[:16] != string(small[:16]) {
			t.Fatalf("%s: body of %d bytes, want %d", tc.name, len(got), tc.size)
		}
		select {
		case <-handler.opened:
			t.Fatalf("%s: the handler opened without a switch", tc.name)
		default:
		}
	}
}

// TestClientStartUpgradeSwitchesWhenStreaming checks that a 101, which has
// no body, still switches the connection when the client streams bodies.
func TestClientStartUpgradeSwitchesWhenStreaming(t *testing.T) {
	echo := &echoTunnel{greeting: []byte("hello"), closed: make(chan error, 1)}
	addr := tunnelServer(t, echo)
	config := streamConfig(0)
	config.StreamUnknownLengthAtHeader = true
	client := newTestClient(t, config)

	handler := newUpgradeClientTunnel()
	answered := make(chan *ClientResponse, 1)
	client.StartUpgrade(upgradeRequest(t, "http://"+addr+"/"), handler, func(cr *ClientResponse, err error) {
		if err != nil {
			t.Errorf("upgrade: %v", err)
		}
		answered <- cr
	})
	var cr *ClientResponse
	select {
	case cr = <-answered:
	case <-time.After(10 * time.Second):
		t.Fatal("no answer to the upgrade")
	}
	if cr == nil || cr.StatusCode != stdhttp.StatusSwitchingProtocols || cr.Tunnel() == nil {
		t.Fatalf("response %+v", cr)
	}
	tn := <-handler.opened
	if got := <-handler.data; got != "hello" {
		t.Fatalf("first data %q", got)
	}
	if err := tn.Send([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if got := <-handler.data; got != "ping" {
		t.Fatalf("echo %q", got)
	}
	_ = tn.Close()
	select {
	case <-handler.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnTunnelClose not called")
	}
}

// TestClientStartUpgradeRefusalCut checks that a streamed refusal whose
// connection is cut mid-body ends with an error, after the part that came:
// one of unknown length handed on at its header, one with a Content-Length.
func TestClientStartUpgradeRefusalCut(t *testing.T) {
	for _, tc := range []struct {
		name, head, part string
	}{
		{"chunked", "HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n", "5\r\nhello\r\n"},
		{"content-length", "HTTP/1.1 403 Forbidden\r\nContent-Length: 1048576\r\n\r\n", "hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			cut := make(chan struct{})
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				buf := make([]byte, 4096)
				_, _ = conn.Read(buf)
				_, _ = io.WriteString(conn, tc.head+tc.part)
				<-cut
			}()
			config := streamConfig(0)
			config.StreamUnknownLengthAtHeader = true
			client := newTestClient(t, config)

			handler := newUpgradeClientTunnel()
			arrived := make(chan struct{}, 1)
			done := make(chan struct{})
			var mu sync.Mutex
			var data []byte
			var bodyErr error
			cr := startRefusedUpgrade(t, client, upgradeRequest(t, "http://"+listener.Addr().String()+"/"), handler,
				func(cr *ClientResponse) {
					cr.OnBody(func(b []byte, fin bool, err error) {
						mu.Lock()
						data = append(data, b...)
						if fin {
							bodyErr = err
						}
						mu.Unlock()
						if fin {
							close(done)
						} else if len(b) > 0 {
							select {
							case arrived <- struct{}{}:
							default:
							}
						}
					})
				})
			if cr.StatusCode != stdhttp.StatusForbidden {
				t.Fatalf("status %d", cr.StatusCode)
			}
			select {
			case <-arrived:
			case <-done:
				t.Fatalf("the body ended before the cut: %q, %v", data, bodyErr)
			case <-time.After(5 * time.Second):
				t.Fatal("the first part of the body did not arrive")
			}
			close(cut)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the cut did not end the body")
			}
			if string(data) != "hello" || bodyErr == nil {
				t.Fatalf("data %q, err %v: want the part that came and an error", data, bodyErr)
			}
			select {
			case <-handler.opened:
				t.Fatal("the handler opened without a switch")
			default:
			}
		})
	}
}
