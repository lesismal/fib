//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdtls "crypto/tls"
	"fmt"
	"io"
	"math/rand"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/debughook"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// TestSendFile checks, over HTTP/1.1 and HTTP/2, in cleartext and over TLS,
// that SendFile sends a file whole, and a range of it as a 206, and a file
// smaller than sendfile is worth, to a client that reads nothing until the
// handler has returned, which it does at once; and that done hears of the
// end.
func TestSendFile(t *testing.T) {
	data := make([]byte, 6<<20)
	rand.New(rand.NewSource(1)).Read(data)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 1)
	returns := make(chan time.Duration, 1)
	handler := HandlerFunc(func(c *Context) {
		start := time.Now()
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		offset, n := int64(0), int64(len(data))
		switch c.Request.URL.Path {
		case "/range":
			offset, n = 1000, 3<<20
			c.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+n-1, len(data)))
			c.Header().Set("Content-Length", strconv.FormatInt(n, 10))
			c.WriteHeader(stdhttp.StatusPartialContent)
		case "/small":
			n = 1000
		}
		c.SendFile(f, offset, n, func(err error) {
			f.Close()
			results <- err
		})
		returns <- time.Since(start)
	})
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	plain := serve(t, NewHandler(handler))
	secure := serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandler(handler)))
	type client struct {
		name, base string
		*stdhttp.Client
	}
	var clients []client
	for _, c := range hookClients(t) {
		clients = append(clients, client{c.proto, "http://" + plain, c.Client})
	}
	for _, h2 := range []bool{false, true} {
		config := clientTLS.Clone()
		transport := &stdhttp.Transport{TLSClientConfig: config, ForceAttemptHTTP2: h2}
		if !h2 {
			transport.TLSNextProto = map[string]func(string, *stdtls.Conn) stdhttp.RoundTripper{}
		}
		t.Cleanup(transport.CloseIdleConnections)
		clients = append(clients, client{fmt.Sprintf("tls/h2=%v", h2), "https://" + secure, &stdhttp.Client{Transport: transport}})
	}
	for _, cl := range clients {
		for _, path := range []string{"/", "/range", "/small"} {
			t.Run(cl.name+path, func(t *testing.T) {
				resp, err := cl.Get(cl.base + path)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				// Read nothing for a moment: the handler has returned
				// meanwhile, whatever the client has taken.
				time.Sleep(50 * time.Millisecond)
				got, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				want, status := data, stdhttp.StatusOK
				switch path {
				case "/range":
					want, status = data[1000:1000+3<<20], stdhttp.StatusPartialContent
				case "/small":
					want = data[:1000]
				}
				if resp.StatusCode != status || !bytes.Equal(got, want) {
					t.Fatalf("status %d with %d bytes, want %d with %d", resp.StatusCode, len(got), status, len(want))
				}
				if resp.ContentLength != int64(len(want)) {
					t.Errorf("Content-Length %d, want %d", resp.ContentLength, len(want))
				}
				if returned := <-returns; returned > time.Second {
					t.Errorf("SendFile took %v to return", returned)
				}
				select {
				case err := <-results:
					if err != nil {
						t.Errorf("done heard %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("done never ran")
				}
			})
		}
	}
}

// TestSendFileClientGone checks that a file SendFile reads a piece at a
// time, here over h2c, to a client that goes away part way, tells done so.
func TestSendFileClientGone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, make([]byte, 32<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	debughook.SetTrace(true)
	t.Cleanup(func() { debughook.SetTrace(false) })
	results := make(chan error, 1)
	var served atomic.Pointer[fib.Connection]
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		served.Store(c.Conn)
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		c.SendFile(f, 0, 32<<20, func(err error) {
			f.Close()
			results <- err
		})
	})))
	client := hookClients(t)[1]
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	client.CloseIdleConnections()
	select {
	case err := <-results:
		if err == nil {
			t.Fatal("done heard nothing wrong")
		}
	case <-time.After(10 * time.Second):
		state := connDebugState(served.Load())
		t.Log(state)
		fmt.Printf("\n::error title=TestSendFileClientGone state::%s\n", strings.ReplaceAll(state, "\n", "%0A"))
		trace := debughook.ConnTrace(served.Load())
		if len(trace) > 60000 {
			trace = trace[len(trace)-60000:]
		}
		fmt.Printf("\n::error title=TestSendFileClientGone trace::%s\n",
			strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(trace))
		dumpForCI("TestSendFileClientGone stacks")
		t.Fatal("done never ran")
	}
}

// connDebugState describes c's I/O state, and that of the HTTP/2 connection
// it carries, for a hung test to report. Diagnostic only.
func connDebugState(c *fib.Connection) string {
	if c == nil {
		return "no connection served"
	}
	out := "conn: " + debughook.ConnState(c)
	if sc, ok := c.Attachment().(*h2ServerConn); ok {
		sc.lock()
		out += fmt.Sprintf("\nh2: streams=%d closed=%v goAway=%v peerGoingAway=%v sendWindow=%d",
			len(sc.streams), sc.closed, sc.goAway, sc.peerGoingAway, sc.sendWindow)
		for id, st := range sc.streams {
			out += fmt.Sprintf("\nstream %d: pending=%d sendWindow=%d reset=%v localDone=%v remoteDone=%v writable=%v",
				id, len(st.pending), st.sendWindow, st.reset, st.localDone, st.remoteDone, st.out != nil && st.out.writable != nil)
		}
		sc.unlock()
	} else {
		out += fmt.Sprintf("\nattachment %T", c.Attachment())
	}
	return out
}

// dumpForCI prints the stacks of the goroutines in fib as a GitHub Actions
// error annotation, which can be read without the job's log. Diagnostic only.
func dumpForCI(title string) {
	buf := make([]byte, 8<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var kept []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "lesismal/fib") && !strings.Contains(g, "dumpForCI") {
			kept = append(kept, g)
		}
	}
	out := strings.Join(kept, "\n\n")
	if len(out) > 60000 {
		out = out[:60000]
	}
	out = strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(out)
	fmt.Printf("\n::error title=%s::%s\n", title, out)
}

// TestSendFileOnWritable checks that on a plain HTTP/1 connection, where
// SendFile hands the range to sendfile, what is queued of the file counts
// against the response's limit: OnWritable, asked once a piece has been
// queued for a client that reads nothing, reports that it will call back,
// and does once the client reads.
func TestSendFileOnWritable(t *testing.T) {
	const size = 32 << 20
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	waits := make(chan bool, 1)
	writable := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		c.Retain()
		c.SendFile(f, 0, size, func(err error) {
			f.Close()
			if err != nil {
				t.Error(err)
			}
			waits <- c.OnWritable(func(err error) {
				writable <- err
				c.Release()
			})
		})
	})))
	client := hookClients(t)[0]
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !<-waits {
		t.Fatal("OnWritable saw room with the file queued and unread")
	}
	select {
	case err := <-writable:
		t.Fatalf("OnWritable's function ran with the file unread: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if n, err := io.Copy(io.Discard, resp.Body); err != nil || n != size {
		t.Fatalf("read %d bytes, %v", n, err)
	}
	select {
	case err := <-writable:
		if err != nil {
			t.Fatalf("OnWritable's function heard %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnWritable's function never ran")
	}
}

// TestSendFileStall checks, over HTTP/1.1 and HTTP/2, in cleartext and over
// TLS, that SendFileStall cuts off a client that stops reading after its
// stall bound, telling done so, and sends the whole file to one that reads
// slowly, waiting for it between pieces as long as it takes something.
func TestSendFileStall(t *testing.T) {
	const size = 64 << 20
	const stall = 300 * time.Millisecond
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	type result struct {
		err error
		at  time.Time
	}
	results := make(chan result, 1)
	handler := HandlerFunc(func(c *Context) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		n := int64(size)
		if c.Request.URL.Path == "/slow" {
			n = 8 << 20
		}
		c.SendFileStall(f, 0, n, stall, func(err error) {
			f.Close()
			results <- result{err, time.Now()}
		})
	})
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	plain := serve(t, NewHandler(handler))
	secure := serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandler(handler)))
	type client struct {
		name, base string
		*stdhttp.Client
	}
	var clients []client
	for _, c := range hookClients(t) {
		clients = append(clients, client{c.proto, "http://" + plain, c.Client})
	}
	for _, h2 := range []bool{false, true} {
		transport := &stdhttp.Transport{TLSClientConfig: clientTLS.Clone(), ForceAttemptHTTP2: h2}
		if !h2 {
			transport.TLSNextProto = map[string]func(string, *stdtls.Conn) stdhttp.RoundTripper{}
		}
		t.Cleanup(transport.CloseIdleConnections)
		clients = append(clients, client{fmt.Sprintf("tls/h2=%v", h2), "https://" + secure, &stdhttp.Client{Transport: transport}})
	}
	for _, cl := range clients {
		t.Run(cl.name+"/stuck", func(t *testing.T) {
			resp, err := cl.Get(cl.base + "/stuck")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			// Read nothing.
			select {
			case r := <-results:
				if r.err == nil {
					t.Fatal("done heard of no error with the client reading nothing")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a client that reads nothing was not cut off")
			}
		})
		t.Run(cl.name+"/slow", func(t *testing.T) {
			resp, err := cl.Get(cl.base + "/slow")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			buf := make([]byte, 256<<10)
			var got int
			for {
				n, err := io.ReadFull(resp.Body, buf)
				got += n
				if err != nil {
					break
				}
				// Slower than the server would send, faster than the stall.
				time.Sleep(stall / 10)
			}
			if got != 8<<20 {
				t.Fatalf("read %d bytes, want %d", got, 8<<20)
			}
			select {
			case r := <-results:
				if r.err != nil {
					t.Fatalf("done heard %v", r.err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("done never ran")
			}
		})
	}
}
