//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// TestContextRecycleResetsEveryField keeps recycle in step with Context: a
// field added to Context has to be reset there, and then listed here.
func TestContextRecycleResetsEveryField(t *testing.T) {
	kept := map[string]bool{"mu": true, "word": true, "pooled": true, "deliverMu": true}
	reset := []string{
		"Conn", "Request", "wrote", "closing", "w", "stream", "external", "err",
		"body", "bodyDone", "bodyHeld", "handover", "cancel", "server", "parser", "whole", "block", "streamed",
		"route",
	}
	var fields []string
	typ := reflect.TypeFor[Context]()
	for i := 0; i < typ.NumField(); i++ {
		if name := typ.Field(i).Name; !kept[name] {
			fields = append(fields, name)
		}
	}
	sort.Strings(fields)
	sort.Strings(reset)
	if !reflect.DeepEqual(fields, reset) {
		t.Fatalf("Context has fields %v; recycle resets %v", fields, reset)
	}
}

// TestServerReuseKeepsRequestsApart serves pipelined requests, some of them
// retained and answered from another goroutine, with each combination of
// what the server recycles, and checks that every handler sees its own
// request and nothing of the one its objects served before.
func TestServerReuseKeepsRequestsApart(t *testing.T) {
	for mask := 1; mask < 16; mask++ {
		config := DefaultConfig()
		config.ReuseRequests, config.ReuseHeaders = mask&1 != 0, mask&2 != 0
		config.ReuseURLs, config.ReuseContexts = mask&4 != 0, mask&8 != 0
		t.Run(fmt.Sprintf("%04b", mask), func(t *testing.T) {
			testServerReuse(t, config)
		})
	}
}

func testServerReuse(t *testing.T, config Config) {
	var retained sync.WaitGroup
	t.Cleanup(retained.Wait)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		n := strings.TrimPrefix(r.URL.Path, "/r")
		body, _ := io.ReadAll(r.Body)
		var problems []string
		if got := r.Header.Get("X-N"); got != n {
			problems = append(problems, "X-N "+got)
		}
		if _, ok := r.Header["X-Odd"]; ok != (len(n)%2 == 1) {
			problems = append(problems, "X-Odd")
		}
		if got := r.URL.Query().Get("n"); got != n {
			problems = append(problems, "query "+got)
		}
		if want := strings.Repeat(n, len(body)/max(len(n), 1)); string(body) != want {
			problems = append(problems, "body")
		}
		reply := []byte(r.URL.Path + " " + strings.Join(problems, ","))
		if len(n)%3 != 0 {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", reply)
			return
		}
		c.Retain()
		retained.Add(1)
		go func() {
			defer retained.Done()
			time.Sleep(time.Millisecond)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", reply)
			c.Release()
		}()
	})))
	conn := dialRaw(t, addr)
	var stream bytes.Buffer
	const requests = 200
	for i := 0; i < requests; i++ {
		n := fmt.Sprint(i * 7919 % 100003)
		odd := ""
		if len(n)%2 == 1 {
			odd = "X-Odd: 1\r\n"
		}
		body := strings.Repeat(n, i%5)
		fmt.Fprintf(&stream, "POST /r%s?n=%s HTTP/1.1\r\nHost: x\r\nX-N: %s\r\n%sContent-Length: %d\r\n\r\n%s",
			n, n, n, odd, len(body), body)
	}
	go func() { _, _ = conn.Write(stream.Bytes()) }()
	for i := 0; i < requests; i++ {
		n := fmt.Sprint(i * 7919 % 100003)
		if _, body := conn.response("POST"); body != "/r"+n+" " {
			t.Fatalf("request %d: %q", i, body)
		}
	}
}

// TestReuseWaitsForTheLastRelease retains a request whose connection the
// server closes before the handler returns, as a timeout would, and keeps
// using it from another goroutine while other requests churn through the
// pools. Everything it was served with has to stay its own until it releases
// the request. The cancellation the close brings reaches it only once the
// handler has returned, since OnClose follows the round the handler runs in.
func TestReuseWaitsForTheLastRelease(t *testing.T) {
	config := DefaultConfig()
	setReuseAll(&config)
	result := make(chan string, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path != "/held" {
			body, _ := io.ReadAll(r.Body)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", append([]byte(r.Header.Get("X-Id")+" "), body...))
			return
		}
		c.Retain()
		cancelled := make(chan struct{})
		c.OnCancel(func(error) { close(cancelled) })
		go func() {
			<-cancelled
			// Long enough for the other connection's requests to have been
			// served with whatever this one's would have given back.
			time.Sleep(200 * time.Millisecond)
			body, err := io.ReadAll(r.Body)
			result <- fmt.Sprintf("%s %s %s %s %v", r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Id"), body, err)
			c.Release()
		}()
		c.Conn.Close()
	})))
	held := dialRaw(t, addr)
	held.send("POST /held?q=held HTTP/1.1\r\nHost: x\r\nX-Id: held\r\nContent-Length: 9\r\n\r\nheld-body")
	other := dialRaw(t, addr)
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("other-%d", i)
		other.send(fmt.Sprintf("POST /o?q=%s HTTP/1.1\r\nHost: x\r\nX-Id: %s\r\nContent-Length: %d\r\n\r\n%s",
			id, id, len(id), id))
		if _, body := other.response("POST"); body != id+" "+id {
			t.Fatalf("other request %d: %q", i, body)
		}
	}
	select {
	case got := <-result:
		if want := "/held q=held held held-body <nil>"; got != want {
			t.Fatalf("the retained request read %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retained request never finished")
	}
}

// TestRecycledHooksStayWithTheirRequest serves requests that register hooks
// alternately with ones that register others, on one connection, so that
// each reuses the Context, response writer and hooks the one before it had,
// and Respond's pooled header: a field one request's hook added must not
// reach the next response, and no hook may run for a request but its own.
func TestRecycledHooksStayWithTheirRequest(t *testing.T) {
	config := DefaultConfig()
	var seq atomic.Int64
	var ran sync.Map
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		path := r.URL.Path
		switch path {
		case "/tag":
			c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Tag", "tag") })
		case "/strip":
			// As a 304 does: Content-Type goes, and a field of the hook's
			// own is all the header has left.
			c.OnResponse(func(response *Response) {
				delete(response.Header, "Content-Type")
				response.Header["X-Tag"] = []string{"strip"}
			})
		default:
			c.OnResponse(func(*Response) {})
		}
		id := seq.Add(1)
		c.OnFinish(func(int, stdhttp.Header, int64) {
			if _, loaded := ran.LoadOrStore(id, path); loaded {
				t.Errorf("%s: finish hook ran twice", path)
			}
		})
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(path))
	})))
	conn := dialRaw(t, addr)
	paths := []string{"/tag", "/plain", "/strip", "/plain"}
	for i := 0; i < 50; i++ {
		path := paths[i%len(paths)]
		conn.send("GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n")
		resp, body := conn.response("GET")
		if string(body) != path {
			t.Fatalf("%s: body %q", path, body)
		}
		if got, want := resp.Header.Get("X-Tag"), map[string]string{"/tag": "tag", "/strip": "strip"}[path]; got != want {
			t.Fatalf("%s: X-Tag %q, want %q", path, got, want)
		}
		if got, want := resp.Header.Get("Content-Type"), map[bool]string{false: "text/plain"}[path == "/strip"]; got != want {
			t.Fatalf("%s: Content-Type %q, want %q", path, got, want)
		}
	}
	// The last finish hook runs once its response is handed over, which
	// the client can have read by then.
	finished := 0
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		finished = 0
		ran.Range(func(any, any) bool { finished++; return true })
		if finished >= 50 || time.Now().After(deadline) {
			break
		}
	}
	if finished != 50 {
		t.Fatalf("%d finish hooks ran for 50 requests", finished)
	}
}

// TestH2StreamRecycleResetsEveryField keeps h2ServerStream.recycle in step
// with the stream and its StreamRequest: a field added to either has to be
// cleared there, and then listed here. The stream also has to stay in the
// allocator's size class of 1024 bytes.
func TestH2StreamRecycleResetsEveryField(t *testing.T) {
	check := func(typ reflect.Type, cleared ...string) {
		var fields []string
		for i := 0; i < typ.NumField(); i++ {
			fields = append(fields, typ.Field(i).Name)
		}
		sort.Strings(fields)
		sort.Strings(cleared)
		if !reflect.DeepEqual(fields, cleared) {
			t.Fatalf("%s has fields %v; recycle clears %v", typ, fields, cleared)
		}
	}
	check(reflect.TypeFor[h2ServerStream](), "sc", "id", "req", "pushed", "body", "declared", "recvWindow",
		"recvUnacked", "feed", "creditSkip", "remoteDone", "responded", "trailer", "localDone", "reset",
		"sendWindow", "pending", "tunnel", "block", "values", "pooled", "refs", "nextDead", "headerFromPool")
	check(reflect.TypeFor[StreamRequest](), "Request", "URL", "context", "body", "task")
	if size := reflect.TypeFor[h2ServerStream]().Size(); size > 1024 && reflect.TypeFor[uintptr]().Size() == 8 {
		t.Fatalf("an h2ServerStream takes %d bytes, past the 1024-byte size class", size)
	}
}

// TestH2RecycledStreamsKeepRequestsApart serves many concurrent HTTP/2
// streams on one connection, half of them retained and answered from another
// goroutine after a pause, with the server recycling its streams, and checks
// that every handler sees its own request throughout, and every response is
// its own: nothing of a stream's earlier request leaks into its next one.
func TestH2RecycledStreamsKeepRequestsApart(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	var retained sync.WaitGroup
	t.Cleanup(retained.Wait)
	describe := func(r *stdhttp.Request, body []byte) string {
		return fmt.Sprintf("%s %s n=%s x=%s %s", r.Method, r.URL.Path, r.URL.Query().Get("n"), r.Header.Get("X-N"), body)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		n := strings.TrimPrefix(r.URL.Path, "/r")
		if len(n)%2 == 0 {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(describe(r, body)))
			return
		}
		c.Retain()
		retained.Add(1)
		go func() {
			defer retained.Done()
			time.Sleep(time.Millisecond)
			want := describe(r, body)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(want))
			// Until its last Release the request is still this handler's,
			// response written or not.
			time.Sleep(time.Millisecond)
			if got := describe(r, body); got != want {
				t.Errorf("request changed under its handler before Release: %q, was %q", got, want)
			}
			c.Release()
		}()
	}))))
	client := &stdhttp.Client{
		Timeout:   20 * time.Second,
		Transport: &stdhttp.Transport{TLSClientConfig: clientConfig, ForceAttemptHTTP2: true},
	}
	defer client.CloseIdleConnections()

	var wg sync.WaitGroup
	errs := make(chan error, 1000)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := strconv.Itoa(i)
			method, body := stdhttp.MethodGet, ""
			if i%3 == 0 {
				method, body = stdhttp.MethodPost, strings.Repeat(n, 1+i%7)
			}
			req, _ := stdhttp.NewRequest(method, fmt.Sprintf("https://%s/r%s?n=%s", addr, n, n), strings.NewReader(body))
			req.Header.Set("X-N", n)
			resp, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if want := fmt.Sprintf("%s /r%s n=%s x=%s %s", method, n, n, n, body); resp.ProtoMajor != 2 || string(got) != want {
				errs <- fmt.Errorf("request %d over %s: got %q, want %q", i, resp.Proto, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
