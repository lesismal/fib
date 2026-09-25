//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestContextRecycleResetsEveryField keeps recycle in step with Context: a
// field added to Context has to be reset there, and then listed here.
func TestContextRecycleResetsEveryField(t *testing.T) {
	kept := map[string]bool{"mu": true, "word": true, "pooled": true, "deliverMu": true}
	reset := []string{
		"Conn", "Request", "wrote", "closing", "w", "stream", "external", "err",
		"body", "bodyDone", "cancel", "server", "parser", "whole", "block", "streamed",
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
// the request.
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
		<-cancelled
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
