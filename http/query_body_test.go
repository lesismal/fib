//go:build linux || darwin || windows

package http

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// TestLookupQueryMatchesParseQuery checks that Context.Query finds what
// Request.URL.Query().Get finds, malformed pairs and all.
func TestLookupQueryMatchesParseQuery(t *testing.T) {
	queries := []string{
		"", "a=1", "a=1&b=2", "b=2&a=1&a=3", "a", "a=", "=1", "&&a=1&&",
		"a=1;b=2&b=3", "a;=1&a=2", "a=%41%42", "a=x+y", "a+b=1", "a%20b=2",
		"a=%zz&a=ok", "a%zz=1&a=2", "%61=decoded", "a=1&A=2", "ab=1&a=2",
		"a=%", "a=1=2", "a=%2B+%20", "&", "a=1&b", "b&a",
	}
	names := []string{"a", "b", "A", "a b", "ab", "", "c"}
	for _, q := range queries {
		values, _ := url.ParseQuery(q)
		for _, name := range names {
			want := values.Get(name)
			got, _ := lookupQuery(q, name)
			if got != want {
				t.Errorf("lookupQuery(%q, %q) = %q, want %q", q, name, got, want)
			}
		}
	}
}

func TestContextQueryAllocatesNothing(t *testing.T) {
	c := &Context{Request: &stdhttp.Request{URL: &url.URL{RawQuery: "a=1&b=20&c=%41"}}}
	allocs := testing.AllocsPerRun(100, func() {
		if c.Query("b") != "20" || c.Query("missing") != "" {
			t.Fatal("wrong value")
		}
	})
	if allocs != 0 {
		t.Fatalf("Query allocated %v times a call, want 0", allocs)
	}
	if got := c.Query("c"); got != "A" {
		t.Fatalf("Query(c) = %q, want A", got)
	}
}

// TestContextBody checks that Body hands the handler the body that arrived
// whole, however it was framed and whatever was read of it already, and
// nothing for a body that streams.
func TestContextBody(t *testing.T) {
	handler := func(c *Context, r *stdhttp.Request) {
		if r.URL.Query().Get("read") != "" {
			// Read part of it first: Body is still all of it.
			_, _ = io.ReadFull(r.Body, make([]byte, 3))
		}
		body := c.Body()
		if body == nil {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("<nil>"))
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", body)
	}
	payload := strings.Repeat("0123456789", 2000)

	t.Run("http1", func(t *testing.T) {
		addr := serveHTTP1(t, handler)
		client, _ := stdClient(t)
		for _, target := range []string{"/", "/?read=1"} {
			// A known length, and chunked, which a reader without a length
			// makes the client send.
			for _, body := range []io.Reader{strings.NewReader(payload), io.MultiReader(strings.NewReader(payload))} {
				resp, err := client.Post("http://"+addr+target, "text/plain", body)
				if err != nil {
					t.Fatal(err)
				}
				got, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if string(got) != payload {
					t.Fatalf("%s: echoed %d bytes, want the %d sent", target, len(got), len(payload))
				}
			}
		}
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != "<nil>" {
			t.Fatalf("GET without a body: Body = %q, want nil", got)
		}
	})

	t.Run("http2", func(t *testing.T) {
		addr := serve(t, NewHandler(HandlerFunc(handler)))
		client := netHTTPClient(t, false)
		resp, err := client.Post("http://"+addr+"/?read=1", "text/plain", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.ProtoMajor != 2 || string(got) != payload {
			t.Fatalf("HTTP/%d echoed %d bytes, want HTTP/2 and the %d sent", resp.ProtoMajor, len(got), len(payload))
		}
	})

	t.Run("streamed", func(t *testing.T) {
		config := DefaultConfig()
		config.StreamRequestBody = true
		addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
			answer := "streams"
			if c.Body() != nil {
				answer = "whole"
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(answer))
		})
		client, _ := stdClient(t)
		resp, err := client.Post("http://"+addr+"/", "text/plain", bytes.NewReader([]byte(payload)))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != "streams" {
			t.Fatalf("Body of a streamed request: %s, want nil", got)
		}
	})
}

// TestDeclaredBodyHeldWhole checks a body written through Write under a
// declared Content-Length short enough to be held back: it arrives whole, with
// its length, and an explicit Flush still hands what was written so far to the
// socket while the handler waits.
func TestDeclaredBodyHeldWhole(t *testing.T) {
	const size = 20000
	firstPart := make(chan struct{})
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		c.Header().Set("Content-Length", strconv.Itoa(size))
		part := bytes.Repeat([]byte("x"), size/4)
		if r.URL.Path == "/flush" {
			_, _ = c.Write(part)
			c.Flush()
			// The client reads the first part before the rest is written.
			<-firstPart
			for i := 1; i < 4; i++ {
				_, _ = c.Write(part)
			}
			return
		}
		for i := 0; i < 4; i++ {
			_, _ = c.Write(part)
		}
	})
	conn := dialRaw(t, addr)
	conn.send("GET /held HTTP/1.1\r\nHost: test\r\n\r\n")
	resp, body := conn.response("GET")
	if resp.ContentLength != size || len(body) != size {
		t.Fatalf("held: Content-Length %d, %d bytes, want %d", resp.ContentLength, len(body), size)
	}

	conn.send("GET /flush HTTP/1.1\r\nHost: test\r\n\r\n")
	resp, err := stdhttp.ReadResponse(conn.r, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, size/4)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatalf("flushed part: %v", err)
	}
	close(firstPart)
	rest, _ := io.ReadAll(resp.Body)
	if len(rest) != size-size/4 {
		t.Fatalf("flush: %d more bytes, want %d", len(rest), size-size/4)
	}
}
