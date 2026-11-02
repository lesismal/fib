package http

import (
	"bufio"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// rawExchange sends request, as it is, to addr and returns the head of the
// response, up to its blank line.
func rawExchange(t *testing.T, addr, request string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	var head strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("head %q: %v", head.String(), err)
		}
		head.WriteString(line)
		if line == "\r\n" {
			return head.String()
		}
	}
}

// TestRequestHeaderBlock checks that Context.RequestHeaderBlock is the field
// lines as they arrived, in their order and case, Host and all, and that it
// does not follow changes to Request.Header.
func TestRequestHeaderBlock(t *testing.T) {
	got := make(chan string, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Request.Header.Set("X-Changed", "1")
		got <- c.RequestHeaderBlock()
		_ = c.Respond(200, "text/plain", nil)
	})))
	rawExchange(t, addr, "GET /a?b HTTP/1.1\r\nhost: example.com\r\nzz: 1\r\nAa:  2 \r\nzz: 3\r\n\r\n")
	if block, want := <-got, "host: example.com\r\nzz: 1\r\nAa:  2 \r\nzz: 3\r\n"; block != want {
		t.Fatalf("RequestHeaderBlock %q, want %q", block, want)
	}
	// A head the server leaves to net/http has none.
	rawExchange(t, addr, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if block := <-got; block != "" {
		t.Fatalf("RequestHeaderBlock %q for an absolute-form target, want none", block)
	}
}

// TestResponseHeaderBlock checks that Response.HeaderBlock goes out as it is
// after Header's fields over HTTP/1, with no Date of the server's beside one
// of its own; and that an OnHeader hook, and HTTP/2, see its fields in the
// header.
func TestResponseHeaderBlock(t *testing.T) {
	block := []byte("zz-first: 1\r\nDate: Mon, 02 Jan 2006 15:04:05 GMT\r\nx-multi: a\r\nx-multi: b\r\n")
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/hooked" {
			c.OnHeader(func(_ int, h stdhttp.Header) {
				h.Set("X-Seen", strings.Join(h["X-Multi"], ","))
			})
		}
		_ = c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"Content-Type": {"text/plain"}},
			HeaderBlock: block, Body: []byte("ok")})
	})))
	head := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	want := "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\n" + string(block) + "\r\n"
	if head != want {
		t.Fatalf("head\n%q\nwant\n%q", head, want)
	}
	head = rawExchange(t, addr, "GET /hooked HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.Contains(head, "X-Seen: a,b\r\n") || !strings.Contains(head, "Zz-First: 1\r\n") ||
		strings.Count(head, "Date:") != 1 {
		t.Fatalf("hooked head\n%s", head)
	}

	// Without a Date of its own, the block gets the server's.
	addr = serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.WriteResponse(Response{StatusCode: 200, HeaderBlock: []byte("X-A: 1\r\n")})
	})))
	head = rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.Contains(head, "\r\nDate: ") || !strings.HasSuffix(head, "\r\nX-A: 1\r\n\r\n") {
		t.Fatalf("head\n%s", head)
	}

	client := &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: h2cOnly()}}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.Header.Get("X-A") != "1" {
		t.Fatalf("HTTP/2 response %s %v", resp.Proto, resp.Header)
	}
}

func h2cOnly() *stdhttp.Protocols {
	p := new(stdhttp.Protocols)
	p.SetUnencryptedHTTP2(true)
	return p
}

// TestStartFramed checks a request sent with a header block: written as it
// is after the request line, Host and Content-Length on HTTP/1, with no
// User-Agent of the client's; and a response taken as its block, with no
// Header.
func TestStartFramed(t *testing.T) {
	heads := make(chan string, 1)
	url := rawServer(t, func(conn net.Conn, _ int) {
		br := bufio.NewReader(conn)
		var head strings.Builder
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			head.WriteString(line)
			if line == "\r\n" {
				break
			}
		}
		_, _ = io.CopyN(io.Discard, br, 3)
		heads <- head.String()
		_, _ = io.WriteString(conn, "HTTP/1.1 201 Created\r\nzz: 1\r\nContent-Length: 2\r\nconnection: keep-alive\r\n\r\nok")
	})
	client := newTestClient(t, DefaultClientConfig())
	req := mustRequest(t, "POST", url+"/p?q", strings.NewReader("abc"))
	req.Host = "app.example"
	type result struct {
		status int
		header stdhttp.Header
		block  string
		body   string
		err    error
	}
	done := make(chan result, 1)
	client.StartFramed(req, Framing{HeaderBlock: []byte("zz: 1\r\nAa: 2\r\n"), ResponseHeaderBlock: true},
		func(resp *ClientResponse, err error) {
			if err != nil {
				done <- result{err: err}
				return
			}
			body, _ := io.ReadAll(resp.Body)
			done <- result{resp.StatusCode, resp.Header, resp.HeaderBlock(), string(body), nil}
		})
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if head, want := <-heads, "POST /p?q HTTP/1.1\r\nHost: app.example\r\nContent-Length: 3\r\nzz: 1\r\nAa: 2\r\n\r\n"; head != want {
		t.Fatalf("the server saw\n%q\nwant\n%q", head, want)
	}
	if r.status != 201 || r.header != nil || r.body != "ok" ||
		r.block != "zz: 1\r\nContent-Length: 2\r\nconnection: keep-alive\r\n" {
		t.Fatalf("response %d %v %q %q", r.status, r.header, r.block, r.body)
	}
}

// TestStartFramedFallback checks that a framed request the client leaves to
// net/http, as it does one that asks to close, has its block's fields added
// to its header, with no User-Agent of the client's.
func TestStartFramedFallback(t *testing.T) {
	req := mustRequest(t, "GET", "http://example.com/", nil)
	req.Close = true
	data, _, err := marshalFramed(req, nil, []byte("zz: 1\r\nAa: 2\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, "Zz: 1\r\n") || !strings.Contains(got, "Aa: 2\r\n") ||
		strings.Contains(got, "User-Agent") || !strings.Contains(got, "Connection: close\r\n") {
		t.Fatalf("request %q", got)
	}
	bad := mustRequest(t, "GET", "http://example.com/", nil)
	bad.Close = true
	if _, _, err := marshalFramed(bad, nil, []byte("bad line\r\n")); err == nil {
		t.Fatal("an invalid block was added to a header")
	}
}
