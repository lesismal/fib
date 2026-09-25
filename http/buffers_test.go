//go:build linux || darwin || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// TestServerPipelinedAcrossReads sends pipelined requests in pieces that
// split them anywhere, so that most reads end part of the way into a request
// and the server has to carry the rest over to the next one. Every body has to
// come back whole and in order.
func TestServerPipelinedAcrossReads(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading %s: %v", r.URL.Path, err)
		}
		_ = c.Respond(200, "text/plain", append([]byte(r.URL.Path+":"), body...))
	})
	conn := dialRaw(t, addr)
	var stream bytes.Buffer
	var want []string
	for i := 0; i < 60; i++ {
		body := strings.Repeat(string(rune('a'+i%26)), 1+i*37%900)
		fmt.Fprintf(&stream, "POST /r%d HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s", i, len(body), body)
		want = append(want, fmt.Sprintf("/r%d:%s", i, body))
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		data := stream.Bytes()
		for piece := 1; len(data) > 0; piece = piece*7%1013 + 1 {
			n := min(piece, len(data))
			if _, err := conn.Write(data[:n]); err != nil {
				t.Error(err)
				return
			}
			data = data[n:]
			time.Sleep(50 * time.Microsecond)
		}
	}()
	for _, w := range want {
		if _, body := conn.response("POST"); body != w {
			t.Fatalf("body %.40q..., want %.40q...", body, w)
		}
	}
	<-done
}

func TestParserHoldsNoBufferWhenIdle(t *testing.T) {
	parser := NewParser(DefaultConfig())
	raw := "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc" +
		"GET /b HTTP/1.1\r\nHost: x\r\n\r\n"
	requests, err := parser.Feed([]byte(raw))
	if err != nil || len(requests) != 2 {
		t.Fatalf("Feed = %d requests, %v", len(requests), err)
	}
	if parser.buffer != nil || parser.base != nil {
		t.Fatalf("an idle parser holds %d bytes in an array of %d", len(parser.buffer), cap(parser.base))
	}
}

// TestParserOwnsWhatIsLeftOfABorrow checks that the part of a request left
// over when a round ends survives the round's buffer being reused.
func TestParserOwnsWhatIsLeftOfABorrow(t *testing.T) {
	parser := NewParser(DefaultConfig())
	round := []byte("GET /a HTTP/1.1\r\nHost: x\r\n\r\nGET /b HTTP/1.1\r\nHo")
	request, complete, err := parser.feedBorrowed(round)
	if err != nil || !complete || request.URL.Path != "/a" {
		t.Fatalf("first request: %v, %v, %v", request, complete, err)
	}
	if _, complete, err = parser.feedBorrowed(nil); err != nil || complete {
		t.Fatalf("a partial request parsed: %v, %v", complete, err)
	}
	parser.own()
	for i := range round {
		round[i] = 'X'
	}
	request, complete, err = parser.feedBorrowed([]byte("st: x\r\n\r\n"))
	if err != nil || !complete || request.URL.Path != "/b" || request.Host != "x" {
		t.Fatalf("second request: %v, %v, %v", request, complete, err)
	}
}

func TestWholeBodyReleased(t *testing.T) {
	body := new(wholeBody)
	body.fill([]byte("hello"))
	first := make([]byte, 2)
	if n, err := body.Read(first); n != 2 || err != nil || string(first) != "he" {
		t.Fatalf("Read = %d, %v, %q", n, err, first)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(body)
	if err != nil || string(rest) != "llo" {
		t.Fatalf("ReadAll after Close = %q, %v", rest, err)
	}
	body.release()
	if _, err := body.Read(first); !errors.Is(err, ErrBodyReleased) {
		t.Fatalf("Read after release: %v", err)
	}
	body.release()
}

func TestResponseDate(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(200, "text/plain", []byte("ok"))
	})
	conn := dialRaw(t, addr)
	for i := 0; i < 3; i++ {
		conn.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		resp, _ := conn.response("GET")
		date, err := stdhttp.ParseTime(resp.Header.Get("Date"))
		if err != nil {
			t.Fatalf("Date %q: %v", resp.Header.Get("Date"), err)
		}
		if skew := time.Since(date); skew < -time.Second || skew > 2*time.Second {
			t.Fatalf("Date %v is %v away from now", date, skew)
		}
		if resp.Header.Get("Content-Type") != "text/plain" {
			t.Fatalf("Content-Type %q", resp.Header.Get("Content-Type"))
		}
	}
}
