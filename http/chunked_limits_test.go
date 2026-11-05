package http

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

const chunkedHead = "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"

// A chunk extension that never ends is refused once it passes maxChunkLine,
// rather than buffered for as long as it is sent.
func TestParserChunkLineNeverEnds(t *testing.T) {
	parser := NewParser(DefaultConfig())
	if _, complete, err := parser.FeedOne([]byte(chunkedHead + "1;")); complete || err != nil {
		t.Fatalf("FeedOne = %v, %v", complete, err)
	}
	piece := bytes.Repeat([]byte("a"), 1024)
	for fed := 0; fed <= maxChunkLine; fed += len(piece) {
		_, complete, err := parser.FeedOne(piece)
		if complete {
			t.Fatal("a request with no end completed")
		}
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("error = %v, want ErrMalformed", err)
			}
			return
		}
	}
	t.Fatalf("an extension of more than %d bytes was taken", maxChunkLine)
}

// A chunk size padded with more leading zeros than net/http reads is
// malformed, as is any chunk-size line net/http would refuse.
func TestParserChunkLineShapes(t *testing.T) {
	for _, line := range []string{
		strings.Repeat("0", 20) + "1",
		" 1", "1 ;a", "", ";a", "g", "1\r;a", "1;a\nb", "1\n",
		"1;" + strings.Repeat("x", maxChunkLine-3),
	} {
		parser := NewParser(DefaultConfig())
		_, _, err := parser.FeedOne([]byte(chunkedHead + line + "\r\na\r\n0\r\n\r\n"))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("chunk line %.40q: error = %v, want ErrMalformed", line, err)
		}
	}
	for _, line := range []string{"1", "01", "1 ", "1\t", "1;a", "1;a=b ", strings.Repeat("0", 15) + "1",
		"1;" + strings.Repeat("x", maxChunkLine-4)} {
		parser := NewParser(DefaultConfig())
		req, complete, err := parser.FeedOne([]byte(chunkedHead + line + "\r\na\r\n0\r\n\r\n"))
		if err != nil || !complete {
			t.Errorf("chunk line %.40q: FeedOne = %v, %v", line, complete, err)
			continue
		}
		if body, _ := io.ReadAll(req.Body); string(body) != "a" {
			t.Errorf("chunk line %.40q: body = %q", line, body)
		}
	}
}

// Chunks of one byte each carry six bytes on the wire for every byte of
// body: what is buffered of them is bounded by MaxBodyBytes and
// MaxHeaderBytes together, before the body ends, rather than by what it
// decodes to alone.
func TestParserTinyChunksBounded(t *testing.T) {
	parser := NewParser(Config{MaxHeaderBytes: 1024, MaxBodyBytes: 4096})
	if _, _, err := parser.FeedOne([]byte(chunkedHead)); err != nil {
		t.Fatal(err)
	}
	piece := bytes.Repeat([]byte("1\r\na\r\n"), 100)
	for fed := 0; fed < 64<<10; fed += len(piece) {
		_, complete, err := parser.FeedOne(piece)
		if complete {
			t.Fatal("a request with no end completed")
		}
		if err != nil {
			if !errors.Is(err, ErrBodyTooLarge) {
				t.Fatalf("error = %v, want ErrBodyTooLarge", err)
			}
			if fed > 4096+1024 {
				t.Fatalf("%d bytes buffered before the body was refused", fed)
			}
			return
		}
	}
	t.Fatal("tiny chunks were buffered past the limits")
}

// A chunked body still arriving is scanned once: each read carries on from
// where the last left off, rather than parsing the header and scanning the
// body from its start again, and a body cut anywhere, its trailer included,
// frames as it does whole.
func TestParserChunkedScanResumes(t *testing.T) {
	parser := NewParser(DefaultConfig())
	if _, _, err := parser.FeedOne([]byte(chunkedHead + "3\r\nabc\r\n")); err != nil {
		t.Fatal(err)
	}
	if parser.awaiting.request == nil || parser.chunks.at != len("3\r\nabc\r\n") {
		t.Fatalf("awaiting %v, scanned to %d", parser.awaiting.request != nil, parser.chunks.at)
	}
	awaiting := parser.awaiting.request
	if _, _, err := parser.FeedOne([]byte("2\r\nde\r\n")); err != nil {
		t.Fatal(err)
	}
	if parser.awaiting.request != awaiting || parser.chunks.at != len("3\r\nabc\r\n2\r\nde\r\n") || parser.chunks.decoded != 5 {
		t.Fatalf("the request was parsed again, or the scan did not carry on: at %d, decoded %d",
			parser.chunks.at, parser.chunks.decoded)
	}

	raw := chunkedHead + "3;x=y\r\nabc\r\n10\r\n" + strings.Repeat("z", 16) + "\r\n0\r\nX-T: 1\r\nX-U: 2\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: a\r\n\r\n"
	for cut := 1; cut < len(raw); cut++ {
		parser := NewParser(DefaultConfig())
		var got []*stdhttp.Request
		for _, piece := range []string{raw[:cut], raw[cut:]} {
			reqs, err := parser.Feed([]byte(piece))
			if err != nil {
				t.Fatalf("cut %d: %v", cut, err)
			}
			got = append(got, reqs...)
		}
		if len(got) != 2 || got[1].URL.Path != "/next" {
			t.Fatalf("cut %d: %d requests", cut, len(got))
		}
		body, _ := io.ReadAll(got[0].Body)
		if string(body) != "abc"+strings.Repeat("z", 16) || got[0].Trailer.Get("X-U") != "2" {
			t.Fatalf("cut %d: body %q, trailer %v", cut, body, got[0].Trailer)
		}
	}
}

// A response's chunked body is bounded the same way on the client.
func TestResponseParserChunkLimits(t *testing.T) {
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://a/", nil)
	const head = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
	t.Run("extension", func(t *testing.T) {
		p := &responseParser{maxHeader: 1 << 20, maxBody: 1 << 20}
		defer p.reset()
		if _, err := p.feed([]byte(head+"1;"), req); err != nil {
			t.Fatal(err)
		}
		_, err := p.feed(bytes.Repeat([]byte("a"), maxChunkLine), req)
		if !errors.Is(err, ErrMalformedResponse) {
			t.Fatalf("error = %v, want ErrMalformedResponse", err)
		}
	})
	t.Run("tiny chunks", func(t *testing.T) {
		p := &responseParser{maxHeader: 1024, maxBody: 4096}
		defer p.reset()
		_, err := p.feed([]byte(head+strings.Repeat("1\r\na\r\n", 2000)), req)
		if !errors.Is(err, ErrResponseBodyTooLarge) {
			t.Fatalf("error = %v, want ErrResponseBodyTooLarge", err)
		}
	})
	t.Run("resumes", func(t *testing.T) {
		p := &responseParser{maxHeader: 1 << 20, maxBody: 1 << 20}
		defer p.reset()
		raw := head + "3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
		var resp *stdhttp.Response
		for i := 0; i < len(raw); i++ {
			r, err := p.feed([]byte{raw[i]}, req)
			if err != nil {
				t.Fatal(err)
			}
			if r != nil {
				resp = r
			}
		}
		if resp == nil {
			t.Fatal("no response")
		}
		if body, _ := io.ReadAll(resp.Body); string(body) != "abcde" {
			t.Fatalf("body = %q", body)
		}
	})
}
