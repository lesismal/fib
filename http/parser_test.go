package http

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httputil"
	"strings"
	"testing"
)

func TestParserFragmentedAndPipelinedRequests(t *testing.T) {
	parser := NewParser(DefaultConfig())
	first, err := parser.Feed([]byte("POST /one?x=1 HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n\r\nhe"))
	if err != nil || len(first) != 0 {
		t.Fatalf("first Feed = %d requests, %v", len(first), err)
	}
	requests, err := parser.Feed([]byte("lloGET /two HTTP/1.1\r\nHost: example.test\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 {
		t.Fatalf("got %d requests, want 2", len(requests))
	}
	body, err := io.ReadAll(requests[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if requests[0].URL.Path != "/one" || string(body) != "hello" {
		t.Fatalf("first request path/body = %q/%q", requests[0].URL.Path, body)
	}
	if requests[1].URL.Path != "/two" {
		t.Fatalf("second request path = %q", requests[1].URL.Path)
	}
}

func TestParserReleasesOversizedBuffer(t *testing.T) {
	body := bytes.Repeat([]byte{'x'}, maxRetainedBuffer+1)
	raw := append([]byte("POST / HTTP/1.1\r\nContent-Length: 65537\r\n\r\n"), body...)
	parser := NewParser(DefaultConfig())
	requests, err := parser.Feed(raw)
	if err != nil || len(requests) != 1 {
		t.Fatalf("Feed returned %d requests, %v", len(requests), err)
	}
	if cap(parser.buffer) > maxRetainedBuffer {
		t.Fatalf("retained buffer capacity = %d", cap(parser.buffer))
	}
}

func TestFeedOnePreservesProtocolUpgradeBytes(t *testing.T) {
	parser := NewParser(DefaultConfig())
	request, complete, err := parser.FeedOne([]byte("GET /chat HTTP/1.1\r\nHost: test\r\n\r\n\x81\x80"))
	if err != nil || !complete {
		t.Fatalf("FeedOne complete/error = %v/%v", complete, err)
	}
	if request.URL.Path != "/chat" {
		t.Fatalf("path = %q", request.URL.Path)
	}
	if got := parser.TakeBuffered(); string(got) != "\x81\x80" {
		t.Fatalf("buffered = %x", got)
	}
}

func TestParserChunkedRequestWithTrailer(t *testing.T) {
	parser := NewParser(DefaultConfig())
	raw := "POST /chunks HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\nTrailer: X-Checksum\r\n\r\n" +
		"4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Checksum: ok\r\n\r\n"
	requests, err := parser.Feed([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("got %d requests, want 1", len(requests))
	}
	body, err := io.ReadAll(requests[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "Wikipedia" || requests[0].Trailer.Get("X-Checksum") != "ok" {
		t.Fatalf("body/trailer = %q/%q", body, requests[0].Trailer.Get("X-Checksum"))
	}
}

func TestParserLimitsAndMalformedInput(t *testing.T) {
	t.Run("header", func(t *testing.T) {
		parser := NewParser(Config{MaxHeaderBytes: 32, MaxBodyBytes: 100})
		_, err := parser.Feed([]byte("GET / HTTP/1.1\r\nX-Long: " + strings.Repeat("x", 40)))
		if !errors.Is(err, ErrHeaderTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("body", func(t *testing.T) {
		parser := NewParser(Config{MaxHeaderBytes: 1024, MaxBodyBytes: 3})
		_, err := parser.Feed([]byte("POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\ntest"))
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("chunk", func(t *testing.T) {
		parser := NewParser(DefaultConfig())
		_, err := parser.Feed([]byte("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nnope\r\n"))
		if !errors.Is(err, ErrMalformed) {
			t.Fatalf("error = %v", err)
		}
	})
}

// plainChunkedBodies are chunked bodies in and around the shape
// decodePlainChunks takes on, each ending where chunkedEnd ends it.
var plainChunkedBodies = []string{
	"2\r\n20\r\n0\r\n\r\n",
	"0\r\n\r\n",
	"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n",
	"A\r\n0123456789\r\na\r\n0123456789\r\n0\r\n\r\n",
	"0004\r\nWiki\r\n000\r\n\r\n",
	"4;ext=1\r\nWiki\r\n0\r\n\r\n",
	"4 \r\nWiki\r\n0\r\n\r\n",
	" 4\r\nWiki\r\n0\r\n\r\n",
	"4\r\nWiki\r\n0\r\nX-T: 1\r\n\r\n",
	"4\r\nWikiXX0\r\n\r\n",
	"\r\nWiki\r\n0\r\n\r\n",
	"0000000000000004\r\nWiki\r\n0\r\n\r\n",
	"4\r\nWiki\r\n0\r\n\r\nextra",
}

// A body decodePlainChunks takes on decodes to the bytes net/http's chunked
// reader makes of it.
func TestDecodePlainChunksMatchesNetHTTP(t *testing.T) {
	plain := 0
	for _, raw := range plainChunkedBodies {
		if comparePlainChunks(t, []byte(raw)) {
			plain++
		}
	}
	if plain < 5 {
		t.Fatalf("decodePlainChunks took %d of the bodies, want at least 5", plain)
	}
}

func FuzzDecodePlainChunks(f *testing.F) {
	for _, raw := range plainChunkedBodies {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, data []byte) { comparePlainChunks(t, data) })
}

// comparePlainChunks checks what decodePlainChunks makes of body against
// net/http, and reports whether it took the body on.
func comparePlainChunks(t *testing.T, body []byte) bool {
	t.Helper()
	var got wholeBody
	if !decodePlainChunks(body, &got) {
		return false
	}
	r := bufio.NewReader(bytes.NewReader(body))
	want, err := io.ReadAll(httputil.NewChunkedReader(r))
	if err != nil {
		t.Fatalf("%q: decoded to %q, net/http refused it: %v", body, got.data, err)
	}
	// What follows the last chunk is the trailer section, which must be
	// empty, and nothing may come after it.
	if rest, _ := io.ReadAll(r); string(rest) != "\r\n" {
		t.Fatalf("%q: decoded to %q, net/http left %q after the last chunk", body, got.data, rest)
	}
	if !bytes.Equal(got.data, want) {
		t.Fatalf("%q: decoded to %q, net/http to %q", body, got.data, want)
	}
	return true
}

// A chunked request whose chunks are plain is read without net/http, into
// the block its header was parsed into, and every other one as before; the
// request each comes out as is the one net/http reads.
func TestParserChunkedRequestsMatchNetHTTP(t *testing.T) {
	const head = "POST /chunks?a=1 HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n"
	for _, tc := range []struct {
		body  string
		plain bool
	}{
		{"2\r\n20\r\n0\r\n\r\n", true},
		{"0\r\n\r\n", true},
		{"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n", true},
		{"4;ext\r\nWiki\r\n0\r\n\r\n", false},
		{"4\r\nWiki\r\n0\r\nX-T: 1\r\n\r\n", false},
	} {
		for _, reuse := range []bool{false, true} {
			config := DefaultConfig()
			config.ReuseRequests = reuse
			parser := NewParser(config)
			raw := head + tc.body
			got, complete, err := parser.FeedOne([]byte(raw))
			if err != nil || !complete {
				t.Fatalf("%q: FeedOne complete/error = %v/%v", raw, complete, err)
			}
			plain := parser.block != nil && &parser.block.request == got
			if plain != tc.plain {
				t.Fatalf("%q: read into its block = %v, want %v", raw, plain, tc.plain)
			}
			want, err := stdhttp.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
			if err != nil {
				t.Fatal(err)
			}
			gotBody, err := io.ReadAll(got.Body)
			if err != nil {
				t.Fatal(err)
			}
			wantBody, err := io.ReadAll(want.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(gotBody) != string(wantBody) {
				t.Fatalf("%q: body %q, net/http's %q", raw, gotBody, wantBody)
			}
			if got.ContentLength != want.ContentLength || len(got.TransferEncoding) != 1 ||
				got.TransferEncoding[0] != "chunked" || got.Header.Get("Transfer-Encoding") != "" {
				t.Fatalf("%q: ContentLength %d, TransferEncoding %q, header %q", raw,
					got.ContentLength, got.TransferEncoding, got.Header.Get("Transfer-Encoding"))
			}
			if len(want.Trailer) != len(got.Trailer) {
				t.Fatalf("%q: trailer %v, net/http's %v", raw, got.Trailer, want.Trailer)
			}
			if rest := parser.TakeBuffered(); len(rest) != 0 {
				t.Fatalf("%q: %q left in the buffer", raw, rest)
			}
		}
	}
}
