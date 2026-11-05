package http

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

// cvParseStreamed feeds raw to a parser that streams every body and returns
// the parser and the streamed body of the request raw starts.
func cvParseStreamed(t *testing.T, config Config, raw string) (*Parser, *BodyStream) {
	t.Helper()
	parser := NewParser(config)
	req, complete, err := parser.FeedOne([]byte(raw))
	if err != nil || !complete {
		t.Fatalf("FeedOne complete/error = %v/%v", complete, err)
	}
	stream := bodyOf(req)
	if stream == nil {
		t.Fatal("the request's body was not streamed")
	}
	return parser, stream
}

// A zero Config takes the default header and body limits.
func TestParserZeroConfigTakesDefaults(t *testing.T) {
	parser := NewParser(Config{})
	defaults := DefaultConfig()
	if parser.config.MaxHeaderBytes != defaults.MaxHeaderBytes || parser.config.MaxBodyBytes != defaults.MaxBodyBytes {
		t.Fatalf("limits = %d/%d, want %d/%d", parser.config.MaxHeaderBytes, parser.config.MaxBodyBytes,
			defaults.MaxHeaderBytes, defaults.MaxBodyBytes)
	}
}

// Reset drops a half-arrived request, so the parser starts over cleanly.
func TestParserResetDropsAPartialRequest(t *testing.T) {
	parser := NewParser(DefaultConfig())
	if _, complete, err := parser.FeedOne([]byte("POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nab")); complete || err != nil {
		t.Fatalf("FeedOne complete/error = %v/%v", complete, err)
	}
	parser.Reset()
	if parser.waitState() != waitIdle || len(parser.buffer) != 0 {
		t.Fatalf("after Reset wait state = %v, buffered = %d", parser.waitState(), len(parser.buffer))
	}
	req, complete, err := parser.FeedOne([]byte("GET /next HTTP/1.1\r\nHost: a\r\n\r\n"))
	if err != nil || !complete || req.URL.Path != "/next" {
		t.Fatalf("FeedOne after Reset = %v/%v/%v", req, complete, err)
	}
}

// A parser whose request is with its handler waits on the handler.
func TestParserWaitStateServing(t *testing.T) {
	parser := NewParser(DefaultConfig())
	parser.busy = true
	if got := parser.waitState(); got != waitServing {
		t.Fatalf("waitState = %v, want waitServing", got)
	}
}

// A header block that ends past MaxHeaderBytes is refused even when it
// arrives in one piece.
func TestParserHeaderEndingPastTheLimit(t *testing.T) {
	parser := NewParser(Config{MaxHeaderBytes: 64, MaxBodyBytes: 100})
	raw := "GET / HTTP/1.1\r\nX-Long: " + strings.Repeat("x", 60) + "\r\n\r\n"
	if _, _, err := parser.FeedOne([]byte(raw)); !errors.Is(err, ErrHeaderTooLarge) {
		t.Fatalf("error = %v, want ErrHeaderTooLarge", err)
	}
}

// A body whose head the simple parser leaves to net/http still gets its
// Content-Length body.
func TestParserContentLengthBodyOnTheNetHTTPPath(t *testing.T) {
	parser := NewParser(DefaultConfig())
	req, complete, err := parser.FeedOne([]byte("PROPFIND /a%20b HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc"))
	if err != nil || !complete {
		t.Fatalf("FeedOne complete/error = %v/%v", complete, err)
	}
	if parser.block != nil {
		t.Fatal("the simple parser took a request it should have left to net/http")
	}
	body, err := io.ReadAll(req.Body)
	if err != nil || string(body) != "abc" {
		t.Fatalf("body = %q, %v", body, err)
	}
}

// A chunked body net/http refuses to decode, though its framing ends where
// chunkedEnd says, is malformed.
func TestParserChunkedBodyNetHTTPRefuses(t *testing.T) {
	parser := NewParser(DefaultConfig())
	raw := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n 4\r\nWiki\r\n0\r\n\r\n"
	if _, _, err := parser.FeedOne([]byte(raw)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("error = %v, want ErrMalformed", err)
	}
}

// Chunk framing overhead counts against the body limit: a few bytes of body
// wrapped in a long chunk extension are too large.
func TestParserChunkedFramingPastTheBodyLimit(t *testing.T) {
	parser := NewParser(Config{MaxHeaderBytes: 64, MaxBodyBytes: 4})
	raw := "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"1;" + strings.Repeat("x", 80) + "\r\na\r\n0\r\n\r\n"
	if _, _, err := parser.FeedOne([]byte(raw)); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("error = %v, want ErrBodyTooLarge", err)
	}
}

// chunkedEnd waits for a trailer section still arriving, and refuses one
// that grows, or ends, past MaxHeaderBytes, and a chunk with a bad
// terminator.
func TestParserChunkedTrailerAndTerminator(t *testing.T) {
	const head = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
	t.Run("partial", func(t *testing.T) {
		parser := NewParser(DefaultConfig())
		if _, complete, err := parser.FeedOne([]byte(head + "1\r\na\r\n0\r\nX-T: 1\r\n")); complete || err != nil {
			t.Fatalf("FeedOne complete/error = %v/%v", complete, err)
		}
		req, complete, err := parser.FeedOne([]byte("\r\n"))
		if err != nil || !complete || req.Trailer.Get("X-T") != "1" {
			t.Fatalf("FeedOne = %v/%v, trailer %v", complete, err, req.Trailer)
		}
	})
	t.Run("growing", func(t *testing.T) {
		parser := NewParser(Config{MaxHeaderBytes: 64, MaxBodyBytes: 100})
		raw := head + "1\r\na\r\n0\r\nX-T: " + strings.Repeat("x", 80)
		if _, _, err := parser.FeedOne([]byte(raw)); !errors.Is(err, ErrHeaderTooLarge) {
			t.Fatalf("error = %v, want ErrHeaderTooLarge", err)
		}
	})
	t.Run("complete", func(t *testing.T) {
		parser := NewParser(Config{MaxHeaderBytes: 64, MaxBodyBytes: 100})
		raw := head + "1\r\na\r\n0\r\nX-T: " + strings.Repeat("x", 80) + "\r\n\r\n"
		if _, _, err := parser.FeedOne([]byte(raw)); !errors.Is(err, ErrHeaderTooLarge) {
			t.Fatalf("error = %v, want ErrHeaderTooLarge", err)
		}
	})
	t.Run("terminator", func(t *testing.T) {
		parser := NewParser(DefaultConfig())
		if _, _, err := parser.FeedOne([]byte(head + "2\r\nabXX")); !errors.Is(err, ErrMalformed) {
			t.Fatalf("error = %v, want ErrMalformed", err)
		}
	})
}

// plainChunkSize refuses a size larger than what it was given, and a line
// with no end, neither of which a body chunkedEnd framed can hold.
func TestPlainChunkSizeOutsideAFramedBody(t *testing.T) {
	for _, raw := range []string{"ff\r\nab", "abc"} {
		if size, n := plainChunkSize([]byte(raw)); size != 0 || n != 0 {
			t.Fatalf("plainChunkSize(%q) = %d, %d, want 0, 0", raw, size, n)
		}
	}
}

// A key longer than the stack buffer is canonicalized all the same, and the
// Connection tokens compare only ASCII without regard to case.
func TestParseRequestHeadLongKeyAndConnectionTokens(t *testing.T) {
	key := "x-" + strings.Repeat("a", 70)
	for _, head := range []string{
		"GET / HTTP/1.1\r\nHost: a\r\n" + key + ": 1\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nConnection: clos\xe9\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nConnection: clone\r\n\r\n",
	} {
		if !compareRequestHead(t, []byte(head)) {
			t.Fatalf("the simple parser left %q to net/http", head)
		}
	}
	if !asciiEqualFold("close", "CLOSE") {
		t.Fatal("asciiEqualFold is not case-insensitive in its second argument")
	}
	if headString(nil) != "" {
		t.Fatal("headString of nothing is not empty")
	}
}

// A streamed body's Read takes an empty buffer as a no-op, reports
// ErrBodyAbandoned after Close, and the framing error a body failed with.
func TestBodyStreamReadAfterCloseAndFailure(t *testing.T) {
	_, stream := cvParseStreamed(t, immediateStreamingConfig(), "POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabc")
	if n, err := stream.Read(nil); n != 0 || err != nil {
		t.Fatalf("Read(nil) = %d, %v", n, err)
	}
	if !stream.unfinished() {
		t.Fatal("a body still arriving reads as finished")
	}
	_ = stream.Close()
	if _, err := stream.Read(make([]byte, 4)); !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("Read after Close = %v, want ErrBodyAbandoned", err)
	}

	parser, stream := cvParseStreamed(t, immediateStreamingConfig(), "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nz")
	if _, err := parser.pumpBody([]byte("z\r\n")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("pumpBody error = %v, want ErrMalformed", err)
	}
	if _, err := stream.Read(make([]byte, 4)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Read of a failed body = %v, want ErrMalformed", err)
	}
	if stream.unfinished() {
		t.Fatal("a failed body reads as unfinished")
	}
}

// Close hands a callback that took the body its last call, with
// ErrBodyAbandoned.
func TestBodyStreamCloseEndsTheSink(t *testing.T) {
	_, stream := cvParseStreamed(t, immediateStreamingConfig(), "POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\n")
	got := make(chan error, 1)
	if stream.setSink(func(_ []byte, fin bool, err error, _ bool) {
		if fin {
			got <- err
		}
	}, false) {
		t.Fatal("setSink found something owed with nothing arrived")
	}
	_ = stream.Close()
	if err := <-got; !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("the sink's last call = %v, want ErrBodyAbandoned", err)
	}
}

// failSink with no reason fails the body as cut short.
func TestBodyStreamFailSinkWithoutAReason(t *testing.T) {
	_, stream := cvParseStreamed(t, immediateStreamingConfig(), "POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\n")
	var last error
	stream.setSink(func(_ []byte, fin bool, err error, _ bool) {
		if fin {
			last = err
		}
	}, false)
	if !stream.failSink(nil) {
		t.Fatal("failSink found no sink")
	}
	if !errors.Is(last, io.ErrUnexpectedEOF) || !errors.Is(stream.failure(), io.ErrUnexpectedEOF) {
		t.Fatalf("sink/failure = %v/%v, want io.ErrUnexpectedEOF", last, stream.failure())
	}
}

// A streamed chunked body publishes its trailer through Trailer once it
// ends, and fails on a trailer that is not a header section.
func TestBodyStreamTrailer(t *testing.T) {
	const head = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
	parser, stream := cvParseStreamed(t, immediateStreamingConfig(), head+"1")
	if done, err := parser.pumpBody([]byte("\r\na\r\n0\r\nX-T: 1\r\n\r\n")); !done || err != nil {
		t.Fatalf("pumpBody = %v, %v", done, err)
	}
	if stream.Trailer().Get("X-T") != "1" {
		t.Fatalf("Trailer = %v", stream.Trailer())
	}

	parser, _ = cvParseStreamed(t, immediateStreamingConfig(), head+"0")
	if _, err := parser.pumpBody([]byte("\r\nnot a field\r\n\r\n")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("pumpBody error = %v, want ErrMalformed", err)
	}
}

// The streaming chunk decoder bounds a chunk-size line by maxChunkLine, as
// the buffered path does, and a trailer section by MaxHeaderBytes, whether
// they have ended or not.
func TestBodyStreamChunkedLimits(t *testing.T) {
	const head = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
	config := immediateStreamingConfig()
	config.MaxHeaderBytes = 64
	for name, c := range map[string]struct {
		body string
		want error
	}{
		"size":             {"1;" + strings.Repeat("x", maxChunkLine), ErrMalformed},
		"trailer-growing":  {"0\r\nX-T: " + strings.Repeat("x", 80), ErrHeaderTooLarge},
		"trailer-complete": {"0\r\nX-T: " + strings.Repeat("x", 80) + "\r\n\r\n", ErrHeaderTooLarge},
	} {
		t.Run(name, func(t *testing.T) {
			// A chunked body streams once some of it is past the header.
			parser, _ := cvParseStreamed(t, config, head+c.body[:1])
			if _, err := parser.pumpBody([]byte(c.body[1:])); !errors.Is(err, c.want) {
				t.Fatalf("pumpBody error = %v, want %v", err, c.want)
			}
		})
	}
}

// A whole body hands nothing more to WriteTo once read, nothing at all once
// released, and Len counts what is left.
func TestWholeBodyWriteToAndLen(t *testing.T) {
	parser := NewParser(DefaultConfig())
	req, _, err := parser.FeedOne([]byte("POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc"))
	if err != nil {
		t.Fatal(err)
	}
	body := req.Body.(*wholeBody)
	if body.Len() != 3 {
		t.Fatalf("Len = %d, want 3", body.Len())
	}
	var out bytes.Buffer
	if n, err := body.WriteTo(&out); n != 3 || err != nil || out.String() != "abc" {
		t.Fatalf("WriteTo = %d, %v, %q", n, err, out.String())
	}
	if n, err := body.WriteTo(&out); n != 0 || err != nil {
		t.Fatalf("WriteTo of a read body = %d, %v", n, err)
	}
	body.release()
	if body.Len() != 0 {
		t.Fatalf("Len of a released body = %d", body.Len())
	}
	if _, err := body.WriteTo(&out); !errors.Is(err, ErrBodyReleased) {
		t.Fatalf("WriteTo of a released body = %v, want ErrBodyReleased", err)
	}
}

// cvParseFeed opens a request through StreamContext with a body fed through
// the returned BodyFeed, counting what is credited.
func cvParseFeed(config BodyFeedConfig, credited *int) (*StreamRequest, *BodyFeed) {
	r := new(StreamRequest)
	config.Credit = func(n int) { *credited += n }
	_, feed := r.StreamContext(nil, discardStream{}, config)
	return r, feed
}

// A BodyFeed refuses a body longer than declared or past its limit, credits
// what arrives after the body failed, and fails a body that ends short.
func TestBodyFeedLimitsAndEnds(t *testing.T) {
	t.Run("declared", func(t *testing.T) {
		var credited int
		r, feed := cvParseFeed(BodyFeedConfig{Declared: 3}, &credited)
		if r.Request.ContentLength != 3 {
			t.Fatalf("ContentLength = %d", r.Request.ContentLength)
		}
		if err := feed.Write(nil); err != nil {
			t.Fatalf("empty Write = %v", err)
		}
		if err := feed.Write([]byte("abcd")); !errors.Is(err, ErrMalformed) {
			t.Fatalf("Write past the declared length = %v, want ErrMalformed", err)
		}
		if err := feed.Write([]byte("ef")); err != nil {
			t.Fatalf("Write after failure = %v", err)
		}
		if credited != 6 {
			t.Fatalf("credited %d bytes, want 6", credited)
		}
		if _, err := r.Request.Body.Read(make([]byte, 8)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("Read = %v, want ErrMalformed", err)
		}
	})
	t.Run("limit", func(t *testing.T) {
		var credited int
		_, feed := cvParseFeed(BodyFeedConfig{Declared: -1, Limit: 2}, &credited)
		if err := feed.Write([]byte("abc")); !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("Write past the limit = %v, want ErrBodyTooLarge", err)
		}
	})
	t.Run("short", func(t *testing.T) {
		var credited int
		_, feed := cvParseFeed(BodyFeedConfig{Declared: 3}, &credited)
		if err := feed.Write([]byte("ab")); err != nil {
			t.Fatal(err)
		}
		if err := feed.End(nil); !errors.Is(err, ErrMalformed) {
			t.Fatalf("End of a short body = %v, want ErrMalformed", err)
		}
	})
	t.Run("fail", func(t *testing.T) {
		// A Fail without a reason is a body cut short.
		var credited int
		r, feed := cvParseFeed(BodyFeedConfig{Declared: -1}, &credited)
		feed.Fail(nil)
		if _, err := r.Request.Body.Read(make([]byte, 8)); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Read = %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("whole", func(t *testing.T) {
		var credited int
		r, feed := cvParseFeed(BodyFeedConfig{Declared: 2}, &credited)
		if err := feed.Write([]byte("ab")); err != nil {
			t.Fatal(err)
		}
		if err := feed.End(stdhttp.Header{"X-T": {"1"}}); err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(r.Request.Body)
		if err != nil || string(body) != "ab" || r.Request.Trailer.Get("X-T") != "1" {
			t.Fatalf("body/trailer = %q/%v, %v", body, r.Request.Trailer, err)
		}
	})
}
