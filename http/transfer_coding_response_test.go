//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
)

// rawCodedResponse is a one-request raw server that answers with response.
func rawCodedResponse(t *testing.T, response string) string {
	t.Helper()
	return rawServer(t, func(conn net.Conn, _ int) {
		_, _ = stdhttp.ReadRequest(bufio.NewReader(conn))
		_, _ = io.WriteString(conn, response)
	})
}

// TestTransferCodingResponse decodes gzip, x-gzip and both shapes of deflate
// on HTTP/1 responses, buffered whole and streamed, with the body chunked.
func TestTransferCodingResponse(t *testing.T) {
	payload := bytes.Repeat([]byte("a response body sent in a transfer coding. "), 200)
	for _, tc := range codings() {
		for _, stream := range []bool{false, true} {
			name := tc.name + "/buffered"
			config := DefaultClientConfig()
			if stream {
				name = tc.name + "/streamed"
				config = streamConfig(0)
				config.MaxResponseBodyBytes = 1 << 10 // a streamed body is not bound by it
			}
			t.Run(name, func(t *testing.T) {
				url := rawCodedResponse(t, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: "+
					tc.name+", chunked\r\n\r\n"+chunkedBody(tc.encode(t, payload)))
				client := newTestClient(t, config)
				var run streamRun
				run.run(t, client, mustRequest(t, "GET", url, nil))
				if run.callErr != nil || run.status != 200 || run.err != nil || run.fins != 1 {
					t.Fatalf("status %d, call error %v, body error %v, fins %d", run.status, run.callErr, run.err, run.fins)
				}
				if !bytes.Equal(run.data, payload) {
					t.Fatalf("got %d bytes, want %d, or they differ", len(run.data), len(payload))
				}
			})
		}
	}
}

// A coded body with no chunked coding ends when the server closes, as RFC
// 9112 section 6.3 has it, and is decoded from what arrived.
func TestTransferCodingResponseUntilClose(t *testing.T) {
	payload := bytes.Repeat([]byte("until the server closes. "), 200)
	for _, stream := range []bool{false, true} {
		name := "buffered"
		config := DefaultClientConfig()
		if stream {
			name = "streamed"
			config = streamConfig(0)
		}
		t.Run(name, func(t *testing.T) {
			url := rawCodedResponse(t, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: gzip\r\n\r\n"+
				string(gzipBody(t, payload)))
			client := newTestClient(t, config)
			var run streamRun
			run.run(t, client, mustRequest(t, "GET", url, nil))
			if run.callErr != nil || run.err != nil || run.fins != 1 || !bytes.Equal(run.data, payload) {
				t.Fatalf("status %d, call error %v, body error %v, fins %d, %d bytes",
					run.status, run.callErr, run.err, run.fins, len(run.data))
			}
		})
	}
}

// A trailer follows the coded body, not the coded bytes.
func TestTransferCodingResponseTrailer(t *testing.T) {
	payload := []byte("a body with a trailer, sent in a transfer coding")
	for _, stream := range []bool{false, true} {
		name := "buffered"
		config := DefaultClientConfig()
		if stream {
			name = "streamed"
			config = streamConfig(0)
		}
		t.Run(name, func(t *testing.T) {
			framed := strings.TrimSuffix(chunkedBody(gzipBody(t, payload)), "0\r\n\r\n") + "0\r\nX-Sum: 7\r\n\r\n"
			url := rawCodedResponse(t, "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\nTrailer: X-Sum\r\n\r\n"+framed)
			client := newTestClient(t, config)
			var run streamRun
			run.run(t, client, mustRequest(t, "GET", url, nil))
			if run.callErr != nil || run.err != nil || !bytes.Equal(run.data, payload) || run.trailer != "7" {
				t.Fatalf("call error %v, body error %v, %d bytes, trailer %q", run.callErr, run.err, len(run.data), run.trailer)
			}
		})
	}
}

// An unsupported coding fails the response, and so does a body that does not
// decode.
func TestTransferCodingResponseRefused(t *testing.T) {
	for _, tc := range []struct {
		name, response string
	}{
		{"compress", "HTTP/1.1 200 OK\r\nTransfer-Encoding: compress, chunked\r\n\r\n0\r\n\r\n"},
		{"br", "HTTP/1.1 200 OK\r\nTransfer-Encoding: br, chunked\r\n\r\n0\r\n\r\n"},
		{"two codings", "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, deflate, chunked\r\n\r\n0\r\n\r\n"},
		{"gzip that does not decode", "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n" + chunkedBody([]byte("not gzip"))},
		{"gzip cut short", "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n" +
			chunkedBody(gzipBody(t, bytes.Repeat([]byte("cut short"), 100))[:20])},
	} {
		for _, stream := range []bool{false, true} {
			name := tc.name + "/buffered"
			config := DefaultClientConfig()
			if stream {
				name = tc.name + "/streamed"
				config = streamConfig(0)
			}
			t.Run(name, func(t *testing.T) {
				url := rawCodedResponse(t, tc.response)
				client := newTestClient(t, config)
				var run streamRun
				run.run(t, client, mustRequest(t, "GET", url, nil))
				if !errors.Is(run.callErr, ErrMalformedResponse) && !errors.Is(run.err, ErrMalformedResponse) {
					t.Fatalf("call error %v, body error %v, want %v", run.callErr, run.err, ErrMalformedResponse)
				}
			})
		}
	}
}

// MaxResponseBodyBytes and MaxStreamedBodyBytes bound what a coded response
// decodes to, not what it was sent as.
func TestTransferCodingResponseDecodedLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 64<<10)
	encoded := gzipBody(t, payload)
	if len(encoded) >= 1<<10 {
		t.Fatalf("the coded body is %d bytes, too big to prove the point", len(encoded))
	}
	response := "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n" + chunkedBody(encoded)

	t.Run("buffered", func(t *testing.T) {
		config := DefaultClientConfig()
		config.MaxResponseBodyBytes = 1 << 10
		client := newTestClient(t, config)
		run := streamRun{}
		run.run(t, client, mustRequest(t, "GET", rawCodedResponse(t, response), nil))
		if !errors.Is(run.callErr, ErrResponseBodyTooLarge) {
			t.Fatalf("call error %v, want %v", run.callErr, ErrResponseBodyTooLarge)
		}
	})

	t.Run("streamed", func(t *testing.T) {
		config := streamConfig(0)
		config.MaxStreamedBodyBytes = 1 << 10
		client := newTestClient(t, config)
		run := streamRun{}
		run.run(t, client, mustRequest(t, "GET", rawCodedResponse(t, response), nil))
		if !errors.Is(run.err, ErrResponseBodyTooLarge) {
			t.Fatalf("body error %v, want %v", run.err, ErrResponseBodyTooLarge)
		}
	})
}

// Content-Encoding is the caller's on a response too: the transfer coding is
// decoded and the representation it carried is handed over unchanged.
func TestTransferCodingResponseLeavesContentEncoding(t *testing.T) {
	inner := gzipBody(t, []byte("the representation inside the coding"))
	url := rawCodedResponse(t, "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"+
		chunkedBody(gzipBody(t, inner)))
	client := newTestClient(t, DefaultClientConfig())
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(readBody(t, resp))
	if resp.Header.Get("Content-Encoding") != "gzip" || !bytes.Equal(body, inner) {
		t.Fatalf("Content-Encoding %q, body %d bytes", resp.Header.Get("Content-Encoding"), len(body))
	}
}

// A coded body that arrives in pieces is decoded across reads: the decoder
// keeps its place in the coded stream, and the body keeps its place in the
// chunked framing, between one pump and the next.
func TestTransferCodingRequestAcrossReads(t *testing.T) {
	payload := bytes.Repeat([]byte("a coded body arriving in pieces. "), 100)
	coded := chunkedBody(gzipBody(t, payload))

	parser := NewParser(immediateStreamingConfig())
	// The first piece goes with the header, so the request is handed over as
	// one whose body streams.
	first := min(len(coded), 13)
	req, complete, err := parser.FeedOne([]byte(
		"POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: gzip, chunked\r\n\r\n" + coded[:first]))
	if err != nil || !complete {
		t.Fatalf("FeedOne complete/error = %v/%v", complete, err)
	}
	stream := bodyOf(req)
	if stream == nil {
		t.Fatal("the request's body was not streamed")
	}
	if _, err := parser.pumpBody(nil); err != nil {
		t.Fatalf("pumpBody first: %v", err)
	}

	var got []byte
	buf := make([]byte, 37) // an odd size, so output does not line up with input
	drain := func() error {
		t.Helper()
		for {
			n, err := stream.Read(buf)
			got = append(got, buf[:n]...)
			switch {
			case err == nil:
			case errors.Is(err, ErrWouldBlock):
				return nil
			case errors.Is(err, io.EOF):
				return io.EOF
			default:
				return err
			}
		}
	}
	if err := drain(); err != nil {
		t.Fatalf("before any body: %v", err)
	}
	for at := first; at < len(coded); {
		n := min(len(coded)-at, 13)
		if _, err := parser.pumpBody([]byte(coded[at : at+n])); err != nil {
			t.Fatalf("pumpBody at %d: %v", at, err)
		}
		at += n
		if err := drain(); err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("after %d bytes: %v", at, err)
		}
	}
	if parser.stream != nil {
		if _, err := parser.pumpBody(nil); err != nil {
			t.Fatalf("pumpBody end: %v", err)
		}
	}
	if err := drain(); !errors.Is(err, io.EOF) {
		t.Fatalf("at the end: %v, want io.EOF", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, want %d, or they differ", len(got), len(payload))
	}
}
