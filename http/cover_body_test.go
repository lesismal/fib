//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// cvBodyCoded parses the head of a request whose body is in gzip and
// chunked, and the first bytes of that body, and returns its stream and the
// rest of the body, to pump.
func cvBodyCoded(t *testing.T, payload []byte, first int) (*Parser, *BodyStream, []byte) {
	t.Helper()
	return cvBodyChunked(t, []byte(chunkedBody(gzipBody(t, payload))), first)
}

// cvBodyChunked is cvBodyCoded for a chunked body, which may not be gzip.
func cvBodyChunked(t *testing.T, body []byte, first int) (*Parser, *BodyStream, []byte) {
	t.Helper()
	head := "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"
	parser, stream := cvParseStreamed(t, immediateStreamingConfig(), head+string(body[:first]))
	return parser, stream, body[first:]
}

// cvBodySink collects what a sink is handed.
type cvBodySink struct {
	data []byte
	fins int
	err  error
}

func (s *cvBodySink) take(data []byte, fin bool, err error, _ bool) {
	s.data = append(s.data, data...)
	if fin {
		s.fins++
		s.err = err
	}
}

// A coded body taken as a callback is decoded a piece at a time into it,
// ending with its last call, and Consumed counts what it decoded to.
func TestCoverBodyCodedSink(t *testing.T) {
	payload := bytes.Repeat([]byte("a body in gzip handed to a callback. "), 100)
	parser, stream, rest := cvBodyCoded(t, payload, 1)
	if _, err := parser.pumpBody(rest); err != nil {
		t.Fatal(err)
	}
	var sink cvBodySink
	if !stream.setSink(sink.take, false) {
		t.Fatal("nothing owed with the whole body arrived")
	}
	stream.drain(false)
	if !bytes.Equal(sink.data, payload) || sink.fins != 1 || sink.err != nil {
		t.Fatalf("sink had %d bytes, %d last calls, %v", len(sink.data), sink.fins, sink.err)
	}
	if got := stream.Consumed(); got != int64(len(payload)) {
		t.Fatalf("Consumed = %d, want %d decoded", got, len(payload))
	}
}

// A coded body that fails to decode hands its callback the failure, and
// nothing decoded past it.
func TestCoverBodyCodedSinkMalformed(t *testing.T) {
	parser, stream, rest := cvBodyChunked(t, []byte(chunkedBody(bytes.Repeat([]byte("not gzip"), 20))), 1)
	if _, err := parser.pumpBody(rest); err != nil {
		t.Fatal(err)
	}
	var sink cvBodySink
	if stream.setSink(sink.take, false) {
		stream.drain(false)
	}
	if sink.fins != 1 || !errors.Is(sink.err, ErrMalformed) {
		t.Fatalf("%d last calls, %v", sink.fins, sink.err)
	}
}

// Read of a coded body reports why it cannot go on: ErrBodyAbandoned once
// closed, the decoding's failure again after it was first reported, and the
// error the body ended short with.
func TestCoverBodyCodedReadEnds(t *testing.T) {
	payload := bytes.Repeat([]byte("a coded body read in pieces. "), 100)
	_, stream, _ := cvBodyCoded(t, payload, 1)
	_ = stream.Close()
	if _, err := stream.Read(make([]byte, 64)); !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("Read after Close = %v, want ErrBodyAbandoned", err)
	}

	parser, stream, rest := cvBodyChunked(t, []byte(chunkedBody(bytes.Repeat([]byte("not gzip"), 20))), 1)
	_, _ = parser.pumpBody(rest)
	buf := make([]byte, 64)
	if _, err := stream.Read(buf); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Read of a malformed body = %v", err)
	}
	if _, err := stream.Read(buf); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Read again of a malformed body = %v", err)
	}

	_, stream, _ = cvBodyCoded(t, payload, 1)
	stream.fail(io.EOF, false)
	if _, err := stream.Read(buf); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Read of a body cut short = %v", err)
	}
}

// Closing a coded body a callback has taken hands it its last call, with
// ErrBodyAbandoned.
func TestCoverBodyCodedSinkClosed(t *testing.T) {
	_, stream, _ := cvBodyCoded(t, []byte("never all sent"), 1)
	last := make(chan error, 1)
	if stream.setSink(func(_ []byte, fin bool, err error, _ bool) {
		if fin {
			last <- err
		}
	}, false) {
		stream.drain(false)
	}
	_ = stream.Close()
	// The last call may come on the handler pool.
	if err := cvCliWait(t, last, "the sink's last call"); !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("the sink's last call = %v, want ErrBodyAbandoned", err)
	}
}
