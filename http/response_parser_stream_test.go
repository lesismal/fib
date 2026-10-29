//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdhttp "net/http"
	"testing"
	"unsafe"
)

// aliasSink is a bodySink that records where the last piece it was given
// starts.
type aliasSink struct {
	cvCliSink
	last *byte
}

func (s *aliasSink) bodyData(b []byte) {
	s.last = unsafe.SliceData(b)
	s.cvCliSink.bodyData(b)
}

// A Content-Length body that streams goes to the sink from the reads it
// arrives in, past the read that carried the header, without the parser
// copying any of it; what follows the body in its last read is kept for the
// next response.
func TestResponseParserStreamsLengthBodyFromReads(t *testing.T) {
	get := &stdhttp.Request{Method: "GET"}
	body := bytes.Repeat([]byte("0123456789"), 3000)
	sink := &aliasSink{}
	p := &responseParser{maxHeader: 1 << 10, maxBody: 1 << 20, sink: sink, streamOK: true}
	head := "HTTP/1.1 200 OK\r\nContent-Length: 30000\r\n\r\n"
	if resp, err := p.feed(append([]byte(head), body[:100]...), get); resp != nil || err != nil {
		t.Fatalf("header: %v, %v", resp, err)
	}
	if !p.streaming || sink.starts != 1 {
		t.Fatalf("streaming %v, starts %d", p.streaming, sink.starts)
	}
	for off := 100; off < 20000; off += 4000 {
		read := body[off : off+4000]
		if resp, err := p.feed(read, get); resp != nil || err != nil {
			t.Fatalf("at %d: %v, %v", off, resp, err)
		}
		if sink.last != &read[0] {
			t.Fatalf("at %d: the sink was given a copy of the read", off)
		}
	}
	next := "HTTP/1.1 204 No Content\r\n\r\n"
	resp, err := p.feed(append(append([]byte(nil), body[20100:]...), next...), get)
	if err != nil || resp == nil || resp.StatusCode != stdhttp.StatusOK || !p.streamedDone {
		t.Fatalf("end: %v, %v, streamedDone %v", resp, err, p.streamedDone)
	}
	if sink.ends != 1 || !bytes.Equal(sink.data, body) {
		t.Fatalf("ends %d, %d bytes passed, want 1 and the %d sent", sink.ends, len(sink.data), len(body))
	}
	if string(p.buffer) != next {
		t.Fatalf("kept %q after the body, want %q", p.buffer, next)
	}
	resp, err = p.feed(nil, get)
	if err != nil || resp == nil || resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("the response after the body: %v, %v", resp, err)
	}
}
