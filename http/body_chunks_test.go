//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"testing"
)

// chunkPattern is n bytes that tell their offsets apart.
func chunkPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i>>8)
	}
	return b
}

func crc(b []byte) uint32 { return crc32.ChecksumIEEE(b) }

// A body that arrives on a multiplexed connection waits in chunks of
// streamChunk, which Read moves through in order and gives back as it goes,
// rather than in one buffer grown to the whole of it.
func TestStreamedBodyBuffersInChunks(t *testing.T) {
	s := &BodyStream{remaining: -1, limit: -1, highWater: math.MaxInt, offload: true}
	body := chunkPattern(3*streamChunk + 1234)
	for rest := body; len(rest) > 0; {
		n := min(len(rest), 16<<10+7)
		s.push(rest[:n])
		rest = rest[n:]
	}
	if len(s.more) != 3 || cap(s.buf) != streamChunk {
		t.Fatalf("buffered in %d chunks after one of cap %d, want 3 after %d", len(s.more), cap(s.buf), streamChunk)
	}
	s.finish(nil)
	var got []byte
	p := make([]byte, 10000)
	for {
		n, err := s.Read(p)
		got = append(got, p[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("read %d bytes, not the %d pushed", len(got), len(body))
	}
	if s.buf != nil || len(s.more) != 0 {
		t.Fatalf("still holding %d bytes and %d chunks", cap(s.buf), len(s.more))
	}
}
