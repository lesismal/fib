package http3

import (
	"bytes"
	stdhttp "net/http"
	"testing"

	"github.com/lesismal/fib/bufferpool"
)

// A body read whole has no more than maxBodyReserve set aside for it before
// it arrives, however long it says it is, and grows as it comes: a client
// once had 16 MiB held for each stream it declared that long for and sent a
// byte of.
func TestBodyReserveBounded(t *testing.T) {
	h := &ServerHandler{config: DefaultConfig()}
	newStream := func(declared int64) *requestStream {
		return &requestStream{sc: &serverConn{h: h}, req: &stdhttp.Request{}, declared: declared}
	}
	rs := newStream(h.config.MaxBodyBytes)
	if err := rs.onData([]byte{'b'}); err != nil {
		t.Fatal(err)
	}
	if cap(rs.body) > maxBodyReserve || len(rs.body) != 1 {
		t.Fatalf("%d bytes set aside for 1 of a %d-byte body", cap(rs.body), rs.declared)
	}
	want := []byte{'b'}
	chunk := bytes.Repeat([]byte("0123456789abcdef"), 1<<10)
	for len(want) < 1<<20 {
		if err := rs.onData(chunk); err != nil {
			t.Fatal(err)
		}
		want = append(want, chunk...)
	}
	if !bytes.Equal(rs.body, want) {
		t.Fatalf("body of %d bytes, want %d", len(rs.body), len(want))
	}
	bufferpool.Put(rs.body)

	// A short body is still set aside once, at its length.
	rs = newStream(1000)
	if err := rs.onData([]byte{'b'}); err != nil {
		t.Fatal(err)
	}
	if cap(rs.body) < 1000 || cap(rs.body) > maxBodyReserve {
		t.Fatalf("%d bytes set aside for a 1000-byte body", cap(rs.body))
	}
	bufferpool.Put(rs.body)
}
