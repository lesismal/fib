package qpack

import (
	"strconv"
	"strings"
	"testing"
)

// requestMix is the requests of an RPC client with fields that repeat, a
// large cookie and bearer token among them, and a few that change on every
// request: the path among four, a request ID and a trace ID.
func requestMix(i int) []HeaderField {
	return []HeaderField{
		{":method", "POST"},
		{":scheme", "https"},
		{":authority", "api.internal.example.com"},
		{":path", "/inventory.v1.Items/" + [...]string{"List", "Get", "Put", "Watch"}[i%4]},
		{"content-type", "application/grpc"},
		{"te", "trailers"},
		{"user-agent", "grpc-go/1.70.0"},
		{"cookie", "session=" + strings.Repeat("c0ffee", 100)},
		{"authorization", "Bearer " + strings.Repeat("eyJhbGciOi", 20)},
		{"x-tenant", "acme-corp-production"},
		{"x-request-id", strconv.Itoa(1000000 + i)},
		{"x-trace-id", strconv.FormatUint(uint64(i)*0x9e3779b97f4a7c15, 16)},
	}
}

// benchSections encodes and decodes the request mix, with the dynamic table
// at capacity or without one, and reports what goes on the wire for each
// section: its own bytes and its share of the encoder and decoder streams.
func benchSections(b *testing.B, capacity uint64) {
	var mixes [64][]HeaderField
	for i := range mixes {
		mixes[i] = requestMix(i)
	}
	var p *pair
	var enc *Encoder
	var dec Decoder
	if capacity > 0 {
		p = newPair(capacity, 0)
		enc = p.enc
	}
	var block []byte
	var fields []HeaderField
	var wire, streams int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s Section
		s, block = enc.Begin(block[:0], uint64(4*i))
		for _, f := range mixes[i%len(mixes)] {
			block = s.Append(block, f.Name, f.Value, sensitive(f.Name))
		}
		block = s.End(block)
		var err error
		if p == nil {
			fields, err = dec.AppendSection(fields[:0], block, 0, uint64(4*i))
		} else {
			fields, err = p.dec.AppendSection(fields[:0], block, 0, uint64(4*i))
			streams += len(p.encoder)
			if err == nil {
				err = p.dec.FeedEncoder(p.encoder)
			}
			p.encoder = p.encoder[:0]
			out := p.dec.AppendInstructions(nil)
			streams += len(out)
			if err == nil {
				err = p.enc.FeedDecoder(out)
			}
		}
		if err != nil || len(fields) != len(mixes[0]) {
			b.Fatalf("decoding: %v, %d fields", err, len(fields))
		}
		wire += len(block)
	}
	b.ReportMetric(float64(wire+streams)/float64(b.N), "wire-B/op")
}

// BenchmarkSections measures encoding and decoding a stream of requests
// whose large fields repeat, with no dynamic table, the default, and with
// one of 4 KiB.
func BenchmarkSections(b *testing.B) {
	b.Run("static", func(b *testing.B) { benchSections(b, 0) })
	b.Run("dynamic4k", func(b *testing.B) { benchSections(b, 4096) })
}
