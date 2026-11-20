//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package compress

import (
	"compress/flate"
	"strings"
	"testing"
)

// TestWriterPoolsReuse checks that a recycled compressor, reset onto a new
// output, compresses as well as a fresh one. The pools are per-P, so a
// handler's requests may never meet a recycled one; this does, on one
// goroutine.
func TestWriterPoolsReuse(t *testing.T) {
	pools := &writerPools{level: flate.DefaultCompression, newGzip: stdGzip, newDeflate: stdDeflate}
	body := []byte(strings.Repeat("recycled compressor ", 100))
	for _, encoding := range []string{encodingGzip, encodingDeflate} {
		first, ok := pools.compress(encoding, body)
		if !ok {
			t.Fatalf("%s: not compressed", encoding)
		}
		first = append([]byte(nil), first...)
		second, ok := pools.compress(encoding, body)
		if !ok || string(second) != string(first) {
			t.Errorf("%s: recycled compressor wrote %d bytes, fresh one %d", encoding, len(second), len(first))
		}
	}
}
