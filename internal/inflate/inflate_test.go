package inflate

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"testing"
)

// samples are bodies of the shapes that exercise each kind of block: empty,
// tiny, repetitive (long matches, fixed codes), text-like (dynamic codes) and
// random (stored blocks), some longer than the window.
func samples() [][]byte {
	rng := rand.New(rand.NewPCG(1, 2))
	random := make([]byte, 200<<10)
	for i := range random {
		random[i] = byte(rng.Uint32())
	}
	var text bytes.Buffer
	words := []string{"fib ", "engine ", "poller ", "chunked ", "gzip ", "deflate ", "\n", "window ", "match "}
	for text.Len() < 300<<10 {
		text.WriteString(words[rng.IntN(len(words))])
	}
	return [][]byte{
		nil,
		[]byte("a"),
		[]byte("hello, hello, hello, hello"),
		bytes.Repeat([]byte{'x'}, 100<<10),
		text.Bytes(),
		random,
		append(random[:40<<10:40<<10], bytes.Repeat([]byte("abc"), 30<<10)...),
	}
}

func compress(t testing.TB, format Format, level int, data []byte) []byte {
	var buf bytes.Buffer
	var w io.WriteCloser
	var err error
	switch format {
	case Gzip:
		w, err = gzip.NewWriterLevel(&buf, level)
	case Zlib:
		w, err = zlib.NewWriterLevel(&buf, level)
	default:
		w, err = flate.NewWriter(&buf, level)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// decodeSplit decodes src handing it over in pieces of at most in bytes,
// with at most out bytes of room at a time, as a connection would deliver it
// and a reader take it.
func decodeSplit(format Format, src []byte, in, out int) ([]byte, error) {
	d := NewDecoder(format)
	var result []byte
	buf := make([]byte, out)
	for {
		piece := src[:min(in, len(src))]
		final := len(piece) == len(src)
		n, used, err := d.Decode(buf, piece, final)
		result = append(result, buf[:n]...)
		src = src[used:]
		if err != nil {
			return result, err
		}
		if d.Done() && len(src) == 0 {
			return result, nil
		}
		if n == 0 && used == 0 {
			return result, errors.New("no progress")
		}
	}
}

func TestDecodeMatchesStandardLibrary(t *testing.T) {
	splits := [][2]int{{1 << 30, 1 << 30}, {1, 1 << 20}, {7, 3}, {1 << 20, 1}, {4096, 1000}, {13, 64 << 10}}
	for _, format := range []Format{Raw, Zlib, Gzip, ZlibOrRaw} {
		for _, level := range []int{flate.NoCompression, flate.BestSpeed, flate.DefaultCompression, flate.HuffmanOnly} {
			for i, data := range samples() {
				encodeAs := format
				if format == ZlibOrRaw {
					// Both of what it takes.
					encodeAs = Zlib
					if i%2 == 1 {
						encodeAs = Raw
					}
				}
				coded := compress(t, encodeAs, level, data)
				for _, split := range splits {
					if split[0] < 64 && len(coded) > 100<<10 || split[1] < 64 && len(data) > 100<<10 {
						continue // slow, and covered by the smaller samples
					}
					got, err := decodeSplit(format, coded, split[0], split[1])
					if err != nil {
						t.Fatalf("format %d level %d sample %d split %v: %v", format, level, i, split, err)
					}
					if !bytes.Equal(got, data) {
						t.Fatalf("format %d level %d sample %d split %v: decoded %d bytes, want %d", format, level, i, split, len(got), len(data))
					}
				}
			}
		}
	}
}

func TestGzipMembers(t *testing.T) {
	a, b := []byte("first member, "), bytes.Repeat([]byte("second "), 1000)
	coded := append(compress(t, Gzip, 6, a), compress(t, Gzip, 6, b)...)
	for _, in := range []int{1, 5, 1 << 20} {
		got, err := decodeSplit(Gzip, coded, in, 100)
		if err != nil || !bytes.Equal(got, append(append([]byte(nil), a...), b...)) {
			t.Fatalf("pieces of %d: %q, %v", in, got, err)
		}
	}
	// Between members the stream may end, until more arrives.
	d := NewDecoder(Gzip)
	first := compress(t, Gzip, 6, a)
	buf := make([]byte, 100)
	n, used, err := d.Decode(buf, first, false)
	if err != nil || used != len(first) || string(buf[:n]) != string(a) || !d.Done() {
		t.Fatalf("first member: n %d used %d err %v done %v", n, used, err, d.Done())
	}
	if _, _, err := d.Decode(buf, []byte("garbage!!!"), true); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("garbage after a member: %v", err)
	}
}

func TestGzipHeaderFields(t *testing.T) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Name, w.Comment, w.Extra = "name.txt", "a comment", []byte("extra field")
	_, _ = w.Write([]byte("body"))
	_ = w.Close()
	coded := buf.Bytes()
	// Set FHCRC with the header's CRC, which Go's writer never sends.
	end := bytes.Index(coded[10:], []byte("a comment\x00")) + 10 + len("a comment\x00")
	head := append([]byte(nil), coded[:end]...)
	head[3] |= gzipHCRC
	crc := crc32.ChecksumIEEE(head)
	withCRC := append(append(head, byte(crc), byte(crc>>8)), coded[end:]...)
	for _, c := range [][]byte{coded, withCRC} {
		got, err := decodeSplit(Gzip, c, 1, 2)
		if err != nil || string(got) != "body" {
			t.Fatalf("%q, %v", got, err)
		}
	}
	withCRC[end] ^= 1
	if _, err := decodeSplit(Gzip, withCRC, 1<<20, 100); !errors.Is(err, ErrChecksum) {
		t.Fatalf("bad header CRC: %v", err)
	}
}

func TestDecodeErrors(t *testing.T) {
	data := bytes.Repeat([]byte("checksummed "), 500)
	gz := compress(t, Gzip, 6, data)
	zl := compress(t, Zlib, 6, data)
	raw := compress(t, Raw, 6, data)
	flip := func(b []byte, at int) []byte {
		b = append([]byte(nil), b...)
		b[at] ^= 0x55
		return b
	}
	cases := []struct {
		name   string
		format Format
		src    []byte
		want   error
	}{
		{"gzip crc", Gzip, flip(gz, len(gz)-8), ErrChecksum},
		{"gzip size", Gzip, flip(gz, len(gz)-1), ErrChecksum},
		{"gzip magic", Gzip, flip(gz, 0), ErrCorrupt},
		{"zlib adler", Zlib, flip(zl, len(zl)-1), ErrChecksum},
		{"zlib header", Zlib, flip(zl, 0), ErrCorrupt},
		{"zlib dictionary", Zlib, []byte{0x78, 0xbb, 0, 0, 0, 0}, ErrCorrupt},
		{"truncated gzip", Gzip, gz[:len(gz)-3], io.ErrUnexpectedEOF},
		{"truncated raw", Raw, raw[:len(raw)/2], io.ErrUnexpectedEOF},
		{"trailing raw", Raw, append(append([]byte(nil), raw...), 'x'), ErrTrailingData},
		{"trailing zlib", Zlib, append(append([]byte(nil), zl...), 'x'), ErrTrailingData},
		{"block type 3", Raw, []byte{0x07, 0, 0}, ErrCorrupt},
		{"stored length", Raw, []byte{0x01, 5, 0, 5, 0, 'a'}, ErrCorrupt},
		{"distance too far", Raw, farDistance(), ErrCorrupt},
		{"empty", Gzip, nil, io.ErrUnexpectedEOF},
	}
	for _, c := range cases {
		for _, in := range []int{1, 1 << 20} {
			_, err := decodeSplit(c.format, c.src, in, 64)
			if !errors.Is(err, c.want) {
				t.Errorf("%s, pieces of %d: %v, want %v", c.name, in, err, c.want)
			}
		}
	}
	// An error is the decoder's last word.
	d := NewDecoder(Raw)
	buf := make([]byte, 10)
	_, _, first := d.Decode(buf, []byte{0x07}, false)
	if _, _, again := d.Decode(buf, raw, true); first == nil || again != first {
		t.Fatalf("errors %v then %v", first, again)
	}
}

// farDistance is a fixed-code block whose first symbol is a match, reaching
// back before the start of the output.
func farDistance() []byte {
	var w bitWriter
	w.write(1, 1) // final
	w.write(1, 2) // fixed codes
	// Length 3 is symbol 257, code 0000001 in 7 bits, sent most significant
	// bit first.
	w.writeCode(0b0000001, 7)
	w.writeCode(0, 5) // distance 1
	w.writeCode(0, 7) // end of block
	return w.bytes()
}

type bitWriter struct {
	out   []byte
	bits  uint64
	nbits uint
}

func (w *bitWriter) write(v uint64, n uint) {
	w.bits |= v << w.nbits
	w.nbits += n
	for w.nbits >= 8 {
		w.out = append(w.out, byte(w.bits))
		w.bits >>= 8
		w.nbits -= 8
	}
}

func (w *bitWriter) writeCode(code uint64, n uint) {
	for i := int(n) - 1; i >= 0; i-- {
		w.write(code>>uint(i)&1, 1)
	}
}

func (w *bitWriter) bytes() []byte {
	if w.nbits > 0 {
		return append(w.out, byte(w.bits))
	}
	return w.out
}

// TestOutputBounded checks that a body that inflates a thousandfold is
// decoded into no more room than it is given.
func TestOutputBounded(t *testing.T) {
	data := make([]byte, 10<<20)
	coded := compress(t, Gzip, flate.BestCompression, data)
	if len(coded) > 20<<10 {
		t.Fatalf("compressed to %d bytes", len(coded))
	}
	d := NewDecoder(Gzip)
	buf := make([]byte, 4096)
	n, used, err := d.Decode(buf, coded, true)
	if err != nil || n != len(buf) || d.Done() {
		t.Fatalf("n %d used %d err %v", n, used, err)
	}
	total := n
	coded = coded[used:]
	for !d.Done() {
		n, used, err = d.Decode(buf, coded, true)
		if err != nil {
			t.Fatal(err)
		}
		coded = coded[used:]
		total += n
	}
	if total != len(data) {
		t.Fatalf("decoded %d bytes", total)
	}
}

func FuzzDecode(f *testing.F) {
	for _, data := range samples()[:4] {
		f.Add(compress(f, Raw, 6, data), uint8(0), uint16(7), uint16(100))
		f.Add(compress(f, Gzip, 1, data), uint8(2), uint16(1), uint16(3))
		f.Add(compress(f, Zlib, 0, data), uint8(1), uint16(1000), uint16(1))
	}
	f.Fuzz(func(t *testing.T, src []byte, format uint8, in, out uint16) {
		fm := Format(format % 4)
		got, err := decodeSplit(fm, src, int(in)+1, int(out)+1)
		var want []byte
		var stdErr error
		switch fm {
		case Raw:
			want, stdErr = io.ReadAll(flate.NewReader(bytes.NewReader(src)))
		case Zlib:
			var r io.ReadCloser
			if r, stdErr = zlib.NewReader(bytes.NewReader(src)); stdErr == nil {
				want, stdErr = io.ReadAll(r)
			}
		case Gzip:
			var r *gzip.Reader
			if r, stdErr = gzip.NewReader(bytes.NewReader(src)); stdErr == nil {
				want, stdErr = io.ReadAll(r)
			}
		default:
			return
		}
		if err == nil && stdErr == nil && !bytes.Equal(got, want) {
			t.Fatalf("decoded %d bytes, the standard library %d", len(got), len(want))
		}
		// One decode at once and in pieces agree.
		whole, wholeErr := decodeSplit(fm, src, len(src)+1, 1<<20)
		if (err == nil) != (wholeErr == nil) || err == nil && !bytes.Equal(got, whole) {
			t.Fatalf("in pieces: %v; at once: %v", err, wholeErr)
		}
	})
}

func BenchmarkDecode(b *testing.B) {
	data := samples()[4]
	coded := compress(b, Gzip, 6, data)
	buf := make([]byte, 32<<10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := decodeSplit(Gzip, coded, 64<<10, len(buf)); err != nil {
			b.Fatal(err)
		}
	}
}
