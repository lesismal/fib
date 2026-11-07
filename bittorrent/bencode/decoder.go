package bencode

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// A Decoder decodes a sequence of bencoded values from a byte slice or an
// io.Reader.
//
// Over a byte slice (NewBytesDecoder) it decodes in place: InputOffset
// says where the next value starts, so whatever follows a value, such as
// the piece data after a ut_metadata dictionary, is the rest of the slice,
// and with NoCopy []byte and RawMessage values alias the slice.
//
// Over an io.Reader (NewDecoder) it reads one value at a time, up to
// Limits.MaxSize bytes, and may read ahead of it; Buffered returns what it
// read ahead.
type Decoder struct {
	data []byte // the slice of NewBytesDecoder
	off  int

	r      *bufio.Reader
	buf    []byte
	offset int64 // bytes of r consumed by decoded values

	limits Limits
	strict bool
	noCopy bool
}

// NewDecoder returns a Decoder that reads from r.
func NewDecoder(r io.Reader) *Decoder {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &Decoder{r: br, limits: Limits{}.withDefaults()}
}

// NewBytesDecoder returns a Decoder that decodes data in place.
func NewBytesDecoder(data []byte) *Decoder {
	return &Decoder{data: data, limits: Limits{}.withDefaults()}
}

// SetLimits sets the decoder's limits; zero fields keep the defaults.
func (d *Decoder) SetLimits(l Limits) {
	d.limits = l.withDefaults()
}

// Strict makes the decoder require canonical bencoding: dictionary keys
// sorted and unique. Syntax (leading zeros, "-0") is always checked.
func (d *Decoder) Strict() {
	d.strict = true
}

// NoCopy makes []byte and RawMessage values alias the input instead of
// copying it, so that the input must not change while they are in use. It
// applies only to a decoder over a byte slice: one over an io.Reader
// reuses its buffer and always copies.
func (d *Decoder) NoCopy() {
	d.noCopy = true
}

// InputOffset returns the offset in the input just past the last value
// decoded.
func (d *Decoder) InputOffset() int64 {
	if d.r == nil {
		return int64(d.off)
	}
	return d.offset
}

// More reports whether there is input left to decode.
func (d *Decoder) More() bool {
	if d.r == nil {
		return d.off < len(d.data)
	}
	_, err := d.r.Peek(1)
	return err == nil
}

// Buffered returns the input the decoder holds that it has not decoded:
// the rest of the slice, or what it read ahead from the io.Reader.
func (d *Decoder) Buffered() io.Reader {
	if d.r == nil {
		return bytes.NewReader(d.data[d.off:])
	}
	b, _ := d.r.Peek(d.r.Buffered())
	return bytes.NewReader(b)
}

// Decode decodes the next value into the value v points to. At the end of
// the input it returns io.EOF; a value cut short is io.ErrUnexpectedEOF
// over an io.Reader and a *SyntaxError over a byte slice. After a
// *UnmarshalTypeError the decoder is past the value, the rest of which
// was decoded.
func (d *Decoder) Decode(v any) error {
	ds := decodeState{maxDepth: d.limits.MaxDepth, strict: d.strict}
	if d.r == nil {
		if d.off >= len(d.data) {
			return io.EOF
		}
		ds.data, ds.off, ds.noCopy = d.data, d.off, d.noCopy
		if err := ds.unmarshal(v); err != nil {
			return err
		}
		d.off = ds.off
		return ds.typeErr
	}
	raw, err := d.readValue()
	if err != nil {
		return err
	}
	ds.data, ds.base = raw, d.offset
	d.offset += int64(len(raw))
	if err := ds.unmarshal(v); err != nil {
		return err
	}
	return ds.typeErr
}

var errTooLarge = errors.New("bencode: value larger than Limits.MaxSize")

// readValue reads the bytes of exactly one value from d.r into d.buf,
// checking only as much syntax as it needs to find the value's end; the
// decoding that follows checks the rest.
func (d *Decoder) readValue() ([]byte, error) {
	buf := d.buf[:0]
	depth := 0
	unexpected := func(err error) error {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	syntax := func(msg string) error {
		return &SyntaxError{Offset: d.offset + int64(len(buf)-1), msg: msg}
	}
	for {
		c, err := d.r.ReadByte()
		if err != nil {
			if len(buf) == 0 {
				return nil, err
			}
			return nil, unexpected(err)
		}
		if len(buf) >= d.limits.MaxSize {
			return nil, errTooLarge
		}
		buf = append(buf, c)
		switch {
		case c == 'i':
			for c != 'e' {
				if c, err = d.r.ReadByte(); err != nil {
					return nil, unexpected(err)
				}
				if len(buf) >= d.limits.MaxSize {
					return nil, errTooLarge
				}
				buf = append(buf, c)
				if c != 'e' && c != '-' && (c < '0' || c > '9') {
					return nil, syntax("invalid byte in integer")
				}
			}
		case c >= '0' && c <= '9':
			n := int(c - '0')
			for c != ':' {
				if c, err = d.r.ReadByte(); err != nil {
					return nil, unexpected(err)
				}
				buf = append(buf, c)
				if c == ':' {
					break
				}
				if c < '0' || c > '9' {
					return nil, syntax("invalid byte in string length")
				}
				if n = n*10 + int(c-'0'); n > d.limits.MaxSize {
					return nil, errTooLarge
				}
			}
			if n > d.limits.MaxSize-len(buf) {
				return nil, errTooLarge
			}
			// Grow as the bytes arrive rather than trusting the length: a
			// peer could announce a long string and send nothing.
			for n > 0 {
				chunk := min(n, 64<<10)
				buf = append(buf, make([]byte, chunk)...)
				if _, err := io.ReadFull(d.r, buf[len(buf)-chunk:]); err != nil {
					return nil, unexpected(err)
				}
				n -= chunk
			}
		case c == 'l' || c == 'd':
			if depth++; depth > d.limits.MaxDepth {
				return nil, syntax("nesting too deep")
			}
			continue
		case c == 'e':
			if depth == 0 {
				return nil, syntax("unexpected 'e'")
			}
			depth--
		default:
			return nil, syntax("invalid byte")
		}
		if depth == 0 {
			d.buf = buf
			return buf, nil
		}
	}
}
