package http

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/inflate"
)

// The transfer codings (RFC 9112 section 7) besides chunked that are decoded:
// gzip, its old name x-gzip, and deflate, which is zlib, or raw DEFLATE from
// the senders that have always sent that under the name. They are decoded on
// both sides, a server request and a client response, whether the body is
// buffered whole or streamed, and Content-Encoding is left to the caller, as
// it is net/http's.
//
// compress (LZW) is not: package compress/lzw reads the formats of GIF, TIFF
// and PDF, which stop at 12-bit codes, while compress(1) grows them to 16, so
// it could decode only the first few kilobytes of a stream. A message sent in
// it, or in more than one coding besides chunked, is refused: a request with
// 501 Not Implemented, a response by failing it.
//
// A coded body is decoded as it is read, never ahead of the reader: what
// waits in memory is the coded bytes, which pace the peer as any body's do,
// and a body that inflates a thousandfold takes no more room than its coded
// size until something takes it. The limits on a body — MaxBodyBytes,
// MaxStreamedBodyBytes, MaxResponseBodyBytes — bound what it decodes to as
// well as what arrives, so a zip bomb is refused rather than buffered.

// transferCoding is a coding a message body arrived in, other than chunked:
// "gzip" or "deflate", as its names are normalized, or "" for none.
type transferCoding = string

// splitTransferCodings reads the Transfer-Encoding fields of head, a request
// or response head ending with its blank line, and when they name a coding
// besides chunked, returns head rewritten without it, for net/http's reader,
// which takes chunked alone: every Transfer-Encoding field becomes a lone
// "Transfer-Encoding: chunked" when chunked is the last coding, and goes
// otherwise, along with any Content-Length, which the codings override. It
// returns the coding and the codings as they were listed, for the message's
// TransferEncoding, and a nil head when there is nothing to rewrite.
//
// A request whose last coding is not chunked cannot be framed (RFC 9112
// section 6.3), and is malformed; a response that ends that way ends when the
// connection does, which its caller sees from its having no chunked field
// left. A coding listed twice, chunked anywhere but last, or more than one
// coding besides chunked, are refused, as are codings other than those above.
func splitTransferCodings(head []byte, request bool) (rewritten []byte, coding transferCoding, list []string, err error) {
	values, ok := transferEncodingFields(head)
	if !ok {
		return nil, "", nil, nil
	}
	for _, value := range values {
		for element := range strings.SplitSeq(value, ",") {
			element = strings.ToLower(strings.Trim(element, " \t"))
			if element != "" {
				list = append(list, element)
			}
		}
	}
	chunked := len(list) > 0 && list[len(list)-1] == "chunked"
	codings := list
	if chunked {
		codings = list[:len(list)-1]
	}
	switch {
	case len(list) == 0:
		return nil, "", nil, errors.New("empty Transfer-Encoding")
	case request && !chunked:
		return nil, "", nil, errors.New("chunked is not the last transfer coding")
	}
	for _, c := range codings {
		if c == "chunked" {
			return nil, "", nil, errors.New("chunked applied more than once")
		}
	}
	switch {
	case len(codings) == 0:
		// Chunked alone, spread over fields or padded with empty elements,
		// which net/http refuses: it reads as one field naming it.
	case len(codings) > 1:
		return nil, "", nil, fmt.Errorf("%w: more than one transfer coding %q", ErrUnsupportedTransferEncoding, list)
	case codings[0] == "gzip" || codings[0] == "x-gzip":
		coding = "gzip"
	case codings[0] == "deflate":
		coding = "deflate"
	default:
		return nil, "", nil, fmt.Errorf("%w: %q", ErrUnsupportedTransferEncoding, codings[0])
	}
	rewritten = make([]byte, 0, len(head))
	replaced := false
	for line := range bytes.SplitSeq(head[:len(head)-2], []byte("\r\n")) {
		name, _, ok := bytes.Cut(line, []byte(":"))
		switch {
		case !ok:
		case strings.EqualFold(string(name), "Transfer-Encoding"):
			if chunked && !replaced {
				replaced = true
				rewritten = append(rewritten, "Transfer-Encoding: chunked\r\n"...)
			}
			continue
		case !chunked && strings.EqualFold(string(name), "Content-Length"):
			continue
		}
		rewritten = append(append(rewritten, line...), "\r\n"...)
	}
	return append(rewritten, "\r\n"...), coding, list, nil
}

// transferEncodingFields returns the values of head's Transfer-Encoding
// fields, as they were written, and whether head has any.
func transferEncodingFields(head []byte) ([]string, bool) {
	var values []string
	for line := range bytes.SplitSeq(head, []byte("\r\n")) {
		if name, value, ok := bytes.Cut(line, []byte(":")); ok && strings.EqualFold(string(name), "Transfer-Encoding") {
			values = append(values, string(value))
		}
	}
	return values, values != nil
}

// plainChunked reports whether head's Transfer-Encoding is the one field
// naming chunked alone, the shape net/http's readers take as it is.
func plainChunked(head []byte) bool {
	values, ok := transferEncodingFields(head)
	return ok && len(values) == 1 && strings.EqualFold(strings.Trim(values[0], " \t"), "chunked")
}

// transferEncodingError reports whether err is net/http's refusal of a
// Transfer-Encoding, which it gives no type to tell from other malformed
// messages.
func transferEncodingError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "transfer encoding")
}

// codingDecoder decodes a body sent in a transfer coding.
type codingDecoder struct {
	d *inflate.Decoder
}

func newCodingDecoder(coding transferCoding) *codingDecoder {
	format := inflate.Gzip
	if coding == "deflate" {
		format = inflate.ZlibOrRaw
	}
	return &codingDecoder{d: inflate.NewDecoder(format)}
}

// decode decodes as much of src into dst as both allow, and reports how much
// it wrote and used, and whether the body has ended, which final says src
// does: then a body that does not end within it fails.
func (c *codingDecoder) decode(dst, src []byte, final bool) (n, used int, done bool, err error) {
	for n < len(dst) {
		k, u, err := c.d.Decode(dst[n:], src[used:], final)
		n += k
		used += u
		if err != nil {
			return n, used, false, err
		}
		if k == 0 && u == 0 || c.d.Done() && used == len(src) {
			break
		}
	}
	return n, used, c.d.Done() && used == len(src), nil
}

// codingPiece is how much a coded body is decoded into at a time, for a
// callback that takes it.
const codingPiece = 32 << 10

// decodeWhole decodes body, a whole body sent in coding, into a buffer from
// the pool, refusing one that decodes to more than limit bytes, where limit
// below one is no limit, with errTooLarge. A body that is not in the coding
// fails with malformed.
func decodeWhole(coding transferCoding, body []byte, limit int64, errTooLarge, malformed error) ([]byte, error) {
	dec := newCodingDecoder(coding)
	out := bufferpool.Get(min(max(2*len(body), 4096), cappedLimit(limit)))[:0]
	for {
		if len(out) == cap(out) {
			if limit > 0 && int64(len(out)) > limit {
				bufferpool.Put(out)
				return nil, errTooLarge
			}
			// Never more room than the limit and a byte, which is what
			// tells a body past it.
			grown := bufferpool.Get(min(2*cap(out), cappedLimit(limit)))[:len(out)]
			copy(grown, out)
			bufferpool.Put(out)
			out = grown
		}
		n, used, done, err := dec.decode(out[len(out):cap(out)], body, true)
		out = out[:len(out)+n]
		body = body[used:]
		switch {
		case err != nil:
			bufferpool.Put(out)
			return nil, fmt.Errorf("%w: %s transfer coding: %v", malformed, coding, err)
		case limit > 0 && int64(len(out)) > limit:
			bufferpool.Put(out)
			return nil, errTooLarge
		case done:
			return out, nil
		}
	}
}

// cappedLimit is the largest buffer decodeWhole takes for limit: the limit and
// a byte, or what an int holds for none.
func cappedLimit(limit int64) int {
	const maxInt = int(^uint(0) >> 1)
	if limit <= 0 || limit >= int64(maxInt) {
		return maxInt
	}
	return int(limit) + 1
}
