//go:build linux || darwin || windows

package http

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"sync"
)

// JSONEncoder is how Context.JSON encodes a value: it appends v's JSON to dst
// and returns the extended slice, as strconv's Append functions do. It is
// encoding/json/v2's by default, which encodes as json.Marshal does. A program
// may set it, once, before it serves anything, to another encoder, such as
// one that appends sonic's encoding:
//
//	fibhttp.JSONEncoder = func(dst []byte, v any) ([]byte, error) {
//		err := encoder.EncodeInto(&dst, v, 0)
//		return dst, err
//	}
var JSONEncoder func(dst []byte, v any) ([]byte, error) = func(dst []byte, v any) ([]byte, error) { return appendJSON(dst, v) }

// JSON responds with status and v encoded as JSON by JSONEncoder, with a
// Content-Type of application/json. If v cannot be encoded, nothing is sent
// and the encoder's error is returned, so that the handler can answer
// otherwise.
func (c *Context) JSON(status int, v any) error {
	return c.respondEncoded(status, "application/json", func(dst []byte) ([]byte, error) {
		return JSONEncoder(dst, v)
	})
}

// respondEncoded responds with status and what encode appends to a pooled
// buffer. If encode fails nothing is sent and its error is returned.
func (c *Context) respondEncoded(status int, contentType string, encode func(dst []byte) ([]byte, error)) error {
	buf := jsonBuffers.Get().(*[]byte)
	body, err := encode((*buf)[:0])
	if err == nil {
		if c.isHTTP1() {
			// HTTP/1 has copied the body out by the time Respond returns,
			// and a hook keeps none of it past its call, so the buffer serves
			// the next response.
			err = c.Respond(status, contentType, body)
		} else {
			// HTTP/2 holds a body until flow control lets the rest go, and a
			// stream served outside this package may hold it too, so theirs
			// is a copy of its own, made at its length, as json.Marshal
			// makes one.
			err = c.Respond(status, contentType, bytes.Clone(body))
		}
	}
	if cap(body) <= maxJSONBuffer {
		*buf = body[:0]
		jsonBuffers.Put(buf)
	}
	return err
}

// maxJSONBuffer bounds the buffers JSON keeps, so that one large response
// does not pin its buffer for every small one after it.
const maxJSONBuffer = 64 << 10

var jsonBuffers = sync.Pool{New: func() any { return new([]byte) }}

// appendWriter is an io.Writer appending to out.
type appendWriter struct{ out []byte }

func (w *appendWriter) Write(p []byte) (int, error) {
	w.out = append(w.out, p...)
	return len(p), nil
}

var appendWriters = sync.Pool{New: func() any { return new(appendWriter) }}

// appendJSON appends v's encoding/json/v2 encoding to dst, without a trailing
// newline. On an error it returns dst as it was.
func appendJSON(dst []byte, v any, opts ...json.Options) ([]byte, error) {
	w := appendWriters.Get().(*appendWriter)
	w.out = dst
	err := json.MarshalWrite(w, v, opts...)
	out := w.out
	w.out = nil
	appendWriters.Put(w)
	if err != nil {
		return dst, err
	}
	return out, nil
}

var indentOptions = jsontext.WithIndent("    ")
