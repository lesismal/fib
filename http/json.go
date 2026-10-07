//go:build linux || darwin || windows

package http

import (
	"bytes"
	"encoding/json"
	"sync"
)

// JSONEncoder is how Context.JSON encodes a value: it appends v's JSON to dst
// and returns the extended slice, as strconv's Append functions do. It is
// encoding/json's by default, which encodes as json.Marshal does. A program
// may set it, once, before it serves anything, to another encoder, such as
// one that appends sonic's encoding:
//
//	fibhttp.JSONEncoder = func(dst []byte, v any) ([]byte, error) {
//		err := encoder.EncodeInto(&dst, v, 0)
//		return dst, err
//	}
var JSONEncoder func(dst []byte, v any) ([]byte, error) = appendJSON

// JSON responds with status and v encoded as JSON by JSONEncoder, with a
// Content-Type of application/json. If v cannot be encoded, nothing is sent
// and the encoder's error is returned, so that the handler can answer
// otherwise.
func (c *Context) JSON(status int, v any) error {
	buf := jsonBuffers.Get().(*[]byte)
	body, err := JSONEncoder((*buf)[:0], v)
	if err == nil {
		if c.isHTTP1() {
			// HTTP/1 has copied the body out by the time Respond returns,
			// and a hook keeps none of it past its call, so the buffer serves
			// the next response.
			err = c.Respond(status, "application/json", body)
		} else {
			// HTTP/2 holds a body until flow control lets the rest go, and a
			// stream served outside this package may hold it too, so theirs
			// is a copy of its own, made at its length, as json.Marshal
			// makes one.
			err = c.Respond(status, "application/json", bytes.Clone(body))
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

// jsonEncoder is an encoding/json Encoder writing into the slice appendJSON
// appends to. The Encoder keeps encoding/json's own buffer pool, and with
// HTML escaping left on it encodes exactly as json.Marshal does, save for
// the newline it ends with, which appendJSON drops.
type jsonEncoder struct {
	out []byte
	enc *json.Encoder
}

func (e *jsonEncoder) Write(p []byte) (int, error) {
	e.out = append(e.out, p...)
	return len(p), nil
}

var jsonEncoders = sync.Pool{New: func() any {
	e := new(jsonEncoder)
	e.enc = json.NewEncoder(e)
	return e
}}

func appendJSON(dst []byte, v any) ([]byte, error) {
	e := jsonEncoders.Get().(*jsonEncoder)
	e.out = dst
	err := e.enc.Encode(v)
	out := e.out
	e.out = nil
	jsonEncoders.Put(e)
	if err != nil {
		return dst, err
	}
	return out[:len(out)-1], nil
}
