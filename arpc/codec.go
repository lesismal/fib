//go:build linux || darwin || windows

package arpc

import (
	json "encoding/json/v2"
	"errors"
	"log/slog"
	"sync/atomic"
	"unsafe"
)

// Codec encodes and decodes the payloads of messages.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec is a Codec of encoding/json/v2.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// DefaultCodec is the Codec of a Server or Client that is given none.
var DefaultCodec Codec = JSONCodec{}

// valueToBytes is the payload of v: a []byte, a string or an error, or a
// pointer to one, as it is, and anything else as codec, or DefaultCodec if it
// is nil, encodes it. A nil v is an empty payload.
func valueToBytes(codec Codec, v any) ([]byte, error) {
	switch vt := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return vt, nil
	case *[]byte:
		return *vt, nil
	case string:
		return unsafe.Slice(unsafe.StringData(vt), len(vt)), nil
	case *string:
		return unsafe.Slice(unsafe.StringData(*vt), len(*vt)), nil
	case error:
		s := vt.Error()
		return unsafe.Slice(unsafe.StringData(s), len(s)), nil
	case *error:
		s := (*vt).Error()
		return unsafe.Slice(unsafe.StringData(s), len(s)), nil
	}
	if codec == nil {
		codec = DefaultCodec
	}
	return codec.Marshal(v)
}

// bytesToValue decodes data into v: a *[]byte gets a copy, a *string and an
// *error get the text, and anything else is what codec, or DefaultCodec if it
// is nil, decodes. A nil v takes nothing.
func bytesToValue(codec Codec, data []byte, v any) error {
	switch vt := v.(type) {
	case nil:
		return nil
	case *[]byte:
		*vt = append([]byte(nil), data...)
		return nil
	case *string:
		*vt = string(data)
		return nil
	case *error:
		*vt = errors.New(string(data))
		return nil
	}
	if codec == nil {
		codec = DefaultCodec
	}
	return codec.Unmarshal(data, v)
}

var loggerValue atomic.Pointer[slog.Logger]

// SetLogger sets where the package logs what goes wrong away from any caller:
// a message no handler takes, a response nobody waits for, a handler that
// panics. Nil restores slog.Default.
func SetLogger(l *slog.Logger) { loggerValue.Store(l) }

func logger() *slog.Logger {
	if l := loggerValue.Load(); l != nil {
		return l
	}
	return slog.Default()
}
