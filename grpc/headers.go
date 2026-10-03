//go:build linux || darwin || windows

package grpc

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/hpack"
)

// encodeTimeout is d as grpc-timeout carries it: at most eight digits and a
// unit.
func encodeTimeout(d time.Duration) string {
	if d <= 0 {
		return "1n"
	}
	units := []struct {
		unit byte
		size time.Duration
	}{{'n', time.Nanosecond}, {'u', time.Microsecond}, {'m', time.Millisecond},
		{'S', time.Second}, {'M', time.Minute}, {'H', time.Hour}}
	const maxValue = 1e8 - 1
	for _, u := range units {
		v := (d + u.size - 1) / u.size
		if v <= maxValue {
			return strconv.FormatInt(int64(v), 10) + string(u.unit)
		}
	}
	return strconv.FormatInt(maxValue, 10) + "H"
}

// decodeTimeout parses grpc-timeout.
func decodeTimeout(s string) (time.Duration, bool) {
	if len(s) < 2 || len(s) > 9 {
		return 0, false
	}
	v, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	var unit time.Duration
	switch s[len(s)-1] {
	case 'n':
		unit = time.Nanosecond
	case 'u':
		unit = time.Microsecond
	case 'm':
		unit = time.Millisecond
	case 'S':
		unit = time.Second
	case 'M':
		unit = time.Minute
	case 'H':
		unit = time.Hour
	default:
		return 0, false
	}
	if v > int64(1<<63-1)/int64(unit) {
		return 1<<63 - 1, true
	}
	return time.Duration(v) * unit, true
}

// encodeGRPCMessage percent-encodes what grpc-message cannot carry as it is.
func encodeGRPCMessage(msg string) string {
	clean := true
	for i := 0; i < len(msg); i++ {
		if c := msg[i]; c < ' ' || c > '~' || c == '%' {
			clean = false
			break
		}
	}
	if clean {
		return msg
	}
	var b strings.Builder
	const hex = "0123456789ABCDEF"
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c >= ' ' && c <= '~' && c != '%' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0xf])
	}
	return b.String()
}

// decodeGRPCMessage undoes encodeGRPCMessage, leaving a malformed escape as
// it is.
func decodeGRPCMessage(msg string) string {
	if !strings.Contains(msg, "%") {
		return msg
	}
	var b strings.Builder
	for i := 0; i < len(msg); i++ {
		if msg[i] == '%' && i+2 < len(msg) {
			if v, err := strconv.ParseUint(msg[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(msg[i])
	}
	return b.String()
}

// reservedHeader reports whether a header is gRPC's or HTTP/2's own rather
// than metadata.
func reservedHeader(name string) bool {
	if name != "" && name[0] == ':' {
		return true
	}
	switch name {
	case "content-type", "te", "grpc-message-type", "grpc-encoding", "grpc-message",
		"grpc-status", "grpc-timeout", "grpc-status-details-bin", "grpc-accept-encoding",
		"connection", "transfer-encoding", "keep-alive", "upgrade", "proxy-connection":
		return true
	}
	return false
}

// addMetadata adds a received header to md, decoding a binary one.
func addMetadata(md metadata.MD, name, value string) error {
	if strings.HasSuffix(name, "-bin") {
		b, err := decodeBinHeader(value)
		if err != nil {
			return err
		}
		value = string(b)
	}
	md[name] = append(md[name], value)
	return nil
}

func encodeBin(b []byte) string { return base64.RawStdEncoding.EncodeToString(b) }

func decodeBinHeader(v string) ([]byte, error) {
	if len(v)%4 == 0 {
		return base64.StdEncoding.DecodeString(v)
	}
	return base64.RawStdEncoding.DecodeString(v)
}

// appendMetadata encodes md into a header block, binary values in base64,
// leaving out what is reserved.
func appendMetadata(enc *hpack.Encoder, block []byte, md metadata.MD) []byte {
	for k, vs := range md {
		k = strings.ToLower(k)
		if reservedHeader(k) || !validHeaderName(k) {
			continue
		}
		bin := strings.HasSuffix(k, "-bin")
		for _, v := range vs {
			if bin {
				v = base64.RawStdEncoding.EncodeToString([]byte(v))
			}
			block = enc.AppendField(block, k, v, false)
		}
	}
	return block
}

func validHeaderName(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// contentSubtype returns the codec name a content type asks for, and
// whether it is a gRPC content type at all.
func contentSubtype(contentType string) (string, bool) {
	const base = "application/grpc"
	if !strings.HasPrefix(contentType, base) {
		return "", false
	}
	rest := contentType[len(base):]
	if rest == "" || rest[0] == ';' {
		return "proto", true
	}
	if rest[0] != '+' {
		return "", false
	}
	sub, _, _ := strings.Cut(rest[1:], ";")
	return strings.ToLower(sub), true
}

func contentType(subtype string) string {
	if subtype == "" || subtype == "proto" {
		return "application/grpc"
	}
	return "application/grpc+" + subtype
}

// httpStatusCode maps the HTTP status of a response that carries no
// grpc-status to a code, as the gRPC HTTP/2 protocol says.
func httpStatusCode(status int) codes.Code {
	switch status {
	case 400:
		return codes.Internal
	case 401:
		return codes.Unauthenticated
	case 403:
		return codes.PermissionDenied
	case 404:
		return codes.Unimplemented
	case 429, 502, 503, 504:
		return codes.Unavailable
	}
	return codes.Unknown
}
