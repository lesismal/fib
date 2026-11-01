package rtsp

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

// Defaults of a Parser's bounds.
const (
	DefaultMaxHeaderBytes = 16 << 10
	DefaultMaxBodyBytes   = 256 << 10
)

// Errors of the message layer.
var (
	// ErrMalformed is what the Parser reports for bytes that are not an
	// RTSP message or interleaved frame.
	ErrMalformed = errors.New("rtsp: malformed message")
	// ErrTooLarge is what the Parser reports for a header or body over its
	// bounds.
	ErrTooLarge = errors.New("rtsp: message too large")
)

// Methods of RFC 2326.
const (
	MethodOptions      = "OPTIONS"
	MethodDescribe     = "DESCRIBE"
	MethodAnnounce     = "ANNOUNCE"
	MethodSetup        = "SETUP"
	MethodPlay         = "PLAY"
	MethodPause        = "PAUSE"
	MethodRecord       = "RECORD"
	MethodTeardown     = "TEARDOWN"
	MethodGetParameter = "GET_PARAMETER"
	MethodSetParameter = "SET_PARAMETER"
)

// Status codes of RFC 2326.
const (
	StatusOK                      = 200
	StatusMovedPermanently        = 301
	StatusBadRequest              = 400
	StatusUnauthorized            = 401
	StatusForbidden               = 403
	StatusNotFound                = 404
	StatusMethodNotAllowed        = 405
	StatusNotAcceptable           = 406
	StatusRequestEntityTooLarge   = 413
	StatusUnsupportedMediaType    = 415
	StatusParameterNotUnderstood  = 451
	StatusNotEnoughBandwidth      = 453
	StatusSessionNotFound         = 454
	StatusMethodNotValidInState   = 455
	StatusHeaderFieldNotValid     = 456
	StatusAggregateNotAllowed     = 459
	StatusUnsupportedTransport    = 461
	StatusDestinationUnreachable  = 462
	StatusInternalServerError     = 500
	StatusNotImplemented          = 501
	StatusServiceUnavailable      = 503
	StatusRTSPVersionNotSupported = 505
	StatusOptionNotSupported      = 551
)

var statusText = map[int]string{
	StatusOK:                      "OK",
	StatusMovedPermanently:        "Moved Permanently",
	StatusBadRequest:              "Bad Request",
	StatusUnauthorized:            "Unauthorized",
	StatusForbidden:               "Forbidden",
	StatusNotFound:                "Not Found",
	StatusMethodNotAllowed:        "Method Not Allowed",
	StatusNotAcceptable:           "Not Acceptable",
	StatusRequestEntityTooLarge:   "Request Entity Too Large",
	StatusUnsupportedMediaType:    "Unsupported Media Type",
	StatusParameterNotUnderstood:  "Parameter Not Understood",
	StatusNotEnoughBandwidth:      "Not Enough Bandwidth",
	StatusSessionNotFound:         "Session Not Found",
	StatusMethodNotValidInState:   "Method Not Valid in This State",
	StatusHeaderFieldNotValid:     "Header Field Not Valid for Resource",
	StatusAggregateNotAllowed:     "Aggregate Operation Not Allowed",
	StatusUnsupportedTransport:    "Unsupported Transport",
	StatusDestinationUnreachable:  "Destination Unreachable",
	StatusInternalServerError:     "Internal Server Error",
	StatusNotImplemented:          "Not Implemented",
	StatusServiceUnavailable:      "Service Unavailable",
	StatusRTSPVersionNotSupported: "RTSP Version Not Supported",
	StatusOptionNotSupported:      "Option Not Supported",
}

// StatusText returns the reason phrase of a status code, "" for one it
// does not know.
func StatusText(code int) string { return statusText[code] }

// HeaderField is one header line.
type HeaderField struct {
	Name  string
	Value string
}

// Header is a message's header lines, in order and as written; names
// compare without regard to case.
type Header []HeaderField

// Get returns the value of the first field named name, "" when there is
// none.
func (h Header) Get(name string) string {
	v, _ := h.Lookup(name)
	return v
}

// Lookup returns the value of the first field named name.
func (h Header) Lookup(name string) (string, bool) {
	for _, f := range h {
		if strings.EqualFold(f.Name, name) {
			return f.Value, true
		}
	}
	return "", false
}

// Values returns the values of every field named name.
func (h Header) Values(name string) []string {
	var vs []string
	for _, f := range h {
		if strings.EqualFold(f.Name, name) {
			vs = append(vs, f.Value)
		}
	}
	return vs
}

// Set replaces the fields named name by one with value.
func (h *Header) Set(name, value string) {
	h.Del(name)
	*h = append(*h, HeaderField{name, value})
}

// Add adds a field.
func (h *Header) Add(name, value string) { *h = append(*h, HeaderField{name, value}) }

// Del removes the fields named name.
func (h *Header) Del(name string) {
	kept := (*h)[:0]
	for _, f := range *h {
		if !strings.EqualFold(f.Name, name) {
			kept = append(kept, f)
		}
	}
	*h = kept
}

func appendHeader(dst []byte, h Header, body []byte) []byte {
	for _, f := range h {
		if strings.EqualFold(f.Name, "Content-Length") {
			continue
		}
		dst = append(dst, f.Name...)
		dst = append(dst, ": "...)
		dst = append(dst, f.Value...)
		dst = append(dst, "\r\n"...)
	}
	if len(body) > 0 {
		dst = append(dst, "Content-Length: "...)
		dst = strconv.AppendInt(dst, int64(len(body)), 10)
		dst = append(dst, "\r\n"...)
	}
	dst = append(dst, "\r\n"...)
	return append(dst, body...)
}

// Request is an RTSP request.
type Request struct {
	Method string
	// URL is the Request-URI as written: an absolute rtsp:// URL, or "*".
	URL    string
	Header Header
	Body   []byte
}

// Append appends the request to dst, with a Content-Length for its body.
func (r *Request) Append(dst []byte) []byte {
	dst = append(dst, r.Method...)
	dst = append(dst, ' ')
	dst = append(dst, r.URL...)
	dst = append(dst, " RTSP/1.0\r\n"...)
	return appendHeader(dst, r.Header, r.Body)
}

// CSeq returns the request's sequence number, -1 when it has none.
func (r *Request) CSeq() int { return cseq(r.Header) }

// Response is an RTSP response.
type Response struct {
	StatusCode int
	// Reason is the reason phrase; Append writes StatusText's when it is
	// empty.
	Reason string
	Header Header
	Body   []byte
}

// Append appends the response to dst, with a Content-Length for its body.
func (r *Response) Append(dst []byte) []byte {
	dst = append(dst, "RTSP/1.0 "...)
	dst = strconv.AppendInt(dst, int64(r.StatusCode), 10)
	dst = append(dst, ' ')
	reason := r.Reason
	if reason == "" {
		reason = StatusText(r.StatusCode)
	}
	dst = append(dst, reason...)
	dst = append(dst, "\r\n"...)
	return appendHeader(dst, r.Header, r.Body)
}

// CSeq returns the response's sequence number, -1 when it has none.
func (r *Response) CSeq() int { return cseq(r.Header) }

func cseq(h Header) int {
	n, err := strconv.Atoi(strings.TrimSpace(h.Get("CSeq")))
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// Interleaved is a frame of RTP or RTCP sent on the RTSP connection
// itself (RFC 2326 section 10.12): '$', the channel, a 16-bit length and
// the packet.
type Interleaved struct {
	Channel uint8
	Data    []byte
}

// AppendInterleaved appends an interleaved frame of data on channel ch to
// dst. data is at most 65535 bytes.
func AppendInterleaved(dst []byte, ch uint8, data []byte) []byte {
	dst = append(dst, '$', ch)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(data)))
	return append(dst, data...)
}

// Parser splits the bytes of an RTSP connection into requests, responses
// and interleaved frames. It is sans-IO: the caller feeds it what it reads.
// It is not safe for concurrent use.
type Parser struct {
	// MaxHeaderBytes and MaxBodyBytes bound a message's start line and
	// header, and its body; 0 means DefaultMaxHeaderBytes and
	// DefaultMaxBodyBytes.
	MaxHeaderBytes int
	MaxBodyBytes   int

	buf []byte
}

// Feed parses data, after what an earlier call left incomplete, and calls
// fn with each whole message in turn: a *Request, a *Response or an
// *Interleaved. The messages' bodies and an interleaved frame's data are
// only valid during fn. Feed stops at the first error, of the bytes or of
// fn, and returns it; a Parser that has failed should be dropped with its
// connection.
func (p *Parser) Feed(data []byte, fn func(msg any) error) error {
	b := data
	if len(p.buf) > 0 {
		p.buf = append(p.buf, data...)
		b = p.buf
	}
	n, err := p.parse(b, fn)
	rest := b[n:]
	switch {
	case len(rest) == 0:
		p.buf = p.buf[:0]
	case len(p.buf) > 0:
		// Keep the rest at the start of the buffer.
		p.buf = p.buf[:copy(p.buf, rest)]
	default:
		p.buf = append(p.buf[:0], rest...)
	}
	return err
}

// Buffered returns how many bytes of an incomplete message the Parser
// holds.
func (p *Parser) Buffered() int { return len(p.buf) }

func (p *Parser) parse(b []byte, fn func(any) error) (int, error) {
	maxHeader, maxBody := p.MaxHeaderBytes, p.MaxBodyBytes
	if maxHeader <= 0 {
		maxHeader = DefaultMaxHeaderBytes
	}
	if maxBody <= 0 {
		maxBody = DefaultMaxBodyBytes
	}
	off := 0
	for off < len(b) {
		rest := b[off:]
		if rest[0] == '$' {
			if len(rest) < 4 {
				return off, nil
			}
			n := int(binary.BigEndian.Uint16(rest[2:]))
			if len(rest) < 4+n {
				return off, nil
			}
			if err := fn(&Interleaved{Channel: rest[1], Data: rest[4 : 4+n]}); err != nil {
				return off + 4 + n, err
			}
			off += 4 + n
			continue
		}
		// Blank lines between messages are skipped, as some clients send
		// a CRLF after a body.
		if rest[0] == '\r' || rest[0] == '\n' {
			off++
			continue
		}
		end, sep := headerEnd(rest)
		if end < 0 {
			if len(rest) > maxHeader {
				return off, ErrTooLarge
			}
			return off, nil
		}
		if end > maxHeader {
			return off, ErrTooLarge
		}
		msg, header, err := parseHead(rest[:end])
		if err != nil {
			return off, err
		}
		size := 0
		if v, ok := header.Lookup("Content-Length"); ok {
			size, err = strconv.Atoi(strings.TrimSpace(v))
			if err != nil || size < 0 {
				return off, ErrMalformed
			}
			if size > maxBody {
				return off, ErrTooLarge
			}
		}
		total := end + sep + size
		if len(rest) < total {
			return off, nil
		}
		body := rest[end+sep : total]
		switch m := msg.(type) {
		case *Request:
			m.Header, m.Body = header, body
		case *Response:
			m.Header, m.Body = header, body
		}
		if err := fn(msg); err != nil {
			return off + total, err
		}
		off += total
	}
	return off, nil
}

// headerEnd finds the blank line that ends a header, CRLF CRLF or LF LF,
// and returns where the header ends and the length of the blank line's
// endings, or -1.
func headerEnd(b []byte) (int, int) {
	for i := 0; i < len(b); i++ {
		if b[i] != '\n' {
			continue
		}
		if i+1 < len(b) && b[i+1] == '\n' {
			return i + 1, 1
		}
		if i+2 < len(b) && b[i+1] == '\r' && b[i+2] == '\n' {
			return i + 1, 2
		}
	}
	return -1, 0
}

// parseHead parses a start line and header lines.
func parseHead(b []byte) (any, Header, error) {
	lines := strings.Split(strings.TrimRight(string(b), "\r\n"), "\n")
	start := strings.TrimRight(lines[0], "\r")
	var msg any
	if rest, ok := strings.CutPrefix(start, "RTSP/"); ok {
		version, rest, _ := strings.Cut(rest, " ")
		code, reason, _ := strings.Cut(rest, " ")
		n, err := strconv.Atoi(code)
		if !strings.HasPrefix(version, "1.") || err != nil || n < 100 || n > 999 {
			return nil, nil, ErrMalformed
		}
		msg = &Response{StatusCode: n, Reason: reason}
	} else {
		method, rest, ok1 := strings.Cut(start, " ")
		url, version, ok2 := strings.Cut(rest, " ")
		if !ok1 || !ok2 || method == "" || url == "" || !strings.HasPrefix(version, "RTSP/1.") || !isToken(method) {
			return nil, nil, ErrMalformed
		}
		msg = &Request{Method: method, URL: url}
	}
	var h Header
	for _, line := range lines[1:] {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			// A continuation of the field before.
			if len(h) == 0 {
				return nil, nil, ErrMalformed
			}
			h[len(h)-1].Value += " " + strings.TrimSpace(line)
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok || !isToken(name) {
			return nil, nil, ErrMalformed
		}
		h = append(h, HeaderField{name, strings.TrimSpace(value)})
	}
	return msg, h, nil
}

func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c >= 0x7f || strings.IndexByte("()<>@,;:\\\"/[]?={}", c) >= 0 {
			return false
		}
	}
	return true
}
