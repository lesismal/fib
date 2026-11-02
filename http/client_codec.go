//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdhttp "net/http"
	"net/http/httptrace"
	"slices"
	"strconv"
	"strings"

	"github.com/lesismal/fib/bufferpool"
)

// The client writes request heads and reads response heads itself, as the
// server reads requests and writes responses, rather than through
// Request.Write and ReadResponse: those cost a proxied request more than the
// rest of the client together, ReadResponse a 4 KiB reader and a dozen
// allocations for a head of a hundred bytes. Each fast path below produces
// exactly what net/http would, for the requests and responses most traffic
// is made of, and reports false for anything else, which then goes through
// net/http as before: a Close or a trace on the request, a host that needs
// cleaning, a header folded over lines, a field net/http would refuse. The
// tests hold both to net/http's output.

// appendRequestStart appends the request line and the Host and User-Agent
// fields of req as Request.Write writes them, as HTTP/1.0 when http10 is set.
// It reports false for a request it leaves to net/http.
func appendRequestStart(dst []byte, req *stdhttp.Request, http10 bool) ([]byte, bool) {
	if req.URL == nil || req.Close || req.Method == stdhttp.MethodConnect ||
		httptrace.ContextClientTrace(req.Context()) != nil {
		return dst, false
	}
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	if !plainHost(host) {
		return dst, false
	}
	uri := req.URL.RequestURI()
	for i := 0; i < len(uri); i++ {
		if c := uri[i]; c < ' ' || c == 0x7f {
			return dst, false
		}
	}
	method := req.Method
	if method == "" {
		method = stdhttp.MethodGet
	} else if !validToken(method) {
		return dst, false
	}
	dst = append(dst, method...)
	dst = append(dst, ' ')
	dst = append(dst, uri...)
	if http10 {
		dst = append(dst, " HTTP/1.0\r\nHost: "...)
	} else {
		dst = append(dst, " HTTP/1.1\r\nHost: "...)
	}
	dst = append(dst, host...)
	dst = append(dst, "\r\n"...)

	// The default unless the header has one, which may be empty to send none.
	userAgent := "Go-http-client/1.1"
	if values, ok := req.Header["User-Agent"]; ok {
		userAgent = ""
		if len(values) > 0 {
			userAgent = values[0]
		}
	}
	if userAgent != "" {
		dst = append(dst, "User-Agent: "...)
		dst = appendFieldValue(dst, userAgent)
		dst = append(dst, "\r\n"...)
	}
	return dst, true
}

// plainHost reports whether host is written as it is by Request.Write: ASCII
// that is valid in a Host field, with no IPv6 zone to remove. Others are left
// to net/http.
func plainHost(host string) bool {
	for i := 0; i < len(host); i++ {
		c := host[i]
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' {
			continue
		}
		switch c {
		case '.', '-', ':', '[', ']', '_':
			continue
		}
		return false
	}
	return true
}

// requestStartExcluded are the fields Request.Write writes itself, or not at
// all, rather than from the header.
func requestStartExcluded(key string) bool {
	switch key {
	case "Host", "User-Agent", "Content-Length", "Transfer-Encoding", "Trailer":
		return true
	}
	return false
}

// headerField is a field of a header with its values, as a header is
// gathered to be written in order of name.
type headerField struct {
	key    string
	values []string
	// prefix is the key's first eight bytes, big-endian and padded with
	// zeros, which orders two keys as the keys do wherever they differ.
	prefix uint64
}

// newHeaderField returns key's field, with its prefix.
func newHeaderField(key string, values []string) headerField {
	var prefix uint64
	for i := range 8 {
		prefix <<= 8
		if i < len(key) {
			prefix |= uint64(key[i])
		}
	}
	return headerField{key: key, values: values, prefix: prefix}
}

// sortHeaderFields orders fields by name. A header has a dozen fields or so,
// which an insertion sort orders with the fewest moves and no calls, telling
// most names apart by their prefixes alone; a longer one is left to slices.
func sortHeaderFields(fields []headerField) {
	if len(fields) > 16 {
		slices.SortFunc(fields, func(a, b headerField) int { return strings.Compare(a.key, b.key) })
		return
	}
	for i := 1; i < len(fields); i++ {
		f := fields[i]
		j := i
		for ; j > 0; j-- {
			prior := &fields[j-1]
			if prior.prefix < f.prefix || prior.prefix == f.prefix && prior.key <= f.key {
				break
			}
			fields[j] = *prior
		}
		fields[j] = f
	}
}

// collectRequestFields gathers h's fields, but those Request.Write writes
// itself, in one pass over the map, which also sums what the whole header
// takes framed, those fields included: the fields come with their values, so
// that writing them looks nothing up in the map again. They go in stack
// while they fit, sorted by name as net/http writes them.
func collectRequestFields(h stdhttp.Header, stack *[16]headerField) ([]headerField, int) {
	fields := stack[:0]
	size := 0
	for key, values := range h {
		for _, v := range values {
			size += len(key) + len(v) + 4
		}
		if !requestStartExcluded(key) {
			fields = append(fields, newHeaderField(key, values))
		}
	}
	sortHeaderFields(fields)
	return fields, size
}

// appendCollectedFields appends fields, as collectRequestFields gathered
// them, each value on a line of its own; a field whose name is not a token is
// left out, as net/http leaves it.
func appendCollectedFields(dst []byte, fields []headerField) []byte {
	for _, f := range fields {
		if !validToken(f.key) {
			continue
		}
		for _, v := range f.values {
			dst = append(dst, f.key...)
			dst = append(dst, ": "...)
			dst = appendFieldValue(dst, v)
			dst = append(dst, "\r\n"...)
		}
	}
	return dst
}

// appendFieldValue appends v as net/http writes a field's value: with CR and
// LF made spaces, and without the whitespace around it.
func appendFieldValue(dst []byte, v string) []byte {
	start, end := 0, len(v)
	for start < end && asciiSpace(v[start]) {
		start++
	}
	for end > start && asciiSpace(v[end-1]) {
		end--
	}
	v = v[start:end]
	if strings.IndexByte(v, '\r') < 0 && strings.IndexByte(v, '\n') < 0 {
		return append(dst, v...)
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		dst = append(dst, c)
	}
	return dst
}

func asciiSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

// validToken reports whether s is an HTTP token, as a field name must be.
func validToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !tokenByte[s[i]] {
			return false
		}
	}
	return true
}

// fastRequest frames req, whose body has been read into body, as
// marshalRequest does, in a buffer from package bufferpool, or reports false
// for a request it leaves to net/http.
func fastRequest(req *stdhttp.Request, body []byte, http10 bool) ([]byte, bool) {
	hasBody := req.Body != nil && req.Body != stdhttp.NoBody
	if len(req.TransferEncoding) != 0 || len(req.Trailer) != 0 {
		return nil, false
	}
	// Whether a Content-Length is sent, as net/http's transfer writer decides
	// it for the cases taken here: a body of the length declared, or none.
	length := -1
	switch {
	case !hasBody || http10 && len(body) == 0:
		if req.ContentLength != 0 && !http10 {
			return nil, false // net/http refuses a length with no body
		}
		if req.Method == stdhttp.MethodPost || req.Method == stdhttp.MethodPut || req.Method == stdhttp.MethodPatch {
			length = 0
		}
		body = nil
	case http10 || req.ContentLength == int64(len(body)) && len(body) > 0:
		length = len(body)
	default:
		return nil, false
	}
	var stack [16]headerField
	fields, size := collectRequestFields(req.Header, &stack)
	size += 64 + len(body)
	// The framing is the pool's, and goes back to it once the response has
	// arrived (clientRequest.releaseData).
	buf := bufferpool.Get(size)
	dst, ok := appendRequestStart(buf[:0], req, http10)
	if !ok {
		bufferpool.Put(buf)
		return nil, false
	}
	if length >= 0 {
		dst = append(dst, "Content-Length: "...)
		dst = strconv.AppendInt(dst, int64(length), 10)
		dst = append(dst, "\r\n"...)
	}
	dst = appendCollectedFields(dst, fields)
	dst = append(dst, "\r\n"...)
	return append(dst, body...), true
}

// fastStreamedHead is the start of marshalStreamedHead's head, without its
// framing and the blank line, or false for a request left to net/http.
func fastStreamedHead(req *stdhttp.Request) ([]byte, bool) {
	var stack [16]headerField
	fields, size := collectRequestFields(req.Header, &stack)
	dst, ok := appendRequestStart(make([]byte, 0, 64+size), req, false)
	if !ok {
		return nil, false
	}
	return appendCollectedFields(dst, fields), true
}

// decodeChunks appends the body that src, a whole chunked body, carries to
// dst, or reports false for one with a trailer or anything net/http's chunked
// reader refuses: a line not ended by CRLF alone or longer than its 4 KiB
// buffer, a chunk size that is not 1 to 16 hex digits after trailing
// whitespace and an extension are cut, data not followed by CRLF, or more
// overhead than it allows.
func decodeChunks(dst, src []byte) ([]byte, bool) {
	var excess int64
	for {
		at := bytes.IndexByte(src, '\n')
		if at < 1 || at+1 > 4096 || src[at-1] != '\r' || bytes.IndexByte(src[:at-1], '\r') >= 0 {
			return dst, false
		}
		line := src[:at-1]
		src = src[at+1:]
		excess += int64(len(line)) + 2
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t') {
			line = line[:len(line)-1]
		}
		for i, c := range line {
			if c == ';' {
				line = line[:i]
				break
			}
		}
		if len(line) == 0 || len(line) > 16 {
			return dst, false
		}
		var n uint64
		for _, c := range line {
			switch {
			case '0' <= c && c <= '9':
				c -= '0'
			case 'a' <= c && c <= 'f':
				c -= 'a' - 10
			case 'A' <= c && c <= 'F':
				c -= 'A' - 10
			default:
				return dst, false
			}
			n = n<<4 | uint64(c)
		}
		excess = max(excess-16-2*int64(n), 0)
		if excess > 16*1024 {
			return dst, false
		}
		if n == 0 {
			// No trailer: the blank line alone, and the end of the body.
			return dst, len(src) == 2 && src[0] == '\r' && src[1] == '\n'
		}
		if uint64(len(src)) < n+2 || src[n] != '\r' || src[n+1] != '\n' {
			return dst, false
		}
		dst = append(dst, src[:n]...)
		src = src[n+2:]
	}
}

// The fields parseResponseHead reads once the head is parsed, as it notes
// them in passing.
const (
	sawPragma uint8 = 1 << iota
	sawTransferEncoding
	sawConnection
	sawContentLength
	sawTrailer
)

// parseResponseHead reads a response head, head ending with its blank line,
// into what ReadResponse returns for it, with Body left nil, or reports false
// for one it leaves to ReadResponse: anything but HTTP/1.1 and HTTP/1.0 with
// a three-digit status, a field folded over lines or with a byte net/http
// refuses, more than one Content-Length or Transfer-Encoding, one it cannot
// read, an announced Trailer on a chunked body, or a Transfer-Encoding on
// HTTP/1.0.
func parseResponseHead(head []byte, req *stdhttp.Request) (*stdhttp.Response, bool) {
	if len(head) < 14 || head[0] != 'H' || string(head[:7]) != "HTTP/1." || head[8] != ' ' ||
		head[7] != '0' && head[7] != '1' {
		return nil, false
	}
	// One string for the whole head: the status, the field names that are
	// not interned and the values are pieces of it.
	s := string(head[:len(head)-2])
	// textproto ends a line at a bare LF too; such a head is left to it. Every
	// LF found here has its CR, so a line ends at its LF below.
	n := 0
	for at := 0; ; {
		i := strings.IndexByte(s[at:], '\n')
		if i < 0 {
			break
		}
		at += i
		if s[at-1] != '\r' {
			return nil, false
		}
		at++
		n++
	}
	lineEnd := strings.IndexByte(s, '\n') - 1
	if lineEnd < 0 {
		return nil, false
	}
	status := s[9:lineEnd]
	if len(status) < 3 || len(status) > 3 && status[3] != ' ' {
		return nil, false
	}
	for i := 0; i < len(status); i++ {
		if c := status[i]; c < ' ' && c != '\t' || c == 0x7f {
			return nil, false
		}
	}
	code := 0
	for i := range 3 {
		c := status[i]
		if c < '0' || c > '9' {
			return nil, false
		}
		code = code*10 + int(c-'0')
	}
	resp := &stdhttp.Response{
		Status:     status,
		StatusCode: code,
		Proto:      s[:8],
		ProtoMajor: 1,
		ProtoMinor: int(head[7] - '0'),
		Request:    req,
		Body:       stdhttp.NoBody,
	}
	fields := s[lineEnd+2:]
	n--
	header := make(stdhttp.Header, n)
	values := make([]string, n)
	// The fields read after the loop, noted as they pass so that one a
	// response does not have costs no lookup.
	var special uint8
	for fields != "" {
		end := strings.IndexByte(fields, '\n') - 1
		if end < 0 {
			return nil, false
		}
		line := fields[:end]
		fields = fields[end+2:]
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			return nil, false
		}
		key := line[:colon]
		upper, canonical := true, true
		for i := 0; i < len(key); i++ {
			c := key[i]
			if !tokenByte[c] {
				return nil, false
			}
			if upper && 'a' <= c && c <= 'z' || !upper && 'A' <= c && c <= 'Z' {
				canonical = false
			}
			upper = c == '-'
		}
		if !canonical {
			key = canonicalKey(key)
		}
		switch key {
		case "Pragma":
			special |= sawPragma
		case "Transfer-Encoding":
			special |= sawTransferEncoding
		case "Connection":
			special |= sawConnection
		case "Content-Length":
			special |= sawContentLength
		case "Trailer":
			special |= sawTrailer
		}
		v := line[colon+1:]
		start, stop := 0, len(v)
		for start < stop && (v[start] == ' ' || v[start] == '\t') {
			start++
		}
		for stop > start && (v[stop-1] == ' ' || v[stop-1] == '\t') {
			stop--
		}
		v = v[start:stop]
		if !plainFieldValue(v) {
			return nil, false
		}
		if prior := header[key]; prior != nil {
			header[key] = append(prior, v)
		} else {
			values[0] = v
			header[key] = values[:1:1]
			values = values[1:]
		}
	}
	resp.Header = header

	if special&sawPragma != 0 {
		if hp := header["Pragma"]; len(hp) > 0 && hp[0] == "no-cache" {
			if _, ok := header["Cache-Control"]; !ok {
				header["Cache-Control"] = []string{"no-cache"}
			}
		}
	}

	chunked := false
	if special&sawTransferEncoding != 0 {
		te := header["Transfer-Encoding"]
		if resp.ProtoMinor == 0 || len(te) != 1 || !asciiEqualFold(te[0], "chunked") {
			return nil, false
		}
		delete(header, "Transfer-Encoding")
		chunked = true
	}
	var connection []string
	if special&sawConnection != 0 {
		connection = header["Connection"]
	}
	hasClose := headerValuesHaveToken(connection, "close")
	if resp.ProtoMinor == 0 {
		resp.Close = hasClose || !headerValuesHaveToken(connection, "keep-alive")
	} else {
		resp.Close = hasClose
		if hasClose {
			delete(header, "Connection")
		}
	}

	declared := int64(-1)
	if special&sawContentLength != 0 {
		cl := header["Content-Length"]
		if len(cl) != 1 {
			return nil, false
		}
		v := strings.Trim(cl[0], " \t\r\n")
		n, err := strconv.ParseUint(v, 10, 63)
		if err != nil {
			return nil, false
		}
		declared = int64(n)
	}
	head1 := req != nil && req.Method == stdhttp.MethodHead
	var length int64
	switch {
	case head1 || code/100 == 1 || code == 204 || code == 304:
		length = 0
	case chunked:
		delete(header, "Content-Length")
		length = -1
	default:
		length = declared
	}
	resp.ContentLength = length
	if head1 {
		resp.ContentLength = declared
	}
	if special&sawTrailer != 0 && chunked {
		return nil, false
	}
	if chunked {
		resp.TransferEncoding = []string{"chunked"}
	}
	if length == -1 && !chunked && code/100 != 1 && code != 204 && code != 304 {
		resp.Close = true
	}
	return resp, true
}
