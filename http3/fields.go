//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/internal/strtab"
)

// HTTP/3 carries fields as HTTP/2 does (RFC 9114 section 4.2): names
// lowercase, pseudo-headers first, and no connection-specific fields.

// connectionHeader reports whether name is one of the HTTP/1 hop-by-hop
// fields, which HTTP/3 forbids.
func connectionHeader(name string) bool {
	switch name {
	case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
		return true
	}
	return false
}

// tokenByte and hostByte are the bytes of a token (RFC 9110 section 5.6.2),
// as :method and :protocol are, and those an authority's host and port may
// hold (RFC 3986 section 3.2.2): unreserved, sub-delims, percent-encoding,
// an IP literal's brackets and the port's colon, but no userinfo's '@'.
var tokenByte, hostByte = func() (token, host [256]bool) {
	for c := '0'; c <= '9'; c++ {
		token[c], host[c] = true, true
	}
	for c := 'a'; c <= 'z'; c++ {
		token[c], host[c] = true, true
		token[c-'a'+'A'], host[c-'a'+'A'] = true, true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		token[c] = true
	}
	for _, c := range "-._~!$&'()*+,;=%:[]" {
		host[c] = true
	}
	return
}()

// validToken reports whether s is a token, as a method is.
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

// validScheme reports whether s has a URI scheme's syntax (RFC 3986 section
// 3.1).
func validScheme(s string) bool {
	if s == "" || !(s[0] >= 'a' && s[0] <= 'z' || s[0] >= 'A' && s[0] <= 'Z') {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

// validAuthority reports whether s, an :authority or the Host standing in
// for it, holds only what a host and port may: no userinfo, which HTTP/3
// forbids there (RFC 9114 section 4.3.1), and nothing a handler writing it
// into an HTTP/1 message or a log could have taken for more than a host.
func validAuthority(s string) bool {
	for i := 0; i < len(s); i++ {
		if !hostByte[s[i]] {
			return false
		}
	}
	return true
}

// validPath reports whether s may be a request's :path: an origin-form
// target, starting with '/' and holding no space or control character, or
// '*' for OPTIONS alone (RFC 9114 section 4.3.1).
func validPath(s, method string) bool {
	if s == "*" {
		return method == stdhttp.MethodOptions
	}
	if s == "" || s[0] != '/' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c <= ' ' || c == 0x7f {
			return false
		}
	}
	return true
}

// validName reports whether name is a valid lowercase field name.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'A' && c <= 'Z' || c <= ' ' || c >= 0x7f || c == ':' && i > 0 {
			return false
		}
	}
	return true
}

// validValue reports whether value may appear in a field, which rules out
// NUL, CR, LF and leading or trailing whitespace.
func validValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; c == 0 || c == '\r' || c == '\n' {
			return false
		}
	}
	n := len(value)
	return n == 0 || value[0] != ' ' && value[0] != '\t' && value[n-1] != ' ' && value[n-1] != '\t'
}

// sendableValue is what this side lets through from a handler or caller:
// the whitespace rules are the peer's to enforce.
func sendableValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; c < ' ' && c != '\t' || c == 0x7f {
			return false
		}
	}
	return true
}

func sensitive(name string) bool {
	return name == "authorization" || name == "cookie" || name == "set-cookie" || name == "proxy-authorization"
}

// checkHeader checks the fields of a message about to be sent, before any
// is encoded.
func checkHeader(header stdhttp.Header) error {
	_, err := checkHeaderSize(header, nil, nil)
	return err
}

// isContentLength is the skip of the fields a response with a length of its
// own leaves out.
func isContentLength(name string) bool { return name == "content-length" }

// checkHeaderSize is checkHeader that also returns the weight of the fields
// appendHeader sends of header, as headerSize does, in the same pass over
// it, and lists them in list, if it is not nil, for appendHeaderList.
func checkHeaderSize(header stdhttp.Header, skip func(string) bool, list *headerList) (uint64, error) {
	var n uint64
	for key, values := range header {
		name := lowerKey(key)
		if key == "" || !validName(name) {
			return 0, errors.New("http3: invalid header name " + strconv.Quote(key))
		}
		for _, value := range values {
			if !sendableValue(value) {
				return 0, errors.New("http3: invalid header value for " + key)
			}
		}
		if connectionHeader(name) || skip != nil && skip(name) {
			continue
		}
		for _, value := range values {
			n += fieldSize(name, value)
		}
		if list != nil {
			if list.n < len(list.fields) {
				list.fields[list.n] = listedField{name, values}
				list.n++
			} else {
				list.more = true
			}
		}
	}
	return n, nil
}

// headerList is what checkHeaderSize lists of a header for appendHeaderList
// to encode, so that a response's header is gone over, and its keys
// lowercased, once rather than twice: the names and values of the fields
// sent, as far as there is room for them, and whether there were more.
type headerList struct {
	n      int
	more   bool
	fields [12]listedField
}

type listedField struct {
	name   string
	values []string
}

// appendHeaderList is appendHeader of the header list was listed from, with
// the same skip, which it takes from list when list has all of it.
func appendHeaderList(sec *qpack.Section, block []byte, header stdhttp.Header, skip func(string) bool, list *headerList) []byte {
	if list.more {
		return appendHeader(sec, block, header, skip)
	}
	for _, f := range list.fields[:list.n] {
		for _, value := range f.values {
			block = sec.Append(block, f.name, value, sensitive(f.name))
		}
	}
	return block
}

// appendHeader encodes header's fields in sec, leaving out what HTTP/3 does
// not carry and what skip names.
func appendHeader(sec *qpack.Section, block []byte, header stdhttp.Header, skip func(string) bool) []byte {
	for key, values := range header {
		name := lowerKey(key)
		if connectionHeader(name) || skip != nil && skip(name) {
			continue
		}
		for _, value := range values {
			block = sec.Append(block, name, value, sensitive(name))
		}
	}
	return block
}

// beginFields starts the field section of streamID in a buffer from the
// pool, encoded with enc, which may be nil.
func beginFields(enc *qpack.Encoder, streamID uint64) (qpack.Section, []byte) {
	return enc.Begin(bufferpool.Grow(nil, len(qpack.Prefix)), streamID)
}

// appendFields encodes fields in a section of its own for streamID, in a
// buffer from the pool.
func appendFields(enc *qpack.Encoder, fields []qpack.HeaderField, streamID uint64) []byte {
	sec, block := beginFields(enc, streamID)
	for _, f := range fields {
		block = sec.Append(block, f.Name, f.Value, sensitive(f.Name))
	}
	return sec.End(block)
}

// commonKeys are the header keys messages nearly always carry, whose
// lowercase names lowerKeys looks up rather than builds, and canonicalKeys
// the other way.
var commonKeys = []string{
	"Accept", "Accept-Encoding", "Accept-Language", "Accept-Ranges",
	"Access-Control-Allow-Headers", "Access-Control-Allow-Methods", "Access-Control-Allow-Origin",
	"Age", "Allow", "Authorization", "Cache-Control", "Content-Disposition", "Content-Encoding",
	"Content-Language", "Content-Length", "Content-Location", "Content-Range", "Content-Type",
	"Cookie", "Date", "Etag", "Expires", "Last-Modified", "Link", "Location", "Origin",
	"Referer", "Retry-After", "Server", "Set-Cookie", "Strict-Transport-Security",
	"Trailer", "User-Agent", "Vary", "Via", "Www-Authenticate", "X-Content-Type-Options",
	"X-Forwarded-For", "X-Frame-Options", "X-Request-Id",
}

var lowerKeys, canonicalKeys = func() (*strtab.Table[string], *strtab.Table[string]) {
	names := make([]string, len(commonKeys))
	for i, key := range commonKeys {
		names[i] = strings.ToLower(key)
	}
	return strtab.New(commonKeys, names), strtab.New(names, commonKeys)
}()

// canonicalKey is textproto.CanonicalMIMEHeaderKey of a lowercase field name.
func canonicalKey(name string) string {
	if key, ok := canonicalKeys.Get(name); ok {
		return key
	}
	return textproto.CanonicalMIMEHeaderKey(name)
}

// lowerKey is a header key in lower case, as HTTP/3 sends it, without the
// allocation strings.ToLower makes of a canonical key for the keys messages
// nearly always carry.
func lowerKey(key string) string {
	if name, ok := lowerKeys.Get(key); ok {
		return name
	}
	return strings.ToLower(key)
}

// appendHeadersFrame appends a HEADERS frame holding block.
func appendHeadersFrame(b, block []byte) []byte {
	b = appendFrameHeader(b, frameHeaders, len(block))
	return append(b, block...)
}

// newRequest builds a request from its fields, rejecting what RFC 9114
// section 4.3.1 calls malformed. The request and its URL are block's, and
// the header's values, as far as they go, are slices of values; block may be
// nil, for a request of its own.
func newRequest(fields []qpack.HeaderField, block *fibhttp.StreamRequest, header stdhttp.Header, values []string) (*stdhttp.Request, error) {
	var method, scheme, authority, path, protocol string
	var seen [5]bool
	if header == nil {
		header = make(stdhttp.Header, len(fields))
	}
	var cookies []string
	regular := false
	host, hosts := "", 0
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			if regular {
				return nil, errors.New("pseudo-header after regular header")
			}
			var slot int
			switch f.Name {
			case ":method":
				slot, method = 0, f.Value
			case ":scheme":
				slot, scheme = 1, f.Value
			case ":authority":
				slot, authority = 2, f.Value
			case ":path":
				slot, path = 3, f.Value
			case ":protocol":
				// Extended CONNECT (RFC 9220), which the server allows.
				slot, protocol = 4, f.Value
			default:
				return nil, fmt.Errorf("unknown pseudo-header %q", f.Name)
			}
			if seen[slot] {
				return nil, fmt.Errorf("duplicate %s", f.Name)
			}
			seen[slot] = true
			continue
		}
		regular = true
		if !validName(f.Name) || !validValue(f.Value) || connectionHeader(f.Name) {
			return nil, fmt.Errorf("invalid header %q", f.Name)
		}
		if f.Name == "te" && f.Value != "trailers" {
			return nil, errors.New("TE other than trailers")
		}
		if f.Name == "cookie" {
			cookies = append(cookies, f.Value)
			continue
		}
		if f.Name == "host" {
			// Kept out of the header, as net/http keeps it, and so never
			// looked up there.
			if hosts++; hosts == 1 {
				host = f.Value
			}
			continue
		}
		key := canonicalKey(f.Name)
		if existing, ok := header[key]; ok || len(values) == 0 {
			header[key] = append(existing, f.Value)
			continue
		}
		// Capped, so that a handler appending to it gets a copy rather
		// than writing over the next key's value.
		header[key] = values[:1:1]
		values[0] = f.Value
		values = values[1:]
	}
	if len(cookies) > 0 {
		header["Cookie"] = []string{strings.Join(cookies, "; ")}
	}
	if method == "" {
		return nil, errors.New("missing :method")
	}
	// What the pseudo-headers hold reaches the handler as a request line's
	// parts would, and on to whatever it writes them into: each has to have
	// its own syntax, never a line break, a space or a NUL that could be
	// taken for more (RFC 9114 section 4.3.1).
	switch {
	case !validToken(method):
		return nil, errors.New("malformed :method")
	case seen[4] && !validToken(protocol):
		return nil, errors.New("malformed :protocol")
	case seen[1] && !validScheme(scheme):
		return nil, errors.New("malformed :scheme")
	case !validAuthority(authority) || !validAuthority(host):
		return nil, errors.New("malformed :authority or Host")
	case path != "" && !validPath(path, method):
		return nil, errors.New("malformed :path")
	}
	if block == nil {
		block = new(fibhttp.StreamRequest)
	}
	req := &block.Request
	*req = stdhttp.Request{
		Method:     method,
		Proto:      "HTTP/3.0",
		ProtoMajor: 3,
		Header:     header,
		Body:       stdhttp.NoBody,
	}
	u := &block.URL
	if seen[4] {
		// An extended CONNECT names the resource it opens a tunnel to as
		// any request does (RFC 8441 section 4), and reports the protocol
		// in the header, as package http does for HTTP/2.
		if method != stdhttp.MethodConnect || protocol == "" || scheme == "" || path == "" || authority == "" {
			return nil, errors.New("malformed extended CONNECT")
		}
		if err := fibhttp.ParseRequestTarget(u, path); err != nil {
			return nil, err
		}
		req.RequestURI = path
		header[":protocol"] = []string{protocol}
	} else if method == stdhttp.MethodConnect {
		if scheme != "" || path != "" || authority == "" {
			return nil, errors.New("malformed CONNECT")
		}
		*u = url.URL{Host: authority}
		req.RequestURI = authority
	} else {
		if scheme == "" || path == "" {
			return nil, errors.New("missing :scheme or :path")
		}
		if path == "*" {
			*u = url.URL{Path: "*"}
		} else if err := fibhttp.ParseRequestTarget(u, path); err != nil {
			return nil, err
		}
		req.RequestURI = path
	}
	req.URL = u
	// A request for a scheme with a mandatory authority has to name it,
	// once, either way and with one value (RFC 9114 section 4.3.1).
	if hosts > 1 {
		return nil, errors.New("multiple Host fields")
	}
	switch {
	case authority != "" && host != "" && !strings.EqualFold(authority, host):
		return nil, errors.New(":authority and Host disagree")
	case authority == "" && host == "" && (scheme == "http" || scheme == "https"):
		return nil, errors.New("neither :authority nor Host")
	}
	req.Host = authority
	if req.Host == "" {
		req.Host = host
	}
	return req, nil
}

// headerValue is header.Get for a key already in canonical form, which it
// does not canonicalize again.
func headerValue(header stdhttp.Header, key string) string {
	if values := header[key]; len(values) > 0 {
		return values[0]
	}
	return ""
}

// trailerFrom builds a trailer section, which may hold no pseudo-headers.
func trailerFrom(fields []qpack.HeaderField) (stdhttp.Header, error) {
	trailer := make(stdhttp.Header, len(fields))
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") || !validName(f.Name) || !validValue(f.Value) || connectionHeader(f.Name) {
			return nil, fmt.Errorf("invalid trailer %q", f.Name)
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		trailer[key] = append(trailer[key], f.Value)
	}
	return trailer, nil
}

// isContinue reports whether an interim response's fields are a
// 100 (Continue)'s.
func isContinue(fields []qpack.HeaderField) bool {
	for _, f := range fields {
		if f.Name == ":status" {
			return f.Value == "100"
		}
	}
	return false
}

// newResponse builds a response from its fields. It returns nil for an
// interim response.
func newResponse(fields []qpack.HeaderField, req *stdhttp.Request) (*stdhttp.Response, error) {
	status := ""
	header := make(stdhttp.Header, len(fields))
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			if f.Name != ":status" || status != "" || len(header) > 0 {
				return nil, errors.New("malformed pseudo-header")
			}
			status = f.Value
			continue
		}
		if !validName(f.Name) || connectionHeader(f.Name) {
			return nil, fmt.Errorf("invalid header %q", f.Name)
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		header[key] = append(header[key], f.Value)
	}
	code, err := strconv.Atoi(status)
	if err != nil || len(status) != 3 || code < 100 {
		return nil, errors.New("malformed :status")
	}
	if code < 200 {
		if code == stdhttp.StatusSwitchingProtocols {
			return nil, errors.New("101 over HTTP/3")
		}
		return nil, nil
	}
	resp := &stdhttp.Response{
		Status:        status + " " + stdhttp.StatusText(code),
		StatusCode:    code,
		Proto:         "HTTP/3.0",
		ProtoMajor:    3,
		Header:        header,
		ContentLength: -1,
		Request:       req,
		Body:          stdhttp.NoBody,
	}
	if cl := headerValue(header, "Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			return nil, errors.New("malformed content-length")
		}
		resp.ContentLength = n
	}
	return resp, nil
}

// bodyAllowed reports whether a response to req with status has a body.
func bodyAllowed(req *stdhttp.Request, status int) bool {
	return req.Method != stdhttp.MethodHead && status != stdhttp.StatusNoContent &&
		status != stdhttp.StatusNotModified && status >= 200
}

// responseLength is the content-length reported for response to req: the
// length of its body, except that a response to HEAD, which sends no body,
// reports the one its header declares when it declares one, as HTTP/1 does.
func responseLength(req *stdhttp.Request, response fibhttp.Response) int64 {
	if req.Method == stdhttp.MethodHead {
		if values := response.Header["Content-Length"]; len(values) == 1 {
			if n, err := strconv.ParseInt(strings.TrimSpace(values[0]), 10, 64); err == nil && n >= 0 {
				return n
			}
		}
	}
	return int64(len(response.Body))
}

// validTrailer reports whether a response may send name, in lower case, as
// a trailer: a field HTTP/3 carries, with sendable values, that does not
// frame, route or authenticate the message (RFC 9110 section 6.5.1).
func validTrailer(name string, values []string) bool {
	switch name {
	case "content-length", "transfer-encoding", "trailer", "host", "content-type", "content-encoding",
		"content-range", "cache-control", "expect", "max-forwards", "pragma", "range", "te",
		"authorization", "set-cookie":
		return false
	}
	if name == "" || connectionHeader(name) || !validName(name) {
		return false
	}
	for _, value := range values {
		if !sendableValue(value) {
			return false
		}
	}
	return true
}
