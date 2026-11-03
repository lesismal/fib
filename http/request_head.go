package http

import (
	"bytes"
	stdhttp "net/http"
	"net/url"
	"strings"
	"sync"
	"unsafe"
)

// parseRequestHead parses the request line and header that head holds, up to
// and including the blank line that ends them, into a request whose Body is
// http.NoBody.
//
// Nearly every request is in a shape simple enough to parse here, into what
// net/http.ReadRequest would make of it, at a fraction of the cost: one string
// holds the whole header, and every key and value is a slice of it. Whatever
// is not in that shape — an unusual method, a request target that is not a
// plain origin-form path, a folded or otherwise irregular header line, a
// Transfer-Encoding other than a lone chunked on an HTTP/1.1 request with no
// Content-Length or Trailer beside it, a Content-Length that is not a single
// plain number — goes to net/http instead, so that it is accepted or refused
// exactly as net/http would, with net/http's own error.
//
// A request the simple parser takes comes with the block it was allocated in,
// which has room for the rest of what serving it takes.
func parseRequestHead(head []byte, reuse reuseOptions) (*stdhttp.Request, *requestBlock, error) {
	if block := parseSimpleRequestHead(head, reuse); block != nil {
		return &block.request, block, nil
	}
	req, _, err := readRequest(head, false)
	return req, nil, err
}

// requestBlock is a request together with everything else serving it needs
// an object for, which all live exactly as long as one another, in a single
// allocation rather than one each. Nothing in it is reused: a handler may
// keep any of it for as long as it likes, as it could keep them apart.
type requestBlock struct {
	request stdhttp.Request
	url     url.URL
	// values holds the header's values for a header with few enough fields,
	// as ordinary requests have once Host has moved out of it.
	values [4]string
	// transferEncoding is the request's TransferEncoding, for a chunked one.
	transferEncoding [1]string
	// fields are the header's lines as they arrived, for
	// Context.RequestHeaderBlock.
	fields string
	// body is the request's body when it arrived whole.
	body wholeBody
	// pooled records that the block came from blockPool, and headerFromPool
	// and urlFromPool are the request's Header and URL when they came from
	// pools of their own; see Config.ReuseRequests.
	pooled         bool
	headerFromPool *pooledHeader
	urlFromPool    *url.URL
	// keptHeader is the pooled Header a recycled block keeps from one
	// request to the next, when headers are recycled too, rather than
	// giving it back to headerPool and taking another.
	keptHeader *pooledHeader
	// blockServerState is what only a server serves the request with, which
	// is built only on the platforms that have one.
	blockServerState
}

// reuseOptions is which of a request's objects are recycled; see
// Config.ReuseRequests.
type reuseOptions struct{ requests, headers, urls bool }

// The pools the Reuse options recycle through. An object a request does not
// share a lifetime with — a URL that is recycled on a request that is not, or
// the other way round — cannot live in the request's block, and has a pool or
// an allocation of its own.
var (
	blockPool  = sync.Pool{New: func() any { return &requestBlock{pooled: true} }}
	headerPool = sync.Pool{New: func() any { return &pooledHeader{header: make(stdhttp.Header)} }}
	urlPool    = sync.Pool{New: func() any { return new(url.URL) }}
)

// pooledHeader is a recycled Header, with room for its values.
type pooledHeader struct {
	header stdhttp.Header
	values [8]string
}

// newRequestBlock takes the block a request is parsed into.
func newRequestBlock(reuse reuseOptions) *requestBlock {
	if reuse.requests {
		return blockPool.Get().(*requestBlock)
	}
	return new(requestBlock)
}

// recycle gives back what the request took from the pools, once its response
// is finished and the server has done with it, and reports whether the block
// itself went back, which takes with it whatever it holds.
func (b *requestBlock) recycle() bool {
	if h := b.headerFromPool; h != nil {
		b.headerFromPool = nil
		clear(h.header)
		clear(h.values[:])
		if b.pooled {
			b.keptHeader = h
		} else {
			headerPool.Put(h)
		}
	}
	if u := b.urlFromPool; u != nil {
		b.urlFromPool = nil
		*u = url.URL{}
		urlPool.Put(u)
	}
	if !b.pooled {
		return false
	}
	b.request = stdhttp.Request{}
	b.url = url.URL{}
	b.values = [len(b.values)]string{}
	b.transferEncoding = [1]string{}
	b.fields = ""
	b.body = wholeBody{}
	b.recycleServerState()
	blockPool.Put(b)
	return true
}

// headEnd is the blank line that ends a head, for bytes.Index to look for:
// a variable rather than a conversion at the call, which escapes, and
// costs an allocation, wherever the call is inlined into a caller, as
// profile-guided builds inline the parsers' calls.
var headEnd = []byte("\r\n\r\n")

// maxSimpleHeaderFields bounds the header parseSimpleRequestHead takes on,
// which is well past what ordinary requests carry. A longer one goes to
// net/http, which applies its own limits.
const maxSimpleHeaderFields = 64

// parseSimpleRequestHead parses head if it is in the shape parseRequestHead
// describes, and returns nil otherwise.
func parseSimpleRequestHead(head []byte, reuse reuseOptions) *requestBlock {
	lineEnd := bytes.IndexByte(head, '\n')
	if lineEnd < 1 || head[lineEnd-1] != '\r' {
		return nil
	}
	// The request line splits at its first two spaces, as net/http splits
	// it: the method, the target, and a protocol that must be all the rest.
	line := head[:lineEnd-1]
	methodEnd := bytes.IndexByte(line, ' ')
	if methodEnd <= 0 {
		return nil
	}
	method := simpleMethod(line[:methodEnd])
	if method == "" {
		return nil
	}
	targetStart := methodEnd + 1
	targetLen := bytes.IndexByte(line[targetStart:], ' ')
	if targetLen <= 0 {
		return nil
	}
	targetEnd := targetStart + targetLen
	var proto string
	var minor int
	switch string(line[targetEnd+1:]) {
	case "HTTP/1.1":
		proto, minor = "HTTP/1.1", 1
	case "HTTP/1.0":
		proto, minor = "HTTP/1.0", 0
	default:
		return nil
	}
	if !simpleTarget(line[targetStart:targetEnd]) {
		return nil
	}

	block := newRequestBlock(reuse)
	req := &block.request
	// One string for everything the request keeps of head: its target and
	// every field's key and value are slices of it.
	text := headString(head)
	target := text[targetStart:targetEnd]
	u := &block.url
	switch {
	case reuse.urls == reuse.requests:
		// The URL lives, and is recycled or not, with the block.
	case reuse.urls:
		u = urlPool.Get().(*url.URL)
		block.urlFromPool = u
	default:
		// The block is recycled and the URL is not, so a handler may keep
		// it past the request the block goes on to hold.
		u = new(url.URL)
	}
	splitTarget(u, target)

	// A map this small is allocated on its first insert whatever it is
	// sized for, so there is no count to take first.
	var header stdhttp.Header
	values := block.values[:]
	switch {
	case reuse.headers:
		pooled := block.keptHeader
		if pooled == nil {
			pooled = headerPool.Get().(*pooledHeader)
		}
		block.keptHeader = nil
		block.headerFromPool = pooled
		header, values = pooled.header, pooled.values[:]
	case reuse.requests:
		// A header that is not recycled cannot keep its values in a block
		// that is, since a handler may keep the header past the request.
		header, values = make(stdhttp.Header), nil
	default:
		header = make(stdhttp.Header)
	}
	// The fields net/http gives a meaning are noticed on the way past, so
	// that the map is not searched for each of them afterwards.
	var sawHost, sawLength, sawConnection, sawPragma, sawChunked, sawTrailer bool
	for at, fields := lineEnd+1, 0; ; fields++ {
		n := strings.IndexByte(text[at:], '\n')
		if n <= 0 || text[at+n-1] != '\r' || fields > maxSimpleHeaderFields {
			// A bare line feed, which net/http takes for a line end too,
			// with whatever that implies for the lines around it; or more
			// fields than are worth taking on here.
			return nil
		}
		fieldLine := text[at : at+n-1]
		at += n + 1
		if fieldLine == "" {
			block.fields = text[lineEnd+1 : at-2]
			break
		}
		key, value, ok := simpleField(fieldLine)
		if !ok {
			return nil
		}
		switch key {
		case "Host":
			// It moves to req.Host, as net/http.ReadRequest moves it, and a
			// second one is refused there, with net/http's own error.
			if sawHost {
				return nil
			}
			sawHost, req.Host = true, value
			continue
		case "Content-Length":
			if sawLength {
				return nil
			}
			if req.ContentLength, ok = simpleContentLength(value); !ok {
				return nil
			}
			sawLength = true
		case "Transfer-Encoding":
			// net/http takes one field naming chunked alone, drops it from
			// the header and records it in TransferEncoding, and refuses
			// anything else, which is left to it to refuse. HTTP/1.0 has no
			// transfer codings, and the parser refuses those itself.
			if sawChunked || minor == 0 || !strings.EqualFold(value, "chunked") {
				return nil
			}
			sawChunked = true
			continue
		case "Trailer":
			sawTrailer = true
		case "Connection":
			sawConnection = true
		case "Pragma":
			sawPragma = true
		}
		// The field is stored without looking for one of its name first,
		// which a header nearly never repeats: a map that did not grow had
		// one, which the store replaced, and the values are gathered again
		// from the lines before.
		size := len(header)
		var vv []string
		if len(values) == 0 {
			vv = []string{value}
		} else {
			vv = values[:1:1]
			values = values[1:]
			vv[0] = value
		}
		header[key] = vv
		if len(header) == size {
			header[key] = append(fieldValues(text[lineEnd+1:at-n-1], key), value)
		}
	}

	if sawChunked {
		if sawLength || sawTrailer {
			// Framed two ways, or announcing a trailer net/http would move
			// into Trailer: both are for net/http to settle.
			return nil
		}
		block.transferEncoding[0] = "chunked"
		req.TransferEncoding = block.transferEncoding[:]
		req.ContentLength = -1
	}
	req.Method = method
	req.URL = u
	req.Proto, req.ProtoMajor, req.ProtoMinor = proto, 1, minor
	req.Header = header
	req.Body = stdhttp.NoBody
	req.RequestURI = target
	// What net/http.ReadRequest does to every request it reads.
	if sawPragma {
		if pragma := header["Pragma"]; pragma[0] == "no-cache" {
			if _, ok := header["Cache-Control"]; !ok {
				header["Cache-Control"] = []string{"no-cache"}
			}
		}
	}
	var connection []string
	if sawConnection {
		connection = header["Connection"]
	}
	req.Close = headerValuesHaveToken(connection, "close")
	if minor == 0 && !req.Close {
		req.Close = !headerValuesHaveToken(connection, "keep-alive")
	}
	return block
}

// headChunkSize is the size of the chunks headString copies heads into, and
// maxChunkedHead the longest head it copies into one; a longer head is a
// string of its own.
const (
	headChunkSize  = 4096
	maxChunkedHead = 1024
)

// headChunk is the chunk headString is filling: buf's bytes up to its length
// are strings requests hold, and are never written again.
type headChunk struct{ buf []byte }

var headChunks = sync.Pool{New: func() any { return new(headChunk) }}

// headString returns a string holding head's bytes. Rather than allocating
// one per request, it copies head onto the end of a chunk shared by the
// requests parsed on the same P, so that a busy server allocates one chunk
// for dozens of requests. Every byte of a chunk is written once, before the
// string over it is made, so a string never changes under whoever holds it,
// whatever the request it came from goes on to: a chunk is collected as any
// object is, once no string points into it. What that costs is that a string
// kept past its request keeps its whole chunk with it, as a slice of the head
// keeps the whole head.
func headString(head []byte) string {
	if len(head) == 0 || len(head) > maxChunkedHead {
		return string(head)
	}
	chunk := headChunks.Get().(*headChunk)
	if cap(chunk.buf)-len(chunk.buf) < len(head) {
		chunk.buf = make([]byte, 0, headChunkSize)
	}
	start := len(chunk.buf)
	chunk.buf = append(chunk.buf, head...)
	text := unsafe.String(&chunk.buf[start], len(head))
	headChunks.Put(chunk)
	return text
}

// simpleMethod returns the method b names, as an interned string, when it is
// one of the ones requests ordinarily carry, and "" otherwise.
func simpleMethod(b []byte) string {
	switch string(b) {
	case stdhttp.MethodGet:
		return stdhttp.MethodGet
	case stdhttp.MethodPost:
		return stdhttp.MethodPost
	case stdhttp.MethodPut:
		return stdhttp.MethodPut
	case stdhttp.MethodHead:
		return stdhttp.MethodHead
	case stdhttp.MethodDelete:
		return stdhttp.MethodDelete
	case stdhttp.MethodPatch:
		return stdhttp.MethodPatch
	case stdhttp.MethodOptions:
		return stdhttp.MethodOptions
	}
	return ""
}

// splitTarget fills in u from a target simpleTarget approved, as
// url.ParseRequestURI splits one: a lone trailing '?' forces an empty query,
// and otherwise the query starts at the first.
func splitTarget(u *url.URL, target string) {
	if strings.HasSuffix(target, "?") && strings.Count(target, "?") == 1 {
		u.Path, u.ForceQuery = target[:len(target)-1], true
	} else {
		u.Path, u.RawQuery, _ = strings.Cut(target, "?")
	}
}

// simpleTarget reports whether a request target is an origin-form path that
// url.ParseRequestURI takes as it is: a path of characters it would not
// escape, so that Path is the path and RawPath is empty, with an optional
// query of any printable characters, which it keeps raw.
func simpleTarget[T string | []byte](target T) bool {
	if target[0] != '/' {
		return false
	}
	inQuery := false
	for i := 0; i < len(target); i++ {
		c := target[i]
		if inQuery {
			if c <= ' ' || c == 0x7f {
				return false
			}
			continue
		}
		if c == '?' {
			inQuery = true
			continue
		}
		if !pathByte[c] {
			return false
		}
	}
	return true
}

// pathByte marks the bytes url.URL leaves unescaped in a path: letters,
// digits, the unreserved marks, and the reserved characters other than '?'.
var pathByte = func() (table [256]bool) {
	for c := '0'; c <= '9'; c++ {
		table[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		table[c] = true
		table[c-'a'+'A'] = true
	}
	for _, c := range "-_.~$&+,/:;=@" {
		table[c] = true
	}
	return table
}()

// simpleField splits a header line into its canonical key and its value, when
// the key is a plain token directly followed by its colon and the value holds
// nothing net/textproto refuses. The key is a slice of line when it is
// already canonical, as it nearly always is, and the value always is.
func simpleField(line string) (key, value string, ok bool) {
	// The key is read up to the colon in the same loop that checks it,
	// which a key, a few bytes, costs less than a search for the colon
	// first: a colon is not a token byte, so the first one ends the key.
	upper, canonical := true, true
	colon := 0
	for ; colon < len(line); colon++ {
		c := line[colon]
		if c == ':' {
			break
		}
		if !tokenByte[c] {
			return "", "", false
		}
		if upper && 'a' <= c && c <= 'z' || !upper && 'A' <= c && c <= 'Z' {
			canonical = false
		}
		upper = c == '-'
	}
	if colon == 0 || colon == len(line) {
		return "", "", false
	}
	key = line[:colon]
	if !canonical {
		key = canonicalKey(key)
	}
	value = line[colon+1:]
	if !plainFieldValue(value) {
		return "", "", false
	}
	// A value nearly always has one space before it and none after.
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	if len(value) > 0 && (value[0] == ' ' || value[0] == '\t' ||
		value[len(value)-1] == ' ' || value[len(value)-1] == '\t') {
		value = strings.Trim(value, " \t")
	}
	return key, value, true
}

// fieldValues returns the values of the fields named key among lines, header
// lines each ended by CRLF that simpleField has taken already, in order.
func fieldValues(lines, key string) []string {
	var vv []string
	for lines != "" {
		n := strings.IndexByte(lines, '\n')
		k, v, _ := simpleField(lines[:n-1])
		if k == key {
			vv = append(vv, v)
		}
		lines = lines[n+1:]
	}
	return vv
}

// plainFieldValue reports whether v holds no control byte but a tab: nothing
// below a space but HTAB, and no DEL, as a field's value may hold. Values
// are most of a head's bytes, so it looks at eight at a time, and at each of
// the eight only when one of them is below a space or DEL, which a tab is
// too.
func plainFieldValue(v string) bool {
	const ones, highs = 0x0101010101010101, 0x8080808080808080
	i := 0
	for ; len(v)-i >= 8; i += 8 {
		b := v[i : i+8]
		w := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
			uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
		// A byte below 0x20, or one equal to 0x7f, sets its high bit in one
		// of these; a byte of 0x80 or more sets none.
		del := w ^ 0x7f*ones
		if (w-0x20*ones)&^w&highs != 0 || (del-ones)&^del&highs != 0 {
			break
		}
	}
	for ; i < len(v); i++ {
		if c := v[i]; c < ' ' && c != '\t' || c == 0x7f {
			return false
		}
	}
	return true
}

// canonicalKey is textproto.CanonicalMIMEHeaderKey for a key already known to
// be a token, which spares a string for the keys ordinary requests carry.
func canonicalKey(key string) string {
	var stack [64]byte
	b := stack[:0]
	if len(key) > len(stack) {
		b = make([]byte, 0, len(key))
	}
	upper := true
	for i := 0; i < len(key); i++ {
		c := key[i]
		if upper && 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		} else if !upper && 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		b = append(b, c)
		upper = c == '-'
	}
	if common, ok := commonHeaderKeys[string(b)]; ok {
		return common
	}
	return string(b)
}

// commonHeaderKeys interns the keys requests and, for the client, responses
// most often carry.
var commonHeaderKeys = func() map[string]string {
	keys := map[string]string{}
	for _, key := range []string{
		"Accept", "Accept-Charset", "Accept-Encoding", "Accept-Language", "Authorization",
		"Cache-Control", "Connection", "Content-Encoding", "Content-Length", "Content-Type",
		"Cookie", "Date", "Expect", "Forwarded", "Host", "If-Match", "If-Modified-Since",
		"If-None-Match", "If-Range", "If-Unmodified-Since", "Keep-Alive", "Origin", "Pragma",
		"Range", "Referer", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "User-Agent", "Via",
		"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-Ip", "X-Request-Id",
		"Accept-Ranges", "Access-Control-Allow-Credentials", "Access-Control-Allow-Headers",
		"Access-Control-Allow-Methods", "Access-Control-Allow-Origin", "Access-Control-Expose-Headers",
		"Access-Control-Max-Age", "Age", "Alt-Svc", "Content-Disposition", "Content-Language",
		"Content-Location", "Content-Range", "Content-Security-Policy", "Etag", "Expires",
		"Last-Modified", "Link", "Location", "Referrer-Policy", "Retry-After", "Server", "Set-Cookie",
		"Strict-Transport-Security", "Vary", "Www-Authenticate", "X-Content-Type-Options",
		"X-Frame-Options", "X-Xss-Protection",
	} {
		keys[key] = key
	}
	return keys
}()

// tokenByte marks the bytes a token may hold (RFC 9110 section 5.6.2), which
// is what net/textproto accepts in a header key.
var tokenByte = func() (table [256]bool) {
	for c := '0'; c <= '9'; c++ {
		table[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		table[c] = true
		table[c-'a'+'A'] = true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		table[c] = true
	}
	return table
}()

// simpleContentLength parses a Content-Length that is a plain decimal
// number, short enough that it cannot overflow.
func simpleContentLength(value string) (int64, bool) {
	if value == "" || len(value) > 18 {
		return 0, false
	}
	var n int64
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// headerValuesHaveToken reports whether any of values, each a comma-separated
// list, holds token, as golang.org/x/net/http/httpguts decides it for
// net/http: elements are trimmed of spaces and tabs and compared without
// regard to ASCII case, and only ASCII case.
func headerValuesHaveToken(values []string, token string) bool {
	for _, value := range values {
		for element := range strings.SplitSeq(value, ",") {
			if asciiEqualFold(strings.Trim(element, " \t"), token) {
				return true
			}
		}
	}
	return false
}

// asciiEqualFold reports whether a and b are equal but for ASCII case, and
// hold nothing but ASCII.
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if x >= 0x80 || y >= 0x80 {
			return false
		}
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
