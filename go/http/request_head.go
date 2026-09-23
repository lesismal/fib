package http

import (
	"bytes"
	stdhttp "net/http"
	"net/url"
	"strings"
	"sync"
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
// Transfer-Encoding, a Content-Length that is not a single plain number — goes
// to net/http instead, so that it is accepted or refused exactly as net/http
// would, with net/http's own error.
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
	// body is the request's body when it arrived whole.
	body wholeBody
	// pooled records that the block came from blockPool, and headerFromPool
	// and urlFromPool are the request's Header and URL when they came from
	// pools of their own; see Config.ReuseRequests.
	pooled         bool
	headerFromPool *pooledHeader
	urlFromPool    *url.URL
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
		headerPool.Put(h)
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
	b.body = wholeBody{}
	b.recycleServerState()
	blockPool.Put(b)
	return true
}

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
	text := string(head)
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
	// As url.ParseRequestURI splits a request target: a lone trailing '?'
	// forces an empty query, and otherwise the query starts at the first.
	if strings.HasSuffix(target, "?") && strings.Count(target, "?") == 1 {
		u.Path, u.ForceQuery = target[:len(target)-1], true
	} else {
		u.Path, u.RawQuery, _ = strings.Cut(target, "?")
	}

	// A map this small is allocated on its first insert whatever it is
	// sized for, so there is no count to take first.
	var header stdhttp.Header
	values := block.values[:]
	switch {
	case reuse.headers:
		pooled := headerPool.Get().(*pooledHeader)
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
	var sawHost, sawLength, sawConnection, sawPragma bool
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
			return nil
		case "Connection":
			sawConnection = true
		case "Pragma":
			sawPragma = true
		}
		if existing, ok := header[key]; ok {
			header[key] = append(existing, value)
			continue
		}
		if len(values) == 0 {
			header[key] = []string{value}
			continue
		}
		vv := values[:1:1]
		values = values[1:]
		vv[0] = value
		header[key] = vv
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

// simpleTarget reports whether a request target is an origin-form path that
// url.ParseRequestURI takes as it is: a path of characters it would not
// escape, so that Path is the path and RawPath is empty, with an optional
// query of any printable characters, which it keeps raw.
func simpleTarget(target []byte) bool {
	if target[0] != '/' {
		return false
	}
	inQuery := false
	for _, c := range target {
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
	colon := strings.IndexByte(line, ':')
	if colon <= 0 {
		return "", "", false
	}
	upper, canonical := true, true
	for i := 0; i < colon; i++ {
		c := line[i]
		if !tokenByte[c] {
			return "", "", false
		}
		if upper && 'a' <= c && c <= 'z' || !upper && 'A' <= c && c <= 'Z' {
			canonical = false
		}
		upper = c == '-'
	}
	key = line[:colon]
	if !canonical {
		key = canonicalKey(key)
	}
	value = line[colon+1:]
	for i := 0; i < len(value); i++ {
		if c := value[i]; c < ' ' && c != '\t' || c == 0x7f {
			return "", "", false
		}
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

// commonHeaderKeys interns the keys requests most often carry.
var commonHeaderKeys = func() map[string]string {
	keys := map[string]string{}
	for _, key := range []string{
		"Accept", "Accept-Charset", "Accept-Encoding", "Accept-Language", "Authorization",
		"Cache-Control", "Connection", "Content-Encoding", "Content-Length", "Content-Type",
		"Cookie", "Date", "Expect", "Forwarded", "Host", "If-Match", "If-Modified-Since",
		"If-None-Match", "If-Range", "If-Unmodified-Since", "Keep-Alive", "Origin", "Pragma",
		"Range", "Referer", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "User-Agent", "Via",
		"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-Ip", "X-Request-Id",
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
