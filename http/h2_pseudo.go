package http

import stdhttp "net/http"

// The checks RFC 9113 section 8.3 asks of a request's pseudo-header fields,
// beyond what section 8.2.1 asks of every field's value: a handler is never
// handed a method, scheme, authority or path that would not survive being
// written out again as an HTTP/1 request line or Host field.

// h2ValidMethod reports whether method is a token, as :method must be (RFC
// 9110 section 9.1).
func h2ValidMethod(method string) bool {
	return validFieldName(method)
}

// h2ValidScheme reports whether scheme has the form RFC 3986 section 3.1
// gives a scheme: a letter, then letters, digits, '+', '-' and '.'.
func h2ValidScheme(scheme string) bool {
	if scheme == "" {
		return false
	}
	for i := 0; i < len(scheme); i++ {
		c := scheme[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case i > 0 && (c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.'):
		default:
			return false
		}
	}
	return true
}

// h2ValidAuthority reports whether authority holds only what a host and port
// may, as golang.org/x/net/http/httpguts.ValidHostHeader decides it for
// net/http: no space, control character, '@' (RFC 9113 section 8.3.1 rules
// out userinfo), '/', '?', '#' or byte past ASCII.
func h2ValidAuthority(authority string) bool {
	for i := 0; i < len(authority); i++ {
		if !h2HostByte[authority[i]] {
			return false
		}
	}
	return true
}

// h2HostByte marks the bytes h2ValidAuthority allows: letters, digits, the
// unreserved marks, the sub-delimiters, '%' for escapes and IPv6 zones, and
// ':', '[' and ']' for ports and IPv6 addresses.
var h2HostByte = func() (table [256]bool) {
	for c := '0'; c <= '9'; c++ {
		table[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		table[c] = true
		table[c-'a'+'A'] = true
	}
	for _, c := range "!$%&'()*+,-.:;=[]_~" {
		table[c] = true
	}
	return table
}()

// h2ValidPath reports whether path may be a request's :path (RFC 9113
// section 8.3.1): an origin-form target, starting with '/', or "*" for an
// OPTIONS request, and in either case without a space, a control character
// or DEL.
func h2ValidPath(method, path string) bool {
	if path == "*" {
		return method == stdhttp.MethodOptions
	}
	if path == "" || path[0] != '/' {
		return false
	}
	for i := 0; i < len(path); i++ {
		if c := path[i]; c <= ' ' || c == 0x7f {
			return false
		}
	}
	return true
}
