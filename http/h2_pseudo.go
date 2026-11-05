package http

import (
	"errors"
	"net/url"
)

// The checks RFC 9113 section 8.3 asks of a request's pseudo-header fields,
// beyond what section 8.2.1 asks of every field's value: a handler is never
// handed a method, scheme, authority or path that would not survive being
// written out again as an HTTP/1 request line or Host field.
//
// newRequest makes them as it takes each field from the decoded block, and
// a path's as it parses it, so that a request's fields are each looked at
// once; each byte costs one lookup in h2ByteClass.

// The classes of h2ByteClass, one bit each.
const (
	// h2TokenByte marks the bytes of a token (RFC 9110 section 5.6.2), as
	// :method and :protocol must be.
	h2TokenByte uint8 = 1 << iota
	// h2SchemeByte marks what may follow a scheme's first letter (RFC 3986
	// section 3.1): letters, digits, '+', '-' and '.'.
	h2SchemeByte
	// h2HostByte marks what an :authority, or the Host field standing in for
	// it, may hold, as golang.org/x/net/http/httpguts.ValidHostHeader decides
	// it for net/http: letters, digits, the unreserved marks, the
	// sub-delimiters, '%' for escapes and IPv6 zones, and ':', '[' and ']'
	// for ports and IPv6 addresses. That leaves out spaces, controls, '@'
	// (RFC 9113 section 8.3.1 rules out userinfo), '/', '?', '#' and bytes
	// past ASCII.
	h2HostByte
	// h2PathByte marks what a :path may hold: anything but a space, a
	// control or DEL.
	h2PathByte
	// h2NameByte marks what a field name may hold besides a pseudo-header's
	// leading ':': no upper case, space, control, DEL, byte past ASCII or
	// ':'.
	h2NameByte
	// h2ValueByte marks what a field's value may hold: anything but NUL, CR
	// and LF (RFC 9113 section 8.2.1).
	h2ValueByte
)

var h2ByteClass = func() (table [256]uint8) {
	for c := 0; c < 256; c++ {
		if tokenByte[c] {
			table[c] |= h2TokenByte
		}
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '+' || c == '-' || c == '.' {
			table[c] |= h2SchemeByte
		}
		if c > ' ' && c != 0x7f {
			table[c] |= h2PathByte
		}
		if c > ' ' && c < 0x7f && !(c >= 'A' && c <= 'Z') && c != ':' {
			table[c] |= h2NameByte
		}
		if c != 0 && c != '\r' && c != '\n' {
			table[c] |= h2ValueByte
		}
	}
	for c := '0'; c <= '9'; c++ {
		table[c] |= h2HostByte
	}
	for c := 'a'; c <= 'z'; c++ {
		table[c] |= h2HostByte
		table[c-'a'+'A'] |= h2HostByte
	}
	for _, c := range "!$%&'()*+,-.:;=[]_~" {
		table[c] |= h2HostByte
	}
	return table
}()

// h2Bytes reports whether every byte of s is of class.
func h2Bytes(s string, class uint8) bool {
	for i := 0; i < len(s); i++ {
		if h2ByteClass[s[i]]&class == 0 {
			return false
		}
	}
	return true
}

// h2ValidMethod reports whether method is a token, as :method must be (RFC
// 9110 section 9.1).
func h2ValidMethod(method string) bool {
	return method != "" && h2Bytes(method, h2TokenByte)
}

// h2ValidScheme reports whether scheme has the form RFC 3986 section 3.1
// gives a scheme: a letter, then letters, digits, '+', '-' and '.'.
func h2ValidScheme(scheme string) bool {
	switch scheme {
	case "https", "http":
		return true
	case "":
		return false
	}
	c := scheme[0] | 0x20
	return c >= 'a' && c <= 'z' && h2Bytes(scheme, h2SchemeByte)
}

// h2ValidAuthority reports whether authority holds only what a host and port
// may; see h2HostByte.
func h2ValidAuthority(authority string) bool {
	return h2Bytes(authority, h2HostByte)
}

var errH2MalformedPath = errors.New("malformed :path")

// h2ParsePath parses an origin-form :path into u, which RFC 9113 section
// 8.3.1 has start with '/' and hold no space, control character or DEL ("*"
// for an OPTIONS request is newRequest's to take). It checks in the pass
// that parses: a target simpleTarget takes, as nearly every request's is,
// holds none of those, so only one it does not take is looked at again.
func h2ParsePath(u *url.URL, path string) error {
	if path != "" && simpleTarget(path) {
		*u = url.URL{}
		splitTarget(u, path)
		return nil
	}
	if path == "" || path[0] != '/' || !h2Bytes(path, h2PathByte) {
		return errH2MalformedPath
	}
	parsed, err := url.ParseRequestURI(path)
	if err != nil {
		return err
	}
	*u = *parsed
	return nil
}
