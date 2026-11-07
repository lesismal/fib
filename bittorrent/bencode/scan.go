package bencode

import (
	"errors"
	"strconv"
)

// SyntaxError is invalid bencoding, at Offset bytes into the input.
type SyntaxError struct {
	Offset int64
	msg    string
}

func (e *SyntaxError) Error() string {
	return "bencode: " + e.msg + " at offset " + strconv.FormatInt(e.Offset, 10)
}

// ErrNotFound is returned by Lookup when a key is missing.
var ErrNotFound = errors.New("bencode: key not found")

func syntaxErr(off int, msg string) error {
	return &SyntaxError{Offset: int64(off), msg: msg}
}

// scanInt checks the integer starting at data[off] ('i') and returns the
// offset past its 'e'. Leading zeros, "-0" and an empty integer are
// invalid; the integer may be any length.
func scanInt(data []byte, off int) (int, error) {
	start := off
	off++ // 'i'
	if off < len(data) && data[off] == '-' {
		off++
	}
	digits := off
	for off < len(data) && data[off] >= '0' && data[off] <= '9' {
		off++
	}
	if off >= len(data) {
		return 0, syntaxErr(off, "unterminated integer")
	}
	if data[off] != 'e' {
		return 0, syntaxErr(off, "invalid byte in integer")
	}
	n := off - digits
	switch {
	case n == 0:
		return 0, syntaxErr(start, "empty integer")
	case data[digits] == '0' && n > 1:
		return 0, syntaxErr(start, "integer with a leading zero")
	case data[digits] == '0' && digits > start+1:
		return 0, syntaxErr(start, "negative zero")
	}
	return off + 1, nil
}

// scanString checks the byte string starting at data[off] (a digit) and
// returns its content's bounds.
func scanString(data []byte, off int) (start, end int, err error) {
	begin := off
	if data[off] == '0' && off+1 < len(data) && data[off+1] != ':' {
		return 0, 0, syntaxErr(off, "string length with a leading zero")
	}
	n := 0
	for off < len(data) && data[off] >= '0' && data[off] <= '9' {
		if n > len(data) { // also keeps n*10 from overflowing
			return 0, 0, syntaxErr(begin, "string longer than the input")
		}
		n = n*10 + int(data[off]-'0')
		off++
	}
	if off >= len(data) {
		return 0, 0, syntaxErr(off, "unterminated string length")
	}
	if data[off] != ':' {
		return 0, 0, syntaxErr(off, "invalid byte in string length")
	}
	off++
	if n > len(data)-off {
		return 0, 0, syntaxErr(begin, "string longer than the input")
	}
	return off, off + n, nil
}

// scanValue checks the value at the start of data and returns its length.
func scanValue(data []byte, maxDepth int) (int, error) {
	return scanValueAt(data, 0, maxDepth)
}

// scanValueAt checks the value at data[start:] and returns the offset past
// it. It does not recurse, so maxDepth bounds only the memory of its
// stack.
func scanValueAt(data []byte, start, maxDepth int) (int, error) {
	// stack holds, for each open list or dictionary, whether it is a
	// dictionary; wantKey says the next item of the innermost dictionary
	// is a key.
	var stackBuf [32]bool
	stack := stackBuf[:0]
	wantKey := false
	off := start
	for {
		if off >= len(data) {
			if off == start {
				return 0, syntaxErr(0, "empty input")
			}
			return 0, syntaxErr(off, "unexpected end of input")
		}
		c := data[off]
		if wantKey && c != 'e' && (c < '0' || c > '9') {
			return 0, syntaxErr(off, "dictionary key is not a string")
		}
		switch {
		case c == 'i':
			end, err := scanInt(data, off)
			if err != nil {
				return 0, err
			}
			off = end
		case c >= '0' && c <= '9':
			_, end, err := scanString(data, off)
			if err != nil {
				return 0, err
			}
			off = end
		case c == 'l' || c == 'd':
			if len(stack) >= maxDepth {
				return 0, syntaxErr(off, "nesting too deep")
			}
			stack = append(stack, c == 'd')
			off++
			wantKey = c == 'd'
			continue
		case c == 'e':
			if len(stack) == 0 {
				return 0, syntaxErr(off, "unexpected 'e'")
			}
			if stack[len(stack)-1] && !wantKey {
				return 0, syntaxErr(off, "dictionary key without a value")
			}
			stack = stack[:len(stack)-1]
			off++
		default:
			return 0, syntaxErr(off, "invalid byte "+strconv.QuoteRune(rune(c)))
		}
		if len(stack) == 0 {
			return off, nil
		}
		// In a dictionary a value follows a key and a key a value; a
		// child list or dictionary just closed was a value.
		switch {
		case !stack[len(stack)-1]:
			wantKey = false
		case c == 'e':
			wantKey = true
		default:
			wantKey = !wantKey
		}
	}
}

// Valid reports whether data is exactly one bencoded value.
func Valid(data []byte) bool {
	n, err := scanValue(data, defaultMaxDepth)
	return err == nil && n == len(data)
}

// Split returns the first bencoded value of data and what follows it,
// both aliasing data, without decoding the value.
func Split(data []byte) (value, rest []byte, err error) {
	n, err := scanValue(data, defaultMaxDepth)
	if err != nil {
		return nil, data, err
	}
	return data[:n:n], data[n:], nil
}

// Lookup returns the bytes of the value at keys in the dictionary data,
// following one key per level of nested dictionaries, as a slice of data:
// Lookup(torrent, "info") is the info dictionary exactly as encoded, the
// bytes its info hash is taken over. It returns ErrNotFound if a key is
// missing, and an error if data or a level on the path is not a
// dictionary. Only the path is decoded; other values are skipped.
func Lookup(data []byte, keys ...string) ([]byte, error) {
	if n, err := scanValue(data, defaultMaxDepth); err != nil {
		return nil, err
	} else {
		data = data[:n]
	}
	for _, key := range keys {
		if data[0] != 'd' {
			return nil, errors.New("bencode: Lookup of " + strconv.Quote(key) + " in a value that is not a dictionary")
		}
		// The last of repeated keys wins, as it does for Unmarshal.
		var found []byte
		for off := 1; data[off] != 'e'; {
			ks, ke, _ := scanString(data, off) // already validated
			n, _ := scanValue(data[ke:], defaultMaxDepth)
			if string(data[ks:ke]) == key {
				found = data[ke : ke+n : ke+n]
			}
			off = ke + n
		}
		if found == nil {
			return nil, ErrNotFound
		}
		data = found
	}
	return data, nil
}
