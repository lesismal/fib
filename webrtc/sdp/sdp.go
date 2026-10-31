package sdp

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrEmpty              = errors.New("sdp: empty session description")
	ErrInvalidLine        = errors.New("sdp: invalid line")
	ErrMissingVersion     = errors.New("sdp: session description does not start with v=")
	ErrUnsupportedVersion = errors.New("sdp: unsupported version")
	ErrMissingOrigin      = errors.New("sdp: missing o= line")
	ErrInvalidOrigin      = errors.New("sdp: invalid o= line")
	ErrMissingSessionName = errors.New("sdp: missing s= line")
	ErrMissingTiming      = errors.New("sdp: missing t= line")
	ErrInvalidMedia       = errors.New("sdp: invalid m= line")
	ErrMisplacedLine      = errors.New("sdp: line not allowed here")
	ErrInvalidAttribute   = errors.New("sdp: invalid attribute value")
)

// Line is one line of a description: its type letter and everything after
// the '=', without the line ending.
type Line struct {
	Type  byte
	Value string
}

// String returns the line as it is written, without the line ending.
func (l Line) String() string { return string(l.Type) + "=" + l.Value }

// Attribute splits the value of an a= line into the attribute's name and
// value. A property attribute, such as "a=rtcp-mux", has no value, and
// hasValue tells it apart from one whose value is empty.
func (l Line) Attribute() (name, value string, hasValue bool) {
	if i := strings.IndexByte(l.Value, ':'); i >= 0 {
		return l.Value[:i], l.Value[i+1:], true
	}
	return l.Value, "", false
}

// Section is the lines of either the session part of a description or one
// media section, in the order they are written. A media section's first
// line is its m= line. The methods find and edit attributes among them; the
// Lines may also be edited directly.
type Section struct {
	Lines []Line
}

// SessionDescription is a parsed description: the session-level lines,
// which start with v=, o= and s=, and the media sections that follow them.
type SessionDescription struct {
	Section
	Media []*MediaDescription
}

// MediaDescription is one media section, from its m= line to the next.
type MediaDescription struct {
	Section
}

// Parse parses a session description. Lines may end in CRLF or LF, and the
// last may lack an ending; blank lines are allowed only at the end. The
// description keeps every line as it was written.
func Parse(b []byte) (*SessionDescription, error) {
	return ParseString(string(b))
}

// ParseString is Parse for a description held in a string, whose lines the
// result refers to instead of copying them.
func ParseString(s string) (*SessionDescription, error) {
	if strings.TrimLeft(s, "\r\n") == "" {
		return nil, ErrEmpty
	}
	sd := &SessionDescription{}
	sd.Lines = make([]Line, 0, 16)
	cur := &sd.Section
	inMedia := false
	timing := false
	n := 0
	for len(s) > 0 {
		var line string
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			line, s = s[:i], s[i+1:]
		} else {
			line, s = s, ""
		}
		n++
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if strings.TrimLeft(s, "\r\n") == "" {
				break
			}
			return nil, fmt.Errorf("%w: line %d is blank", ErrInvalidLine, n)
		}
		if len(line) < 2 || line[1] != '=' || line[0] < 'a' || line[0] > 'z' {
			return nil, fmt.Errorf("%w: line %d %q", ErrInvalidLine, n, truncate(line))
		}
		if strings.ContainsAny(line, "\r\x00") {
			return nil, fmt.Errorf("%w: line %d holds a CR or NUL", ErrInvalidLine, n)
		}
		t, v := line[0], line[2:]
		switch n {
		case 1:
			if t != 'v' {
				return nil, ErrMissingVersion
			}
			if v != "0" {
				return nil, fmt.Errorf("%w: v=%s", ErrUnsupportedVersion, truncate(v))
			}
		case 2:
			if t != 'o' {
				return nil, ErrMissingOrigin
			}
			if _, err := ParseOrigin(v); err != nil {
				return nil, err
			}
		case 3:
			if t != 's' {
				return nil, ErrMissingSessionName
			}
		default:
			switch t {
			case 'v', 'o', 's':
				return nil, fmt.Errorf("%w: line %d %c= repeated", ErrMisplacedLine, n, t)
			case 'm':
				if _, err := ParseMediaName(v); err != nil {
					return nil, fmt.Errorf("line %d: %w", n, err)
				}
				md := &MediaDescription{}
				md.Lines = make([]Line, 0, 16)
				sd.Media = append(sd.Media, md)
				cur = &md.Section
				inMedia = true
			case 't', 'r', 'z', 'u', 'e', 'p':
				if inMedia {
					return nil, fmt.Errorf("%w: line %d %c= in a media section", ErrMisplacedLine, n, t)
				}
				if t == 't' {
					timing = true
				}
			}
		}
		cur.Lines = append(cur.Lines, Line{Type: t, Value: v})
	}
	switch {
	case len(sd.Lines) < 2:
		return nil, ErrMissingOrigin
	case len(sd.Lines) < 3:
		return nil, ErrMissingSessionName
	case !timing:
		return nil, ErrMissingTiming
	}
	return sd, nil
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

// Marshal writes the description with CRLF line endings.
func (sd *SessionDescription) Marshal() []byte {
	return sd.AppendTo(make([]byte, 0, sd.size()))
}

// String returns the description as Marshal writes it.
func (sd *SessionDescription) String() string { return string(sd.Marshal()) }

// AppendTo appends the description, as Marshal writes it, to dst.
func (sd *SessionDescription) AppendTo(dst []byte) []byte {
	dst = appendLines(dst, sd.Lines)
	for _, md := range sd.Media {
		dst = appendLines(dst, md.Lines)
	}
	return dst
}

func (sd *SessionDescription) size() int {
	n := linesSize(sd.Lines)
	for _, md := range sd.Media {
		n += linesSize(md.Lines)
	}
	return n
}

func linesSize(lines []Line) int {
	n := 0
	for _, l := range lines {
		n += len(l.Value) + 4
	}
	return n
}

func appendLines(dst []byte, lines []Line) []byte {
	for _, l := range lines {
		dst = append(dst, l.Type, '=')
		dst = append(dst, l.Value...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// Clone returns a copy of the description that shares nothing it could
// edit with the original.
func (sd *SessionDescription) Clone() *SessionDescription {
	c := &SessionDescription{}
	c.Lines = append([]Line(nil), sd.Lines...)
	c.Media = make([]*MediaDescription, len(sd.Media))
	for i, md := range sd.Media {
		c.Media[i] = md.Clone()
	}
	return c
}

// Clone returns a copy of the media section.
func (md *MediaDescription) Clone() *MediaDescription {
	return &MediaDescription{Section{Lines: append([]Line(nil), md.Lines...)}}
}

// Line returns the value of the first line of type t.
func (s *Section) Line(t byte) (string, bool) {
	for _, l := range s.Lines {
		if l.Type == t {
			return l.Value, true
		}
	}
	return "", false
}

// SetLine replaces the first line of type t, removing any others of that
// type, or adds one where RFC 8866 orders it.
func (s *Section) SetLine(t byte, value string) {
	for i, l := range s.Lines {
		if l.Type == t {
			s.Lines[i].Value = value
			s.Lines = deleteFrom(s.Lines, i+1, func(l Line) bool { return l.Type == t })
			return
		}
	}
	s.AddLine(t, value)
}

// AddLine adds a line of type t after the last line that RFC 8866 orders
// before or with it, so a c= line added to a section that already has
// attributes goes ahead of them. An a= line goes at the end.
func (s *Section) AddLine(t byte, value string) {
	order := sessionOrder
	if len(s.Lines) > 0 && s.Lines[0].Type == 'm' {
		order = mediaOrder
	}
	r := rank(order, t)
	pos := 0
	for i, l := range s.Lines {
		if rank(order, l.Type) <= r {
			pos = i + 1
		}
	}
	s.Lines = append(s.Lines, Line{})
	copy(s.Lines[pos+1:], s.Lines[pos:])
	s.Lines[pos] = Line{Type: t, Value: value}
}

// The order RFC 8866 section 5 gives a description's lines. An r= line goes
// with the t= line before it, and a type it does not name goes before the
// attributes.
const (
	sessionOrder = "vosiuepcbtzka"
	mediaOrder   = "micbka"
)

func rank(order string, t byte) int {
	if t == 'r' {
		t = 't'
	}
	if i := strings.IndexByte(order, t); i >= 0 {
		return i
	}
	return len(order) - 2
}

// deleteFrom removes the lines from index i on that match.
func deleteFrom(lines []Line, i int, match func(Line) bool) []Line {
	out := lines[:i]
	for _, l := range lines[i:] {
		if !match(l) {
			out = append(out, l)
		}
	}
	clear(lines[len(out):])
	return out
}

func isAttr(l Line, name string) bool {
	if l.Type != 'a' || !strings.HasPrefix(l.Value, name) {
		return false
	}
	return len(l.Value) == len(name) || l.Value[len(name)] == ':'
}

// Attribute returns the value of the first a= line with the given name. A
// property attribute is found with an empty value.
func (s *Section) Attribute(name string) (string, bool) {
	for _, l := range s.Lines {
		if isAttr(l, name) {
			_, v, _ := l.Attribute()
			return v, true
		}
	}
	return "", false
}

// Attributes returns the values of every a= line with the given name, in
// order.
func (s *Section) Attributes(name string) []string {
	var out []string
	for _, l := range s.Lines {
		if isAttr(l, name) {
			_, v, _ := l.Attribute()
			out = append(out, v)
		}
	}
	return out
}

// HasAttribute reports whether an a= line has the given name.
func (s *Section) HasAttribute(name string) bool {
	for _, l := range s.Lines {
		if isAttr(l, name) {
			return true
		}
	}
	return false
}

// AddAttribute adds "a=name:value" at the end of the section.
func (s *Section) AddAttribute(name, value string) {
	s.Lines = append(s.Lines, Line{Type: 'a', Value: name + ":" + value})
}

// AddFlag adds the property attribute "a=name" at the end of the section.
func (s *Section) AddFlag(name string) {
	s.Lines = append(s.Lines, Line{Type: 'a', Value: name})
}

// SetAttribute replaces the first a= line with the given name by
// "a=name:value", where it stands, and removes any others of that name; if
// there is none it adds one at the end.
func (s *Section) SetAttribute(name, value string) {
	s.replace(Line{Type: 'a', Value: name + ":" + value}, name)
}

// SetFlag adds the property attribute "a=name" if on and the section lacks
// it, and removes every a= line of that name if not.
func (s *Section) SetFlag(name string, on bool) {
	if !on {
		s.DeleteAttribute(name)
	} else if !s.HasAttribute(name) {
		s.AddFlag(name)
	}
}

// DeleteAttribute removes every a= line with the given name and returns
// how many there were.
func (s *Section) DeleteAttribute(name string) int {
	n := len(s.Lines)
	s.Lines = deleteFrom(s.Lines, 0, func(l Line) bool { return isAttr(l, name) })
	return n - len(s.Lines)
}

// replace puts l in place of the first a= line with any of the names and
// removes the rest of them, or appends l if there is none.
func (s *Section) replace(l Line, names ...string) {
	match := func(l Line) bool {
		for _, name := range names {
			if isAttr(l, name) {
				return true
			}
		}
		return false
	}
	for i := range s.Lines {
		if match(s.Lines[i]) {
			s.Lines[i] = l
			s.Lines = deleteFrom(s.Lines, i+1, match)
			return
		}
	}
	s.Lines = append(s.Lines, l)
}
