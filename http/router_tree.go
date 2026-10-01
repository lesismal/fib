//go:build linux || darwin || windows

package http

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// segmentKind is what a piece of a route's pattern matches.
type segmentKind uint8

const (
	// segmentStatic matches its text exactly.
	segmentStatic segmentKind = iota
	// segmentParam matches a non-empty value that stops at the byte after it
	// in the pattern, and never takes a "/".
	segmentParam
	// segmentCatchAll matches the rest of the path, which may be empty.
	segmentCatchAll
)

// segment is one piece of a route's pattern.
type segment struct {
	kind segmentKind
	// text is a static segment's text, and the regular expression a
	// parameter's value must match, if it has one.
	text string
	// name is the key a parameter's or a catch-all's value is found under.
	name string
	// tail is the byte that ends a parameter's value: the first of the
	// static text after it, or "/" when the pattern ends with it.
	tail byte
}

// parsePattern splits a route's pattern into its segments, and returns with
// them the keys of the values its parameters and catch-all match, in the
// order they appear. It panics on a pattern it cannot route.
func parsePattern(pattern string) ([]segment, []string) {
	if pattern == "" || pattern[0] != '/' {
		panic(fmt.Sprintf("http: route pattern %q must begin with /", pattern))
	}
	var segments []segment
	var keys []string
	for i := 0; i < len(pattern); {
		switch pattern[i] {
		case '{':
			end := paramEnd(pattern, i)
			name, expr, isRegexp := strings.Cut(pattern[i+1:end], ":")
			kind := segmentParam
			if !isRegexp && strings.HasSuffix(name, "...") {
				name, kind = strings.TrimSuffix(name, "..."), segmentCatchAll
				if end != len(pattern)-1 {
					panic(fmt.Sprintf("http: route pattern %q: {%s...} must end the pattern", pattern, name))
				}
			}
			if name == "" || strings.ContainsAny(name, "/{}*") {
				panic(fmt.Sprintf("http: route pattern %q has an invalid parameter name %q", pattern, name))
			}
			if isRegexp && expr == "" {
				panic(fmt.Sprintf("http: route pattern %q: {%s:} has no regular expression", pattern, name))
			}
			segments = append(segments, segment{kind: kind, text: expr, name: name})
			keys = append(keys, name)
			i = end + 1
		case '*':
			if i != len(pattern)-1 {
				panic(fmt.Sprintf("http: route pattern %q: the catch-all * must end the pattern", pattern))
			}
			segments = append(segments, segment{kind: segmentCatchAll, name: "*"})
			keys = append(keys, "*")
			i++
		case '}':
			panic(fmt.Sprintf("http: route pattern %q has an unmatched }", pattern))
		default:
			end := strings.IndexAny(pattern[i:], "{}*")
			if end < 0 {
				end = len(pattern)
			} else {
				end += i
			}
			segments = append(segments, segment{kind: segmentStatic, text: pattern[i:end]})
			i = end
		}
	}
	for i := range segments {
		if segments[i].kind != segmentParam {
			continue
		}
		segments[i].tail = '/'
		if i+1 < len(segments) {
			next := segments[i+1]
			if next.kind != segmentStatic {
				panic(fmt.Sprintf("http: route pattern %q: {%s} must be followed by static text, not another parameter", pattern, segments[i].name))
			}
			segments[i].tail = next.text[0]
		}
	}
	for i, key := range keys {
		if slices.Contains(keys[:i], key) {
			panic(fmt.Sprintf("http: route pattern %q has the parameter %q twice", pattern, key))
		}
	}
	return segments, keys
}

// paramEnd is the index of the "}" that closes the parameter opened at
// pattern[start], counting the braces of a regular expression such as
// {id:[0-9]{4}}.
func paramEnd(pattern string, start int) int {
	depth := 0
	for i := start; i < len(pattern); i++ {
		switch pattern[i] {
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	panic(fmt.Sprintf("http: route pattern %q has an unclosed {", pattern))
}

// valueMatcher returns what tests a parameter's value against expr: a plain
// loop for the expressions that only take digits, which routes use most, and
// the compiled expression, anchored at both ends, for any other.
func valueMatcher(expr string) func(string) bool {
	switch expr {
	case "[0-9]+", `\d+`:
		return allDigits
	}
	if !strings.HasPrefix(expr, "^") {
		expr = "^" + expr
	}
	if !strings.HasSuffix(expr, "$") {
		expr += "$"
	}
	return regexp.MustCompile(expr).MatchString
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// node is a node of a Router's radix tree. A static node matches its prefix,
// a parameter node a value up to its tail, and a catch-all node the rest of
// the path; its children are tried in that order too, so that the most
// specific route a path matches is the one it is served by.
type node struct {
	prefix string
	// indices holds the first byte of each static child's prefix, which is
	// how the child for a path is found; no two children share one.
	indices string
	statics []*node
	// params are the parameter children, those whose value must match a
	// regular expression ahead of those that take any.
	params   []*node
	catchAll *node
	// eps are the routes that end at the node, nil if none do.
	eps *endpoints

	// tail, expr and match are a parameter node's: the byte that ends its
	// value, and the expression its value must match, if any.
	tail  byte
	expr  string
	match func(string) bool
}

// insert adds the nodes segments walk through below n, and returns the one
// they end at.
func (n *node) insert(segments []segment) *node {
	for _, s := range segments {
		switch s.kind {
		case segmentStatic:
			n = n.insertStatic(s.text)
		case segmentParam:
			n = n.paramChild(s)
		case segmentCatchAll:
			if n.catchAll == nil {
				n.catchAll = new(node)
			}
			n = n.catchAll
		}
	}
	return n
}

// insertStatic adds the static nodes text walks through below n, splitting
// one whose prefix it shares only part of, and returns the one it ends at.
func (n *node) insertStatic(text string) *node {
	for text != "" {
		i := strings.IndexByte(n.indices, text[0])
		if i < 0 {
			child := &node{prefix: text}
			n.indices += text[:1]
			n.statics = append(n.statics, child)
			return child
		}
		child := n.statics[i]
		common := commonPrefix(text, child.prefix)
		if common < len(child.prefix) {
			split := &node{prefix: child.prefix[:common], indices: child.prefix[common : common+1], statics: []*node{child}}
			child.prefix = child.prefix[common:]
			n.statics[i] = split
			child = split
		}
		n, text = child, text[common:]
	}
	return n
}

func commonPrefix(a, b string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// paramChild returns the parameter child of n that s matches through,
// adding it if there is none. Parameters that only differ in name share one:
// the name is the route's, not the node's.
func (n *node) paramChild(s segment) *node {
	for _, child := range n.params {
		if child.tail == s.tail && child.expr == s.text {
			return child
		}
	}
	child := &node{tail: s.tail, expr: s.text}
	at := len(n.params)
	if s.text != "" {
		child.match = valueMatcher(s.text)
		at = slices.IndexFunc(n.params, func(p *node) bool { return p.match == nil })
		if at < 0 {
			at = len(n.params)
		}
	}
	n.params = slices.Insert(n.params, at, child)
	return child
}

// endpointsOf returns the routes ending at n, adding the set if there is
// none.
func (n *node) endpointsOf() *endpoints {
	if n.eps == nil {
		n.eps = new(endpoints)
	}
	return n.eps
}

// search is one walk of the tree for a request.
type search struct {
	c      *Context
	st     *routeState
	method string
	index  int
	// ep is the route found.
	ep *endpoint
	// allow is the Allow field of the first route the path matched but the
	// method did not, for a 405 answer if nothing else matches.
	allow string
}

// find looks for the route that serves path below n, whose own prefix path
// has already matched. The values parameters match are pushed on the route
// state as the walk goes, and taken off again when it backs out.
//
// It recurses only where there is something to back out to: a child to try
// should the one before it fail. The last child to try is walked by the loop
// instead, which takes the values it pushed back off itself.
func (n *node) find(s *search, path string) bool {
	base := s.depth()
walk:
	for {
		if path == "" {
			if n.eps != nil && s.accept(n.eps) {
				return true
			}
			if c := n.catchAll; c != nil && c.eps != nil {
				s.push("")
				if s.accept(c.eps) {
					return true
				}
			}
			break
		}
		// The loops here stand in for strings.IndexByte, which is not
		// inlined: an index is a few bytes long and a value seldom more than
		// a few dozen, too short for its call to pay.
		for i, first := 0, path[0]; i < len(n.indices); i++ {
			if n.indices[i] != first {
				continue
			}
			c := n.statics[i]
			if len(path) < len(c.prefix) || path[:len(c.prefix)] != c.prefix {
				break
			}
			if len(n.params) == 0 && n.catchAll == nil {
				n, path = c, path[len(c.prefix):]
				continue walk
			}
			if c.find(s, path[len(c.prefix):]) {
				return true
			}
			break
		}
		for i, c := range n.params {
			end := valueEnd(path, c.tail)
			if end <= 0 {
				continue
			}
			value := path[:end]
			if c.match != nil && !c.match(value) {
				continue
			}
			mark := s.push(value)
			if i == len(n.params)-1 && n.catchAll == nil {
				n, path = c, path[end:]
				continue walk
			}
			if c.find(s, path[end:]) {
				return true
			}
			s.pop(mark)
		}
		if c := n.catchAll; c != nil && c.eps != nil {
			s.push(path)
			if s.accept(c.eps) {
				return true
			}
		}
		break
	}
	s.pop(base)
	return false
}

// valueEnd is where a parameter's value that ends at tail ends in path: at
// the first tail, or at the end of path when tail is "/". It is 0 when the
// value would be empty, and -1 when it would take a "/" or has no end.
func valueEnd(path string, tail byte) int {
	for i := 0; i < len(path); i++ {
		switch path[i] {
		case tail:
			return i
		case '/':
			return -1
		}
	}
	if tail == '/' {
		return len(path)
	}
	return -1
}

// accept reports whether the routes a path ends at serve the request: one
// for its method, or a Route's fallback, which answers whatever under its
// prefix nothing else does.
func (s *search) accept(e *endpoints) bool {
	if ep := e.lookup(s.index, s.method); ep != nil {
		s.ep = ep
		return true
	}
	if s.allow == "" {
		s.allow = e.allow
	}
	if e.fallback != nil {
		s.ep = e.fallback
		return true
	}
	return false
}

// push adds a parameter's value, returning what pop takes the values back
// to.
func (s *search) push(value string) int {
	st := s.st
	if st == nil {
		st = s.c.routeState()
		s.st = st
	}
	mark := len(st.values)
	st.values = append(st.values, value)
	return mark
}

// pop takes the values back to what depth or push reported.
func (s *search) pop(mark int) {
	if s.st != nil {
		s.st.values = s.st.values[:mark]
	}
}

// depth is how many values have been pushed.
func (s *search) depth() int {
	if s.st == nil {
		return 0
	}
	return len(s.st.values)
}

// routeMethods are the methods a route's endpoints are indexed by; others go
// in a map.
var routeMethods = [...]string{
	"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "CONNECT", "OPTIONS", "TRACE",
}

const (
	methodGet = iota
	methodHead
)

// methodIndex is method's index in routeMethods, or -1.
func methodIndex(method string) int {
	switch method {
	case "GET":
		return 0
	case "HEAD":
		return 1
	case "POST":
		return 2
	case "PUT":
		return 3
	case "PATCH":
		return 4
	case "DELETE":
		return 5
	case "CONNECT":
		return 6
	case "OPTIONS":
		return 7
	case "TRACE":
		return 8
	}
	return -1
}

// endpoints are the routes that end at one node.
type endpoints struct {
	methods [len(routeMethods)]*endpoint
	custom  map[string]*endpoint
	// any serves the methods nothing in methods or custom does.
	any *endpoint
	// fallback answers a request under a Route's prefix that no route does.
	fallback *endpoint
	// allow is the Allow field of a 405 response for the path, or empty when
	// any method is served.
	allow string
}

// lookup returns the route for the method, whose index in routeMethods is
// index. A GET route serves HEAD when there is no HEAD route, as net/http's
// ServeMux does.
func (e *endpoints) lookup(index int, method string) *endpoint {
	if index >= 0 {
		if ep := e.methods[index]; ep != nil {
			return ep
		}
		if index == methodHead {
			if ep := e.methods[methodGet]; ep != nil {
				return ep
			}
		}
	} else if ep := e.custom[method]; ep != nil {
		return ep
	}
	return e.any
}

// set adds the route for method, every method when it is empty.
func (e *endpoints) set(method string, ep *endpoint) {
	slot := &e.any
	if method != "" {
		if i := methodIndex(method); i >= 0 {
			slot = &e.methods[i]
		} else {
			if e.custom == nil {
				e.custom = make(map[string]*endpoint)
			}
			if e.custom[method] != nil {
				panic(fmt.Sprintf("http: %s %s is routed twice", method, ep.pattern))
			}
			e.custom[method] = ep
			e.updateAllow()
			return
		}
	}
	if *slot != nil {
		if method == "" {
			method = "any method of"
		}
		panic(fmt.Sprintf("http: %s %s is routed twice", method, ep.pattern))
	}
	*slot = ep
	e.updateAllow()
}

func (e *endpoints) updateAllow() {
	e.allow = ""
	if e.any != nil {
		return
	}
	var allowed []string
	for i, ep := range e.methods {
		if ep != nil || i == methodHead && e.methods[methodGet] != nil {
			allowed = append(allowed, routeMethods[i])
		}
	}
	custom := make([]string, 0, len(e.custom))
	for method := range e.custom {
		custom = append(custom, method)
	}
	slices.Sort(custom)
	e.allow = strings.Join(append(allowed, custom...), ", ")
}

// walk calls fn for each route at and below n, in the order find tries them.
func (n *node) walk(fn func(*endpoints) error) error {
	if n.eps != nil {
		if err := fn(n.eps); err != nil {
			return err
		}
	}
	for _, children := range [][]*node{n.statics, n.params} {
		for _, c := range children {
			if err := c.walk(fn); err != nil {
				return err
			}
		}
	}
	if n.catchAll != nil {
		return n.catchAll.walk(fn)
	}
	return nil
}
