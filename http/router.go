//go:build linux || darwin || windows

package http

import (
	"fmt"
	"iter"
	stdhttp "net/http"
	"net/url"
	"slices"
	"strings"
)

// Router routes each request to the handler registered for its method and
// path. Its API follows github.com/go-chi/chi's, but its handlers are this
// package's, func(*Context, *http.Request), as chi's are net/http's:
//
//	r := fibhttp.NewRouter()
//	r.Use(recover.New(), logger.New())
//	r.Get("/", index)
//	r.Route("/users", func(r *fibhttp.Router) {
//		r.Use(auth)
//		r.Get("/", listUsers)
//		r.Post("/", createUser)
//		r.Get("/{id:[0-9]+}", getUser) // c.Param("id")
//		r.Get("/{id}/files/*", getFile) // c.Param("*")
//	})
//	r.Mount("/debug", pprof.New())
//	engine, err := fib.Bind(config, fibhttp.NewHandler(r))
//
// A pattern is a path of static text, parameters and an optional catch-all:
//
//   - {name} matches a non-empty value up to the next "/", or up to the
//     static text that follows it in the pattern, as {year} does in
//     /archive/{year}-{month}. A value never takes a "/".
//   - {name:regexp} matches a value the regular expression matches as a
//     whole, such as {id:[0-9]+}.
//   - * at the end of a pattern matches the rest of the path, empty or not,
//     under the key "*"; {name...} does the same under name.
//
// When more than one route matches a path, static text wins over a
// parameter, a parameter with a regular expression over one without, and a
// parameter over a catch-all, segment by segment. A route for GET also
// serves HEAD unless HEAD has a route of its own. A path a route matches but
// whose method it does not serve is answered 405 Method Not Allowed, with an
// Allow field naming the methods it does; any other path 404 Not Found.
//
// The path routed is the request's URL.RawPath when it has one — the path
// had an escape such as %2F that its decoded form loses — so that an escaped
// "/" is part of a value rather than a separator; Param decodes the value.
//
// Middleware added with Use on the router NewRouter returned wraps all of
// its routing, so it sees every request, 404s and 405s included. Middleware
// added to a Route's router wraps the routes under its prefix and the 404s
// and 405s there; middleware added through With or Group only the routes
// registered through them. Middleware must be added before the routes it
// wraps, and every route before the router serves; a Router is not safe for
// registering routes while it serves.
//
// The zero Router is ready to use, as one NewRouter returns.
type Router struct {
	tree *routeTree
	// parent is the router With, Group or Route made this one from, nil for
	// the router at the tree's root.
	parent *Router
	// prefix is the pattern Route put in front of this router's routes.
	prefix      string
	middlewares []func(Handler) Handler
	// inline records that the router came from With or Group, and so shares
	// its parent's NotFound and MethodNotAllowed handlers.
	inline bool
	// routed records that a route has been registered through the router or
	// one made from it, after which Use is refused.
	routed           bool
	notFound         Handler
	methodNotAllowed Handler
}

// routeTree is what the routers made from one another share.
type routeTree struct {
	root   node
	router *Router
	// handler is the root router's middleware around dispatch.
	handler    Handler
	pathValues bool
}

// NewRouter returns an empty Router.
func NewRouter() *Router {
	r := new(Router)
	r.init()
	return r
}

func (r *Router) init() *routeTree {
	if r.tree == nil {
		t := &routeTree{router: r}
		t.handler = HandlerFunc(t.dispatch)
		r.tree = t
	}
	return r.tree
}

// ServeHTTP routes the request. A router made by With, Group or Route
// serves every route of the router it was made from.
func (r *Router) ServeHTTP(c *Context, req *stdhttp.Request) {
	if r.tree == nil {
		notFound(c, req)
		return
	}
	if t := r.tree; len(t.router.middlewares) == 0 {
		t.dispatch(c, req)
	} else {
		t.handler.ServeHTTP(c, req)
	}
}

// Use adds middleware to the router, the first outermost. It panics once a
// route has been registered through the router.
func (r *Router) Use(middlewares ...func(Handler) Handler) {
	t := r.init()
	if r.routed {
		panic("http: Router.Use after routes: middleware must be added before the routes it wraps")
	}
	for _, m := range middlewares {
		if m != nil {
			r.middlewares = append(r.middlewares, m)
		}
	}
	if r.parent == nil {
		t.handler = chain(r.middlewares, HandlerFunc(t.dispatch))
	}
}

// With returns a router that registers routes on this one with middlewares
// around them, besides this router's own:
//
//	r.With(auth).Post("/admin/users", createUser)
func (r *Router) With(middlewares ...func(Handler) Handler) *Router {
	t := r.init()
	sub := &Router{tree: t, parent: r, prefix: r.prefix, inline: true}
	sub.Use(middlewares...)
	return sub
}

// Group calls fn with a router that registers routes on this one, for the
// middleware fn adds to it to wrap only the routes fn registers.
func (r *Router) Group(fn func(r *Router)) *Router {
	sub := r.With()
	if fn != nil {
		fn(sub)
	}
	return sub
}

// Route calls fn with a router whose routes are under pattern, which may
// have parameters but no catch-all. The router owns everything under its
// prefix: a request there that none of its routes serves is answered by its
// NotFound or MethodNotAllowed handler — this router's, when it was given
// them, and its parent's otherwise — inside the middleware it was given.
//
// Unlike a Mount, a Route's routes join this router's tree, and a request is
// routed through the two in a single walk of it.
func (r *Router) Route(pattern string, fn func(r *Router)) *Router {
	t := r.init()
	if pattern == "" || pattern[0] != '/' {
		panic(fmt.Sprintf("http: Route pattern %q must begin with /", pattern))
	}
	if strings.HasSuffix(pattern, "*") || strings.HasSuffix(pattern, "...}") {
		panic(fmt.Sprintf("http: Route pattern %q cannot end with a catch-all", pattern))
	}
	sub := &Router{tree: t, parent: r, prefix: r.prefix + strings.TrimSuffix(pattern, "/")}
	if fn != nil {
		fn(sub)
	}
	sub.addFallback()
	return sub
}

// addFallback routes what nothing else under the router's prefix serves to
// its NotFound and MethodNotAllowed handlers.
func (r *Router) addFallback() {
	middlewares := r.endpointMiddlewares()
	patterns := []string{r.prefix + "/*"}
	if r.prefix != "" {
		patterns = append(patterns, r.prefix)
	}
	for _, pattern := range patterns {
		segments, keys := parsePattern(pattern)
		ep := newEndpoint(&endpoint{
			pattern:  pattern,
			keys:     keys,
			fallback: true,
			handler:  chain(middlewares, HandlerFunc(func(c *Context, req *stdhttp.Request) { r.notFoundHandler().ServeHTTP(c, req) })),
			alt:      chain(middlewares, HandlerFunc(func(c *Context, req *stdhttp.Request) { r.methodNotAllowedHandler().ServeHTTP(c, req) })),
		})
		r.tree.root.insert(segments).endpointsOf().fallback = ep
	}
	r.markRouted()
}

// Mount routes everything under pattern to handler, whatever the method. A
// Router mounted there routes the rest of the path, from its "/", with its
// own middleware, as chi's does; any other handler is given the request as
// it is, with the rest of the path as Param("*"). A mounted Router that has
// no NotFound or MethodNotAllowed handler is given this router's.
func (r *Router) Mount(pattern string, handler Handler) {
	if handler == nil {
		panic("http: Router.Mount of a nil handler")
	}
	if strings.Contains(pattern, "*") || strings.HasSuffix(pattern, "...}") {
		panic(fmt.Sprintf("http: Mount pattern %q cannot have a catch-all", pattern))
	}
	if sub, ok := handler.(*Router); ok && sub.parent == nil {
		if sub.notFound == nil {
			sub.notFound = r.scope().notFound
		}
		if sub.methodNotAllowed == nil {
			sub.methodNotAllowed = r.scope().methodNotAllowed
		}
	}
	full := r.prefix + strings.TrimSuffix(pattern, "/")
	if full != "" {
		r.add("", full, handler, mountExact)
	}
	r.add("", full+"/*", handler, mountRest)
}

// Handle registers handler for pattern, whatever the method, or for the
// method the pattern begins with, as net/http's ServeMux takes it:
//
//	r.Handle("/static/*", files)
//	r.Handle("DELETE /users/{id}", deleteUser)
func (r *Router) Handle(pattern string, handler Handler) {
	method := ""
	if i := strings.IndexAny(pattern, " \t"); i >= 0 {
		method, pattern = pattern[:i], strings.TrimLeft(pattern[i:], " \t")
		checkMethod(method)
	}
	r.handle(method, pattern, handler)
}

// HandleFunc is Handle for a function.
func (r *Router) HandleFunc(pattern string, handler HandlerFunc) {
	r.Handle(pattern, checkFunc(handler))
}

// Method registers handler for method and pattern. method may be any token,
// not only the methods that have functions of their own here.
func (r *Router) Method(method, pattern string, handler Handler) {
	checkMethod(method)
	r.handle(method, pattern, handler)
}

// MethodFunc is Method for a function.
func (r *Router) MethodFunc(method, pattern string, handler HandlerFunc) {
	r.Method(method, pattern, checkFunc(handler))
}

func (r *Router) Get(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodGet, pattern, checkFunc(handler))
}

func (r *Router) Head(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodHead, pattern, checkFunc(handler))
}

func (r *Router) Post(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodPost, pattern, checkFunc(handler))
}

func (r *Router) Put(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodPut, pattern, checkFunc(handler))
}

func (r *Router) Patch(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodPatch, pattern, checkFunc(handler))
}

func (r *Router) Delete(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodDelete, pattern, checkFunc(handler))
}

func (r *Router) Connect(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodConnect, pattern, checkFunc(handler))
}

func (r *Router) Options(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodOptions, pattern, checkFunc(handler))
}

func (r *Router) Trace(pattern string, handler HandlerFunc) {
	r.handle(stdhttp.MethodTrace, pattern, checkFunc(handler))
}

// NotFound sets the handler for a request no route's path matches; nil
// restores the default, a plain 404. On a router from With or Group it sets
// the handler of the router they were made from.
func (r *Router) NotFound(handler HandlerFunc) {
	r.init()
	r.scope().notFound = handlerOrNil(handler)
}

// MethodNotAllowed sets the handler for a request whose path a route matches
// but whose method none serves; nil restores the default, a plain 405. The
// handler is called with an Allow field naming the methods that are served
// already in c.Header(), which a response written through the
// ResponseWriter methods sends; one written with Respond or WriteResponse
// needs to carry it itself. On a router from With or Group it sets the
// handler of the router they were made from.
func (r *Router) MethodNotAllowed(handler HandlerFunc) {
	r.init()
	r.scope().methodNotAllowed = handlerOrNil(handler)
}

// SetPathValues has every request the router serves carry its route's
// values in its PathValue too, as net/http's ServeMux sets them, for
// handlers written for it. It costs an allocation for each request with
// parameters, which Param does not.
func (r *Router) SetPathValues(enabled bool) { r.init().pathValues = enabled }

// Walk calls fn for every route of the router, with its method — "*" for a
// route that takes any — its pattern, and the handler registered, without
// the middleware around it. The routes of a mounted Router are walked under
// the pattern they are mounted at. Walking stops at the first error fn
// returns, which Walk returns.
func (r *Router) Walk(fn func(method, pattern string, handler Handler) error) error {
	return r.walk("", fn)
}

func (r *Router) walk(prefix string, fn func(method, pattern string, handler Handler) error) error {
	if r.tree == nil {
		return nil
	}
	return r.tree.root.walk(func(e *endpoints) error {
		for i, ep := range e.methods {
			if ep != nil {
				if err := fn(routeMethods[i], prefix+ep.pattern, ep.inner); err != nil {
					return err
				}
			}
		}
		custom := make([]string, 0, len(e.custom))
		for method := range e.custom {
			custom = append(custom, method)
		}
		slices.Sort(custom)
		for _, method := range custom {
			ep := e.custom[method]
			if err := fn(method, prefix+ep.pattern, ep.inner); err != nil {
				return err
			}
		}
		ep := e.any
		if ep == nil {
			return nil
		}
		if sub, ok := ep.inner.(*Router); ok && ep.mount != notMounted {
			if ep.mount == mountRest {
				return sub.walk(prefix+strings.TrimSuffix(ep.pattern, "/*"), fn)
			}
			return nil
		}
		return fn("*", prefix+ep.pattern, ep.inner)
	})
}

// endpoint is one registered route.
type endpoint struct {
	// handler serves the route, inside the middleware of the router it was
	// registered through; inner is the handler as registered.
	handler Handler
	inner   Handler
	// alt is a fallback's handler for a method its prefix does not serve.
	alt Handler
	// pattern is the route's whole pattern, its router's prefix included,
	// and keys are the names of the values it matches, in order.
	pattern  string
	keys     []string
	mount    mountKind
	fallback bool
	// shared is the route state of a request the route serves that needs
	// no state of its own: one with no values, that no Mount handed over.
	shared routeState
}

func newEndpoint(ep *endpoint) *endpoint {
	ep.shared = routeState{ep: ep, shared: true}
	return ep
}

// mountKind is which of a Mount's routes an endpoint is.
type mountKind uint8

const (
	notMounted mountKind = iota
	// mountExact is the Mount's pattern itself, routed to the mounted
	// router as "/".
	mountExact
	// mountRest is the pattern with a catch-all, whose value is routed to
	// the mounted router.
	mountRest
)

func (r *Router) handle(method, pattern string, handler Handler) {
	if handler == nil {
		panic("http: route for a nil handler")
	}
	if pattern == "" || pattern[0] != '/' {
		panic(fmt.Sprintf("http: route pattern %q must begin with /", pattern))
	}
	if pattern == "/" && r.prefix != "" {
		// A Route's "/" serves its prefix with and without the slash, as
		// chi's does.
		r.add(method, r.prefix, handler, notMounted)
	}
	r.add(method, r.prefix+pattern, handler, notMounted)
}

func (r *Router) add(method, pattern string, handler Handler, mount mountKind) {
	t := r.init()
	segments, keys := parsePattern(pattern)
	ep := newEndpoint(&endpoint{
		handler: chain(r.endpointMiddlewares(), handler),
		inner:   handler,
		pattern: pattern,
		keys:    keys,
		mount:   mount,
	})
	t.root.insert(segments).endpointsOf().set(method, ep)
	r.markRouted()
}

func (r *Router) markRouted() {
	for s := r; s != nil; s = s.parent {
		s.routed = true
	}
}

// endpointMiddlewares is the middleware around a route registered through
// the router: that of every router from it up to, but not including, the
// root, whose own wraps the routing itself.
func (r *Router) endpointMiddlewares() []func(Handler) Handler {
	if r.parent == nil {
		return nil
	}
	return append(slices.Clip(r.parent.endpointMiddlewares()), r.middlewares...)
}

// scope is the router whose NotFound and MethodNotAllowed handlers this one
// uses: itself, unless it came from With or Group.
func (r *Router) scope() *Router {
	for r.inline {
		r = r.parent
	}
	return r
}

func (r *Router) notFoundHandler() Handler {
	for s := r; s != nil; s = s.parent {
		if s.notFound != nil {
			return s.notFound
		}
	}
	return HandlerFunc(notFound)
}

func (r *Router) methodNotAllowedHandler() Handler {
	for s := r; s != nil; s = s.parent {
		if s.methodNotAllowed != nil {
			return s.methodNotAllowed
		}
	}
	return HandlerFunc(methodNotAllowed)
}

var notFoundBody = []byte("404 page not found\n")

func notFound(c *Context, _ *stdhttp.Request) {
	_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", notFoundBody)
}

func methodNotAllowed(c *Context, _ *stdhttp.Request) {
	// The Allow field is in the header already, which Respond would not send.
	c.Header().Set("Content-Type", "text/plain; charset=utf-8")
	c.WriteHeader(stdhttp.StatusMethodNotAllowed)
	_, _ = c.WriteString("405 method not allowed\n")
}

// dispatch routes a request through the tree, inside the root router's
// middleware.
func (t *routeTree) dispatch(c *Context, req *stdhttp.Request) {
	path := t.start(c, req)
	// start leaves c.route nil or the Context's own.
	s := search{c: c, st: c.route, method: req.Method, index: methodIndex(req.Method)}
	var base []string
	if s.st != nil {
		base = s.st.keys
	}
	if !t.root.find(&s, path) {
		if s.allow != "" {
			c.Header().Set("Allow", s.allow)
			t.router.methodNotAllowedHandler().ServeHTTP(c, req)
			return
		}
		t.router.notFoundHandler().ServeHTTP(c, req)
		return
	}
	ep := s.ep
	st := s.st
	if st == nil && ep.mount == notMounted {
		// Nothing to keep but the route, which its endpoint's shared state
		// has: a request to a static route allocates nothing, even on a
		// Context of its own.
		c.route = &ep.shared
	} else {
		if st == nil {
			st = c.routeState()
		}
		st.ep = ep
		switch {
		case len(base) == 0:
			st.keys = ep.keys
		case len(ep.keys) > 0:
			st.keys = append(slices.Clip(base), ep.keys...)
		}
		if ep.mount != notMounted {
			st.mounted = true
			st.rest = "/"
			if ep.mount == mountRest {
				rest := st.values[len(st.values)-1]
				st.rest = path[len(path)-len(rest)-1:]
			}
		}
		if t.pathValues {
			for i, key := range st.keys {
				req.SetPathValue(key, st.value(i))
			}
		}
	}
	if ep.fallback && s.allow != "" {
		c.Header().Set("Allow", s.allow)
		ep.alt.ServeHTTP(c, req)
		return
	}
	ep.handler.ServeHTTP(c, req)
	if st != nil {
		// A mounted handler that is no Router, nor reaches one, leaves the
		// rest of the path untaken.
		st.mounted = false
	}
}

// start returns the path to route: what a Mount left of it for this router,
// or the request's own, for which it clears what routing it before left.
func (t *routeTree) start(c *Context, req *stdhttp.Request) string {
	st := c.route
	if st != nil && st.shared {
		// The route state the request before was served with is its
		// endpoint's, which only it may have.
		c.route, st = nil, nil
	}
	if st != nil && st.mounted {
		st.mounted = false
		st.mounts = append(st.mounts, st.ep.pattern)
		st.ep = nil
		return st.rest
	}
	if req.URL == nil {
		if st != nil {
			st.clear()
		}
		return ""
	}
	path, raw := req.URL.Path, req.URL.RawPath != ""
	if raw {
		path = req.URL.RawPath
	}
	if st != nil {
		st.clear()
		st.raw = raw
	} else if raw {
		c.routeState().raw = true
	}
	return path
}

// routeState is what routing a request left in its Context: the Context's
// own, or, for a request whose route needs nothing else kept, the read-only
// one the route's endpoint shares with every request it serves.
type routeState struct {
	// ep is the route that served the request, nil while it is being routed
	// and when nothing matched it.
	ep *endpoint
	// keys are the names of values, which are what the route's parameters
	// and catch-all matched, in the order they appear in its pattern, after
	// those of the Mounts the request came through.
	keys   []string
	values []string
	// mounts are the patterns of the Mounts the request came through.
	mounts []string
	// rest is the path a Mount leaves for the Router it hands the request to,
	// and mounted is set until that Router takes it.
	rest    string
	mounted bool
	// raw records that the path routed was URL.RawPath, whose values are
	// still escaped.
	raw bool
	// shared records that the state is an endpoint's, which is never written.
	shared bool
	buf    [4]string
}

// routeState returns the Context's route state, adding it if it has none. A
// recycled Context keeps it for the next request.
func (c *Context) routeState() *routeState {
	if c.route == nil || c.route.shared {
		st := new(routeState)
		st.values = st.buf[:0]
		c.route = st
	}
	return c.route
}

// reset readies the route state of a recycled Context for its next request,
// letting go of everything the last one left in it.
func (st *routeState) reset() {
	clear(st.values[:cap(st.values)])
	st.clear()
}

// clear readies the route state for routing another request. The values a
// search pushed and took off again are left for the next to overwrite.
func (st *routeState) clear() {
	clear(st.values)
	st.ep, st.keys, st.values = nil, nil, st.values[:0]
	if len(st.mounts) > 0 {
		clear(st.mounts)
		st.mounts = st.mounts[:0]
	}
	st.rest, st.mounted, st.raw = "", false, false
}

func (st *routeState) value(i int) string {
	v := st.values[i]
	if st.raw {
		if unescaped, err := url.PathUnescape(v); err == nil {
			return unescaped
		}
	}
	return v
}

// Param returns the value the route that served the request matched for
// the parameter or catch-all name: "*" for a pattern's trailing "*". It is
// empty when the route has no such parameter. A name that a Mount's pattern
// and the mounted Router's route both have is the route's.
func (c *Context) Param(name string) string {
	st := c.route
	if st == nil {
		return ""
	}
	for i := len(st.keys) - 1; i >= 0; i-- {
		if st.keys[i] == name {
			return st.value(i)
		}
	}
	return ""
}

// Params yields the name and value of every parameter the route that served
// the request matched, in the order Param looks at them last.
func (c *Context) Params() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		st := c.route
		if st == nil {
			return
		}
		for i, key := range st.keys {
			if !yield(key, st.value(i)) {
				return
			}
		}
	}
}

// RoutePattern is the pattern of the route that served the request, with
// the patterns of the Mounts it came through in front, such as
// "/api/users/{id}"; empty when no route did.
func (c *Context) RoutePattern() string {
	st := c.route
	if st == nil || st.ep == nil {
		return ""
	}
	if len(st.mounts) == 0 {
		return st.ep.pattern
	}
	var b strings.Builder
	for _, m := range st.mounts {
		b.WriteString(strings.TrimSuffix(strings.TrimSuffix(m, "*"), "/"))
	}
	b.WriteString(st.ep.pattern)
	return b.String()
}

func chain(middlewares []func(Handler) Handler, handler Handler) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

func checkFunc(handler HandlerFunc) Handler {
	if handler == nil {
		panic("http: route for a nil handler")
	}
	return handler
}

func handlerOrNil(handler HandlerFunc) Handler {
	if handler == nil {
		return nil
	}
	return handler
}

func checkMethod(method string) {
	if method == "" {
		panic("http: route for an empty method")
	}
	for i := 0; i < len(method); i++ {
		if c := method[i]; c <= ' ' || c >= 0x7f || strings.IndexByte(`()<>@,;:\"/[]?={}`, c) >= 0 {
			panic(fmt.Sprintf("http: route for an invalid method %q", method))
		}
	}
}
