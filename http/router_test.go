//go:build linux || darwin || windows

package http

import (
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// routed is what a test route records of the request it served.
type routed struct {
	name    string
	params  map[string]string
	pattern string
}

// recorder registers routes whose handlers record what they served into it.
type recorder struct{ last *routed }

func (rec *recorder) handler(name string) HandlerFunc {
	return func(c *Context, _ *stdhttp.Request) {
		params := map[string]string{}
		for k, v := range c.Params() {
			params[k] = v
		}
		rec.last = &routed{name: name, params: params, pattern: c.RoutePattern()}
	}
}

// route serves one request through h on a Context of its own, and returns
// what the recorder saw of it, nil if no route recorded anything.
func (rec *recorder) route(h Handler, method, target string) *routed {
	return rec.routeOn(new(Context), h, method, target)
}

func (rec *recorder) routeOn(c *Context, h Handler, method, target string) *routed {
	rec.last = nil
	req := httptest.NewRequest(method, target, nil)
	c.Request = req
	h.ServeHTTP(c, req)
	return rec.last
}

// newTestRouter returns a router whose 404 and 405 handlers record the
// request as "404" and "405 <Allow>", so that it can be served without a
// connection.
func newTestRouter(rec *recorder) *Router {
	r := NewRouter()
	r.NotFound(rec.handler("404"))
	r.MethodNotAllowed(func(c *Context, req *stdhttp.Request) {
		rec.handler("405 "+c.Header().Get("Allow"))(c, req)
	})
	return r
}

func expectRoute(t *testing.T, got *routed, name string, params ...string) {
	t.Helper()
	if got == nil {
		t.Fatalf("got no route, want %s", name)
	}
	if got.name != name {
		t.Fatalf("got route %q, want %q", got.name, name)
	}
	want := map[string]string{}
	for i := 0; i+1 < len(params); i += 2 {
		want[params[i]] = params[i+1]
	}
	if len(got.params) != len(want) {
		t.Fatalf("%s: got params %v, want %v", name, got.params, want)
	}
	for k, v := range want {
		if got.params[k] != v {
			t.Fatalf("%s: got params %v, want %v", name, got.params, want)
		}
	}
}

func TestRouterMatching(t *testing.T) {
	rec := new(recorder)
	r := newTestRouter(rec)
	r.Get("/", rec.handler("root"))
	r.Get("/users", rec.handler("users"))
	r.Get("/users/", rec.handler("users/"))
	r.Get("/users/new", rec.handler("new"))
	r.Get("/users/{id:[0-9]+}", rec.handler("id"))
	r.Get("/users/{name}", rec.handler("name"))
	r.Get("/users/{id}/posts/{post}", rec.handler("post"))
	r.Get("/users/{id}/files/*", rec.handler("files"))
	r.Get("/archive/{year}-{month}-{day}", rec.handler("date"))
	r.Get("/file.{ext:[a-z]+}", rec.handler("ext"))
	r.Get("/v{version}/info", rec.handler("version"))
	r.Get("/static/{path...}", rec.handler("static"))
	r.Get("/hex/{h:[0-9a-f]{4}}", rec.handler("hex"))
	r.Get("/back/{a}/x", rec.handler("back-x"))
	r.Get("/back/{a:[0-9]+}/y", rec.handler("back-y"))
	r.Get("/catch/*", rec.handler("catch"))
	r.Get("/catch/exact", rec.handler("catch-exact"))

	tests := []struct {
		target string
		name   string
		params []string
	}{
		{"/", "root", nil},
		{"/users", "users", nil},
		{"/users/", "users/", nil},
		{"/users/new", "new", nil},
		{"/users/42", "id", []string{"id", "42"}},
		{"/users/bob", "name", []string{"name", "bob"}},
		{"/users/new2", "name", []string{"name", "new2"}},
		{"/users/7/posts/9", "post", []string{"id", "7", "post", "9"}},
		{"/users/7/files/", "files", []string{"id", "7", "*", ""}},
		{"/users/7/files/a/b.txt", "files", []string{"id", "7", "*", "a/b.txt"}},
		{"/archive/2026-10-01", "date", []string{"year", "2026", "month", "10", "day", "01"}},
		{"/file.json", "ext", []string{"ext", "json"}},
		{"/v2/info", "version", []string{"version", "2"}},
		{"/static/css/site.css", "static", []string{"path", "css/site.css"}},
		{"/static/", "static", []string{"path", ""}},
		{"/hex/beef", "hex", []string{"h", "beef"}},
		{"/hex/beefy", "404", nil},
		{"/back/12/y", "back-y", []string{"a", "12"}},
		{"/back/12/x", "back-x", []string{"a", "12"}},
		{"/catch/exact", "catch-exact", nil},
		{"/catch/exactly", "catch", []string{"*", "exactly"}},
		{"/users/7/posts/", "404", nil},
		{"/users//posts/9", "404", nil},
		{"/archive/2026-10", "404", nil},
		{"/file.JSON", "404", nil},
		{"/nope", "404", nil},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			expectRoute(t, rec.route(r, "GET", tt.target), tt.name, tt.params...)
		})
	}
}

func TestRouterMethods(t *testing.T) {
	rec := new(recorder)
	r := newTestRouter(rec)
	r.Get("/a", rec.handler("get"))
	r.Post("/a", rec.handler("post"))
	r.MethodFunc("PROPFIND", "/a", rec.handler("propfind"))
	r.Handle("DELETE /a", rec.handler("delete"))
	r.HandleFunc("/any", rec.handler("any"))
	r.Get("/any", rec.handler("any-get"))
	r.Head("/h", rec.handler("head"))
	r.Get("/h", rec.handler("h-get"))
	r.Put("/{id}", rec.handler("put"))

	expectRoute(t, rec.route(r, "GET", "/a"), "get")
	expectRoute(t, rec.route(r, "HEAD", "/a"), "get")
	expectRoute(t, rec.route(r, "POST", "/a"), "post")
	expectRoute(t, rec.route(r, "PROPFIND", "/a"), "propfind")
	expectRoute(t, rec.route(r, "DELETE", "/a"), "delete")
	expectRoute(t, rec.route(r, "PATCH", "/a"), "405 GET, HEAD, POST, DELETE, PROPFIND")
	expectRoute(t, rec.route(r, "PATCH", "/any"), "any")
	expectRoute(t, rec.route(r, "GET", "/any"), "any-get")
	expectRoute(t, rec.route(r, "HEAD", "/h"), "head")
	// A method a path does not serve keeps looking for a route that does.
	expectRoute(t, rec.route(r, "PUT", "/a"), "put", "id", "a")
	expectRoute(t, rec.route(r, "GET", "/b"), "405 PUT")
}

func TestRouterMiddleware(t *testing.T) {
	var trace []string
	mw := func(name string) func(Handler) Handler {
		return func(next Handler) Handler {
			return HandlerFunc(func(c *Context, r *stdhttp.Request) {
				trace = append(trace, name)
				next.ServeHTTP(c, r)
			})
		}
	}
	rec := new(recorder)
	r := newTestRouter(rec)
	r.Use(mw("root1"), nil, mw("root2"))
	r.Get("/", rec.handler("index"))
	r.With(mw("with")).Get("/with", rec.handler("with"))
	r.Group(func(g *Router) {
		g.Use(mw("group"))
		g.Get("/group", rec.handler("group"))
	})
	r.Route("/api/{version}", func(api *Router) {
		api.Use(mw("api"))
		api.Get("/", rec.handler("api"))
		api.Route("/users", func(users *Router) {
			users.Use(mw("users"))
			users.Get("/{id}", rec.handler("user"))
			users.NotFound(rec.handler("users-404"))
		})
	})
	r.Route("/admin", func(admin *Router) {
		admin.Use(mw("admin"))
		admin.Get("/", rec.handler("admin"))
	})

	tests := []struct {
		method, target string
		name           string
		trace          string
		params         []string
	}{
		{"GET", "/", "index", "root1 root2", nil},
		{"GET", "/with", "with", "root1 root2 with", nil},
		{"GET", "/group", "group", "root1 root2 group", nil},
		{"GET", "/api/v1", "api", "root1 root2 api", []string{"version", "v1"}},
		{"GET", "/api/v1/", "api", "root1 root2 api", []string{"version", "v1"}},
		{"GET", "/api/v1/users/7", "user", "root1 root2 api users", []string{"version", "v1", "id", "7"}},
		// What nothing under a Route serves is answered inside its middleware,
		// by its NotFound, or the nearest one above it.
		{"GET", "/api/v1/nope", "404", "root1 root2 api", []string{"version", "v1", "*", "nope"}},
		{"GET", "/api/v1/users/7/x", "users-404", "root1 root2 api users", []string{"version", "v1", "*", "7/x"}},
		{"GET", "/api/v1/users", "users-404", "root1 root2 api users", []string{"version", "v1"}},
		{"POST", "/api/v1/users/7", "405 GET, HEAD", "root1 root2 api users", []string{"version", "v1", "*", "7"}},
		{"GET", "/nope", "404", "root1 root2", nil},
		{"POST", "/admin", "405 GET, HEAD", "root1 root2 admin", nil},
		{"GET", "/admin/x", "404", "root1 root2 admin", []string{"*", "x"}},
		{"POST", "/with", "405 GET, HEAD", "root1 root2", nil},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.target, func(t *testing.T) {
			trace = nil
			expectRoute(t, rec.route(r, tt.method, tt.target), tt.name, tt.params...)
			if got := strings.Join(trace, " "); got != tt.trace {
				t.Fatalf("middleware ran %q, want %q", got, tt.trace)
			}
		})
	}
}

func TestRouterMount(t *testing.T) {
	var trace []string
	rec := new(recorder)
	sub := NewRouter()
	sub.Use(func(next Handler) Handler {
		return HandlerFunc(func(c *Context, r *stdhttp.Request) {
			trace = append(trace, "sub")
			next.ServeHTTP(c, r)
		})
	})
	sub.Get("/", rec.handler("sub-index"))
	sub.Get("/users/{id}", rec.handler("sub-user"))

	r := newTestRouter(rec)
	r.Mount("/api/{version}/", sub)
	r.Mount("/raw", HandlerFunc(func(c *Context, req *stdhttp.Request) {
		rec.handler("raw")(c, req)
	}))
	// A Router inside other middleware still routes what the Mount left.
	wrapped := NewRouter()
	wrapped.Get("/{x}", rec.handler("wrapped"))
	r.Mount("/wrapped", chain([]func(Handler) Handler{func(next Handler) Handler {
		return HandlerFunc(func(c *Context, req *stdhttp.Request) { next.ServeHTTP(c, req) })
	}}, wrapped))

	tests := []struct {
		target  string
		name    string
		pattern string
		params  []string
	}{
		{"/api/v2", "sub-index", "/api/{version}/", []string{"version", "v2"}},
		{"/api/v2/", "sub-index", "/api/{version}/", []string{"version", "v2", "*", ""}},
		{"/api/v2/users/9", "sub-user", "/api/{version}/users/{id}", []string{"version", "v2", "*", "users/9", "id", "9"}},
		// The mounted router has no NotFound of its own, so it was given this
		// router's.
		{"/api/v2/nope", "404", "", []string{"version", "v2", "*", "nope"}},
		{"/raw/a/b", "raw", "/raw/*", []string{"*", "a/b"}},
		{"/raw", "raw", "/raw", nil},
		{"/wrapped/7", "wrapped", "/wrapped/{x}", []string{"*", "7", "x", "7"}},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			trace = nil
			got := rec.route(r, "GET", tt.target)
			expectRoute(t, got, tt.name, tt.params...)
			if got.pattern != tt.pattern {
				t.Fatalf("RoutePattern %q, want %q", got.pattern, tt.pattern)
			}
			if strings.HasPrefix(tt.target, "/api") && len(trace) != 1 {
				t.Fatalf("mounted router's middleware ran %d times, want once", len(trace))
			}
		})
	}

	var walked []string
	_ = r.Walk(func(method, pattern string, _ Handler) error {
		walked = append(walked, method+" "+pattern)
		return nil
	})
	want := []string{"GET /api/{version}/users/{id}", "GET /api/{version}/", "* /raw", "* /raw/*", "* /wrapped", "* /wrapped/*"}
	slices.Sort(walked)
	slices.Sort(want)
	if !slices.Equal(walked, want) {
		t.Fatalf("Walk: %q, want %q", walked, want)
	}
}

func TestRouterReusedContext(t *testing.T) {
	// A Context served again, as a recycled one is, must not carry the
	// values of the request before.
	rec := new(recorder)
	r := newTestRouter(rec)
	r.Get("/a/{x}/{y}/{z}/{w}/{v}", rec.handler("five"))
	r.Get("/b", rec.handler("b"))
	sub := NewRouter()
	sub.Get("/{id}", rec.handler("sub"))
	r.Mount("/m/{m}", sub)
	c := new(Context)
	for range 3 {
		expectRoute(t, rec.routeOn(c, r, "GET", "/a/1/2/3/4/5"), "five", "x", "1", "y", "2", "z", "3", "w", "4", "v", "5")
		expectRoute(t, rec.routeOn(c, r, "GET", "/b"), "b")
		got := rec.routeOn(c, r, "GET", "/m/q/7")
		expectRoute(t, got, "sub", "m", "q", "*", "7", "id", "7")
		if got.pattern != "/m/{m}/{id}" {
			t.Fatalf("RoutePattern %q", got.pattern)
		}
		expectRoute(t, rec.routeOn(c, r, "GET", "/c"), "404")
		c.recycle()
		c.reopen()
	}
}

func TestRouterEscapedPath(t *testing.T) {
	rec := new(recorder)
	r := newTestRouter(rec)
	r.SetPathValues(true)
	var pathValue string
	r.Get("/files/{name}/meta", func(c *Context, req *stdhttp.Request) {
		pathValue = req.PathValue("name")
		rec.handler("meta")(c, req)
	})
	expectRoute(t, rec.route(r, "GET", "/files/a%2Fb/meta"), "meta", "name", "a/b")
	if pathValue != "a/b" {
		t.Fatalf("PathValue %q, want a/b", pathValue)
	}
	expectRoute(t, rec.route(r, "GET", "/files/a/b/meta"), "404")
}

func TestRouterPanics(t *testing.T) {
	h := func(*Context, *stdhttp.Request) {}
	tests := map[string]func(r *Router){
		"no slash":        func(r *Router) { r.Get("users", h) },
		"empty":           func(r *Router) { r.Get("", h) },
		"twice":           func(r *Router) { r.Get("/a/{x}", h); r.Get("/a/{y}", h) },
		"any twice":       func(r *Router) { r.Handle("/a", HandlerFunc(h)); r.HandleFunc("/a", h) },
		"custom twice":    func(r *Router) { r.MethodFunc("X", "/a", h); r.MethodFunc("X", "/a", h) },
		"use after":       func(r *Router) { r.Get("/a", h); r.Use(func(n Handler) Handler { return n }) },
		"sub use after":   func(r *Router) { g := r.With(); g.Get("/a", h); r.Use(func(n Handler) Handler { return n }) },
		"star in middle":  func(r *Router) { r.Get("/a/*/b", h) },
		"named catch mid": func(r *Router) { r.Get("/a/{p...}/b", h) },
		"adjacent params": func(r *Router) { r.Get("/{a}{b}", h) },
		"duplicate key":   func(r *Router) { r.Get("/{a}/{a}", h) },
		"unclosed":        func(r *Router) { r.Get("/{a", h) },
		"unopened":        func(r *Router) { r.Get("/a}", h) },
		"empty name":      func(r *Router) { r.Get("/{}", h) },
		"empty regexp":    func(r *Router) { r.Get("/{a:}", h) },
		"bad regexp":      func(r *Router) { r.Get("/{a:[}", h) },
		"nil handler":     func(r *Router) { r.Get("/a", nil) },
		"nil Handle":      func(r *Router) { r.Handle("/a", nil) },
		"bad method":      func(r *Router) { r.Handle("GE(T /a", HandlerFunc(h)) },
		"route catch-all": func(r *Router) { r.Route("/a/*", nil) },
		"mount catch-all": func(r *Router) { r.Mount("/a/*", HandlerFunc(h)) },
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("did not panic")
				}
			}()
			fn(NewRouter())
		})
	}
}

func TestRouterZeroValue(t *testing.T) {
	var r Router
	rec := new(recorder)
	r.Get("/a", rec.handler("a"))
	expectRoute(t, rec.route(&r, "GET", "/a"), "a")
	if err := r.Walk(func(string, string, Handler) error { return errors.New("stop") }); err == nil {
		t.Fatal("Walk did not return fn's error")
	}
}

// TestRouterServer serves a router over HTTP/1.1 and HTTP/2, for the
// default 404 and 405 responses, a body for GET left out of HEAD, and
// recycled Contexts.
func TestRouterServer(t *testing.T) {
	r := NewRouter()
	r.Get("/users/{id}", func(c *Context, req *stdhttp.Request) {
		_ = c.Respond(200, "text/plain", []byte("user "+c.Param("id")+" "+c.RoutePattern()))
	})
	r.Post("/users/{id}", func(c *Context, req *stdhttp.Request) {
		_, _ = c.Write([]byte("created " + c.Param("id")))
	})
	for _, reuse := range []bool{false, true} {
		config := DefaultConfig()
		if reuse {
			setReuseAll(&config)
		}
		addr := serve(t, NewHandlerWithConfig(config, r))
		var h1, h2 stdhttp.Protocols
		h1.SetHTTP1(true)
		h2.SetUnencryptedHTTP2(true)
		for name, protocols := range map[string]*stdhttp.Protocols{"HTTP/1.1": &h1, "HTTP/2": &h2} {
			client := &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: protocols}}
			t.Cleanup(client.CloseIdleConnections)
			do := func(method, path string) (*stdhttp.Response, string) {
				t.Helper()
				req, _ := stdhttp.NewRequest(method, "http://"+addr+path, nil)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("%s %s over %s: %v", method, path, name, err)
				}
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				return resp, string(body)
			}
			for i := range 3 {
				id := string(rune('a' + i))
				if resp, body := do("GET", "/users/"+id); resp.StatusCode != 200 || body != "user "+id+" /users/{id}" {
					t.Fatalf("%s GET: %d %q", name, resp.StatusCode, body)
				}
				if resp, body := do("POST", "/users/"+id); resp.StatusCode != 200 || body != "created "+id {
					t.Fatalf("%s POST: %d %q", name, resp.StatusCode, body)
				}
			}
			if resp, body := do("HEAD", "/users/x"); resp.StatusCode != 200 || body != "" {
				t.Fatalf("%s HEAD: %d %q", name, resp.StatusCode, body)
			}
			if resp, body := do("GET", "/nope"); resp.StatusCode != 404 || body != "404 page not found\n" {
				t.Fatalf("%s 404: %d %q", name, resp.StatusCode, body)
			}
			resp, body := do("DELETE", "/users/x")
			if resp.StatusCode != 405 || body != "405 method not allowed\n" || resp.Header.Get("Allow") != "GET, HEAD, POST" {
				t.Fatalf("%s 405: %d %q Allow %q", name, resp.StatusCode, body, resp.Header.Get("Allow"))
			}
		}
	}
}
