//go:build linux || darwin || windows

package http

import (
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
)

// benchRoutes are routes in the shape of a REST API's: static paths that
// share prefixes with one another and with parameterised ones.
var benchRoutes = []string{
	"GET /",
	"GET /health",
	"GET /users",
	"POST /users",
	"GET /users/me",
	"GET /users/{id}",
	"PUT /users/{id}",
	"DELETE /users/{id}",
	"GET /users/{id}/repos",
	"GET /users/{id}/repos/{repo}",
	"GET /users/{id}/repos/{repo}/issues/{issue:[0-9]+}",
	"GET /users/{id}/repos/{repo}/issues/{issue}/comments/{comment}",
	"GET /orgs/{org}/teams",
	"GET /orgs/{org}/members/{user}",
	"GET /static/*",
	"GET /archive/{year}-{month}",
}

func benchRouter(b *testing.B, middlewares int) *Router {
	r := NewRouter()
	for range middlewares {
		r.Use(func(next Handler) Handler {
			return HandlerFunc(func(c *Context, req *stdhttp.Request) { next.ServeHTTP(c, req) })
		})
	}
	nop := func(*Context, *stdhttp.Request) {}
	for _, route := range benchRoutes {
		r.HandleFunc(route, nop)
	}
	r.NotFound(nop)
	r.MethodNotAllowed(nop)
	return r
}

// benchRoute serves target through r on one Context, as a server that
// recycles its Contexts does, so that what is measured is the routing.
func benchRoute(b *testing.B, r Handler, method, target string) {
	req := httptest.NewRequest(method, target, nil)
	c := &Context{Request: req}
	r.ServeHTTP(c, req)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		r.ServeHTTP(c, req)
	}
}

func BenchmarkRouter(b *testing.B) {
	r := benchRouter(b, 0)
	for _, bench := range []struct{ name, method, target string }{
		{"Root", "GET", "/"},
		{"Static", "GET", "/users/me"},
		{"Param1", "GET", "/users/12345"},
		{"Param2", "GET", "/users/12345/repos/fib"},
		{"Param4", "GET", "/users/12345/repos/fib/issues/42/comments/7"},
		{"Regexp", "GET", "/users/12345/repos/fib/issues/42"},
		{"Tail", "GET", "/archive/2026-10"},
		{"CatchAll", "GET", "/static/css/site/main.css"},
		{"NotFound", "GET", "/nope/nothing"},
		{"MethodNotAllowed", "PATCH", "/users"},
	} {
		b.Run(bench.name, func(b *testing.B) { benchRoute(b, r, bench.method, bench.target) })
	}
}

func BenchmarkRouterMiddleware(b *testing.B) {
	benchRoute(b, benchRouter(b, 4), "GET", "/users/12345/repos/fib")
}

// BenchmarkRouterFreshContext routes each request on a Context of its own,
// as a server that does not recycle them does, which is where the route
// state a request with parameters needs is allocated.
func BenchmarkRouterFreshContext(b *testing.B) {
	r := benchRouter(b, 0)
	for _, bench := range []struct{ name, target string }{
		{"Static", "/users/me"},
		{"Param2", "/users/12345/repos/fib"},
	} {
		b.Run(bench.name, func(b *testing.B) {
			req := httptest.NewRequest("GET", bench.target, nil)
			b.ReportAllocs()
			for b.Loop() {
				c := &Context{Request: req}
				r.ServeHTTP(c, req)
			}
		})
	}
}
