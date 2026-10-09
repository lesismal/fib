//go:build linux || darwin || windows

// Package routerbench compares fibhttp.Router with chi and net/http's
// ServeMux on the routes of GitHub's API. It is a module of its own so that
// fib does not depend on chi.
//
//	cd http/routerbench && go test -bench . -benchmem
package routerbench

import (
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	fibhttp "github.com/lesismal/fib/http"
)

// sink keeps the parameter a handler reads from being optimised away.
var sink string

// router serves one request the way a router under test does.
type router interface {
	serve(method, target string) func()
}

type fibRouter struct{ r *fibhttp.Router }

func newFib(param string) router {
	r := fibhttp.NewRouter()
	for _, route := range githubAPI {
		r.MethodFunc(route.method, route.path, func(c *fibhttp.Context) {
			if param != "" {
				sink = c.Param(param)
			}
		})
	}
	// The default 404 writes to the connection, which these Contexts have
	// none of.
	r.NotFound(func(*fibhttp.Context) {})
	return fibRouter{r}
}

func (f fibRouter) serve(method, target string) func() {
	req := httptest.NewRequest(method, target, nil)
	// One Context for every request, as a server recycling them gives.
	c := &fibhttp.Context{Request: req}
	return func() { f.r.ServeHTTP(c) }
}

type chiRouter struct{ r *chi.Mux }

func newChi(param string) router {
	r := chi.NewRouter()
	for _, route := range githubAPI {
		r.MethodFunc(route.method, route.path, func(_ stdhttp.ResponseWriter, req *stdhttp.Request) {
			if param != "" {
				sink = chi.URLParam(req, param)
			}
		})
	}
	r.NotFound(func(stdhttp.ResponseWriter, *stdhttp.Request) {})
	return chiRouter{r}
}

func (c chiRouter) serve(method, target string) func() {
	req := httptest.NewRequest(method, target, nil)
	var w nopWriter
	return func() { c.r.ServeHTTP(&w, req) }
}

type muxRouter struct{ m *stdhttp.ServeMux }

func newMux(param string) router {
	m := stdhttp.NewServeMux()
	for _, route := range githubAPI {
		m.HandleFunc(route.method+" "+route.path, func(_ stdhttp.ResponseWriter, req *stdhttp.Request) {
			if param != "" {
				sink = req.PathValue(param)
			}
		})
	}
	return muxRouter{m}
}

func (m muxRouter) serve(method, target string) func() {
	req := httptest.NewRequest(method, target, nil)
	var w nopWriter
	return func() { m.m.ServeHTTP(&w, req) }
}

type nopWriter struct{ h stdhttp.Header }

func (w *nopWriter) Header() stdhttp.Header {
	if w.h == nil {
		w.h = stdhttp.Header{}
	}
	return w.h
}
func (*nopWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*nopWriter) WriteHeader(int)             {}

var routers = []struct {
	name string
	new  func(param string) router
}{
	{"fib", newFib},
	{"chi", newChi},
	{"ServeMux", newMux},
}

// concrete is path with a value in place of each parameter.
func concrete(path string) string {
	var b strings.Builder
	for {
		i := strings.IndexByte(path, '{')
		if i < 0 {
			b.WriteString(path)
			return b.String()
		}
		j := strings.IndexByte(path, '}')
		b.WriteString(path[:i])
		b.WriteString("v-" + path[i+1:j])
		path = path[j+1:]
	}
}

func BenchmarkGitHub(b *testing.B) {
	for _, bench := range []struct {
		name, method, target, param string
	}{
		{"Static", "GET", "/user/repos", ""},
		{"Param1", "GET", "/users/lesismal", "user"},
		{"Param2", "GET", "/repos/lesismal/fib/branches", "repo"},
		{"Param3", "GET", "/repos/lesismal/fib/issues/42/comments", "number"},
		{"Param4", "GET", "/legacy/issues/search/lesismal/fib/open/router", "keyword"},
		{"NotFound", "GET", "/nothing/here/at/all", ""},
	} {
		for _, r := range routers {
			b.Run(bench.name+"/"+r.name, func(b *testing.B) {
				serve := r.new(bench.param).serve(bench.method, bench.target)
				b.ReportAllocs()
				for b.Loop() {
					serve()
				}
			})
		}
	}
	// All serves every route once an iteration.
	for _, r := range routers {
		b.Run("All/"+r.name, func(b *testing.B) {
			router := r.new("")
			serves := make([]func(), len(githubAPI))
			for i, route := range githubAPI {
				serves[i] = router.serve(route.method, concrete(route.path))
			}
			b.ReportAllocs()
			for b.Loop() {
				for _, serve := range serves {
					serve()
				}
			}
		})
	}
}

// TestRoutersAgree checks that each router serves every route of the API.
func TestRoutersAgree(t *testing.T) {
	for _, route := range githubAPI {
		target := concrete(route.path)
		last := route.path[strings.LastIndexByte(route.path, '{')+1:]
		param, _, _ := strings.Cut(last, "}")
		if !strings.Contains(route.path, "{") {
			param = ""
		}
		want := ""
		if param != "" {
			want = "v-" + param
		}
		for _, r := range routers {
			sink = "unset"
			r.new(param).serve(route.method, target)()
			if param == "" && sink == "unset" {
				continue
			}
			if sink != want {
				t.Errorf("%s %s %s: param %q = %q, want %q", r.name, route.method, target, param, sink, want)
			}
		}
	}
}
