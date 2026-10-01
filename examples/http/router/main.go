//go:build linux || darwin || windows

// Command router serves a small REST API through fibhttp.Router: routes with
// parameters, a Route with middleware of its own, and a mounted router.
//
//	go run ./examples/http/router
//	curl http://127.0.0.1:8080/users/42
//	curl -X POST http://127.0.0.1:8080/users/42   # 405, Allow: GET, HEAD, DELETE
//	curl http://127.0.0.1:8080/api/v1/files/a/b.txt
package main

import (
	"flag"
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/logger"
	"github.com/lesismal/fib/middleware/recover"
	"github.com/lesismal/fib/middleware/requestid"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	flag.Parse()

	r := fibhttp.NewRouter()
	// Middleware on the root router sees every request, 404s included.
	r.Use(recover.New(), logger.New())

	r.Get("/", func(c *fibhttp.Context, req *stdhttp.Request) {
		reply(c, "index")
	})
	r.Route("/users", func(r *fibhttp.Router) {
		// Only the requests under /users get a request ID.
		r.Use(requestid.New())
		r.Get("/", func(c *fibhttp.Context, req *stdhttp.Request) {
			reply(c, "all users")
		})
		r.Get("/{id:[0-9]+}", func(c *fibhttp.Context, req *stdhttp.Request) {
			reply(c, "user "+c.Param("id"))
		})
		r.Delete("/{id:[0-9]+}", func(c *fibhttp.Context, req *stdhttp.Request) {
			reply(c, "deleted user "+c.Param("id"))
		})
		r.Get("/{name}", func(c *fibhttp.Context, req *stdhttp.Request) {
			reply(c, "user named "+c.Param("name"))
		})
	})
	r.Mount("/api/{version}", api())

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, fibhttp.NewHandler(r))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("HTTP router listening on http://%s", *addr))
}

// api is a router of its own, which routes what is left of the path under
// the pattern it is mounted at.
func api() *fibhttp.Router {
	r := fibhttp.NewRouter()
	r.Get("/files/*", func(c *fibhttp.Context, req *stdhttp.Request) {
		reply(c, fmt.Sprintf("file %q of API %s, routed by %s", c.Param("*"), c.Param("version"), c.RoutePattern()))
	})
	return r
}

func reply(c *fibhttp.Context, text string) {
	_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(text+"\n"))
}
