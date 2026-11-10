# Router

[English](04-router.md) | [简体中文](04-router.zh-CN.md)

`fibhttp.Router` routes a request to the handler registered for its method and path, with the API of `github.com/go-chi/chi` but fib's handlers — `func(*fibhttp.Context)` instead of `http.HandlerFunc`. It is itself a `fibhttp.Handler`, so it drops into `fibhttp.NewHandler(r)` like any other handler. This chapter covers patterns and their matching order, groups and mounted sub-routers, 404 and 405 handling, and how a `net/http` mux coexists with it.

## Patterns and matching order

A pattern is a path of static text, parameters and an optional catch-all:

- `{name}` matches a non-empty value up to the next `/`, or up to the static text that follows it, as `{year}` does in `/archive/{year}-{month}`. A value never contains a `/`.
- `{name:regexp}` matches a value the regular expression matches as a whole, such as `{id:[0-9]+}`.
- `*` at the end of a pattern matches the rest of the path, `/` included and possibly empty, under the key `"*"`; `{name...}` does the same under `name`.

When several routes match, static text wins over a parameter, a parameter with a regular expression wins over one without, and a parameter wins over a catch-all, segment by segment, with backtracking: with `/users/new` and `/users/{id}` both registered, `/users/new` matches the first and `/users/new2` the second. A route registered for GET also serves HEAD unless HEAD has a route of its own. A path a route matches but whose method it does not serve is answered `405 Method Not Allowed` with an `Allow` header; any other path `404 Not Found`. Parameters are read with `c.Param(name)`; the path routed is `URL.RawPath` when the request has one, so an escaped `%2F` belongs to the value instead of splitting it, and `Param` decodes it.

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	r := fibhttp.NewRouter()
	r.Get("/", func(c *fibhttp.Context) {
		_ = c.String(200, "index\n")
	})
	r.Get("/users/{id:[0-9]+}", func(c *fibhttp.Context) {
		_ = c.String(200, "user %s\n", c.Param("id")) // a value that is all digits
	})
	r.Get("/users/{name}", func(c *fibhttp.Context) {
		_ = c.String(200, "user named %s\n", c.Param("name"))
	})
	r.Get("/files/*", func(c *fibhttp.Context) {
		_ = c.String(200, "file %s\n", c.Param("*")) // the rest of the path, "/" included, may be empty
	})
	r.Post("/users", func(c *fibhttp.Context) {
		_ = c.String(201, "created\n")
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(r),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

`Get`, `Post`, `Put`, `Delete`, `Patch`, `Head`, `Options`, `Connect` and `Trace` cover the standard methods; `Method` and `MethodFunc` take any method token (for example `PROPFIND`), and `Handle` takes a pattern that begins with the method, as `net/http.ServeMux` does: `r.Handle("DELETE /users/{id}", deleteUser)`. A `Handle` pattern without a method matches every method.

## Groups and scoping middleware

`Use` wraps a whole router, so its middleware sees every request, 404s and 405s included. `Route` creates a sub-router under a prefix (which may contain parameters but no catch-all): its routes join the same tree and are routed in one walk, its `Use` middleware wraps only that prefix, and its `NotFound`/`MethodNotAllowed` handlers apply only there. `Group` registers routes through a router whose middleware wraps only those; `With` does the same for a single route. Middleware must be added before the routes it wraps — `Use` panics once a route has been registered — and every route must be registered before the router starts serving.

`Mount` is different: it hands everything under a prefix to another handler. A mounted `*Router` routes the rest of the path from its own `/`, with its own middleware, as chi does; any other handler gets the request as it is, with the rest of the path in `c.Param("*")`.

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/recover"
)

func main() {
	api := fibhttp.NewRouter()
	api.Use(recover.New())
	api.Get("/status", func(c *fibhttp.Context) {
		_ = c.String(200, "ok\n")
	})

	r := fibhttp.NewRouter()
	r.Route("/users", func(r *fibhttp.Router) {
		r.Get("/", func(c *fibhttp.Context) {
			_ = c.String(200, "user list\n")
		})
		r.Get("/{id:[0-9]+}", func(c *fibhttp.Context) {
			_ = c.String(200, "user %s\n", c.Param("id"))
		})
	})
	r.Mount("/api", api) // the mounted router routes from its own "/"

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(r),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

`/api/status` is served by the mounted router; `/users/42` by the `/users` route. A request to `/api/missing` is answered by the mounted router's 404 (or this router's, if it set none), inside the mounted router's middleware.

## 404, 405 and path values

The default 404 and 405 are plain responses. To customise them, register handlers: `r.NotFound(fn)` for a path no route matches, and `r.MethodNotAllowed(fn)` for a path that matches but not with this method. The `Allow` header is already set in `c.Header()` when the 405 handler runs, so a response written with `Respond` or `WriteResponse` must carry it over itself:

```go
r.NotFound(func(c *fibhttp.Context) {
	_ = c.JSON(404, map[string]string{"error": "not found"})
})
r.MethodNotAllowed(func(c *fibhttp.Context) {
	_ = c.String(405, "method %s not allowed\n", c.Request.Method) // c.Header() already has Allow
})
```

A router sets route parameters only for `c.Param`. `r.SetPathValues(true)` makes every request carry them in `c.Request.PathValue` too, so a handler written for `net/http.ServeMux` finds its values; it costs one allocation per request with parameters, where `Param` costs none.

## Routing a `net/http` mux

The router is a `fibhttp.Handler`, and so is a thin adapter over any `net/http.Handler`. That makes the standard library's `ServeMux` a valid route target, including its method patterns and `PathValue`:

```go
package main

import (
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		fmt.Fprintf(w, "hello %s\n", r.PathValue("name"))
	})

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		mux.ServeHTTP(c, c.Request) // Context is the http.ResponseWriter
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

A `ServeMux` mounted this way answers its own 404 and 405. The router's own parameters — one in a `Mount` pattern, for example — reach the mux's handlers as `PathValue`s only with `SetPathValues(true)`; the mux's own patterns set theirs regardless. Handlers from other routers work the same way — the README's compatibility notes for `net/http` handlers apply to all of them.

## Performance notes

The router is a radix tree: static children are indexed by first byte, recursion happens only at a node with backtracking branches, and a parameter value is a substring of the path, not a copy. Its routing state lives on the `Context` and is reused with it, so a routed request allocates nothing when the context is reused; with context reuse off, a parameterised or mounted request allocates once. Registration and serving are not safe to run concurrently: build the router, then serve.

## Pitfalls

- **`Use` after the first route panics.** Add middleware first, then routes; `Route`'s callback is the place for a prefix's middleware.
- **`With`/`Group` middleware is not a catch-all.** It wraps only the routes registered through that router, not every request the parent sees; use `Use` on the parent for that.
- **`Route` is not `Mount`.** A `Route`'s routes are appended to the same tree and routed in one walk; only `Mount` nests a second router and re-routes the rest of the path.
- **A catch-all cannot be followed by anything.** `*` and `{name...}` must end the pattern, and a `Route`/`Mount` prefix cannot contain one.
- **`c.Param` values are borrowed.** They are substrings of the path and are invalid after the handler returns; copy them to keep them.
- **405 handlers must set `Allow` themselves** when they answer with `Respond` or `WriteResponse`; only the `ResponseWriter` path sends the header the router filled in.
- **Don't register routes while serving.** A `Router` is not safe for concurrent registration and dispatch; build it at startup.
- **Trailing slashes are not redirected.** Register the path you mean; a `Route` prefix matches both `/users` and `/users/` for its own `/`, but a plain route does not silently redirect.

## See also

- [Middleware](05-middleware.md) — the middleware `Use`, `With` and `Group` take
- [Handlers: sync and async](03-handlers.md) — what a route handler is and how it answers
- [HTTP/1.x](06-http1.md) — the request and response helpers the handlers use
- [Hacks](../hacks.md) — zero-copy `Param` and the object-reuse rule
- [Router benchmark README](../../http/routerbench/README.md) — fib's router against chi and `http.ServeMux`
- [The README](../../README.md) — the router example and the `net/http` compatibility notes
