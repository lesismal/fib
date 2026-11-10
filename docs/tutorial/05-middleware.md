# Middleware

[English](05-middleware.md) | [简体中文](05-middleware.zh-CN.md)

A middleware is a function from a handler to a handler: it runs something before the handler, after it, or instead of it. fib ships ten of them — `compress`, `cors`, `csrf`, `etag`, `limiter`, `logger`, `pprof`, `recover`, `requestid`, `responsetime` — and the same signature works for your own, on a whole router or on one route group. This chapter covers composition and order, the signature and the response hooks, the built-ins and their key options, and how to scope middleware to a prefix.

## Composition and order

`middleware.Chain(handler, middlewares...)` wraps a handler in middlewares, the **first of them outermost**: it sees each request first and the response last. A middleware that answers the request itself and returns skips everything inside it — that is the early return. The `Middleware` type is a plain function, so a `[]middleware.Middleware` spreads into `Chain` and into `Router.Use` as it is:

```go
handler := middleware.Chain(app,
	recover.New(), // outermost: a panic becomes a 500
	logger.New(),  // sees the request, and the response on the way out
	compress.New(),
	cors.New(),
)
```

Order matters in two ways. **Response-transforming** middleware must nest consistently: `compress` outside `etag`, so that the ETag is made from the body the handler wrote rather than the compressed one. And an **early-returning** middleware, such as `cors` answering a preflight or `limiter` refusing a request, must be outside the handlers it protects but inside whichever middleware must still observe it — `recover` outermost means even a panic while answering is caught.

## Writing a middleware

The signature is `func(next fibhttp.Handler) fibhttp.Handler`, or `middleware.Middleware` when the compiler needs the name. The wrapper is usually a `fibhttp.HandlerFunc`; it may change the request before calling `next`, answer instead of calling it, and change the response after:

```go
func requireToken(next fibhttp.Handler) fibhttp.Handler {
	return fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.Header.Get("Authorization") == "" {
			_ = c.String(stdhttp.StatusUnauthorized, "missing token\n")
			return // an early return: next does not run
		}
		c.Header().Set("X-Served-By", "fib") // sent with whatever next answers
		next.ServeHTTP(c)
	})
}
```

A middleware that must change the response a handler writes does so through the `Context` hooks rather than by wrapping the writer: `OnHeader(func(status int, header http.Header))` runs as the response header is settled and may change it, `OnResponse(func(*fibhttp.Response))` hands over the whole response before it is sent, and `OnFinish(func(status int, header http.Header, size int64))` runs once it has been handed to the connection. Hooks run in the reverse of the order they were registered, like deferred calls, so the outermost middleware sees the response last. That is how `compress`, `etag`, `cors` and `requestid` work, which is why they work whichever way the handler answers — `Respond`, `WriteResponse`, the helpers or the `ResponseWriter` methods.

A field set with `c.Header().Set` before calling `next`, as above, goes out with whatever the handler answers — `Respond`, `WriteResponse`, a helper such as `String` or `JSON`, or the `ResponseWriter` methods. The handler's own fields win: its `Content-Type`, or a field of the same name in a `WriteResponse` header, replaces the middleware's. Once the handler has answered, changing `c.Header()` has no effect; to add a field that depends on the status the handler chose, or to change one it sent, use `OnHeader`.

Every built-in `Config` also has a `Next` skipper: when it reports true, the middleware passes the request straight on.

```go
compress.New(compress.Config{
	Next: func(c *fibhttp.Context) bool { // never hold an event stream to compress it
		return strings.HasPrefix(c.Request.URL.Path, "/events")
	},
})
```

## The built-in middleware

An application usually composes a handful. This complete program runs four of them around one handler:

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/compress"
	"github.com/lesismal/fib/middleware/cors"
	"github.com/lesismal/fib/middleware/logger"
	"github.com/lesismal/fib/middleware/recover"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(200, "hello\n")
	})
	handler := middleware.Chain(app,
		recover.New(),  // outermost: a panic becomes a 500, the connection survives
		logger.New(),   // one record per response
		compress.New(), // gzip/deflate when the client accepts it
		cors.New(),     // answers preflights, adds Access-Control-*
	)
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(handler),
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

The smaller ones are composed the same way, each with its own `Config`:

```go
handler := middleware.Chain(app,
	requestid.New(), // X-Request-ID: the client's, or a new UUID; on request and response
	responsetime.New(responsetime.Config{Header: "X-Took"}), // default X-Response-Time
	etag.New(etag.Config{Weak: true}),                       // ETag, 304 on If-None-Match
	limiter.New(limiter.Config{Max: 100, Expiration: time.Minute, SlidingWindow: true}),
	csrf.New(csrf.Config{TrustedOrigins: []string{"https://app.example.com"}}),
)
```

What each one does, and the options that matter:

- **`compress`** — gzip or deflate when `Accept-Encoding` allows, the body is at least `MinLength` (1 KiB by default), its media type compresses (`Compressible`), and no `Content-Encoding`, partial response or forbidding `Cache-Control` is in the way. It adds `Vary`, weakens a strong ETag, and holds a whole response until the handler returns; give it a `Next` that skips SSE and long downloads.
- **`etag`** — generates a 200 GET/HEAD response's ETag from the body and answers a matching `If-None-Match` with 304 and no body; a handler's own ETag is kept. `Weak: true` makes validators weak.
- **`cors`** — `AllowOrigins` (any origin by default; `"https://*.example.com"` matches its subdomains), `AllowOriginsFunc`, `AllowMethods` (GET, POST, HEAD, PUT, DELETE, PATCH by default), `AllowHeaders` (by default whatever the preflight asks for), `ExposeHeaders`, `AllowCredentials`, `AllowPrivateNetwork`, `MaxAge`; it answers preflights itself with 204. `AllowCredentials` needs explicit origins: with the default any-origin, `New` panics.
- **`csrf`** — a safe request (GET, HEAD, OPTIONS, TRACE) without the token cookie (`csrf_` by default) gets one; an unsafe request is served only when it sends the same token back and `net/http`'s `CrossOriginProtection` finds it same-origin, and is answered 403 otherwise (`ErrorHandler`). Read the token to embed in a form with `csrf.Token(r)`; `Lookup` says where an unsafe request carries it (`header:X-CSRF-Token` by default, or `form:<field>`, `query:<name>`), and `TrustedOrigins` allows a cross-origin caller.
- **`limiter`** — at most `Max` requests (60 by default) per `Expiration` window (one minute by default) per key, the client's address unless `KeyGenerator` says otherwise. It answers `429` with `Retry-After` (or calls `LimitReached`), and every response carries `X-RateLimit-Limit`, `-Remaining` and `-Reset` unless `DisableHeaders`; `SlidingWindow` removes the edge between two windows, and `SkipFailedRequests`/`SkipSuccessfulRequests` leave responses out of the count. The counts live in this process's memory, and the default key is the TCP peer: behind a proxy or load balancer, that is the proxy, so give it a `KeyGenerator` that reads the header your proxy sets.
- **`logger`** — one record per response, once it has been handed to the connection, through package `fib/log` (`http: access status=200 latency=31µs ip=127.0.0.1 method=GET path=/`); `Fields` chooses what it carries from the tags in the package documentation (`status`, `latency`, `ip`, `method`, `path` by default; also `url`, `ua`, `bytesSent`, `reqHeader:Name` and more), `Level` sets the level, and `Format`/`Output` write a line of a chosen layout straight to a writer instead, such as `logger.Config{Format: "${status} ${method} ${path} ${latency}\n", Output: os.Stdout}`.
- **`recover`** — turns a panic while a handler runs into a 500 instead of the loss of the connection; `Handler` answers, `Log` reports (to `fib/log` at error level by default) and `DisableStackTrace` shortens the report. A panic with `http.ErrAbortHandler` is not logged and, on HTTP/1, closes the connection. It recovers only the handler's own panics: work handed to a goroutine of its own must recover itself.
- **`requestid`** — sets `X-Request-ID` (or `Header`) on the request and the response, reusing the client's value when it is printable ASCII of at most 128 bytes, or generating one with `Generator` (a random UUID by default); `requestid.FromRequest(r)` reads it when the default header is used (read `r.Header.Get(yourHeader)` otherwise), `requestid.UUID()` generates one. `logger` records it with the `reqHeader:X-Request-ID` field.
- **`responsetime`** — reports how long the handler took to settle its response in `X-Response-Time` (or `Header`), formatted by `Format` (milliseconds by default, such as `0.017ms`).
- **`pprof`** — serves the runtime profiles under `/debug/pprof/` (`Prefix`), gathering each profile on the engine's handler pool so the worker does not sit through a CPU profile. See below.

## Scoping middleware to routes

On a `Router`, `Use` wraps the whole router — 404s and 405s included — while `Route`'s own `Use` wraps only its prefix, `Group` only the routes registered through it, and `With` only one route. `[]middleware.Middleware` passes to all of them unchanged:

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

// requireToken is a middleware: it takes the next handler and returns one that
// does something before or after it.
func requireToken(next fibhttp.Handler) fibhttp.Handler {
	return fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.Header.Get("Authorization") == "" {
			_ = c.String(stdhttp.StatusUnauthorized, "missing token\n")
			return // returning here skips next: the early return
		}
		c.Header().Set("X-Served-By", "fib") // sent with whatever next answers
		next.ServeHTTP(c)
	})
}

func main() {
	r := fibhttp.NewRouter()
	r.Route("/admin", func(r *fibhttp.Router) {
		r.Use(requireToken) // wraps only the routes registered under /admin
		r.Get("/stats", func(c *fibhttp.Context) {
			_ = c.String(200, "stats\n")
		})
	})
	r.Get("/", func(c *fibhttp.Context) {
		_ = c.String(200, "public\n")
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

`/` is public and `/admin/stats` requires the header:

```sh
curl http://127.0.0.1:8080/                                   # public
curl -i http://127.0.0.1:8080/admin/stats                     # 401 missing token
curl -i -H 'Authorization: Bearer x' http://127.0.0.1:8080/admin/stats  # stats, with X-Served-By: fib
curl -i http://127.0.0.1:8080/admin/nope                      # 401 too: the prefix's 404s are inside its middleware
```

The middleware must be added before the routes it wraps; `Use` panics once a route has been registered.

## pprof wiring

`pprof.New()` is an ordinary middleware: it answers the paths under its prefix itself and passes everything else on, so it slots into a `Chain` in front of the application handler, or into a router's `Use` — not into `Mount`, which takes a handler. No separate admin port is needed, though you can serve it on a second binding (a loopback address, say) with a handler of its own. The profiles reveal a lot about the process; expose them only where the people who reach them are trusted.

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/pprof"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(200, "ok\n")
	})
	// pprof answers /debug/pprof/ itself and passes every other path on to app.
	handler := middleware.Chain(app, pprof.New())

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(handler),
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

Then read a profile with the Go tool:

```sh
curl http://127.0.0.1:8080/debug/pprof/                     # the index
go tool pprof http://127.0.0.1:8080/debug/pprof/heap
go tool pprof http://127.0.0.1:8080/debug/pprof/profile?seconds=10
```

## Pitfalls

- **Order is semantics.** `recover` outermost, `compress` outside `etag`, an early-returning middleware outside the handlers it protects; reversing two of them changes behavior, not just layout.
- **Middleware must be added before routes.** `Router.Use` panics after the first registration; build the stack at startup.
- **`With`/`Group` is not global.** Their middleware wraps only routes registered through them, not the 404s or other routes the parent sees.
- **A middleware must not block.** It runs on the same worker as the handler; a slow middleware is a slow handler.
- **Response hooks have limits.** `compress` and `etag` hold a whole response, so a streaming response must skip them with `Next`, or it stops streaming; `OnResponse` also disables sendfile and HTTP/1 `Flush` for that response.
- **`recover` cannot catch another goroutine's panic.** A handler that answers from a goroutine must recover there itself.
- **Middleware answering instead of the handler must not also call `next`.** Calling `next` after writing a response corrupts it; return.
- **`c.Header().Set` in a middleware goes before `next`.** The handler's own fields of the same name win, and after it has answered the change has no effect; use `OnHeader` to override what the handler sent or to set a field after it.
- **`cors` with `AllowCredentials` needs `AllowOrigins`.** Credentials cannot go with the default any-origin; `cors.New` panics at startup.
- **`limiter` behind a proxy counts the proxy.** Give it a `KeyGenerator` that reads the client address your proxy forwards, and remember each process counts on its own.

## See also

- [Router](04-router.md) — `Use`, `Route`, `Group` and `With`
- [Handlers: sync and async](03-handlers.md) — the response hooks a middleware builds on
- [Configuration](../configuration.md#logging) — how `fib/log` is set up and how the `logger` middleware writes its records
- [Performance and operations](22-performance.md) — `pprof` in production, logging setup, load testing
- [Hacks](../hacks.md) — the object-reuse rules a middleware must respect
- [The README](../../README.md) — the middleware list and the quick start
