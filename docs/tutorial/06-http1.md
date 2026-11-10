# HTTP/1.x

[English](06-http1.md) | [简体中文](06-http1.zh-CN.md)

`fib/http` is fib's HTTP/1.0 and HTTP/1.1 server and client: request parsing, response framing, chunked bodies with trailers, `sendfile` for files, and `net/http` compatibility. This chapter teaches the server side by usage — reading a request, answering with the helpers, streaming, trailers, static files, uploads and `net/http` handlers — and leaves the exhaustive protocol notes to [HTTP/1.x](../http1.md). It assumes the engine from chapter [01](01-getting-started.md) and the handler model from chapter [03](03-handlers.md); the same `*fibhttp.Context` handler also serves HTTP/2 and HTTP/3, which chapters [07](07-http2.md) and [08](08-http3.md) cover.

## The model

An HTTP/1.x connection carries one request at a time. The engine worker that reads the connection parses it and calls the handler on that same worker, so nothing the handler does runs beside the reading of its own connection: blocking the handler holds that connection, and an HTTP/1 connection has no other stream to keep busy. Whatever the handler writes is queued and leaves in one batched write (`writev`) at the end of the round, which is why a handler that answers with `Respond`, `WriteResponse` or a few `Write` calls costs one system call. Pipelined requests are answered in request order; a request held open with `Retain` holds the ones pipelined behind it until it is released. All of this is HTTP/1-specific: on HTTP/2 and HTTP/3 handlers run on a pool and block only themselves.

## A first server

One handler serves everything: the path picks the shape of the answer, `Query` reads a parameter without allocating, `Body` is the request body without a copy, and `JSON`/`String`/`Data`/`HTML` write the common responses.

```go
package main

import (
	"fmt"
	stdhttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/users":
			name := c.Query("name") // decoded like URL.Query().Get, without building the map
			if name == "" {
				name = "guest"
			}
			_ = c.JSON(stdhttp.StatusOK, user{ID: 1, Name: name})
		case "/echo":
			_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", c.Body())
		case "/greet":
			_ = c.String(stdhttp.StatusOK, "hello, %s\n", c.Query("name"))
		case "/set-cookie":
			stdhttp.SetCookie(c, &stdhttp.Cookie{Name: "session", Value: "abc", Path: "/"})
			_ = c.String(stdhttp.StatusOK, "cookie set\n")
		case "/events":
			// A streaming response outlives Config.WriteTimeout: lift the deadline first.
			_ = c.SetWriteDeadline(time.Time{})
			c.Header().Set("Content-Type", "text/event-stream")
			c.Header().Set("Cache-Control", "no-cache")
			for i := 0; ; i++ {
				if _, err := fmt.Fprintf(c, "data: event %d\n\n", i); err != nil {
					return // the client went away
				}
				c.Flush() // send what is written now, instead of at the handler's return
				select {
				case <-time.After(time.Second):
				case <-c.Context().Done():
					return
				}
			}
		default:
			_ = c.HTML(stdhttp.StatusNotFound, "<h1>not found</h1>")
		}
	})

	config := fibhttp.DefaultConfig()
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandlerWithConfig(config, app),
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

`fibhttp.NewHandler(app)` uses the default config; `NewHandlerWithConfig` is how per-server options arrive. The same handler serves HTTP/2 and HTTP/3 on the same port when the binding asks for them (chapters 07 and 08).

## Reading the request

`Context.Request` is a standard `*stdhttp.Request`, so every `net/http` accessor works. The three fib additions are worth knowing: `Body` returns the body the server already buffered, `Query` decodes one parameter without building the map `URL.Query()` builds on every call, and both are borrowed until the response ends.

```go
func inspect(c *fibhttp.Context) {
	name := c.Query("name")                    // no map, no allocation for a plain value
	body := c.Body()                           // the server's buffer, not io.ReadAll's copy
	ctype := c.Request.Header.Get("Content-Type")
	cookie, err := c.Request.Cookie("session") // the standard net/http request API
	_ = name
	_ = ctype
	if err == nil {
		_ = c.String(stdhttp.StatusOK, "cookie=%s body=%d bytes\n", cookie.Value, len(body))
	}
}
```

`Body` returns nil for a request without a body and for one whose body streams; `Request.Body` is the reader in every case. The bytes are the handler's only until the response is finished, so keep a copy if they must outlive the handler.

## Answering: the response helpers

`Respond` sends a whole response in one call, `WriteResponse` sends status, header, body and trailer at once, and the small helpers wrap the common content types. There is no `Redirect` helper: set the header and the status as `net/http` does.

```go
func render(c *fibhttp.Context) {
	switch c.Request.URL.Path {
	case "/json":
		_ = c.JSON(stdhttp.StatusOK, map[string]int{"n": 42})
	case "/xml":
		_ = c.XML(stdhttp.StatusOK, struct{ N int }{42})
	case "/html":
		_ = c.HTML(stdhttp.StatusOK, "<h1>fib</h1>")
	case "/redirect":
		c.Header().Set("Location", "/login")
		c.WriteHeader(stdhttp.StatusFound)
	default:
		_ = c.String(stdhttp.StatusOK, "n=%d", 42)
	}
}
```

`Data(status, contentType, body)`, `IndentedJSON`, `IndentedXML`, `HTMLTemplate`, `File` and `FileAttachment` complete the set. A helper that encodes returns the encoder's error and sends nothing, so a handler can still answer another way. `Context` is an `http.ResponseWriter`, `http.Flusher` and `io.ReaderFrom`, so header, status and body written by hand behave as `net/http` describes.

## Streaming responses and SSE

`Write` followed by `Flush` streams: the header goes out with the first flush, the body as it is written. `FlushError` also reports a connection that went away, so a streaming loop can stop on the first failure instead of writing into a dead socket. `SetWriteDeadline(time.Time{})` lifts `Config.WriteTimeout` for this one response; a long download or an event stream needs that, since `WriteTimeout` would otherwise close it under the handler.

```go
func download(c *fibhttp.Context) {
	c.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for i := 0; i < 3; i++ {
		if _, err := fmt.Fprintf(c, "chunk %d\n", i); err != nil {
			return
		}
		c.Flush() // hand what is written to the socket now, not at the handler's return
	}
}
```

A response written entirely before the handler returns is sent whole, with a `Content-Length` when it is small enough, so ordinary replies need no `Flush`.

## Trailers

Trailers go after a chunked body. With `WriteResponse` the names are announced automatically; with the `http.ResponseWriter` methods, announce them in a `Trailer` header before the first write, then set the values after the body.

```go
func checksummed(c *fibhttp.Context) {
	_ = c.WriteResponse(fibhttp.Response{
		StatusCode: stdhttp.StatusOK,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte("body covered by the digest\n"),
		Trailer:    stdhttp.Header{"X-Checksum": {"sha256:..."}},
	})
}
```

Trailers can only be sent on a response that has a body; a `HEAD`, a `204` or an HTTP/1.0 client gets none of it. `http.TrailerPrefix` works the same way for values whose names are known only at the end.

## Static files and sendfile

`FileCache` serves a directory from memory and follows the disk; a file larger than `MaxFileBytes` is sent from disk through `sendfile`. `File`/`FileAttachment` on `Context` serve one known file, and `http.ServeFile`/`http.ServeContent` work through the `io.ReaderFrom` path.

```go
package main

import (
	"strings"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	files, err := fibhttp.NewFileCache(fibhttp.FileCacheConfig{
		Root:          ".",
		Precompressed: true, // serve name.br / name.gz when the client accepts them
	})
	if err != nil {
		panic(err)
	}
	defer files.Close()

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		files.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
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

`Context.SendFile(f, offset, n, done)` is the asynchronous form for a file the handler already has open: the range is queued with the connection's other sends and `done` runs later, so the worker is not held for the disk or the client. A file read into a `WriteResponse` body is not `sendfile`; the file cache, `Context.SendFile` and `ReadFrom` are the zero-copy paths.

## Uploads: OnBody, SaveBody and resumable uploads

By default a handler runs only after the whole body has arrived. `Config.StreamRequestBody` hands it over as soon as the body begins: the handler takes it through `Context.OnBody` piece by piece, or reads `Request.Body` without waiting (`ErrWouldBlock` when nothing has arrived yet). The body is never buffered whole, and the callback itself is the backpressure: a slow callback stops the connection's reads.

```go
config := fibhttp.DefaultConfig()
config.StreamRequestBody = true             // call the handler once the body has begun
config.StreamRequestBodyThreshold = 1 << 20 // keep bodies up to 1MiB buffered whole
config.MaxStreamedBodyBytes = 64 << 20      // bound a streamed body; zero is unbounded
config.StreamRequestBodyBuffer = 1 << 20    // stop reading the socket when 1MiB waits unread
```

```go
func count(c *fibhttp.Context) {
	var n int64
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil { // the connection went, or the body broke: this is the last call
			return
		}
		n += int64(len(data)) // data is valid only during this call
		if fin {
			_ = c.String(stdhttp.StatusOK, "%d bytes\n", n)
		}
	})
}
```

`OnBody` never calls the callback itself and starts no goroutine; what has already arrived is delivered after the handler returns, and the rest as it is read. It retains the request, so answering from the last call needs no `Retain`. For a body that only has to be stored, `SaveBody` writes it to disk as it arrives, and `SaveBodyResumable` accepts it in `Content-Range` chunks so that a dropped connection continues from the stored offset instead of starting over.

```go
func save(c *fibhttp.Context) {
	c.SaveBody("/var/tmp/upload.bin", nil) // nil done: answers 200/202/409/413/500 itself
}

func resumable(c *fibhttp.Context) {
	c.SaveBodyResumable("/var/tmp/upload.bin", func(saved fibhttp.Saved, err error) {
		if err != nil {
			_ = c.String(stdhttp.StatusBadRequest, "upload: %v\n", err)
			return
		}
		_ = c.RespondSaved(saved, nil) // 202 + Upload-Offset for a chunk, 200 when complete
	})
}
```

`Config.StreamRequestBodyAtHeader` can force a request's body to stream at its header whatever the threshold says — that is what a gRPC call through a proxy needs.

## Full duplex

`ServeDuplex` runs a body-and-response-at-once handler on its own goroutine: `Read` waits for the request body while `Write`/`Flush` produce the response. That is how a protocol that talks both ways over one stream is served on HTTP/1.1, HTTP/2 and HTTP/3 with the same code (gRPC streaming, WebTransport).

```go
func echo(c *fibhttp.Context) {
	_ = c.ServeDuplex(func(d *fibhttp.Duplex) {
		buf := make([]byte, 32<<10)
		for {
			n, err := d.Read(buf) // waits for the client
			if n > 0 {
				if _, werr := d.Write(buf[:n]); werr != nil {
					return
				}
				_ = d.Flush()
			}
			if err != nil {
				return
			}
		}
	})
}
```

The handler must not read `Request.Body` or call `OnBody` itself after `ServeDuplex`; the duplex owns the body. It needs `Config.StreamRequestBody` for the request body to arrive while the response is produced.

## Deadlines

`Config.ReadTimeout`, `ReadBodyTimeout`, `ReadHeaderTimeout`, `IdleTimeout` and `WriteTimeout` are `net/http.Server`'s, with `DefaultConfig` setting the read and idle ones. A handler overrides them per request with `SetReadDeadline` and `SetWriteDeadline`; a zero time lifts the limit. `SetReadDeadline` bounds a body still arriving, `SetWriteDeadline` a response still being written; a handler that has all its input and answers at once needs neither.

```go
func longLived(c *fibhttp.Context) {
	_ = c.SetWriteDeadline(time.Time{}) // this response may stream past Config.WriteTimeout
	_ = c.SetReadDeadline(time.Time{})  // this request's body may arrive past Config.ReadTimeout
	_ = c.String(stdhttp.StatusOK, "no deadline\n")
}
```

## Stdlib handlers, and Hijack

Any `net/http` handler is served by passing the `Context` as its `ResponseWriter`. `http.ServeMux`, `http.FileServer`, `http.StripPrefix`, `http.ServeFile` and third-party routers all work this way.

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
		fmt.Fprintf(w, "hello %s over %s\n", r.PathValue("name"), r.Proto)
	})
	mux.Handle("/files/", stdhttp.StripPrefix("/files/", stdhttp.FileServer(stdhttp.Dir("."))))

	std := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		mux.ServeHTTP(c, c.Request) // Context is the http.ResponseWriter
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(std),
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

Two differences matter. `http.Hijacker` is not implemented (`Hijack` returns `http.ErrNotSupported`), so libraries that take over the connection, gorilla/websocket among them, do not work: switch protocols with `Context.Upgrade` (chapter [09](09-websocket.md)) or `Context.Connect` (chapter [13](13-socks-connect.md)). And `c.Request.Context()` is not cancelled when the client goes away; use `c.Context()`, `c.OnCancel` or `c.Err()`, and hand `c.Request.WithContext(c.Context())` to code such as `httputil.ReverseProxy` that expects cancellation.

## Pitfalls

- **Blocking holds the connection.** The handler runs on the worker reading it, so a database call or an upstream request inside an HTTP/1 handler idles that connection. Move slow work to another goroutine with `Retain`/`Release` or `c.Async` (chapter 03).
- **Request objects are recycled.** `ReuseRequests`, `ReuseHeaders`, `ReuseURLs` and `ReuseContexts` are on in `DefaultConfig`; the `Request`, its `Header`, its `URL` and the `Context` are the handler's only until it returns and releases every `Retain`. Clone what must outlive the request (`Request.Clone`, `Header.Clone`), or turn the options off.
- **`OnBody` calls nothing inside the handler.** It registers and returns; delivery starts after the handler returns and runs on the connection's worker. `Request.Body` is not readable once `OnBody` has taken the body, and `data` is valid only during the call.
- **Streaming is off by default.** A large upload is buffered whole and refused with 413 past `MaxBodyBytes`; it needs `StreamRequestBody` (or `SaveBody`, which works either way).
- **Reading a streamed body never waits.** `Request.Body.Read` reports `fib.ErrWouldBlock` when nothing has arrived; hand the rest over to `OnBody`.
- **Writes do not block.** A handler faster than its client queues the difference in memory; `Config.WriteTimeout`, `WriteNoWait`/`OnWritable` and `SendFile` are the levers. `WriteTimeout` also cuts a streaming response, hence the zero deadline for SSE.
- **`compress` and `br` transfer codings are refused** (501 for a request, a failed response); gzip, x-gzip and deflate are decoded. `Content-Encoding` stays the handler's business.
- **`Hijack` and `EnableFullDuplex` are not supported.** Use `Upgrade`/`Connect` for protocol switches and `ServeDuplex` for two-way streaming.
- **`Expect: 100-continue` becomes lazy with streaming**: the 100 goes out only when the handler first asks for the body, so a handler can refuse an upload before it starts.

## See also

- [HTTP/1.x](../http1.md) — the protocol reference: framing, streaming bodies, timeouts, conformance tests.
- [Handlers: sync and async](03-handlers.md) — `Retain`/`Release`, `OnBody`, cancellation and the goroutine rules.
- [HTTP/2](07-http2.md) — what changes when one connection carries many requests.
- [Clients](20-clients.md) — the HTTP/1.x client, `Do`/`Go` and streaming response bodies.
- [Hacks](../hacks.md) — Retain/Release, `OnBody`, `FileCache`/`sendfile` and the anti-pattern list in depth.
- [Configuration](../configuration.md) — the fields used here and their defaults.
