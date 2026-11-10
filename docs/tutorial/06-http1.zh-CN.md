# HTTP/1.x

[English](06-http1.md) | [简体中文](06-http1.zh-CN.md)

`fib/http` 是 fib 的 HTTP/1.0 与 HTTP/1.1 服务端和客户端实现：请求解析、响应分帧、带 trailer 的 chunked body、文件 `sendfile`，以及与 `net/http` 的兼容。本章按用法讲服务端——读取请求、用响应助手作答、流式响应、trailer、静态文件、上传和 `net/http` handler；协议细节留给 [HTTP/1.x](../http1.zh-CN.md)。阅读前应已了解第 [01](01-getting-started.zh-CN.md) 章的引擎和第 [03](03-handlers.zh-CN.md) 章的 handler 模型；同一个 `*fibhttp.Context` handler 也服务 HTTP/2 与 HTTP/3，分别见第 [07](07-http2.zh-CN.md)、[08](08-http3.zh-CN.md) 章。

## 模型

一条 HTTP/1.x 连接同一时刻只承载一个请求。读取该连接的引擎 worker 解析请求，并在同一个 worker 上调用 handler，所以 handler 的执行与本连接的读取不会并行：handler 一阻塞就会占住这条连接，而 HTTP/1 连接上没有别的流可以先跑。handler 写出的内容先入队，在本轮结束时合并成一次批量写（`writev`）发出，因此用 `Respond`、`WriteResponse` 或几次 `Write` 作答的 handler 只花一次系统调用。pipeline 的请求按请求顺序作答；用 `Retain` 挂起的请求会把它后面 pipeline 的请求一并挡住，直到它被 `Release`。这些都是 HTTP/1 特有的：在 HTTP/2 和 HTTP/3 上 handler 跑在协程池里，阻塞只影响自己。

## 第一个服务

一个 handler 服务所有路径：路径决定回答的形式，`Query` 读取查询参数且不分配内存，`Body` 直接给出请求 body（不拷贝），`JSON`/`String`/`Data`/`HTML` 覆盖常用响应。

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

`fibhttp.NewHandler(app)` 使用默认配置；`NewHandlerWithConfig` 用来传入单个服务的选项。绑定要求 TLS 或 UDP 时，同一个 handler 同样服务 HTTP/2 与 HTTP/3（第 07、08 章）。

## 读取请求

`Context.Request` 是标准的 `*stdhttp.Request`，`net/http` 的所有访问方式都能用。fib 额外提供三样东西：`Body` 返回服务端已经缓冲好的 body，`Query` 解码单个参数而不像 `URL.Query()` 那样每次调用都建 map，两者都是借用，只在响应结束前有效。

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

没有 body 的请求、以及 body 以流式到达的请求，`Body` 返回 nil；这两种情况下都用 `Request.Body` 读取。这些字节只属于 handler 到响应结束为止，需要越过这个时点就自己拷贝。

## 作答：响应助手

`Respond` 一次给出完整响应，`WriteResponse` 一次给出状态、header、body 和 trailer，若干小助手包装了常见的内容类型。没有 `Redirect` 助手：按 `net/http` 的方式设置 header 和状态码即可。

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

`Data(status, contentType, body)`、`IndentedJSON`、`IndentedXML`、`HTMLTemplate`、`File`、`FileAttachment` 补全了这套助手。会做编码的助手在失败时返回编码器的错误且不发送任何内容，handler 仍可换一种方式作答。`Context` 实现了 `http.ResponseWriter`、`http.Flusher` 和 `io.ReaderFrom`，手写 header、状态码和 body 的行为与 `net/http` 描述的一致。

## 流式响应与 SSE

`Write` 之后再 `Flush` 就是流式：header 随第一次 flush 发出，body 边写边发。`FlushError` 还会报告连接已断开，流式循环可以在第一次失败时就停下，而不是继续写进死掉的 socket。`SetWriteDeadline(time.Time{})` 为这一个响应解除 `Config.WriteTimeout`；长下载和事件流都需要它，否则 handler 还没写完 `WriteTimeout` 就会关闭连接。

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

在 handler 返回前就写完的响应会整体发出，够小的时候还带 `Content-Length`，所以普通的应答不需要 `Flush`。

## Trailer

trailer 跟在 chunked body 之后。用 `WriteResponse` 时会自动声明名字；用 `http.ResponseWriter` 的方法写时，要在第一次写之前用 `Trailer` header 声明，等 body 写完后设置值。

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

只有带 body 的响应才能发 trailer；`HEAD`、`204` 和 HTTP/1.0 客户端都收不到。`http.TrailerPrefix` 也适用，用来发那些到结尾才知道名字的值。

## 静态文件与 sendfile

`FileCache` 从内存提供目录，并跟随磁盘变化；超过 `MaxFileBytes` 的文件走磁盘的 `sendfile`。`Context` 上的 `File`/`FileAttachment` 服务单个已知文件，`http.ServeFile`/`http.ServeContent` 则通过 `io.ReaderFrom` 路径工作。

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

`Context.SendFile(f, offset, n, done)` 是 handler 已经打开文件时的异步形式：这个区间与连接的其他发送一起排队，`done` 稍后回调，worker 不必为磁盘或客户端等待。把文件读进 `WriteResponse` 的 body 不走 `sendfile`；文件缓存、`Context.SendFile` 和 `ReadFrom` 才是零拷贝路径。

## 上传：OnBody、SaveBody 与断点续传

默认情况下 handler 要等整个 body 到齐才会运行。`Config.StreamRequestBody` 让 body 一开始到达就把请求交给 handler：handler 通过 `Context.OnBody` 逐段接收，或者不等待地读 `Request.Body`（还没到数据时返回 `ErrWouldBlock`）。整个 body 从不缓冲，回调本身就是背压：回调慢下来，连接的读取就会停下。

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

`OnBody` 自己从不调用回调，也不开 goroutine；已经到达的部分在 handler 返回后交付，其余部分边读边交付。它会 retain 请求，所以在最后一次回调里作答不需要再 `Retain`。如果 body 只需要落盘，`SaveBody` 边到达边写入磁盘；`SaveBodyResumable` 按 `Content-Range` 分块接收，连接中断后可以从已存的偏移继续，而不是从头再来。

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

`Config.StreamRequestBodyAtHeader` 可以无视阈值，让指定请求的 body 在 header 到齐时就流式交付——经代理的 gRPC 调用需要这一点。

## 全双工

`ServeDuplex` 把「边读 body 边写响应」的 handler 放到自己的 goroutine 上：`Read` 等待请求 body，`Write`/`Flush` 产出响应。这样同一条流两个方向同时说话的协议（gRPC streaming、WebTransport）在 HTTP/1.1、HTTP/2 和 HTTP/3 上可以共用同一段代码。

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

调用 `ServeDuplex` 之后，handler 不能再读 `Request.Body` 或调用 `OnBody`，body 归 duplex 所有。要让请求 body 在响应产生的同时到达，需要打开 `Config.StreamRequestBody`。

## 超时与 deadline

`Config.ReadTimeout`、`ReadBodyTimeout`、`ReadHeaderTimeout`、`IdleTimeout`、`WriteTimeout` 与 `net/http.Server` 的语义一致，`DefaultConfig` 会设置读超时和空闲超时。handler 用 `SetReadDeadline`、`SetWriteDeadline` 对单个请求覆盖它们；时间零值表示解除限制。`SetReadDeadline` 约束仍在到达的 body，`SetWriteDeadline` 约束仍在写的响应；输入已经到齐、一次作答的 handler 两者都不需要。

```go
func longLived(c *fibhttp.Context) {
	_ = c.SetWriteDeadline(time.Time{}) // this response may stream past Config.WriteTimeout
	_ = c.SetReadDeadline(time.Time{})  // this request's body may arrive past Config.ReadTimeout
	_ = c.String(stdhttp.StatusOK, "no deadline\n")
}
```

## 标准库 handler 与 Hijack

把 `Context` 当作 `ResponseWriter` 传给任何 `net/http` handler 就能在 fib 上运行。`http.ServeMux`、`http.FileServer`、`http.StripPrefix`、`http.ServeFile` 以及第三方路由都这样使用。

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

有两点差异需要注意。`http.Hijacker` 没有实现（`Hijack` 返回 `http.ErrNotSupported`），所以接管连接的库（包括 gorilla/websocket）不能用：请改用 `Context.Upgrade`（第 [09](09-websocket.zh-CN.md) 章）或 `Context.Connect`（第 [13](13-socks-connect.zh-CN.md) 章）。另外，客户端断开时 `c.Request.Context()` 不会被取消；请用 `c.Context()`、`c.OnCancel` 或 `c.Err()`，并把 `c.Request.WithContext(c.Context())` 交给 `httputil.ReverseProxy` 这类依赖取消的代码。

## 坑与限制

- **阻塞会占住连接。** handler 跑在读取该连接的 worker 上，所以在 HTTP/1 handler 里做数据库查询或转发上游会让这条连接空转。用 `Retain`/`Release` 或 `c.Async` 把慢活挪到别的 goroutine（第 03 章）。
- **请求对象会被复用。** `DefaultConfig` 打开了 `ReuseRequests`、`ReuseHeaders`、`ReuseURLs`、`ReuseContexts`；`Request`、它的 `Header`、`URL` 和 `Context` 只在 handler 返回并释放所有 `Retain` 之前属于它。需要越过请求生命周期的内容请拷贝（`Request.Clone`、`Header.Clone`），或者关掉这些选项。
- **`OnBody` 不会在 handler 内调用回调。** 它只注册然后返回；交付在 handler 返回后开始，在连接的 worker 上运行。`OnBody` 接管 body 后 `Request.Body` 不能再读，`data` 只在本次调用内有效。
- **流式默认关闭。** 大上传会整体缓冲，超过 `MaxBodyBytes` 直接 413；需要打开 `StreamRequestBody`（或者用 `SaveBody`，两种模式下都能工作）。
- **读流式 body 不会等待。** 还没有数据时 `Request.Body.Read` 返回 `fib.ErrWouldBlock`；剩下的交给 `OnBody`。
- **写不会阻塞。** handler 比客户端快时，差额会堆在内存里；`Config.WriteTimeout`、`WriteNoWait`/`OnWritable`、`SendFile` 是调节手段。`WriteTimeout` 也会切断流式响应，所以 SSE 要设零值 deadline。
- **`compress`、`br` transfer coding 不支持**（请求返回 501，响应失败）；gzip、x-gzip、deflate 会被解码。`Content-Encoding` 仍由 handler 自己负责。
- **`Hijack` 与 `EnableFullDuplex` 不支持。** 协议切换用 `Upgrade`/`Connect`，双向流用 `ServeDuplex`。
- **流式模式下 `Expect: 100-continue` 变成惰性的**：只有 handler 第一次索要 body 时才发 100，所以 handler 可以在上传开始前拒绝它。

## 相关文档

- [HTTP/1.x](../http1.zh-CN.md) — 协议参考：分帧、流式 body、超时、一致性测试。
- [Handler：同步与异步](03-handlers.zh-CN.md) — `Retain`/`Release`、`OnBody`、取消与 goroutine 规则。
- [HTTP/2](07-http2.zh-CN.md) — 一条连接承载多个请求后有哪些变化。
- [客户端](20-clients.zh-CN.md) — HTTP/1.x 客户端、`Do`/`Go` 与流式响应 body。
- [进阶技巧](../hacks.zh-CN.md) — Retain/Release、`OnBody`、`FileCache`/`sendfile` 与反模式清单的深入讲解。
- [配置](../configuration.zh-CN.md) — 本章用到的字段及其默认值。
