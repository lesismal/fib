# 中间件

[English](05-middleware.md) | [简体中文](05-middleware.zh-CN.md)

中间件是“把 handler 变成 handler”的函数：它在 handler 之前做事、之后做事，或者代替它作答。fib 自带十个——`compress`、`cors`、`csrf`、`etag`、`limiter`、`logger`、`pprof`、`recover`、`requestid`、`responsetime`——同一个签名也适用于你自己写的中间件，既可以作用于整个 Router，也可以只作用于一个路由组。本章讲组合与顺序、中间件签名与响应钩子、自带中间件及其关键选项，以及如何把中间件限制在某个前缀下。

## 组合与顺序

`middleware.Chain(handler, middlewares...)` 把 handler 一层层包起来，**第一个在最外层**：它最先看到请求，最后看到响应。某个中间件自己作答后直接返回，就跳过了它里面的一切——这就是提前返回。`Middleware` 类型就是个普通函数，所以 `[]middleware.Middleware` 可以原样展开传给 `Chain`，也可以原样展开传给 `Router.Use`：

```go
handler := middleware.Chain(app,
	recover.New(), // outermost: a panic becomes a 500
	logger.New(),  // sees the request, and the response on the way out
	compress.New(),
	cors.New(),
)
```

顺序在两处有意义。**改变响应的**中间件要按依赖嵌套：`compress` 在 `etag` 外层，这样 ETag 是用 handler 写出的 body 算的，而不是压缩后的。**提前返回的**中间件，比如回答预检的 `cors` 或拒绝请求的 `limiter`，要放在它保护的处理逻辑外面，但又必须放在仍需要观察该请求的中间件里面——`recover` 在最外层，意味着连作答过程中发生的 panic 也能被接住。

## 编写中间件

签名是 `func(next fibhttp.Handler) fibhttp.Handler`，编译器需要类型名时可以写 `middleware.Middleware`。包装体通常是 `fibhttp.HandlerFunc`；它可以在调用 `next` 前改请求、可以代替 `next` 作答，也可以在之后改响应：

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

要改写 handler 写出的响应，中间件应使用 `Context` 的钩子，而不是包装 writer：`OnHeader(func(status int, header http.Header))` 在响应头定下来时运行、可以修改它；`OnResponse(func(*fibhttp.Response))` 在发出前拿到完整响应；`OnFinish(func(status int, header http.Header, size int64))` 在响应交给连接后运行。钩子按注册的逆序运行，和 defer 一样，所以最外层的中间件最后看到响应。`compress`、`etag`、`cors`、`requestid` 正是这样实现的，所以无论 handler 用 `Respond`、`WriteResponse`、助手方法还是 `ResponseWriter` 方法作答，它们都有效。

像上面那样在调用 `next` 之前用 `c.Header().Set` 设置的字段，无论 handler 用 `Respond`、`WriteResponse`、`String`/`JSON` 这类助手还是 `ResponseWriter` 方法作答，都会随响应发出。handler 自己的字段优先：它的 `Content-Type`，或 `WriteResponse` header 里的同名字段，会替换中间件设的值。handler 作答之后再改 `c.Header()` 就没有效果了；要按 handler 选定的状态码添加字段，或修改它已发出的字段，请用 `OnHeader`。

每个自带 `Config` 都有 `Next` 跳过函数：返回 true 时该中间件直接放行。

```go
compress.New(compress.Config{
	Next: func(c *fibhttp.Context) bool { // never hold an event stream to compress it
		return strings.HasPrefix(c.Request.URL.Path, "/events")
	},
})
```

## 自带的中间件

应用通常组合几个来用。下面这个完整程序把一个 handler 包进四个中间件：

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

较小的那些用同样的方式组合，各自带自己的 `Config`：

```go
handler := middleware.Chain(app,
	requestid.New(), // X-Request-ID: the client's, or a new UUID; on request and response
	responsetime.New(responsetime.Config{Header: "X-Took"}), // default X-Response-Time
	etag.New(etag.Config{Weak: true}),                       // ETag, 304 on If-None-Match
	limiter.New(limiter.Config{Max: 100, Expiration: time.Minute, SlidingWindow: true}),
	csrf.New(csrf.Config{TrustedOrigins: []string{"https://app.example.com"}}),
)
```

各自的作用与关键选项：

- **`compress`**——在 `Accept-Encoding` 允许、body 至少 `MinLength`（默认 1 KiB）、媒体类型可压缩（`Compressible`）、且没有 `Content-Encoding`、不是部分响应、`Cache-Control` 也不禁止改写时，用 gzip 或 deflate 压缩。它会加 `Vary`、把强 ETag 变弱，并把整个响应缓存到 handler 返回；对 SSE 和长下载请给它一个返回 true 的 `Next` 跳过。
- **`etag`**——为 200 的 GET/HEAD 响应按 body 生成 ETag，`If-None-Match` 命中时回 304 且不带 body；handler 自己设的 ETag 会保留。`Weak: true` 让校验符变成弱 ETag。
- **`cors`**——`AllowOrigins`（默认允许任意来源；`"https://*.example.com"` 匹配其子域名）、`AllowOriginsFunc`、`AllowMethods`（默认 GET、POST、HEAD、PUT、DELETE、PATCH）、`AllowHeaders`（默认为预检请求所要求的那些）、`ExposeHeaders`、`AllowCredentials`、`AllowPrivateNetwork`、`MaxAge`；预检请求由它自己以 204 回答。`AllowCredentials` 需要明确列出来源：配合默认的任意来源时 `New` 会 panic。
- **`csrf`**——没有 token cookie（默认名 `csrf_`）的安全请求（GET、HEAD、OPTIONS、TRACE）会拿到一个；不安全请求只有把同一个 token 带回来、且 `net/http` 的 `CrossOriginProtection` 判定为同源时才会被处理，否则回 403（`ErrorHandler`）。用 `csrf.Token(r)` 取 token 放进表单；`Lookup` 指定不安全请求从哪里带 token（默认 `header:X-CSRF-Token`，也可以是 `form:<field>`、`query:<name>`），`TrustedOrigins` 放行跨源调用方。
- **`limiter`**——每个 key 在 `Expiration` 窗口（默认一分钟）内最多 `Max` 个请求（默认 60），key 默认是客户端地址，可用 `KeyGenerator` 改。超出返回 `429` 并带 `Retry-After`（或调用 `LimitReached`），每个响应都带 `X-RateLimit-Limit`、`-Remaining`、`-Reset`，除非设了 `DisableHeaders`；`SlidingWindow` 消除两个窗口交界处的突刺，`SkipFailedRequests`/`SkipSuccessfulRequests` 让相应响应不计入。计数保存在本进程内存里，默认 key 是 TCP 对端：在代理或负载均衡后面，那就是代理本身，所以请给它一个读取代理所设请求头的 `KeyGenerator`。
- **`logger`**——每个响应在交给连接后记录一条，走 `fib/log`（`http: access status=200 latency=31µs ip=127.0.0.1 method=GET path=/`）；`Fields` 从包文档里的标签中选择记录内容（默认 `status`、`latency`、`ip`、`method`、`path`；还有 `url`、`ua`、`bytesSent`、`reqHeader:Name` 等），`Level` 设置级别，`Format`/`Output` 则改成按自定义布局直接写到某个 writer，例如 `logger.Config{Format: "${status} ${method} ${path} ${latency}\n", Output: os.Stdout}`。
- **`recover`**——把 handler 运行期间的 panic 变成 500，而不是丢掉连接；`Handler` 负责作答，`Log` 负责上报（默认以 error 级别写到 `fib/log`），`DisableStackTrace` 缩短报告。以 `http.ErrAbortHandler` panic 不会记日志，在 HTTP/1 上会关闭连接。它只接 handler 自身的 panic：交给自己的协程执行的活要在那里自行 recover。
- **`requestid`**——在请求和响应上设置 `X-Request-ID`（或 `Header` 指定的头），客户端带来的值是不超过 128 字节的可打印 ASCII 时沿用它，否则用 `Generator`（默认随机 UUID）生成新的；使用默认请求头时 `requestid.FromRequest(r)` 读取它（否则读 `r.Header.Get(yourHeader)`），`requestid.UUID()` 生成一个。`logger` 用 `reqHeader:X-Request-ID` 字段记录它。
- **`responsetime`**——在 `X-Response-Time`（或 `Header`）里报告 handler 确定响应所花的时间，格式由 `Format` 决定（默认是毫秒，如 `0.017ms`）。
- **`pprof`**——在 `/debug/pprof/`（`Prefix`）下提供运行时 profile，每个 profile 在引擎的 handler 池上采集，worker 不用陪着跑 CPU profile。见下文。

## 把中间件限定到路由

在 `Router` 上，`Use` 包住整个 Router——包括 404 和 405——而 `Route` 自己的 `Use` 只包住它的前缀，`Group` 只包住通过它注册的路由，`With` 只包住单条路由。`[]middleware.Middleware` 可以原样传给它们全部：

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

`/` 是公开的，`/admin/stats` 需要带请求头：

```sh
curl http://127.0.0.1:8080/                                   # public
curl -i http://127.0.0.1:8080/admin/stats                     # 401 missing token
curl -i -H 'Authorization: Bearer x' http://127.0.0.1:8080/admin/stats  # stats, with X-Served-By: fib
curl -i http://127.0.0.1:8080/admin/nope                      # 401 too: the prefix's 404s are inside its middleware
```

中间件必须在它包住的路由之前添加；注册过路由后再调 `Use` 会 panic。

## 接入 pprof

`pprof.New()` 是个普通中间件：前缀下的路径由它自己作答，其余请求全部放行，所以直接放进应用 handler 前面的 `Chain`，或放进 Router 的 `Use` 即可——不能交给 `Mount`，它接受的是 handler。不需要单独的运维端口，当然也可以用另一个 binding（比如回环地址）配一个单独的 handler 来承载它。profile 会暴露进程的大量信息，只在信任的访问者能到达的地方开放。

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

用 Go 工具读取 profile：

```sh
curl http://127.0.0.1:8080/debug/pprof/                     # the index
go tool pprof http://127.0.0.1:8080/debug/pprof/heap
go tool pprof http://127.0.0.1:8080/debug/pprof/profile?seconds=10
```

## 坑与限制

- **顺序就是语义。** `recover` 在最外层，`compress` 在 `etag` 外层，提前返回的中间件放在它保护的逻辑外层；互换任意两个改变的是行为，不只是版式。
- **中间件必须在路由之前添加。** 第一次注册路由后再调 `Router.Use` 会 panic；在启动阶段把栈建好。
- **`With`/`Group` 不是全局的。** 它们的中间件只包住通过它们注册的路由，不包父 Router 看到的 404 或其他路由。
- **中间件不能阻塞。** 它和 handler 跑在同一个 worker 上；慢中间件就是慢 handler。
- **响应钩子有代价。** `compress` 和 `etag` 会缓存整个响应，流式响应必须用 `Next` 跳过它们，否则就不再流式；`OnResponse` 还会让该响应不使用 sendfile，HTTP/1 上的 `Flush` 也不再立即发送。
- **`recover` 接不住别的协程里的 panic。** 从协程里作答的 handler 要在那里自己 recover。
- **中间件自己作答后就不要再调 `next`。** 写完响应再调 `next` 会破坏它；请直接返回。
- **中间件里的 `c.Header().Set` 要放在 `next` 之前。** handler 自己的同名字段优先，handler 作答之后再改也没有效果；要覆盖 handler 发出的字段或在它之后设置字段，请用 `OnHeader`。
- **`cors` 的 `AllowCredentials` 需要 `AllowOrigins`。** 凭据不能与默认的任意来源同用；`cors.New` 会在启动时 panic。
- **代理后面的 `limiter` 数的是代理。** 给它一个读取代理转发的客户端地址的 `KeyGenerator`，并记住每个进程各自计数。

## 相关文档

- [路由](04-router.zh-CN.md)——`Use`、`Route`、`Group` 与 `With`
- [Handler：同步与异步](03-handlers.zh-CN.md)——中间件所依赖的响应钩子
- [配置](../configuration.zh-CN.md#日志)——`fib/log` 如何设置、`logger` 中间件如何写记录
- [性能与运维](22-performance.zh-CN.md)——生产环境里的 `pprof`、日志配置、压测
- [进阶技巧](../hacks.zh-CN.md)——中间件必须遵守的对象复用规则
- [README](../../README.zh-CN.md)——中间件清单与快速开始
