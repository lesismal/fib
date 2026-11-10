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
		c.Header().Set("X-Served-By", "fib")
		next.ServeHTTP(c)
	})
}
```

要改写 handler 写出的响应，中间件应使用 `Context` 的钩子，而不是包装 writer：`OnHeader` 在响应头定下来时运行、可以修改它；`OnResponse` 在发出前拿到完整响应；`OnFinish` 在响应交给连接后运行。`compress` 和 `etag` 正是这样实现的，所以无论 handler 用 `Respond`、`WriteResponse` 还是 `ResponseWriter` 方法作答，它们都有效。每个自带 `Config` 都有 `Next` 跳过函数：返回 true 时该中间件直接放行。

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
- **`cors`**——`AllowOrigins`（在主机名的某段写 `"*"` 可匹配其子域名）、`AllowMethods`、`AllowHeaders`、`ExposeHeaders`、`AllowCredentials`、`AllowPrivateNetwork`、`MaxAge`；预检请求由它自己回答。
- **`csrf`**——没有 double-submit token cookie 的不安全方法（除 GET、HEAD、OPTIONS、TRACE 之外的方法）会被拒绝，并用 `net/http` 的 `CrossOriginProtection` 检查来源。用 `csrf.Token(r)` 取 token 放进表单；`Lookup` 指定不安全请求从哪里带 token（默认 `header:X-CSRF-Token`），`TrustedOrigins` 放行跨源调用方。
- **`limiter`**——每个 key 在 `Expiration` 窗口（默认一分钟）内最多 `Max` 个请求（默认 60），key 默认是客户端地址，可用 `KeyGenerator` 改。超出返回 `429` 并带 `Retry-After` 和 `X-RateLimit-*`；`SlidingWindow` 消除两个窗口交界处的突刺，`SkipFailedRequests`/`SkipSuccessfulRequests` 让相应响应不计入。
- **`logger`**——每个响应在交给连接后记录一条，走 `fib/log`；`Fields` 从包文档里的标签中选择记录内容（默认 `status`、`latency`、`ip`、`method`、`path`），`Level` 设置级别，`Format`/`Output` 则改成按自定义布局直接写到某个 writer。
- **`recover`**——把 handler 运行期间的 panic 变成 500，而不是丢掉连接；`Handler` 负责作答，`Log` 负责上报，`DisableStackTrace` 缩短报告。它只接 handler 自身的 panic：交给自己的协程执行的活要在那里自行 recover。
- **`requestid`**——在请求和响应上设置 `X-Request-ID`（或 `Header` 指定的头），沿用客户端带来的值或生成新的；`requestid.FromRequest(r)` 读取它，`requestid.UUID()` 生成一个。
- **`responsetime`**——在 `X-Response-Time`（或 `Header`）里报告 handler 确定响应所花的时间，格式由 `Format` 决定。
- **`pprof`**——在 `/debug/pprof`（`Prefix`）下提供运行时 profile，每个 profile 在引擎的 handler 池上采集，worker 不用陪着跑十秒钟的 CPU profile。见下文。

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
		c.Header().Set("X-Served-By", "fib")
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

`/` 是公开的，`/admin/stats` 需要带请求头。中间件必须在它包住的路由之前添加；注册过路由后再调 `Use` 会 panic。

## 接入 pprof

`pprof.New()` 是个普通中间件：前缀下的路径由它自己作答，其余请求全部放行，所以直接放进应用 handler 前面的 `Chain` 即可——不需要单独的运维端口，当然也可以把另一个 Router 绑到另一个地址来承载它。profile 会暴露进程的大量信息，只在信任的访问者能到达的地方开放。

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

## 坑与限制

- **顺序就是语义。** `recover` 在最外层，`compress` 在 `etag` 外层，提前返回的中间件放在它保护的逻辑外层；互换任意两个改变的是行为，不只是版式。
- **中间件必须在路由之前添加。** 第一次注册路由后再调 `Router.Use` 会 panic；在启动阶段把栈建好。
- **`With`/`Group` 不是全局的。** 它们的中间件只包住通过它们注册的路由，不包父 Router 看到的 404 或其他路由。
- **中间件不能阻塞。** 它和 handler 跑在同一个 worker 上；慢中间件就是慢 handler。
- **响应钩子有代价。** `compress` 和 `etag` 会缓存整个响应，流式响应必须用 `Next` 跳过它们，否则就不再流式；`OnResponse` 还会让该响应不使用 sendfile，HTTP/1 上的 `Flush` 也不再立即发送。
- **`recover` 接不住别的协程里的 panic。** 从协程里作答的 handler 要在那里自己 recover。
- **中间件自己作答后就不要再调 `next`。** 写完响应再调 `next` 会破坏它；请直接返回。
- **响应开始之后再设 header 是无效的。** handler 作答之后中间件里的 `c.Header().Set` 已经太晚；要改 handler 自己定下的 header，请用 `OnHeader`。

## 相关文档

- [路由](04-router.zh-CN.md)——`Use`、`Route`、`Group` 与 `With`
- [Handler：同步与异步](03-handlers.zh-CN.md)——中间件所依赖的响应钩子
- [配置](../configuration.zh-CN.md#日志)——`fib/log` 如何设置、`logger` 中间件如何写记录
- [性能与运维](22-performance.zh-CN.md)——生产环境里的 `pprof`、日志配置、压测
- [进阶技巧](../hacks.zh-CN.md)——中间件必须遵守的对象复用规则
- [README](../../README.zh-CN.md)——中间件清单与快速开始
