# 路由

[English](04-router.md) | [简体中文](04-router.zh-CN.md)

`fibhttp.Router` 按方法和路径把请求交给注册的 handler，API 跟 `github.com/go-chi/chi` 一致，但 handler 是 fib 的——`func(*fibhttp.Context)` 而非 `http.HandlerFunc`。它本身就是一个 `fibhttp.Handler`，所以可以像别的 handler 一样放进 `fibhttp.NewHandler(r)`。本章讲 pattern 与匹配顺序、路由组与挂载的子路由、404 与 405 的处理，以及 `net/http` 的 mux 如何与它共存。

## Pattern 与匹配顺序

pattern 由静态文本、参数和可选的通配组成：

- `{name}` 匹配一个非空、到下一个 `/` 或 pattern 中紧随的静态文本为止的值，比如 `/archive/{year}-{month}` 里的 `{year}`。值里永远不含 `/`。
- `{name:regexp}` 要求整个值匹配正则，比如 `{id:[0-9]+}`。
- pattern 末尾的 `*` 匹配剩余路径，含 `/`、可以为空，键为 `"*"`；`{name...}` 做同样的事，键为 `name`。

多条路由都能匹配时，逐段比较：静态文本优先于参数，带正则的参数优先于普通参数，参数优先于通配，并且会回溯：同时注册 `/users/new` 和 `/users/{id}` 时，`/users/new` 命中前者，`/users/new2` 命中后者。为 GET 注册的路由在 HEAD 没有单独注册时也服务 HEAD。路径匹配但方法不匹配返回 `405 Method Not Allowed`，并带列出可用方法的 `Allow` 头；其他不匹配的路径返回 `404 Not Found`。参数用 `c.Param(name)` 取；请求带 `URL.RawPath` 时按它路由，所以转义的 `%2F` 属于值而不会把路径切开，`Param` 返回解码后的值。

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

用 `go run .` 运行，然后试试这些路由：

```sh
curl http://127.0.0.1:8080/users/42         # user 42           (the regexp route)
curl http://127.0.0.1:8080/users/bob        # user named bob    (the plain parameter)
curl http://127.0.0.1:8080/users/a%2Fb      # user named a/b    (%2F stays in the value)
curl http://127.0.0.1:8080/files/a/b        # file a/b
curl http://127.0.0.1:8080/files            # 404: "/files/*" needs the slash
curl -I http://127.0.0.1:8080/users/7       # 200: the GET route serves HEAD
curl -i -X DELETE http://127.0.0.1:8080/users  # 405 with "Allow: POST"
```

`Get`、`Post`、`Put`、`Delete`、`Patch`、`Head`、`Options`、`Connect`、`Trace` 覆盖标准方法；`Method`/`MethodFunc` 接受任意方法 token（例如 `PROPFIND`），`Handle`/`HandleFunc` 接受可以以方法开头的 pattern，写法与 `net/http.ServeMux` 相同：`r.HandleFunc("DELETE /users/{id}", deleteUser)`。`Handle` 的 pattern 不带方法时匹配所有方法。

除了 `Param`，handler 还可以用 `c.Params()`（一个按名字和值迭代的迭代器）读取所有参数，用 `c.RoutePattern()` 取得服务该请求的路由 pattern——如 `"/users/{id}"`，前面带上请求经过的各个 `Mount` 的 pattern。它适合作为监控指标和访问日志的标签，因为它不会随不同路径的数量增长。`r.Walk(fn)` 按方法和 pattern 遍历所有已注册的路由，可以在启动时打印路由表：

```go
r.Get("/orders/{id}", func(c *fibhttp.Context) {
	for name, value := range c.Params() {
		_ = name  // "id"
		_ = value // "17" for /orders/17
	}
	_ = c.String(200, "served by %s\n", c.RoutePattern()) // served by /orders/{id}
})
_ = r.Walk(func(method, pattern string, _ fibhttp.Handler) error {
	fmt.Println(method, pattern) // GET /orders/{id}, ...
	return nil
})
```

## 路由组与中间件作用域

`Use` 包住整个 Router，所以它的中间件能看到每个请求，包括 404 和 405。`Route` 在某个前缀下创建子路由（前缀可以带参数，但不能含通配）：它的路由并入同一棵树，一次查找就能路由到；它的 `Use` 中间件只包住这个前缀；它的 `NotFound`/`MethodNotAllowed` 也只在这个前缀下生效。`Group` 通过一个 Router 注册路由，它的中间件只作用于这些路由；`With` 对单条路由做同样的事。中间件必须在它包住的路由之前添加——注册过路由后再调 `Use` 会 panic——所有路由都必须在本 Router 开始服务之前注册完。

`Mount` 不一样：它把某个前缀下的一切交给另一个 handler。挂载的 `*Router` 从自己的 `/` 开始路由剩余路径，并带自己的中间件，和 chi 一样；其他 handler 拿到原样的请求，剩余路径放在 `c.Param("*")`。

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

`/api/status` 由挂载的路由服务；`/users/42` 由 `/users` 下的路由服务，`/users` 和 `/users/` 都由它的 `"/"` 服务。访问 `/api/missing` 时，由挂载路由的 404 处理（它没设置就用本路由的），并且在挂载路由的中间件之内执行；`/users/x` 得到 404，因为 `/users` 下没有路由接受它，`POST /users/42` 得到 405 并带 `Allow: GET, HEAD`。

只作用于树的一部分的中间件有三个位置可放，下一章有一个带它的完整程序：

```go
r.Route("/admin", func(r *fibhttp.Router) {
	r.Use(requireToken) // every route under /admin, and its 404s and 405s
	r.Get("/stats", stats)
})
r.Group(func(r *fibhttp.Router) {
	r.Use(rateLimit) // only the two routes registered in this function
	r.Post("/login", login)
	r.Post("/signup", signup)
})
r.With(audit).Delete("/users/{id}", deleteUser) // this one route
```

## 404、405 与 PathValue

默认的 404 和 405 是纯文本响应（`404 page not found`、`405 method not allowed`）。要定制就注册 handler：`r.NotFound(fn)` 处理没有路由匹配的路径，`r.MethodNotAllowed(fn)` 处理路径匹配但方法不支持的情况。405 handler 运行时 `Allow` 头已经设在 `c.Header()` 里了，无论 handler 用哪种方式应答——`String`、`JSON`、`Respond`、`WriteResponse` 还是 `ResponseWriter` 方法——它都会随响应发出：

```go
r.NotFound(func(c *fibhttp.Context) {
	_ = c.JSON(404, map[string]string{"error": "not found"})
})
r.MethodNotAllowed(func(c *fibhttp.Context) {
	// Allow is in c.Header() already and goes out with the answer.
	_ = c.String(405, "method %s not allowed\n", c.Request.Method)
})
```

在 `Route` 回调里设置的 `NotFound` 或 `MethodNotAllowed` 只在该前缀下生效；在 `With` 或 `Group` 产生的 Router 上设置，则设置的是它们所来自的那个 Router 的 handler。传 `nil` 恢复默认。

Router 默认只给 `c.Param` 提供路由参数。`r.SetPathValues(true)` 让每个请求也把参数放进 `c.Request.PathValue`，为 `net/http.ServeMux` 写的 handler 就能读到；代价是每个带参数的请求多一次分配，而 `Param` 不分配。它是调用它的那个 Router 以及由它经 `Route`、`Group`、`With` 产生的 Router 的设置；`Mount` 进来的 `*Router` 有自己的设置。

## 给 `net/http` 的 mux 做路由

Router 是 `fibhttp.Handler`，而包一层薄适配器就能让任意 `net/http.Handler` 也变成 `fibhttp.Handler`。因此标准库的 `ServeMux` 可以作为合法的路由目标，包括它的方法 pattern 和 `PathValue`：

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

这样挂载的 `ServeMux` 用自己的 404 和 405 作答。Router 自己的参数——比如 `Mount` pattern 里的参数——只有在 `SetPathValues(true)` 时才以 `PathValue` 的形式到达 mux 的 handler；mux 自己 pattern 的参数则不受影响。其他路由库的 handler 也一样——README 里关于 `net/http` handler 的兼容性说明适用于它们全部。

## 性能说明

Router 是一棵 radix 树：静态子节点按首字节索引，只有存在可回溯分支的节点才递归，参数值是路径的子串而不是拷贝。路由状态挂在 `Context` 上并随它复用，所以复用 Context 时路由一个请求零分配；关闭 Context 复用时，带参数或经过 `Mount` 的请求会分配一次。注册路由与服务请求不能并发进行：先建好 Router，再开始服务。

## 坑与限制

- **注册过路由后再 `Use` 会 panic。** 先加中间件再加路由；某个前缀的中间件就写在该前缀的 `Route` 回调里。
- **`With`/`Group` 的中间件不是全局的。** 它只包住通过那个 Router 注册的路由，而不是父 Router 看到的所有请求；要全局请对父 Router 用 `Use`。
- **`Route` 不是 `Mount`。** `Route` 的路由并进同一棵树、一次查找完成；只有 `Mount` 会嵌套第二个 Router 并重新路由剩余路径。
- **通配后面不能跟别的段。** `*` 和 `{name...}` 必须在 pattern 末尾，`Route`/`Mount` 的前缀里也不能有通配。
- **`c.Param` 的值是借来的。** 它们是路径的子串，（在默认的请求复用下）handler 返回后失效；要保留请复制，例如用 `strings.Clone`。
- **405 handler 不必自己设 `Allow`。** Router 已把它放进 `c.Header()`，每种应答方式都会发出；`WriteResponse` header 里的 `Allow` 会替换它。
- **`Mount` 接受的是 handler，不是中间件。** `pprof.New()` 等自带的都是中间件；请用 `Use`（或 `middleware.Chain`）把它们放到 Router 上，而不是交给 `Mount`。
- **不要在服务期间注册路由。** Router 的注册与分发不能并发；在启动阶段建好它。
- **结尾斜杠不会重定向。** 注册你真正要匹配的路径；`Route` 前缀下自己的 `/` 同时匹配 `/users` 和 `/users/`，但普通路由不会悄悄重定向。

## 相关文档

- [中间件](05-middleware.zh-CN.md)——`Use`、`With`、`Group` 接受的中间件
- [Handler：同步与异步](03-handlers.zh-CN.md)——路由 handler 是什么、如何作答
- [HTTP/1.x](06-http1.zh-CN.md)——handler 用到的请求与响应辅助方法
- [进阶技巧](../hacks.zh-CN.md)——零拷贝 `Param` 与对象复用规则
- [路由 benchmark 的 README](../../http/routerbench/README.md)——fib 的 Router 对比 chi 与 `http.ServeMux`
- [README](../../README.zh-CN.md)——路由示例与 `net/http` 兼容性说明
