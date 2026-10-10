# Gateway 示例

[English](README.md)

基于 fib 的反向代理 / 网关：下游同时接入 **HTTP/1.1、HTTP/2、HTTP/3 和 WebSocket**，按路径前缀转发到上游。
全程没有阻塞引擎 worker 的地方：请求 body、上游请求、上游响应 body、WebSocket 拨号和 WebSocket 消息，
都是在回调里处理的。

```text
examples/gateway/
├── server/     网关程序：HTTPS/h2、HTTP/3 和明文三类监听
│   └── proxy/      网关本体（路由、HTTP 转发、WebSocket 中继）
├── upstream/   演示用的后端程序
│   └── backend/    演示后端的 handler（server 的测试也用它）
└── client/     演示客户端：用每种协议各请求一遍网关
```

## 运行

开三个终端，按这个顺序：

```sh
go run ./examples/gateway/upstream
go run ./examples/gateway/server
go run ./examples/gateway/client
```

client 每项检查打印一行，最后输出 `all checks passed`：

```text
ok   HTTP/3   -> gateway -> h2 download: HTTP/3.0, 8388608 bytes in 7680 pieces, 49ms
ok   wss:// -> gateway -> h3 websocket: subprotocol "superchat", text and 1024 KiB binary echoed, closed code=4001 reason="bye"
```

自签证书会自动生成：upstream 把自己的写到 `$TMPDIR/fib-example-cert.pem`（网关用 `-ca` 信任它），
网关把自己的写到 `$TMPDIR/fib-gateway-cert.pem`（client 用 `-ca` 信任它）。要用真实证书，传 `-cert` 和 `-key`。

## 端口

| 程序 | 地址 | 提供 |
|------|------|------|
| upstream | `127.0.0.1:9000` | 明文 HTTP/1.1 和 ws:// |
| upstream | `127.0.0.1:9443` | TCP：HTTPS（HTTP/2、HTTP/1.1）；UDP：HTTP/3 |
| server   | `127.0.0.1:8081` | 明文 HTTP/1.1、h2c、ws://（`-plain-addr`，留空则关闭） |
| server   | `127.0.0.1:8446` | TCP：HTTPS（HTTP/2、HTTP/1.1）、wss://；UDP：HTTP/3（`-addr`） |

## 路由

路由写成 `前缀=目标`，用可重复的 `-route` 参数传入。匹配最长前缀；转发给上游的路径会去掉前缀
（`-route /api=http://host/v1` 时，`/api/users` 到上游是 `/v1/users`）。目标的 scheme 决定怎么连上游：

| 目标 | 上游协议 |
|------|----------|
| `http://host:port` | HTTP/1.1 |
| `https://host:port` | 服务端经 ALPN 选 HTTP/2 就用 HTTP/2，否则 HTTP/1.1 |
| `h3://host:port` | HTTP/3 |

WebSocket 请求：`http://` 目标走 `ws://`，其余走 `wss://`（`h3://` 目标用同一 host:port 的 TCP 上的 `wss://`，
因为拨号器只会用 HTTP/1.1 做 WebSocket 握手）。

不传 `-route` 时使用演示上游：

```text
/h1 -> http://127.0.0.1:9000     HTTP/1.1
/h2 -> https://127.0.0.1:9443    HTTP/2
/h3 -> h3://127.0.0.1:9443       HTTP/3
```

## 参数

`server`：

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `-addr` | `127.0.0.1:8446` | TLS 监听地址（TCP 与 UDP 用同一个端口号） |
| `-plain-addr` | `127.0.0.1:8081` | 明文监听地址；留空关闭 |
| `-route` | 见上 | `前缀=目标`，可重复 |
| `-timeout` | `5m` | 单个上游请求（含 body）的最长时间；`0` 不限 |
| `-max-request-body` | `64MiB` | 转发的最大请求 body |
| `-subprotocols` | `chat,superchat` | 接受客户端、并向上游请求的 WebSocket 子协议 |
| `-cert`、`-key`、`-cert-out` | 自签 | 网关自己的证书 |
| `-ca`、`-insecure` | upstream 示例的证书 | 如何验证上游 |

`client`：`-addr`、`-plain-addr`、`-size`（下载大小，默认 8 MiB）、`-ca`（默认信任网关的证书）、`-insecure`。
`upstream`：`-addr`、`-plain-addr`、`-cert`、`-key`、`-cert-out`。

## 手动试一试

先启动 upstream 和网关（`--http3` 需要你的 `curl` 支持 HTTP/3）：

```sh
curl -k --http1.1 -d hello https://127.0.0.1:8446/h2/echo
curl -k --http2   'https://127.0.0.1:8446/h1/stream?n=5'
curl -k --http3   -o /dev/null -w '%{size_download}\n' 'https://127.0.0.1:8446/h3/download?size=104857600'
curl -i 'http://127.0.0.1:8081/h1/status?code=418'
```

演示上游提供 `/echo`、`/download?size=N`、`/stream?n=5&delay=200ms`（结尾带 trailer）、`/upload`、
`/status?code=N` 和 `/ws`（回显，提供子协议 `chat`、`superchat`，收到文本 `close` 时用关闭码 4002 关闭）。

## 网关做了什么

- **请求 body**：请求头到达之后才到的大 body 用 `Context.OnBody` 收；小 body 随请求一起到。body 收齐后用
  `Client.Do` 发上游请求，此时 handler 早已返回（它 `Retain` 了请求）。
- **响应**：在上游 client 的回调里用 `ClientResponse.OnBody` 一段一段写回下游，所以任意大小的下载都以恒定内存
  通过，两边中较慢的一方给另一方施加背压（TCP、HTTP/2、QUIC 各自的流控）。trailer 会转发。
- **请求头**：丢弃逐跳头；加上 `X-Forwarded-For/Proto/Host` 和 `Via`；HTTP/1.1 与 HTTP/2 的响应带 `Alt-Svc`，
  浏览器可以据此切到 HTTP/3。
- **错误**：上游不可达 `502`，太慢 `504`，前缀没有路由 `404`；下游客户端离开会取消上游请求；上游在 body 中途
  失败会中止下游响应。
- **WebSocket**：HTTP/1.1 上可用，HTTP/2 与 HTTP/3 上通过扩展 CONNECT（RFC 8441 / RFC 9220）。消息和关闭码双向转发。

## 限制

- **请求 body 先收齐再转发**，上限 `-max-request-body`，因为客户端请求是整体发送的（`Client.Do` 不支持流式请求体）。
- **WebSocket 先升级客户端、再拨号上游**，因为 `Context.Upgrade` 必须在 handler 返回前调用。所以上游不可达时，
  是用关闭码 1014（Bad Gateway）关闭客户端连接，而不是回 HTTP 502；客户端在此期间发的消息会排队。
- HTTP/2 与 HTTP/3 下，`Context.Write` 在流控窗口里积压约 64 KB 后会等待客户端，进而通过上游自己的流控给上游施加背压。
- fib 的 WebSocket 客户端只会 HTTP/1.1，所以测试和演示 client 覆盖的是 ws:// 和 wss://；网关的 HTTP/2、HTTP/3
  WebSocket 路径留给浏览器等其他客户端。

## 测试

```sh
go test -race ./examples/gateway/...
```

测试在进程内同时启动上游和网关，覆盖 3×3 协议矩阵、大上传、取消、超时、404/502/504、上游失败、慢速客户端，
以及 WebSocket 中继和关闭码转发。
