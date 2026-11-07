# BitTorrent

[English](bittorrent.md)

[`bittorrent`](../bittorrent) 下的各个包在 fib 上实现了 BitTorrent 的协议：bencode 编码、.torrent 文件与磁力链接
（v1、v2 与 hybrid）、HTTP 与 UDP 的 tracker（客户端与服务端）、带扩展和 Message Stream Encryption 的 peer
wire 协议、uTP 以及主线 DHT。它们都与现有客户端（libtorrent、Transmission、anacrolix/torrent）线上兼容，
[`bittorrent/interop`](../bittorrent/interop) 用 anacrolix/torrent、anacrolix/dht、libutp 和 opentracker
做了互通验证。和 fib 其余部分一样，它们只依赖标准库：SHA-1、SHA-256、RC4、ed25519 都来自标准库，MSE 的
768 位 Diffie-Hellman 用 `math/big` 实现。

## 包

| 包 | 内容 | 运行在 |
| --- | --- | --- |
| [`bencode`](../bittorrent/bencode) | bencode 编解码，`encoding/json` 风格的 struct tag，`RawMessage`，原地解码与流式两种 `Decoder` | — |
| [`metainfo`](../bittorrent/metainfo) | v1、v2、hybrid 的 .torrent 文件，info hash，分片与文件的对应，分片校验（SHA-1 与 SHA-256 merkle），`Builder`，磁力链接 | — |
| [`tracker`](../bittorrent/tracker) | HTTP(S) 与 UDP 的 tracker 客户端，BEP 12 的 tier，以及基于可替换 `Store`、同时服务两种协议的 `Server` | fib/http、fib UDP |
| [`peer`](../bittorrent/peer) | peer wire 协议：不带 I/O 的编解码，以及管理一组 torrent 的 `Server`，其 `Conn` 跑在 fib TCP 或任意 `net.Conn` 上 | fib TCP、`net.Conn` |
| [`peer/mse`](../bittorrent/peer/mse) | Message Stream Encryption（MSE/PE），不带 I/O 的握手，以及 `net.Conn` 上的封装 | — |
| [`utp`](../bittorrent/utp) | 带 LEDBAT 的 uTP（BEP 29）：不带 socket 的 `Mux`，以及跑在 fib UDP binding 上的 `Socket` | fib UDP |
| [`dht`](../bittorrent/dht) | 不带 socket 的主线 DHT `Node`，也可跑在自有 binding 上 | fib UDP |
| `bittorrent` | 客户端 | 见[客户端](#客户端bittorrent-包) |
| [`interop`](../bittorrent/interop) | 独立模块，放互通测试，永远不会成为 fib 的依赖 | — |

## TCP 与 UDP 共用一个端口

BitTorrent 客户端只监听一个端口号：TCP 上接 peer，UDP 上同时承载 uTP peer、DHT 以及 UDP tracker 客户端收到的应答。
UDP 上的几个部分都不自带 socket，因此可以共用一个，按数据报的第一个字节区分：

| 首字节 | 协议 | 判断方法 |
| --- | --- | --- |
| `0x01`–`0x41`（类型加版本 1） | uTP | `utp.IsUTP` |
| `'d'`（bencode 字典） | DHT | `dht.Node.HandleDatagram` 认领 |
| `0x00`（action 0–3 的高字节） | UDP tracker 应答 | `tracker.IsUDPReply`、`tracker.UDPClient.HandleDatagram` |

`utp.Socket` 负责读这个端口：uTP 交给它的 `Mux`，其余交给 `OnOther` 设置的函数；DHT 和 tracker 客户端通过
`Socket.WriteTo` 发送。`Socket` 同时是 `net.Listener`，所以 `peer.Server.Serve` 可以接收从 uTP 来的 peer，
而同一个 `peer.Server` 又是同一端口号上 TCP binding 的 `fib.Handler`：

```go
peers := peer.NewServer(peer.Config{Encryption: mse.Prefer})
engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: ":6881", Handler: peers}) // TCP peer
go engine.Run()

sock, err := utp.Listen(engine, "udp", ":6881", utp.Config{})
go peers.Serve(sock) // uTP peer
node, err := dht.New(dht.Config{Send: func(to netip.AddrPort, b []byte) { _ = sock.WriteTo(b, to) }})
trk := tracker.NewUDPClient(tracker.UDPClientConfig{
	Send: func(to netip.AddrPort, b []byte) error { return sock.WriteTo(b, to) },
})
sock.OnOther(func(from netip.AddrPort, b []byte) { // 在 fib worker 上调用，b 只在返回前有效
	if !node.HandleDatagram(from, b) {
		trk.HandleDatagram(from, b)
	}
})
```

## bencode

`Marshal` 和 `Unmarshal` 像 `encoding/json` 那样在 bencode 与 Go 值之间映射：struct tag 支持
`bencode:"key,omitempty"` 和 `bencode:"-"`，字典的键排序后写出，整数解码到各种 int、uint 时检查范围，`bool`
对应 0 或 1，字节串可解码到 `string`、`[]byte` 或数组（如 info hash 的 `[20]byte`）。解码到 `any` 时得到
`int64`、`string`、`[]any` 和 `map[string]any`。类型不对的值会被跳过，等其余部分解码完再报告。

`RawMessage` 原样保留一个值收到时的字节。info hash 需要它：info hash 是文件里 info 字典编码后的哈希。
`Lookup(torrent, "info")` 不解码任何东西就能找到这些字节，`Split` 和 `Valid` 用来遍历值。

语法检查始终是严格的（不允许前导零、`-0`，值之后不能有多余内容），`Limits`（`MaxDepth` 256、`MaxSize` 64 MiB）
限制恶意输入。默认接受未排序和重复的键（以最后一个为准），因为真实的 torrent 里就有；`Decoder.Strict`
要求规范形式。`NewBytesDecoder` 原地解码：设了 `NoCopy` 后，`[]byte` 和 `RawMessage` 直接引用输入而不复制，
`InputOffset` 给出下一个值的起点，ut_metadata 消息（一个字典后跟原始数据）正需要这个。`NewDecoder` 从
`io.Reader` 流式解码。

## metainfo

`Parse` 和 `Load` 读取 v1（BEP 3）、v2 和 hybrid（BEP 52）torrent，保留 info 字典的原始字节，用于
`InfoHashV1`（SHA-1）和 `InfoHashV2`（SHA-256）。客户端不能信任的内容会被拒绝：可能逃出下载目录的文件路径、
冲突的路径、与长度对不上的分片哈希、v1 与 v2 文件列表不一致的 hybrid torrent，以及错误的 piece layer。

`Info.FileList` 按顺序给出各文件及其偏移，包括 BEP 47 的填充文件；纯 v2 torrent 里每个文件都从分片边界开始。
`PieceSize`、`PieceSegments`、`Segments` 把分片和字节区间映射到文件上。`VerifyPiece` 按 v1 的 SHA-1、v2 的
merkle 树（经文件的 piece layer，单分片文件则用它的 pieces root）或两者校验一个分片；`MerkleRoot`、`PadHash`、
`LayerRoot`、`VerifyProof` 是 BEP 52 的 hash request 所需的辅助函数。

`Builder` 对一个 `fs.FS`（`Build`）或路径（`BuildPath`）计算哈希，生成 v1、v2 或 hybrid torrent，可带 tracker
（BEP 12）、web seed（BEP 19）、DHT 节点（BEP 5）和 private 标志（BEP 27）。`ParseMagnet` 与 `Magnet.String`
处理磁力链接（BEP 9）：十六进制或 base32 的 `xt=urn:btih:`、v2 的 `urn:btmh:`、`dn`、`tr`、`ws`、`x.pe`、
`so`（BEP 53）和 `xl`。

## tracker

客户端：

- `HTTPClient` 通过 fib/http 的 `Client` 做 announce 和 scrape，因此自带 HTTPS、keep-alive 和 HTTP/2。它能读
  compact 与字典两种 peer 列表（BEP 23）、`peers6`（BEP 7）、失败与警告消息、interval 与 min interval、
  tracker id 以及 `external ip`（BEP 24）。
- `UDPClient` 不带 socket 地实现 BEP 15：经 `UDPClientConfig.Send` 发送，应答由 `HandleDatagram` 交给它，
  因此可以共用客户端的 UDP 端口。`ListenUDPClient` 则给它自有的 binding。connection ID 保留一分钟，
  重传间隔 15 × 2^n 秒（`Timeout`、`Retransmits` 可调），并发送 BEP 41 的 URL 数据。
- `Client` 按 URL 的 scheme 选用二者之一；`Tiers` 按 BEP 12 依次向 announce-list 的各 tier announce（只洗牌一次，
  应答了的 tracker 提到前面，保留 tracker ID）。

`Server` 基于同一个 `Store` 同时提供两种协议的 tracker：`ServeHTTP` 是 fib/http 的 handler（路径最后一段以
`announce` 或 `scrape` 开头，passkey 放在它前一段），`UDP()` 是 UDP binding 的 `fib.Handler`。UDP 的
connection ID 是对密钥、当前分钟和客户端地址做的 HMAC-SHA256，有效一到两分钟，因此不为每个客户端保存状态。
默认的 `Store` 是 `MemoryStore`：64 个分片，限制 torrent 数、每个 torrent 的 peer 数和 peer 总数，peer 超过
`PeerTTL` 不再 announce 即过期，随机挑选返回的 peer（随机起点加互质步长），给做种者只返回下载者。私有 tracker
可用 `ServerConfig.AllowInfoHash` 做白名单，用 `Authorize` 检查每个请求的 `Auth`：对端地址、URL（UDP 上是
BEP 41 的数据）和 passkey（`?passkey=` 或 `/<key>/announce`）。

```go
srv := tracker.NewServer(tracker.ServerConfig{})
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Addr: ":6969", Handler: fibhttp.NewHandler(srv)},
	fib.Binding{Network: "udp", Addr: ":6969", Handler: srv.UDP()})
```

## peer 与 mse

编解码不带 I/O：`AppendHandshake` 与 `ParseHandshake` 处理握手，`Reserved` 表示扩展位（`SetExtended`、
`SetDHT`、`SetFast`、`SetV2`），`NewPeerID` 生成 Azureus 风格的 peer ID；`Parser` 接受任意大小切片的字节流，
逐条产出 `Message`，分片数据整块到达时直接引用输入；`AppendMessage` 编码。消息覆盖 BEP 3（以及 BEP 5 的
`port`）、Fast 扩展（BEP 6：suggest、have all、have none、reject、allowed fast，`AllowedFastSet` 计算集合）、
扩展协议（BEP 10：`ExtendedHandshake`）、ut_metadata（BEP 9：`MetadataMessage`，以及校验 SHA-1 或 SHA-256
后返回解析好的 `metainfo.Info` 的 `MetadataAssembler`）、ut_pex（BEP 11：`Pex`，以及计算待发增量、限制接收频率的
`PexState`），还有 BEP 52 的 hash request、hashes、hash reject 消息。`Bitfield` 是分片集合。

`Server` 管理一组 torrent（`AddTorrent`、`RemoveTorrent`）。作为 TCP binding 的 `fib.Handler`，它按
`Config.Encryption` 的允许对每个连接做 MSE 或明文握手，然后交给对端所请求 info hash 对应的 `Torrent`：v1 哈希，
或截断的 v2 哈希；对端设置了 v2 位时，hybrid torrent 的 v1 握手会升级到 v2。`Dial` 在 fib 上拨出，`Open`
用于 `net.Conn`，`ServeConn` 和 `Serve` 接收任意 `net.Conn` 或 `net.Listener`（如 uTP 的）上的连接。每个连接
是一个 `Conn`，它的 `Handler` 收到握手（`OnHandshake`）、每条消息（`OnMessage`）和关闭（`OnClose`），
还可实现 `OnWritable`（`WritableHandler`）。

`Conn` 跟踪 choke、interest 和未完成的请求：`Request` 让在途请求不超过对端的 `reqq`（以及
`Config.MaxRequests`，64），并考虑 choke 状态和 allowed fast 分片；没有 Fast 扩展时，choke 会丢弃这些请求。
它每两分钟发一次 keepalive，静默三分钟后关闭。handler 在回调里发送的内容在回调返回时一次写出；`Congested`
和 `OnWritable` 按等待写入 socket 的数据量控制上传节奏。在 `net.Conn` 上，读和写各用一个 goroutine，
双方都不会因为对方没读而互相等待。

`mse` 包是 MSE/PE：768 位 Diffie-Hellman，丢弃前 1024 字节的 RC4，两种角色都支持。`Initiator` 与 `Receiver`
是不带 I/O 的握手；`KeyTable` 以 O(1) 找到接收方被请求的 torrent（SKEY）；`Client` 与 `Server` 在 `net.Conn`
上跑握手。`Policy` 有 `Allow`（默认：优先明文，也接受加密）、`Prefer`、`Require` 三种；接收方也能识别明文握手。

## utp

uTP（BEP 29）是 UDP 上可靠的字节流，拥塞控制用 LEDBAT，与 libutp 线上兼容；Transmission、早期的 libtorrent
以及 anacrolix/torrent（经 go-libutp）用的都是 libutp。

- `Mux` 是不带 socket 的 uTP：一个 UDP 端口上的全部连接，按对端地址和 connection ID 区分。
  `HandleDatagram(from, b)` 接收一个数据报并报告它是否是 uTP；它引起的 ACK 和数据包等到 `Flush` 才发出，
  调用方每处理完读到的一批调用一次，这样一批数据报只需每个连接一个 ACK、一批发出。`Dial` 发起连接；
  `Mux` 也是对端发起的连接的 `net.Listener`（`Accept`、`AcceptUTP`）。
- `Socket` 是跑在 fib UDP binding 上的 `Mux`：`OnDatagrams` 把某个对端的一批数据报一起交给 `Mux`，再用一批
  （Linux 上是 `sendmmsg`）回应；它从同一端口拨出（`DialContext`、`DialAddrPort`），并把非 uTP 数据报交给
  `OnOther`。`Listen` 把它加到运行中的 engine 上，也可以用 `NewSocket` 与 `Attach` 让它成为 `fib.New`
  某个 binding 的 handler。`utp.Dial` 与 `utp.DialContext` 从自有 binding 拨出。
- `Conn` 是带 deadline 的 `net.Conn`，另有 `CloseWrite`、`RTT` 和 `Stats`。

协议细节沿用 libutp：版本 1 的包头，SYN、DATA、STATE、FIN、RESET，libutp 的 connection ID 规则，16 位序号
及回绕，双向 selective ACK，收到三个重复 ACK 或 SACK 显示空洞后已有三个包时快速重传，按平滑 RTT 计算并指数退避的
重传超时（`MinRTO` 500 ms），沿用 libutp 延迟历史的 LEDBAT（`TargetDelay` 100 ms），通告窗口等于读端的空闲缓冲，
小写入的类 Nagle 合并（`NoDelay` 可关闭），每 29 秒一次 keepalive，带 eof_pkt 语义的 FIN。在 libutp 之外，
连接还用 RACK 判丢包并做尾部丢包探测（RFC 8985）：libutp 自己有数据要发时不发 selective ACK，双向传输时
否则每次丢包都只能靠超时发现。包大小固定 1400 字节（`PacketSize`），没有路径 MTU 探测，也没有时钟漂移惩罚。

## dht

`Node` 是主线 DHT（BEP 5）的一个节点：路由表由 k-bucket（K = 8）组成，围绕自身 ID 分裂，节点分 good、
questionable、bad 三态（15 分钟），每个 bucket 有替补缓存，驱逐 questionable 节点前先 ping，安静的 bucket 定期
刷新。只有回应过我方查询的节点才能进表，每个公网 IP 最多一个。它响应 ping、find_node、get_peers、announce_peer
（含 implied_port）、sample_infohashes（BEP 51），以及 BEP 44 不可变与可变（ed25519）数据的 get 与 put。token
绑定请求方 IP，有效 5 到 10 分钟；announce 来的 peer 保留 30 分钟（`PeerTTL`），数据保留两小时，数量受
`MaxTorrents`、`MaxPeersPerTorrent`、`MaxItems` 限制；查询按来源 IP 限速（`QueryRate`）。

查找是 Kademlia 的迭代查找，同时在途 `Alpha`（3）个查询：`FindNode`、`GetPeers`（peer 一到就交给回调）、
`Announce`（之后向给了 token 的最近 K 个节点 announce）、`Get`、`GetMutable` 和 `Put`；`Ping` 与
`SampleInfohashes` 只问一个节点。`Bootstrap` 从 `Config.Routers`（为 nil 时用 `DefaultRouters`）以及
`AddNodes`、`AddTorrentNodes` 加入的节点开始；`State` 保存 ID 与路由表，供重启后恢复。

BEP 42：外部地址已知时（`Config.PublicIP`，或由响应中 `ip` 键投票得出），ID 由它派生；`Config.EnforceNodeID`
把 ID 与地址不符的节点挡在路由表外。BEP 43：`Config.ReadOnly` 的节点只查询、不响应。BEP 32：一个 `Node`
只属于一个地址族，`Config.IPv6` 的节点使用 `nodes6`，双栈客户端跑两个 `Node`。

`New` 创建的 `Node` 经 `Config.Send` 发送，由 `HandleDatagram` 喂入数据，后者认领以 `'d'` 开头的数据报；
`Listen` 让它跑在自有 binding 上。定时器都是 `time.AfterFunc`：每个在途查询一个，另有一个每隔几秒维护节点。

```go
node, err := dht.Listen(engine, ":6881", dht.Config{})
err = node.Bootstrap(ctx)
n, err := node.Announce(ctx, infoHash, 0, true, func(peers []netip.AddrPort) {
	// 连接这些 peer
})
```

## 客户端（bittorrent 包）

<!-- CLIENT SECTION: to be filled -->

## 互通验证

[`bittorrent/interop`](../bittorrent/interop) 是独立模块（`replace github.com/lesismal/fib => ../..`），
anacrolix/torrent v1.61.0、anacrolix/dht v2.23.0 和 go-libutp v1.5.1 永远不会成为 fib 的依赖。go-libutp 通过
cgo 封装 libutp，所以这个模块需要 cgo 和 C++ 编译器，可在 Linux 与 macOS 上跑，不支持 Windows。CI 中对应
`bittorrent-interop` 任务；各包解析网络数据的函数在 `fuzz-bittorrent` 任务里做 fuzz。

| 包 | 验证内容 |
| --- | --- |
| metainfo | fib 生成的 v1、v2、hybrid torrent 在 anacrolix 里读出相同结果（info hash、文件、pieces root、`ValidatePieceLayers`）；anacrolix 的 torrent 在 fib 里解析结果相同，分片校验通过；文件顺序一致时 v1 info 字典逐字节相同；从 anacrolix 模块目录读取它的测试 torrent（含 libtorrent 的 v2 与 hybrid 测试 torrent，找不到则跳过）；磁力链接往返一致 |
| tracker | anacrolix 的 tracker 客户端在 127.0.0.1 与 ::1 上向 fib 的 HTTP、UDP 服务端 announce 和 scrape，passkey 经 BEP 41 传递；fib 的客户端向 anacrolix 的 HTTP、UDP tracker 服务端 announce；手动测试中，fib 的客户端经 HTTP 和 UDP 向 opentracker（Debian 的包，跑在 docker 里）announce 和 scrape |
| peer、mse | 经 TCP，fib 从 anacrolix 做种端取得 metadata 和全部分片并校验通过：明文、提供明文的 MSE、要求 RC4 的 MSE；anacrolix 只凭 info hash 从 fib 做种端下载 3 MB：默认设置、强制 RC4、双方都要求 RC4、强制明文；fib 的 `mse` 与 anacrolix 的 `mse` 包双向握手，SKEY 是 100 个之一 |
| utp | fib 与 libutp（go-libutp）双向互为两端：每个方向 4 MB 并校验；写、关闭、读到 EOF；经过每 20 个数据报丢 1 个、每 7 个压后 1 个的转发器，每个方向 1 MB |
| dht | 回环上 6 个 fib 节点与 6 个 anacrolix 节点：互相 bootstrap 并出现在对方路由表中，双向 ping，find_node 收敛，双向 announce 与 get_peers（显式端口与 implied port），anacrolix 向 fib 发 sample_infohashes，双向存取 BEP 44 不可变与可变数据，双方对 200 个地址算出相同的 BEP 42 ID；手动测试中 fib 从公网 DHT bootstrap（13 秒），2.6 秒内为一个 Ubuntu 镜像找到 240 个 peer |

```sh
cd bittorrent/interop
go test -race -count=1 -timeout 12m ./...                                    # 全部（CI）
go test -race -count=1 -run 'Torrents|Magnet' .                              # metainfo
go test -race -count=1 -run Tracker .                                        # tracker
go test -race -count=1 -run TestPeer .                                       # peer、mse
CGO_ENABLED=1 go test -count=1 -timeout 300s -run TestUTP -v .               # utp，35-65 秒
go test -race -count=1 -run DHT .                                            # dht
./opentracker.sh                                                             # opentracker，需要 docker
FIB_DHT_PUBLIC=1 FIB_DHT_INFOHASH=<40 位十六进制> go test -run TestDHTPublic -v .   # 公网 DHT，需要联网
```

### 发现的上游 bug

- **libutp：发起方的 `fast_resend_seq_nr`。** `utp_connect` 以随机序号开始连接，却把 `fast_resend_seq_nr`
  留在 `utp_create_socket` 设的 1。对大约一半的随机起始序号，这个 1 比较起来总是在已发出的包之前，于是发起方
  收到 selective ACK 也从不重传，只在一秒以上的重传超时后才重传。经过有丢包的转发器，libutp 对 libutp 传 4 MB
  要 45 秒，任一端换成 fib 则是 12–16 秒。记录在 `bittorrent/interop/utp_test.go`。
- **anacrolix/dht v2.23.0：`peer_store.InMemory` 从不返回 announce 来的 peer。** 它按 IP 存 peer，取出时却把键
  当完整地址解码，返回的 IP 和端口是错乱的。互通测试给 anacrolix 服务端换了一个正确的 store。（反过来，
  anacrolix 拒绝不带 `seq` 的 BEP 44 不可变 put，所以 fib 的 put 带上 `seq` 0。）

## BEP 覆盖

| BEP | 标题 | 状态 | 包 |
| --- | --- | --- | --- |
| 3 | The BitTorrent Protocol Specification | 完整 | `bencode`、`metainfo`、`tracker`、`peer` |
| 5 | DHT Protocol | 完整（含 torrent 的 `nodes` 键、`port` 消息与保留位） | `dht`、`metainfo`、`peer` |
| 6 | Fast Extension | 完整（用 BEP 的测试向量验证） | `peer` |
| 7 | IPv6 Tracker Extension | 完整 | `tracker` |
| 9 | Extension for Peers to Send Metadata Files | 完整（ut_metadata、磁力链接） | `peer`、`metainfo` |
| 10 | Extension Protocol | 完整 | `peer` |
| 11 | Peer Exchange (PEX) | 完整 | `peer` |
| 12 | Multitracker Metadata Extension | 完整 | `metainfo`、`tracker` |
| 15 | UDP Tracker Protocol | 完整，客户端与服务端 | `tracker` |
| 19 | WebSeed (GetRight style) | 部分：torrent 的 `url-list` 与磁力链接的 `ws` | `metainfo` |
| 21 | Extension for Partial Seeds | 部分：`EventPaused`、`upload_only`；服务端不做特殊处理 | `tracker`、`peer` |
| 23 | Tracker Returns Compact Peer Lists | 完整 | `tracker` |
| 24 | Tracker Returns External IP | 完整 | `tracker` |
| 27 | Private Torrents | 部分：`private` 标志，以及服务端的白名单与鉴权钩子 | `metainfo`、`tracker` |
| 29 | uTorrent transport protocol | 完整，但没有路径 MTU 探测和时钟漂移惩罚 | `utp` |
| 31 | Failure Retry Extension | 未做 | — |
| 32 | IPv6 extension for DHT | 部分：每个地址族一个 `Node`，忽略对另一地址族的 `want` | `dht` |
| 33 | DHT Scrape | 未做 | — |
| 35 | Torrent Signing | 未做 | — |
| 38 | Finding Local Data Via Torrent File Hints | 未做 | — |
| 41 | UDP Tracker Protocol Extensions | 完整 | `tracker` |
| 42 | DHT Security Extension | 完整 | `dht` |
| 43 | Read-only DHT Nodes | 完整 | `dht` |
| 44 | Storing Arbitrary Data in the DHT | 完整，put 一方不带 `cas` | `dht` |
| 47 | Padding Files and Extended File Attributes | 完整 | `metainfo` |
| 48 | Tracker Protocol Extension: Scrape | 完整，不支持 full scrape | `tracker` |
| 51 | DHT Infohash Indexing | 完整 | `dht` |
| 52 | The BitTorrent Protocol Specification v2 | `metainfo` 完整；`peer` 部分：握手升级与 hash 消息的编解码，尚不能用 piece layer 应答 hash request，也不校验证明（待客户端） | `metainfo`、`peer` |
| 53 | Magnet URI Extension: Select Specific File Indices | 部分：`so` 的解析与生成，由客户端应用 | `metainfo` |
| — | Message Stream Encryption (MSE/PE) | 完整 | `peer/mse`、`peer` |

<!-- CLIENT SECTION: to be filled -->

## 待审的决定

开发中自行做出的决定，记录在此待审：问题、选择及原因，以及备选方案。

### bencode 与 metainfo

- **Builder 默认版本**：hybrid，纯 v1 和 v2 客户端都能读。备选：v1，体积更小，也是多数工具的做法。
- **文件顺序**：按路径分量排序，v1 也一样，使 hybrid 的 v1 列表与 v2 文件树一致。anacrolix 按拼接后的路径字符串
  排序，所以 `z.bin` 与 `z/x` 在它那里顺序不同。
- **hybrid 的填充**：每个不在分片边界结束的非末尾文件之后，加一个属性为 `p` 的 `.pad/<n>` 文件，与 libtorrent
  相同；不记录 `x`、`h` 属性，使生成结果可复现。
- **自动分片大小**：从 16 KiB 起、使分片数少于 2048 的最小 2 的幂，最大 16 MiB（`ChoosePieceLength`）。
- **Builder 接受什么**：普通文件和指向文件的符号链接；跳过指向目录的符号链接、其他特殊文件和空目录；
  什么都没有的 torrent 报错。
- **解析的宽容度**：`info` 之外类型不对的键忽略；`info` 之内的留空，再由校验报错；字典之后的空白接受。
- **piece layer**：缺失的接受（客户端可从 peer 取），错误的报错。hybrid 的 `VerifyPiece` 在缺 layer 时退回 v1；
  纯 v2 返回 `ErrNoPieceLayer`。
- **路径**：拒绝 `..`、`.`、空分量、`/` 和 NUL；Windows 保留名、反斜杠、仅大小写不同的冲突留给存储层处理。
- **v2 单文件 torrent**：只在文件树只有一项且键等于 `name` 时成立。
- **bencode 语法**：前导零与 `-0` 始终拒绝，不设 `Strict` 也一样。
- **解码到 `any`**：整数为 `int64`，字节串为 `string`。
- **限制**：流式解码 `MaxDepth` 256、`MaxSize` 64 MiB；`Load` 最多读 128 MiB（`MaxFileSize`）。
- **磁力链接**：拒绝全零哈希；`so` 按区间保存。
- **Builder 单 goroutine 计算哈希。** 备选：每个 CPU 一个哈希 goroutine。

### tracker

- **HTTP tracker 用 fib/http 的 `Client`，不用 `net/http`。** 备选：一个两者都能满足的 `Doer` 接口。
- **`NumWant`**：0 表示用 tracker 的默认值，负数表示不要（anacrolix 用 -1 表示默认）。
- **负的 `Left`**（大小未知）按 `MaxInt64` 发送。
- **服务端只记录来源地址**；`ip`、`ipv4`、`ipv6` 参数和 UDP 的 IP 字段只在 `TrustIPParam` 时生效，
  这样 tracker 不能被用来把流量反射到第三方。
- **store 按 peer ID 存 peer**，同一 peer 的 IPv4 与 IPv6 announce 合并，代价是知道某个 ID 的人能改写它的地址。
  备选：像 opentracker 那样按 ip:port 存。
- **默认值**：interval 30 分钟，numwant 50、上限 200，UDP 应答最多 1400 字节，`PeerTTL` 一小时；最多 1M 个
  torrent、每个 torrent 64K 个 peer、共 2M 个 peer（最坏约 300 MB）。
- **做种者只拿到下载者**；BEP 21 的 paused peer 不做特殊处理。
- **不支持 full scrape**；HTTP scrape 最多 64 个哈希（`MaxScrape`），UDP 最多 74 个。
- **connection ID 不对的 UDP 请求直接丢弃**，不回错误。
- **UDP scrape 不带 URL 数据**，所以 UDP 上 passkey 只保护 announce。
- **UDP announce 只发往解析出的第一个地址**（`udp` 时 IPv4 优先），不回退到其他地址。
- **UDP 重传**：connect 与 announce 共用一个计数，transaction ID 复用；connection ID 过期则重新 connect。
- **`Tiers.Timeout`** 每个 tracker 30 秒（`DefaultTrackerTimeout`）。
- **8 字节的空 UDP announce 应答视为 `*Error`**，opentracker 就这样发。

### peer 与 mse

- **加密策略**：`Allow`（默认，优先明文）、`Prefer`、`Require`。备选：libtorrent 那样分入站、出站策略并设加密级别。
- **MSE 拨号被拒后不自动改用明文重试**：这是客户端的选择。
- **MSE**：内层 BitTorrent 握手的 info hash 必须等于 SKEY。
- **BEP 52 升级**：作为接收方，对端设置 v2 位时把 hybrid 的 v1 握手升级为 v2（anacrolix 从不这样做）；
  作为发起方，只在用 v1 哈希拨 hybrid torrent 时设置 v2 位。
- **IPv6 的 allowed fast 集合**：地址按 /48 掩码，对全部 16 字节做哈希。
- **peer 连接不开写背压**，两个互相上传的 peer 不会死锁；上传节奏由 `fib.Connection.NotifyOutput` 和
  `MaxQueuedBytes`（1 MiB）经 `Congested` 与 `OnWritable` 控制。在 `net.Conn` 上，写 goroutine 的队列不设上限。
- **限制与默认值**：消息最大 1 MiB + 64，metadata 最大 16 MiB，通告的 `reqq` 为 250，在途请求最多 64 个，
  握手超时 20 秒，空闲 3 分钟，keepalive 2 分钟，扩展 ID ut_metadata 为 1、ut_pex 为 2。
- **PEX**：距上一条被采纳的消息不到 45 秒的消息忽略，不断开连接；发送时每个地址族截断到 50 个 peer；六个键都发。
- **违反协议即关闭连接**：没有 Fast 位却发 Fast 消息、没有 BEP 10 却发扩展消息、扩展握手无效。
- **MSE 填充**：PadC 为空，PadA、PadB、PadD 随机 0–512 字节；最多接受 512。
- **peer ID 前缀** `-FB0001-`（"FB" 不是登记过的客户端代码）。
- **choke 与 cancel**：没有 Fast 扩展时，choke 丢弃在途请求，cancel 立即丢弃请求；有 Fast 扩展时等 reject 或 piece。

### utp

- **`MinRTO` 500 ms**，按 BEP 29（libutp 为 1 秒）。
- **首次超时 1 秒，约 15 秒内发 4 个 SYN**（libutp：3 秒，约 21 秒内 3 个 SYN）。
- **慢启动**每个 RTT 窗口翻倍，直到延迟达到目标的 90%（libutp 每个 RTT 约增加一个包）。
- **窗口不足一个包时**仍允许一个包在途。
- **接收方可以在发起方发数据之前先写**；这些包的 ACK 比 SYN 少一，使 SYN 应答丢失的 libutp 发起方不会卡住。
- **fib 作为发起方**连上后发一个 STATE。
- **重复的 SYN 再次应答。**
- **过时但不超前的 ACK**：保留数据，忽略 ACK。
- **在 libutp 之外加了 RACK、尾部丢包探测和三个重复 ACK 的快速重传。**
- **零窗口探测**从 max(RTO, 500 ms) 开始，翻倍到 15 秒（libutp 固定 15 秒）。
- **超时**：空闲 3 分钟，半开 10 秒，关闭后 linger 30 秒再 RESET；空闲、linger 到期和 `Mux.Close` 时发 RESET。
- **双方 FIN 之后**最多停留 3 秒，以便再次确认对端的 FIN。
- **命名为 `Socket`** 而不是 `Listener`，因为它也拨出、也承载其他协议。
- **`HandleDatagram` 把发送推迟到 `Flush`**，调用方每批之后必须调用。
- **固定 1400 字节的包，32 位 SACK 掩码。**
- **接收方最多接受 `RecvBuffer` 再加 8 个包**，超出则丢弃。
- **没有时钟漂移惩罚，每个包一次内存分配。**

### dht

- **每个地址族一个 `Node`**，与 libtorrent 相同；忽略对另一地址族的 `want`。
- **路由表准入**：只收回应过我方查询的节点；来查询我方的节点，所在 bucket 有空位时先 ping；
  应答里带来的节点要等它们回应后才进表。
- **每个公网 IP 一个节点**（私有与回环地址除外）。
- **节点失效**：连续 2 次不应答为 bad，5 次后移除；不可配置。
- **查找**只把应答了的节点计入 K；超过 `QueryTimeout`/4 未应答的查询不再占 `Alpha` 名额；查找从不重发，
  ping 与单个查询重发一次；每次查找最多 64 个候选、256 个查询。
- **`Bootstrap`** 在查找自身 ID 后返回（若 BEP 42 改了 ID，最多 3 次）；刷新在后台继续，节点数少于 K 时
  自动重新 bootstrap。
- **默认值**：每个 IP 每秒 20 个查询、允许 5 秒的突发；4096 个 torrent × 512 个 peer；2048 条数据；get_peers
  最多返回 64 个值（IPv6 为 32）；BEP 51：20 个样本，间隔 6 小时。
- **未知方法**带 `target` 或 `info_hash` 的按 find_node 应答，其余回错误 204。
- **`k` 与签名为零的 put 视为不可变数据。**
- **版本 `v` = `"FB\x00\x01"`**（未登记的客户端代码）。
- **BEP 42 投票**至少需要 3 个不同投票 IP，最多保留 64 个；`Config.ID` 设定的 ID 永不改变；`EnforceNodeID`
  默认关闭。
- **token**：对请求方 IP 做 HMAC-SHA1，取 8 字节。
- **未做**：BEP 33，以及 put 时的 `cas`。

### 客户端（bittorrent 包）

<!-- CLIENT SECTION: to be filled -->
