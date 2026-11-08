# BitTorrent

[English](bittorrent.md)

[`bittorrent`](../bittorrent) 下的各个包在 fib 上实现了 BitTorrent 的协议：bencode 编码、.torrent 文件与磁力链接
（v1、v2 与 hybrid）、HTTP 与 UDP 的 tracker（客户端与服务端）、带扩展和 Message Stream Encryption 的 peer
wire 协议、uTP 以及主线 DHT，还有一个用上述全部协议下载和做种的客户端。它们都与现有客户端（libtorrent、
Transmission、anacrolix/torrent）线上兼容，[`bittorrent/interop`](../bittorrent/interop) 用
libtorrent-rasterbar、anacrolix/torrent、anacrolix/dht、libutp 和 opentracker 做了互通验证。和 fib 其余部分一样，它们只依赖标准库：SHA-1、SHA-256、RC4、ed25519 都来自标准库，MSE 的
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
| [`bittorrent`](../bittorrent) | 客户端：`Client`、`Torrent`、`File`、流式 `Reader`、文件或内存 `Storage`（[详见](#客户端bittorrent-包)） | 以上全部，跑在自己的 engine 上 |
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
和 `OnWritable` 按等待写入 socket 的数据量控制上传节奏；handler 自己的处理积压时，`HoldReads` 暂停从对端读取。
在 `net.Conn` 上，读和写各用一个 goroutine，
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
`Config.Filter` 与 `fib.Binding.Filter` 含义相同：它拒绝的地址的数据报不读，不进路由表，查找、
`AddNodes`、应答中的 nodes 与 values 都排除它们。

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

一个 `Client` 就是一个 fib engine 加一个端口：TCP 跑 peer wire 协议（一个 `peer.Server`），UDP 由
`utp.Socket`、`dht.Node` 和 `tracker.UDPClient` 共用，见 [TCP 与 UDP 共用一个端口](#tcp-与-udp-共用一个端口)。
`Config` 的零值就是能用的客户端：存到当前工作目录，监听 `:6881`（`DefaultListenAddr`；端口为 0 时挑一个 TCP 与 UDP
都空闲的），支持 TCP、uTP、MSE、DHT、PEX、本地服务发现和 tracker，各项都可用对应的 `Disable*` 字段关闭。

```go
c, err := bittorrent.NewClient(bittorrent.Config{DataDir: "downloads"})
defer c.Close()
t, err := c.AddMagnet("magnet:?xt=urn:btih:...", nil) // 或 AddTorrent、AddTorrentFile
<-t.GotInfo()                                         // 从 peer 取到 info 字典
r := t.Files()[0].NewReader()                         // io.ReadSeekCloser：边下载边读
defer r.Close()
_, err = io.Copy(w, r)
<-t.Done()
```

- **torrent** 来自 .torrent 文件（v1、v2、hybrid）或磁力链接（ut_metadata；`so=` 选择文件）。`AddOptions`
  可让它以暂停状态、顺序下载、带文件优先级、额外的 peer 和 tracker 或自己的存储加入。`Torrent` 有 `GotInfo`、
  `Done`、`Removed` 三个 channel，`State` 与 `Err`，`Start`、`Pause`、`Stop`、`Remove(deleteData)`，
  `SetSequential`，`SetFilePriority`（`PriorityNone`、`Low`、`Normal`、`High`），`AddPeers`、`Files`、`Stats`、
  `Peers` 和 `NewReader`。`Config.OnEvent` 按顺序收到 metadata、校验完成、分片校验通过、哈希失败、下载完成、
  peer 连上、断开、被封禁、达到做种上限、状态变化和错误等事件。
- **存储**是一个 `Storage`（按文件序号的 `ReadAt`、`WriteAt`，以及 `Close`）：`DataDir` 下的文件
  （`NewFileStorage`，路径经 `SanitizePath` 按操作系统处理安全，BEP 47 的填充从不写入），或内存
  （`NewMemoryStorage`）。加入 torrent 时会对存储里已有的数据做哈希校验，中断的下载因此可以续传。
- **快速续传**可以跳过这次校验：`Torrent.ResumeData()` 返回一小段带版本号的 bencode 数据（info hash、
  已有分片的位图，以及存储报告的每个文件的大小和修改时间；存储须实现 `StatStorage`，两种内置存储都实现了），
  `AddOptions.Resume` 在 `AddTorrent`、`AddTorrentFile` 或 `AddMagnet` 时把它交回（磁力链接在拿到 info 字典后才用）。
  其中列出、且所在文件都未变的分片不经哈希直接记为已有；所在文件大小或修改时间变了的分片照常哈希校验；
  所在文件缺失的分片，以及数据中没有列出的分片，既不读也不记为已有。数据解不开、属于别的 torrent、
  版本更新，或存储不是 `StatStorage` 时，以 `Err` 包装 `ErrResumeData` 的 `EventError` 报告，并对存储做完整校验。
  每有一个分片到手 `ResumeGen()` 就会变化：与上次保存时不同就保存，并在 `Stop` 或 `Client.Close` 之后再存一次。
  info 字典到手之前、以及 torrent 正在校验存储时，`ResumeData` 返回错误（请保留之前保存的数据）。

  ```go
  data, _ := t.ResumeData()           // t.Stop() 之后，或 t.ResumeGen() 变化时
  os.WriteFile(resumePath, data, 0o644)
  // 重启时：
  t, _ = c.AddTorrentFile(torrentPath, &bittorrent.AddOptions{Resume: data})
  ```

  和 libtorrent 一样它信任这些文件戳：大小和修改时间都不变的原地修改发现不了，记为已有的分片发给 peer
  时也不再校验（收到坏分片的 peer 会在自己的校验中失败，并可能封禁本客户端）。
- **磁盘池**：存储的读写和分片哈希从不在 fib worker 上（也不在读 uTP 连接的 goroutine 上）进行。
  `Config.DiskWorkers` 个 goroutine（GOMAXPROCS，最多 `DefaultMaxDiskWorkers` = 8）从同一个队列取任务：收到的块
  拷进池化缓冲区后排队写入，分片的最后一块落盘后在池上校验整个分片；上传的读也在池上，每个 peer 同时只有一个
  任务，每个任务最多 16 块，节奏仍受 `Congested`、`OnWritable` 和 `UploadRate` 控制；BEP 52 hash request 的应答和
  取到的 piece layer 的校验也在池上。等待写入的块受 `Config.DiskQueueBytes`（`DefaultDiskQueueBytes`，64 MiB）
  限制：超过后，发来这些块的连接停止读取（`peer.Conn.HoldReads`，即 `fib.Connection.HoldReads` 或暂停读
  goroutine），直到写掉一半。`Stop`、`Remove` 和 `Close` 等正在进行的读写结束后才关闭存储；仍在排队的被丢弃。
- **选片**：最稀有优先，相同时随机，头 4 个分片随机挑；文件优先级、顺序模式和打开着的 `Reader` 的窗口
  （最近的分片最急；默认预读 4 MiB，可用 `SetReadahead` 调整）优先。16 KiB 的块按每个 peer 的 `reqq` 流水请求；
  endgame 时一个块同时向第二个 peer 请求，较慢的那个被取消。60 秒没发任何东西的 peer 被判为 snub，
  它的请求转给别人。
- **校验**：v1 用 SHA-1，v2 用 merkle 树。纯 v2 磁力链接的 torrent 用 BEP 52 的 hash request 取 piece layer，
  客户端也用手里的 layer 应答 peer 的 hash request（hybrid 磁力链接下载完后自行算出 layer）。整个分片都由一个
  peer 发来却校验失败，该 peer 被封禁；由多个 peer 拼成时，等分片校验通过后逐块比对，发了坏块的被封禁。
- **choke**：四个上传名额（`UploadSlots`）给下载速度最快的 peer（做种时给拿得最多的），另有每 30 秒轮换一次的
  optimistic unchoke，新 peer 的机会是三倍。Fast 扩展：have all 与 have none，10 个 allowed fast 分片，
  被 choke 的 peer 的请求回 reject。上传节奏受 socket（`Congested`、`OnWritable`）和 `UploadRate` 控制。
- **连接**先拨 uTP 再拨 TCP；先 MSE、被拒后再明文（默认的 `EncryptionPrefer`），`EncryptionAllow` 反过来，
  `EncryptionRequire` 绝不用明文；受 `MaxConns`（200）、`MaxConnsPerTorrent`（50）、`MaxHalfOpen`（32）和
  `DialsPerTick`（8）限制，失败后退避。与同一 peer 的两条连接，保留 peer ID 较小的一方发起的那条。
- **peer** 来自 tracker（分 tier，发 started、completed、stopped，hybrid 用两个哈希都 announce）、DHT（每 15 分钟
  announce 一次，没有 peer 时每分钟一次；双向 `PORT`）、PEX（每个 peer 每分钟一条）、本地服务发现（BEP 14，
  IPv4）、磁力链接的 `x.pe`、`AddPeers` 和 `Config.PeerSources`；私有 torrent 只用它的 tracker。web seed（BEP 19）
  经 fib/http 客户端用 range 请求提供整个分片。
- **做种**到 `SeedRatio` 或 `SeedTime` 时暂停 torrent；`Ban` 在所有 torrent 中封禁一个地址。
- **限制**可在客户端运行时修改，不断开已有 peer：`Client.SetLimits(Limits)` 替换 `UploadRate`、`DownloadRate`、
  `MaxConns`、`MaxConnsPerTorrent`、`MaxHalfOpen`、`DialsPerTick`、`UploadSlots`、`SeedRatio` 和 `SeedTime`
  （零值保持它在 `Config` 里的含义：速率和做种上限为不限，数量为默认值），`Client.Limits` 返回当前值。每个 torrent
  有自己的 `TorrentLimits`（`AddOptions.Limits`、`Torrent.SetLimits`、`Torrent.Limits`）：`UploadRate`、
  `DownloadRate` 叠加在客户端的速率之下（每个字节两边都要通过），`MaxConns`、`UploadSlots`、`SeedRatio`、
  `SeedTime` 取代客户端的；零值继承客户端，负值表示该 torrent 不限。新速率在 1/4 秒内生效；调低连接上限立即
  关闭多出的 peer，最没用的先关（两个方向都没有数据流动的、再是被 snub 的、再是最慢的、再是最新的）；
  `UploadSlots` 变化立即重新 choke；做种上限每秒检查一次。
- **地址过滤**：`Config.Filter func(netip.AddrPort) bool` 与 `fib.Binding.Filter` 含义相同（返回 true
  即服务该 peer，每次都问、从不缓存，IPv4 映射地址先还原为 IPv4）。它是 TCP 绑定的 `Binding.Filter`、
  uTP `Mux` 的 `Config.Accept`（被拒地址的 SYN 不建连接），也是 DHT 节点的 `dht.Config.Filter`（被拒地址
  得不到应答、不进路由表、查找不经过它、也不作为 peer 返回）；来自 tracker、DHT、PEX、LSD、磁力链接
  `x.pe`、`AddPeers` 和 `PeerSources` 的 peer 在成为候选之前被丢弃，每次拨号前和握手时再检查一次。
  tracker 与 web seed 是 URL，不经过它。已打开的连接直到 `Client.Refilter` 才重新询问：它关闭 Filter
  现在拒绝的连接（按远端地址或监听地址），包括握手中的，并忘掉被拒的候选；策略变更后调用它。`Ban`
  与之分开。
- **共享引擎**：`Config.SharedEngine` 让客户端运行在调用方已在运行的引擎上，而不是自建引擎（此时忽略
  `Config.Engine`）。客户端把它的 TCP 绑定和 UDP 绑定（`utp.Listen`）加到该引擎上，在其上拨号、运行
  DHT 和 UDP tracker 客户端，并在其上建 fib/http 客户端（tracker、web seed）；`Close` 移除这两个绑定
  （TCP 绑定用 `Engine.ShutdownBinding` 加一个已结束的 context，立即关闭它接受的连接），关闭它拨出的
  连接和 HTTP 客户端，引擎及其他绑定和连接照常运行。`Engine()` 返回正在用的引擎。多个客户端可共享
  一个引擎，各用各的端口。

## 互通验证

[`bittorrent/interop`](../bittorrent/interop) 是独立模块（`replace github.com/lesismal/fib => ../..`），
anacrolix/torrent v1.61.0、anacrolix/dht v2.23.0 和 go-libutp v1.5.1 永远不会成为 fib 的依赖。go-libutp 通过
cgo 封装 libutp，所以这个模块需要 cgo 和 C++ 编译器，可在 Linux 与 macOS 上跑，不支持 Windows。
libtorrent-rasterbar 2.0.11 经它的 Python 绑定（`python3-libtorrent`、`libtorrent_peer.py`）驱动，因此相应测试只在
`FIB_LIBTORRENT=1` 时运行：`libtorrent.sh` 在容器里跑它，CI 任务则安装这个包并设置该变量。以上全部在 CI 中对应
`bittorrent-interop` 任务；各包解析网络数据的函数在 `fuzz-bittorrent` 任务里做 fuzz。

| 包 | 验证内容 |
| --- | --- |
| metainfo | fib 生成的 v1、v2、hybrid torrent 在 anacrolix 里读出相同结果（info hash、文件、pieces root、`ValidatePieceLayers`）；anacrolix 的 torrent 在 fib 里解析结果相同，分片校验通过；文件顺序一致时 v1 info 字典逐字节相同；从 anacrolix 模块目录读取它的测试 torrent（含 libtorrent 的 v2 与 hybrid 测试 torrent，找不到则跳过）；磁力链接往返一致 |
| tracker | anacrolix 的 tracker 客户端在 127.0.0.1 与 ::1 上向 fib 的 HTTP、UDP 服务端 announce 和 scrape，passkey 经 BEP 41 传递；fib 的客户端向 anacrolix 的 HTTP、UDP tracker 服务端 announce；手动测试中，fib 的客户端经 HTTP 和 UDP 向 opentracker（Debian 的包，跑在 docker 里）announce 和 scrape |
| peer、mse | 经 TCP，fib 从 anacrolix 做种端取得 metadata 和全部分片并校验通过：明文、提供明文的 MSE、要求 RC4 的 MSE；anacrolix 只凭 info hash 从 fib 做种端下载 3 MB：默认设置、强制 RC4、双方都要求 RC4、强制明文；fib 的 `mse` 与 anacrolix 的 `mse` 包双向握手，SKEY 是 100 个之一 |
| utp | fib 与 libutp（go-libutp）双向互为两端：每个方向 4 MB 并校验；写、关闭、读到 EOF；经过每 20 个数据报丢 1 个、每 7 个压后 1 个的转发器，每个方向 1 MB |
| 客户端，anacrolix | fib 客户端从 anacrolix 下载、anacrolix 从 fib 客户端下载，分别只经 TCP 和只经 uTP（go-libutp），来源为 .torrent 和 btih 磁力链接，含要求 RC4 的情形和 hybrid torrent；fib 客户端经 fib 的 `tracker.Server` 找到 anacrolix 做种端。anacrolix 无法加入纯 v2 torrent，这部分由 libtorrent 覆盖 |
| 客户端，libtorrent | 22 个用例全过：双方互相下载 v1、hybrid 和纯 v2 torrent，来源为 .torrent 文件和磁力链接（btih；v2 用 btmh，piece layer 双向经 BEP 52 hash request 取得），经 TCP 和经 uTP，含要求 RC4 的情形 |
| dht | 回环上 6 个 fib 节点与 6 个 anacrolix 节点：互相 bootstrap 并出现在对方路由表中，双向 ping，find_node 收敛，双向 announce 与 get_peers（显式端口与 implied port），anacrolix 向 fib 发 sample_infohashes，双向存取 BEP 44 不可变与可变数据，双方对 200 个地址算出相同的 BEP 42 ID；手动测试中 fib 从公网 DHT bootstrap（13 秒），2.6 秒内为一个 Ubuntu 镜像找到 240 个 peer |

```sh
cd bittorrent/interop
go test -race -count=1 -timeout 12m ./...                                    # 全部（CI）
go test -race -count=1 -run 'Torrents|Magnet' .                              # metainfo
go test -race -count=1 -run Tracker .                                        # tracker
go test -race -count=1 -run TestPeer .                                       # peer、mse
CGO_ENABLED=1 go test -count=1 -timeout 300s -run TestUTP -v .               # utp，35-65 秒
go test -race -count=1 -run DHT .                                            # dht
go test -race -count=1 -run TestClient .                                     # 客户端对 anacrolix
./libtorrent.sh                                                              # 客户端对 libtorrent，需要 docker
FIB_LIBTORRENT=1 CGO_ENABLED=1 go test -count=1 -run TestClientLibtorrent .  # 同上，本机已装 python3-libtorrent 时
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
- **libutp：SYN 的应答丢了，就永远留下一个半开连接。** libutp 对已有的连接 ID 不理重发的 SYN，而那个半开连接
  从不超时，只会重发 SYN 的发起方会一直等下去。fib 的发起方用同一个 ID 重发一次，之后就重置、换一个新 ID 重来；
  libutp 仍可能把那个被放弃的连接交给它的应用。
- **anacrolix/torrent v1.61.0：连接的写协程会丢失唤醒。** 发请求的理由（比如一次 unchoke）若恰好在写协程填满缓冲之后、
  开始等待之前到来，就被丢掉了（`chansync.BroadcastCond`），直到保活定时器（`KeepAliveTimeout`，一分钟）触发才会
  发请求。对端是无话可说的做种方时，1–2% 的运行里下载会停一分钟；互通测试把保活设为 2 秒。

## BEP 覆盖

| BEP | 标题 | 状态 | 包 |
| --- | --- | --- | --- |
| 3 | The BitTorrent Protocol Specification | 完整 | `bencode`、`metainfo`、`tracker`、`peer`、`bittorrent` |
| 5 | DHT Protocol | 完整（含 torrent 的 `nodes` 键、`port` 消息与保留位）；客户端尚不持久化节点状态 | `dht`、`metainfo`、`peer`、`bittorrent` |
| 6 | Fast Extension | 完整（用 BEP 的测试向量验证） | `peer` |
| 7 | IPv6 Tracker Extension | 完整 | `tracker`、`bittorrent` |
| 9 | Extension for Peers to Send Metadata Files | 完整（ut_metadata、磁力链接） | `peer`、`metainfo` |
| 10 | Extension Protocol | 完整 | `peer` |
| 11 | Peer Exchange (PEX) | 完整 | `peer` |
| 12 | Multitracker Metadata Extension | 完整 | `metainfo`、`tracker`、`bittorrent` |
| 14 | Local Service Discovery | 部分：仅 IPv4 | `bittorrent` |
| 15 | UDP Tracker Protocol | 完整，客户端与服务端 | `tracker`、`bittorrent` |
| 16 | Superseeding | 未做 | — |
| 19 | WebSeed (GetRight style) | 完整：读取 `url-list` 与 `ws`，用 range 请求取整个分片 | `metainfo`、`bittorrent` |
| 21 | Extension for Partial Seeds | 客户端完整（想要的都齐了就发 `upload_only`，也遵从对端的；tracker 客户端有 `EventPaused`）；tracker 服务端对 paused peer 不做特殊处理 | `tracker`、`peer`、`bittorrent` |
| 23 | Tracker Returns Compact Peer Lists | 完整 | `tracker` |
| 24 | Tracker Returns External IP | 完整 | `tracker` |
| 27 | Private Torrents | 完整：`private` 标志，客户端只用 tracker，以及服务端的白名单与鉴权钩子 | `metainfo`、`tracker`、`bittorrent` |
| 29 | uTorrent transport protocol | 完整，但没有路径 MTU 探测和时钟漂移惩罚 | `utp`、`bittorrent` |
| 30 | Merkle Hash Torrent Extension | 未做（已被 BEP 52 取代） | — |
| 31 | Failure Retry Extension | 未做 | — |
| 32 | IPv6 extension for DHT | 部分：每个地址族一个 `Node`，忽略对另一地址族的 `want`；客户端的 IPv6 节点可选（`DHTIPv6`） | `dht`、`bittorrent` |
| 33 | DHT Scrape | 未做 | — |
| 35 | Torrent Signing | 未做 | — |
| 38 | Finding Local Data Via Torrent File Hints | 未做 | — |
| 40 | Canonical Peer Priority | 未做 | — |
| 41 | UDP Tracker Protocol Extensions | 完整 | `tracker` |
| 42 | DHT Security Extension | 完整 | `dht` |
| 43 | Read-only DHT Nodes | 完整 | `dht` |
| 44 | Storing Arbitrary Data in the DHT | 完整，put 一方不带 `cas` | `dht` |
| 47 | Padding Files and Extended File Attributes | 完整 | `metainfo` |
| 48 | Tracker Protocol Extension: Scrape | 完整，不支持 full scrape | `tracker` |
| 51 | DHT Infohash Indexing | 完整 | `dht` |
| 52 | The BitTorrent Protocol Specification v2 | 完整：torrent、握手升级、发出 hash request 并用 piece layer 应答；`proof_layers` > 0 时返回的证明未经互通测试 | `metainfo`、`peer`、`bittorrent` |
| 53 | Magnet URI Extension: Select Specific File Indices | 完整 | `metainfo`、`bittorrent` |
| 55 | Holepunch Extension (ut_holepunch) | 未做 | — |
| — | Message Stream Encryption (MSE/PE) | 完整 | `peer/mse`、`peer`、`bittorrent` |

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
- **`Conn.HoldReads`** 在 `net.Conn` 传输上让读 goroutine 在下一次读之前停住，在 fib 传输上就是
  `fib.Connection.HoldReads`；不嵌套计数。备选：在 handler 里阻塞，而 fib worker 不能阻塞。
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

- **`ListenAddr` 默认 `:6881`**；端口为 0 时取 TCP 拿到的端口，若 UDP 上该端口被占则重试。备选：默认 `:0`。
- **`Encryption` 的零值是 `EncryptionPrefer`**（先 MSE，与 libtorrent、anacrolix 相同），没有完全关闭 MSE 的模式。
- **本地服务发现只支持 IPv4**，用标准库的 `net.ListenMulticastUDP`，因为 fib 没有组播 binding；加入组播组失败时
  静默关闭。
- **封禁**按 IP（回环上的 peer 按它的监听地址），在客户端整个生命周期内有效。
- **块直接写入存储**（经磁盘池），校验分片时再读回；内存里不攒分片，smart ban 的哈希也来自读回的数据。
- **每个客户端一个磁盘池、一个 FIFO 队列**，写入、分片校验、上传读取和 hash 应答共用：简单，也足够公平，因为
  上传任务每 16 块就让出一次。备选：每个 torrent 或每类任务一个队列（像 libtorrent 的磁盘线程那样先写后读），
  或每个任务一个 goroutine，那就什么都限制不住。
- **`DiskWorkers` 默认 min(GOMAXPROCS, 8)**：哈希吃 CPU，而超过 8 个并发写对磁盘很少有帮助。备选：固定数目，
  或把哈希和 I/O 分成两组 worker。
- **字节上限只算收到的块**（`DiskQueueBytes`，64 MiB）：这是 peer 能撑大的内存。上传读取和分片校验只在运行时
  持有缓冲区，最多 DiskWorkers 个；它们的任务数受每个 peer 一个上传任务、每个在途分片一次校验限制。备选：
  所有缓冲区都计入，或按 torrent 设上限。
- **上限是软的**：块从不被拒绝或丢弃，因为它到达时在 fib worker 上，不能等待；达到上限后停止读取，所以会超出
  已经读到的数据（每条连接一轮读）。降到上限一半时恢复读取，所有被暂停的连接一起恢复。备选：超出时丢弃块，
  由对端重发。
- **块写入时发现存储已关闭**（torrent 已停止或移除）则丢弃，不发 `EventError`，该块重新变为缺失。真正的写入
  错误仍发 `EventError`。
- **关闭存储时等待正在进行的读写**（每个 torrent 一把锁，任务在 I/O 期间持有），而不等待队列：排队的任务发现
  存储已不在就什么也不做。因此 `Stop`、`Remove` 和 `Client.Close` 最多等每个磁盘 worker 一次慢 I/O。
- **不在池上的**：初始校验（每个 torrent 一个 goroutine，同以前）、web seed 的写入与校验（在它的下载
  goroutine 上）、`Reader` 的读取（调用者的 goroutine），以及 `StorageOpener`——磁力链接的 torrent 在 info 字典
  到达时于 fib worker 上调用它：opener 不能阻塞（默认的不做 I/O）。收到的 info 字典的 SHA-1 也在那里算，只算一次，
  最多几 MiB。
- **想要的空文件在初始校验结束时创建**，而不是下载完成时，这样 `Done` 关闭时它们已经存在；之后才选中的仍在完成
  时于池上创建。
- **从不请求填充字节**（anacrolix 拒绝落在填充里的请求）。
- **纯 v2 torrent 提供文件最后一个完整的 16 KiB 块**，超出文件末尾的部分补零，因为 libtorrent 请求整块。
- **BEP 52 hash request** 每次最多请求 512 个哈希、`proof_layers` 为 0（与 anacrolix 相同），拼好的 layer 与文件的
  root 比对。应答时附 max(0, `proof_layers` − log2(length)) 层 uncle，这是对 libtorrent 语义的推测，只有
  不带证明的路径做过互通测试；只提供 piece layer。
- **hybrid torrent 用 v1 哈希（并设 v2 位）拨号**，除非 peer 来自 v2 的 announce；hybrid 向 tracker、DHT、LSD
  两个哈希都 announce。
- **做种时按上传速度 unchoke**（libtorrent 的 fastest_upload），而非轮转；完整做种时断开做种者和只上传的 peer；
  候选 peer 失败后在 2 秒 << 失败次数后重试，失败 6 次即丢弃（手动加入的除外）。
- **`Reader` 提升了未选中文件的分片优先级时**，`Done()` 重新打开，直到这些分片到齐。
- **快速续传数据是调用方保存的一段字节**（`ResumeData` / `AddOptions.Resume`），而不是客户端写的文件：
  调用方（fit）本来就持久化自己的 torrent 列表，存在哪、何时存由它决定。备选：在 `Config` 里配续传目录，由客户端写。
- **快速续传信任大小和修改时间**（存储报告的纳秒值），与 libtorrent 相同；同样文件戳下的原地修改发现不了，
  续传得来的分片发给 peer 时也不再校验。备选：每个文件存内容哈希（要读整个文件，正是续传要省的），
  或每个分片第一次上传时校验（每次首传多一次读和哈希，以后可加）。
- **续传对每个分片的处理**：列出且文件都未变：记为已有，不读；有文件变了（大小或修改时间，含变短）：
  照常哈希校验；有文件缺失：不读，不记为已有；未列出且文件未变：不读，重新下载。最后一种会丢掉保存时
  已写完但未校验的分片和部分分片；若要校验它们，每次启动都得读完全部未完成（常是预分配的）数据。
  备选：像 libtorrent 的 `unfinished` 那样把部分分片的块位图存进数据。
- **不能用的续传数据以包装 `ErrResumeData` 的 `EventError` 报告**，并退回完整校验：解不开、版本为 0
  或高于 1、info hash 不符（出现的每个 hash 都须相符）、位图或文件数对不上，或存储不是 `StatStorage`。
  备选：单独的事件类型；`EventError` 不改变 torrent 状态，可用 `errors.Is` 区分。
- **"该保存了"的信号是 `ResumeGen()`**，即已有分片数（只增不减），而不是事件：`EventPieceVerified`
  已经逐片触发，一个廉价计数让调用方可以定时检查、有变化才保存。写未完成分片会改变文件戳但不动它，
  所以调用方要在 `Stop` / `Client.Close` 之后再存一次。
- **torrent 校验存储期间 `ResumeData` 返回错误**：此时位图不完整，保存会丢掉还没校验到的分片；
  已停止的 torrent 只为取文件戳重新打开存储（`fileStorage.Stat` 只调用 `os.Stat`）。备选：校验期间返回加入时的数据。
- **续传数据不含 v2 piece layer**：续传的 v2 磁力 torrent 有分片，但只为仍需校验的分片获取 layer；
  需要的调用方自己保存 `MetaInfo()` 的字节。备选：把 layer 放进数据，每分片 32 字节。
- **`MemoryStorage` 实现了 `StatStorage`**，每次写入（以及 `Corrupt`）用进程内严格递增的时钟，
  因此续传在同一进程内可用（opener 对同一 info hash 返回同一存储）。
- **`Preallocate` 用 truncate** 把文件设到全长（稀疏文件），而不是 `fallocate`。
- **web seed**：每个 seed 同时最多 4 个分片在途，失败 5 次即弃用，只取整个分片。
- **事件**在一个分发 goroutine 上按顺序投递，从不阻塞客户端；连上与断开事件附带 `PeerInfo` 快照。
- **运行时限制是独立的 `Limits` 类型**，而不是嵌入 `Config`，这样 `Config{UploadRate: ...}` 这类字面量仍能编译；
  NewClient 从 `Config` 构造最初的 `Limits`。备选：把这些字段移进嵌入的 `Config.Limits`（只有一处定义，但是
  破坏性改动）。
- **零值沿用 `Config` 中的含义**：速率和做种上限为不限，数量为默认值；`Limits` 里负值等同零值，`Limits()`
  返回填好默认值的数量。备选：零值表示"不变"，那样表达"不限"就需要指针或掩码。
- **`TorrentLimits` 中零值继承、负值不限**（`MaxConns` 只受客户端 `MaxConns` 约束，`UploadSlots` unchoke 所有
  感兴趣的 peer，做种上限永不暂停）。每个字节都要通过两个令牌桶，torrent 的速率只能比客户端低，所以负速率等同零值。
  torrent 的 `MaxConns` 取代 `MaxConnsPerTorrent`，可以更高。备选：单独的 `NoSeedLimit` 标志；每 torrent 值以
  客户端值为上限。
- **下载速率在请求块时扣除**，而不是在数据到达时，这样限速是少发请求（peer 不会多发），而不是读进来再丢掉。
  请求后来被取消（snub、被 choke、peer 断开）时已扣的令牌不退还；令牌桶容量为一秒的速率，每次补充最多透支一个块，
  所以回环上被限速的传输在几秒内可达速率的约 1.5 倍。web seed 仍在到达时扣除。备选：退还取消的请求，或更小的桶。
- **调低连接上限时最没用的 peer 先关**：两边都没有"感兴趣且未被 choke"的，再是被 snub 的，再是上下行速率之和
  最低的，再是最新的；先按每个 torrent，再按客户端的 `MaxConns`。连回已满客户端的拨号和以前一样在握手时被拒。
  `UploadSlots` 无论调高调低都立即重新 choke；调高做种上限不会重启已因它暂停的 torrent（需 `Start`）。
- **`Config.Filter`（A78）**：一个以 `netip.AddrPort` 为参数的谓词，与 `fib.Binding.Filter` 同形，而不是
  IP 段列表类型，便于 fit 用原子指针背后的黑白名单和 eMule `.p2p`/`.dat` 封锁列表实现。UDP 绑定不设
  `Binding.Filter`，因为该端口也是 UDP tracker 的，而 tracker 不是 peer：uTP 用已有的
  `utp.Config.Accept`（不加 utp API），DHT 用新增的 `dht.Config.Filter`，它不读被拒节点的数据报，并把
  它排除在种子节点、查找、应答的 nodes 与 values 以及 `GetPeers`/`Announce` 结果之外。已在路由表中的
  被拒节点不立即删除，而是不再被应答，像沉默节点一样被淘汰。扩展握手声明的监听端口被拒的 peer 会被
  关闭，与按该地址封禁一样。备选：fib 提供 IP 前缀集合类型；或用 UDP 绑定的 `Binding.Filter`，但那也
  会切断被拒地址上的 tracker 和 DHT router。
- **`Client.Refilter()`（A78）**：关闭 Filter 现在拒绝的已打开和握手中的连接，并丢弃既未连接也未在拨号
  的被拒候选，等来源再次给出时才回来。被关闭的连接以 `EventPeerDisconnected` 结束，不是
  `EventPeerBanned`，也不被封禁。备选：一个自带重新过滤的策略设置方法（`SetFilter`）；分开是为了 fit
  一次策略变更可以重新过滤多个客户端和其他模块。
- **`Config.SharedEngine *fib.Engine`（A79）**：放在 `Config.Engine` 旁边的字段，而不是
  `NewClientOn(engine, cfg)` 构造函数，这样单靠一个 Config 就描述了客户端，其他构造路径也不需要第二个
  构造函数。引擎必须已在运行；NewClient 会等它的事件循环。`Close` 时用 `ShutdownBinding` 加一个已结束
  的 context 移除 TCP 绑定，core 文档说明这会立即关闭该绑定的连接；没有新增 core API，因为
  `RemoveBinding` 和 `ShutdownBinding` 已在所有后端存在。拨出的连接由客户端自己关闭（peer 和半开拨号；
  `Close` 之后才完成的拨号会自行关闭）。`Close` 时 HTTP 客户端尚在进行的请求随其 context（torrent 的
  context 已取消）结束，而不是靠停止引擎。备选：`NewClientOn` 构造函数；core 的
  `Engine.RemoveBindingAndConns`。

