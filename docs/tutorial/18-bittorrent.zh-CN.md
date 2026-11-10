# BitTorrent

[English](18-bittorrent.md) | [简体中文](18-bittorrent.zh-CN.md)

本章讲 fib 上的 BitTorrent 栈：一个能从 `.torrent` 文件和磁力链接下载、做种 v1、v2 与 hybrid torrent 的
客户端，以及它由哪些可独立使用的包组成。内容包括客户端的生命周期、边下载边读文件、做种、续传数据，以及 uTP、
DHT 和 UDP tracker 共用的那一个 UDP 端口。示例针对当前 checkout 编译；运行它们只需要网络和一个真实的 torrent，
此外什么都不需要——整个栈就是标准库加 fib。

## 运行模型

一个 `Client` 就是一个 fib engine（自己的，或通过 `Config.SharedEngine` 共用的）加一个端口号。TCP 上这个
端口是一个 `peer.Server`，和任何其他协议一样作为 `fib.Binding` 的 handler 提供。UDP 上它是一个
`utp.Socket`，同一个 socket 同时承载 DHT 节点和 UDP tracker 客户端：它们按数据报的首字节区分协议，所以 peer
能在 tracker 和 DHT 看到的那个端口上找到客户端。客户端内部的 `Torrent` 管着分片、peer 和发现。存储的读写、
分片的哈希都在客户端的磁盘池（`Config.DiskWorkers`）上做，从不在 fib worker 上；peer、请求、choke 和
announce 则跑在 fib 的 worker 和事件循环上。`GotInfo` 和 `Done` 是 channel，所以程序只需等它关心的两个时刻，
不必轮询，其余事件由 `Config.OnEvent` 按顺序送达。每个协议包——`bencode`、`metainfo`、`tracker`、`peer`、
`utp`、`dht`——都能脱离客户端单独使用，这正是"守护进程和库是同一份代码"的原因。

## 第一个下载

```go
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/lesismal/fib/bittorrent"
)

func main() {
	c, err := bittorrent.NewClient(bittorrent.Config{
		DataDir:    "downloads", // the torrents' files are written here
		ListenAddr: ":6881",     // TCP peers, and the one UDP port for uTP, the DHT and trackers
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	t, err := c.AddMagnet(os.Args[1], nil) // AddTorrentFile for a .torrent, AddTorrent for a metainfo
	if err != nil {
		log.Fatal(err)
	}
	<-t.GotInfo() // the info dictionary, fetched from peers (BEP 9)
	fmt.Printf("%q: %d files, %d bytes\n", t.Name(), len(t.Files()), t.Info().TotalLength())

	go progress(t)

	// Stream the first file while the rest downloads: a Read waits for its piece.
	files := t.Files()
	if len(files) == 0 {
		log.Fatal("torrent has no files")
	}
	r := files[0].NewReader()
	if _, err := io.Copy(os.Stdout, r); err != nil {
		log.Fatal(err)
	}
	if err := r.Close(); err != nil {
		log.Fatal(err)
	}

	<-t.Done() // every piece wanted is had; the client keeps seeding until Close
	fmt.Fprintf(os.Stderr, "\ndone: %.0f%%\n", t.Stats().Progress*100)
}

func progress(t *bittorrent.Torrent) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			s := t.Stats()
			fmt.Fprintf(os.Stderr, "\r%3.0f%% %7.0f KiB/s %d peers",
				s.Progress*100, s.DownloadRate/1024, s.Peers)
		case <-t.Done():
			return
		}
	}
}
```

- `AddMagnet` 解析链接（v1 的 `btih`、v2 的 `btmh`、`so` 选择文件），然后用 `ut_metadata` 从 peer 取
  info 字典。没有可用 tracker 的磁力链接需要 `AddOptions.Peers` 或 `Torrent.AddPeers` 找到第一批 peer。
- `File.NewReader` 是单个文件上的 `io.ReadSeekCloser`；`Torrent.NewReader` 按分片顺序读整个 torrent 的数据。
  `SetReadahead` 提前取更远的数据，播放器就是靠它一边下载一边无卡顿播放媒体文件；`SetContext` 用一个
  context 结束 reader。
- torrent 想要的分片全部到手时 `Done` 关闭，之后客户端继续做种。`Stats` 随时给出进度、速率、peer 数和哈希
  失败数；`Recheck` 在文件被客户端之外改动过后重新哈希校验数据。

## 添加 torrent 与选项

torrent 可以来自文件（`AddTorrentFile`）、解析好的 `*metainfo.MetaInfo`（`AddTorrent`）或磁力链接
（`AddMagnet`）。`AddOptions` 说明它如何开始，以及除客户端默认值之外还做什么：

```go
func addWithOptions(c *bittorrent.Client) (*bittorrent.Torrent, error) {
	mi, err := metainfo.Load("example.torrent")
	if err != nil {
		return nil, err
	}
	return c.AddTorrent(mi, &bittorrent.AddOptions{
		Paused:         true,                                                   // add it, Start it later
		Sequential:     true,                                                   // pieces in order, for playback while downloading
		FilePriorities: []int{bittorrent.PriorityHigh, bittorrent.PriorityNone}, // by Info.FileList index
		Trackers:       []string{"udp://tracker.example:6969"},                 // one more tier, beside the torrent's
		Peers:          []string{"192.0.2.10:6881"},                            // beside what discovery finds
	})
}
```

运行中的 torrent 用 `Torrent` 的方法调整：`Start`、`Pause`、`Stop`、`Remove(deleteData)`、`SetSequential`、
`SetFilePriority`、`SetLimits`、`AddPeers`。`PriorityNone` 表示"不要"：只由这类文件组成的分片不会下载，
选择性下载就是这么做的。

## 进度与事件

`Stats` 是给 UI 或日志用的快照；`OnEvent` 是保序的事件流，在它自己的 goroutine 上送达：

```go
func clientWithEvents() (*bittorrent.Client, error) {
	return bittorrent.NewClient(bittorrent.Config{
		OnEvent: func(e bittorrent.Event) {
			switch e.Type {
			case bittorrent.EventMetadata:
				log.Printf("%s: info dictionary in, %d files", e.Torrent.Name(), len(e.Torrent.Files()))
			case bittorrent.EventCompleted:
				log.Printf("%s: every piece wanted is had", e.Torrent.Name())
			case bittorrent.EventError:
				log.Printf("%s: %v", e.Torrent.Name(), e.Err)
			}
		},
	})
}
```

`Event` 还带着分片（校验通过、失败）和 peer（连上、断开、被封禁）。`EventQueue` 限制等待 `OnEvent` 的事件数，
超过时先丢最旧的分片和 peer 事件，随后一个 `EventDropped` 带着条数出现，`DroppedEvents` 统计总数。`State`
说明 torrent 处于哪一步：`StateMetadata`、`StateChecking`、`StateDownloading`、`StateSeeding`。`Trackers`、
`Sources`、`WebSeeds` 和 `Peers` 说明 swarm 的状态：每个 tracker URL 及其最近错误、连续失败次数和下次
announce 时间；每个 peer 来源及其候选数；每个 web seed 及其失败次数和下次重试。

## 做种

做种就是用 `metainfo.Builder` 生成 torrent、再带 `AddOptions.Seed` 加入：客户端把给出的文件当作全部分片，
不再哈希一遍。每个分片在 peer 第一次要它的块时于磁盘池上校验，所以损坏的文件在第一次发送时就会暴露：

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/lesismal/fib/bittorrent"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

func main() {
	c, err := bittorrent.NewClient(bittorrent.Config{
		DataDir:    "seeds",
		ListenAddr: ":6881",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Hash the directory into a hybrid (v1 + v2) torrent. The trackers are
	// where leechers find this seeder.
	mi, err := (&metainfo.Builder{
		Version:  metainfo.Hybrid,
		Trackers: [][]string{{"udp://tracker.example:6969"}},
	}).BuildPath("seeds/data")
	if err != nil {
		log.Fatal(err)
	}

	t, err := c.AddTorrent(mi, &bittorrent.AddOptions{Seed: true})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("seeding %q as %s", t.Name(), mi.Info.InfoHashV1())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done() // serve until interrupted
}
```

`metainfo.Builder` 还接受 `WebSeeds`（BEP 19）、`Nodes`（无 tracker 的 torrent 用的 DHT 节点）、`Private`
（BEP 27，关掉 DHT、PEX 和本地发现）以及固定的 `PieceLength`。`Seed` 与 `Resume` 同时设置会被拒绝
（`ErrSeedResume`）：做种 torrent 的续传数据已经列出了尚未校验的分片。别处生成的 torrent 不用 `Seed` 做种：
常规校验会把存储里已有的数据哈希一遍，中断或已完成的下载都由此续上。

## 续传与存储

完整校验要哈希每个分片。`ResumeData` 记下哪些分片已有、每个文件当时是什么样，下次启动时由
`AddOptions.Resume` 交回：

```go
func saveResume(t *bittorrent.Torrent, path string) error {
	data, err := t.ResumeData() // errs before the info dictionary and during a check
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644) // save after Stop, or when ResumeGen moved
}

func addResumed(c *bittorrent.Client, torrentPath, resumePath string) (*bittorrent.Torrent, error) {
	data, err := os.ReadFile(resumePath)
	if err != nil {
		return nil, err
	}
	return c.AddTorrentFile(torrentPath, &bittorrent.AddOptions{Resume: data})
}
```

每有一个分片到手 `ResumeGen` 就会变化：与上次保存时不同就保存，`Stop` 或 `Close` 之后再存一次。所在文件的
大小或修改时间变了的那些分片仍会哈希校验；这里的信任方式和 libtorrent 一样，所以大小和文件戳都不变的原地
替换是发现不了的。存储是 `Config.DataDir` 下的文件树（`NewFileStorage`、`Preallocate`、`MaxOpenFiles`），
可以用 `AddOptions.Storage` 给单个 torrent 换掉，也可以用 `NewMemoryStorage()` 创建一个完全不碰磁盘的 torrent。

## 独立可用的包

客户端由一组能独立工作的包组合而成：`bencode`（带 struct tag 的编解码、`RawMessage`、`Lookup`）、
`metainfo`（torrent、磁力链接、分片布局、`Builder`）、`tracker`（HTTP 与 UDP 客户端，以及基于 `Store`
同时服务两种协议的一个 `Server`）、`peer`（作为编解码的 wire 协议，外加管理一组 torrent 的 `Server`）、
`utp`、`dht` 和 `peer/mse`。tracker 和它服务的 peer 可以是同一个 engine 上的两个 handler：

```go
func swarmBindings(mi *metainfo.MetaInfo, h peer.Handler) []fib.Binding {
	peers := peer.NewServer(peer.Config{Encryption: mse.Prefer})
	pt := &peer.Torrent{InfoHash: mi.Info.InfoHashV1(), InfoHashV2: mi.Info.InfoHashV2(), Handler: h}
	if err := peers.AddTorrent(pt); err != nil {
		panic(err) // that info hash is already served
	}
	trk := tracker.NewServer(tracker.ServerConfig{})
	return []fib.Binding{
		{Addr: "127.0.0.1:6881", Handler: peers},                     // peers, over TCP
		{Addr: "127.0.0.1:6969", Handler: fibhttp.NewHandler(trk)},   // the HTTP tracker
		{Network: "udp", Addr: "127.0.0.1:6969", Handler: trk.UDP()}, // and the UDP one, same port
	}
}
```

共用那一个 UDP 端口，客户端才能以 tracker 和 DHT 看到的端口被访问。`utp.Socket` 读这个端口，把不是 uTP 的
数据报交给 `OnOther`，而 DHT 节点和 UDP tracker 客户端通过 `Socket.WriteTo` 发送：

```go
func shareOnePort(engine *fib.Engine, peers *peer.Server) (*utp.Socket, error) {
	sock, err := utp.Listen(engine, "udp", ":6881", utp.Config{})
	if err != nil {
		return nil, err
	}
	go func() { _ = peers.Serve(sock) }() // the peers that arrive over uTP
	node, err := dht.New(dht.Config{Send: func(to netip.AddrPort, b []byte) { _ = sock.WriteTo(b, to) }})
	if err != nil {
		return nil, err
	}
	trk := tracker.NewUDPClient(tracker.UDPClientConfig{
		Send: func(to netip.AddrPort, b []byte) error { return sock.WriteTo(b, to) },
	})
	sock.OnOther(func(from netip.AddrPort, b []byte) { // b is valid only during this call
		if !node.HandleDatagram(from, b) {
			trk.HandleDatagram(from, b) // a tracker reply, then
		}
	})
	return sock, nil
}
```

DHT 本身也是一个 peer 来源：`Node.Bootstrap` 从公共 router 填满路由表，`GetPeers` 在 peer 到达时陆续交出
某个 info hash 的 peer：

```go
err := node.GetPeers(ctx, ih, func(addrs []netip.AddrPort) {
	log.Print(addrs) // dial them, or hand them to Torrent.AddPeers
})
```

`utp.Mux`、`dht.Node` 和 tracker 客户端都不自带 socket，这正是它们能共用一个的原因；`utp.Listen`、
`dht.Listen` 和 `tracker.ListenUDPClient` 则各自添加自己的 binding，适合只需要其中之一的程序。

## 加密、版本与限制

```go
func limitedClient() (*bittorrent.Client, error) {
	c, err := bittorrent.NewClient(bittorrent.Config{
		Encryption: bittorrent.EncryptionPrefer, // MSE first; plaintext only when the peer refuses
		MaxConns:   100,                         // peer connections, all torrents together
	})
	if err != nil {
		return nil, err
	}
	c.SetLimits(bittorrent.Limits{DownloadRate: 10 << 20}) // 10 MiB/s, changed while it runs
	return c, nil
}
```

`EncryptionAllow` 先试明文，`EncryptionRequire` 两个方向都拒绝明文。速率、连接数、上传槽和做种上限属于客户端的
`Limits` 和单个 torrent 的 `TorrentLimits`；`SeedRatio` 或 `SeedTime` 达到时暂停该 torrent。v1、v2 和 hybrid
torrent 都是一等公民：纯 v2 磁力链接用 BEP 52 的 hash request 取 piece layer，hybrid 在两个 info hash 下
都会 announce。与 libtorrent、Transmission、anacrolix/torrent、libutp 和 opentracker 的线上兼容性由
[`bittorrent/interop`](../bittorrent.zh-CN.md#互通验证) 验证。

## 坑与限制

- **磁力链接只是元数据，不是 torrent。** 如果没有 peer 或 web seed 持有 info 字典，`GotInfo` 可能永远不关闭；
  给 torrent 配上 peer 或 tracker，并给等待设个上限。
- **对客户端调用 `Close` 会结束一切。** 它关闭 torrent、binding 和存储；如果程序要在同一个 engine 上继续
  提供 HTTP，就传 `Config.SharedEngine`。
- **info hash 就是身份。** 两个客户端看到的 info hash 不同，即使文件一模一样也是在下载不同的 torrent：
  `source`、不同的分片长度、或重新编码过的 info 字典都会改变 hash。请用 `metainfo.Load`/`Parse` 读 torrent。
- **续传数据和 `Seed` 的信任不等于校验。** 它们记为已有的分片在第一次被读之前不会校验；拿到坏分片的 peer
  可能封禁本客户端。
- **不要把文件拷进它正在下载的目录。** `Files()[0].NewReader` 在客户端写存储的同时从存储读；请写到别的
  writer，不要写回 `DataDir`。
- **磁盘池不是 fib 池。** 磁盘慢或有争用时调 `DiskWorkers`/`DiskQueueBytes`；在 `OnEvent` 里做存储 I/O
  阻塞的是事件流，不是 fib worker。

## 相关文档

- [BitTorrent 参考](../bittorrent.zh-CN.md) — 每个包、单端口协议对照表、客户端细节与互通验证
- [原生 TCP、UDP 与 Unix socket](19-raw-io.zh-CN.md) — 客户端所依赖的 fib 层
- [引擎与配置](02-engine.zh-CN.md) — 客户端继承的池、缓冲区和 binding
- [进阶技巧](../hacks.zh-CN.md) — UDP 的 `DatagramsHandler`、`SendBatch` 与 goroutine 速查
- [README](../../README.zh-CN.md#bittorrent) — 客户端与各包概览
