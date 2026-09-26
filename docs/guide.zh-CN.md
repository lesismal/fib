# Go 版网络库

这是 [`c/`](../c) 目录下 C11 实现的 Go 移植版，保留相同的核心架构：

- 单个 edge-triggered event loop（Linux 上是 epoll，macOS 上是 kqueue，Windows 上是 IOCP）
  独占所有事件注册和 fd 关闭操作。Linux 和 macOS 上默认开启 `IOPollers`，连接分到多个 event loop
  上（见下）；关闭后只有一个 event loop。
- connection 是本地 `taskpool.TaskPool` 的任务单位。TaskPool 使用常驻、有界
  worker，避免短事件触发大量 goroutine 创建和栈扩容；connection 不与某个
  worker 固定绑定。
- 每个 connection 有 FIFO 事件队列；同一 connection 串行执行，不同 connection 动态负载均衡。
- ET 读写均排空到 `EAGAIN`；仅发送队列非空时关注 `EPOLLOUT`。
- 每轮事件处理先 flush 发送队列，再读 OOB，再读普通数据；发送队列仍有数据时跳过读取，并把可读状态保留到下一轮，待可写事件清空队列后再读，既限制用户态缓冲又不会漏读。
- 读 buffer 由 `bufferpool` 子 package 按字节对齐的尺寸档复用（见下），大小通过
  `Config.ReadBufferSize` 设置，默认 16 KiB。
- `Config.TaskPoolMode` 可选 `taskpool.ModeCond`（基于 `sync.Cond` 的有界环形
  队列，按 worker 数分片）、`taskpool.ModeElastic`（nbio 风格的弹性
  fork/dispatcher）或 `taskpool.ModeAdaptive`（见下，所有后端的默认值，
  `taskpool.New` 也默认使用它）。`taskpool.ModeInline`（`taskpool.NewInline`）
  不起 worker，也没有队列，直接在提交任务的 goroutine 上带 recover 执行，开启
  `IOPollers` 时 Engine 自己的池就是这种（见下）。
- `taskpool.ModeAdaptive` 同样分片，但常驻数量随负载在下限和上限之间变化，调度方式
  也不同：
  - 唤醒：每个空闲 worker 有自己的唤醒通道，按后进先出压在空闲栈上，被唤醒的总是
    最近停下、栈和缓存都还热的那个。唤醒的对象是"队列"而不是"某个任务"：到达队列的
    worker 会一直取任务直到取空。每个分片最多保持 2 个"已唤醒、尚未到达队列"的
    worker；worker 取走一个任务后如果还剩下比正在赶来的更多的任务，就再叫醒下一个。
    于是任务阻塞时 worker 逐跳扇出，任务很短时已经在跑的 worker 就能消化，不必为
    一批 n 个任务付出 n 次唤醒（ModeCond 的做法，在繁忙服务器上这部分调度开销占了
    池的大部分 CPU）。
  - 扩容：只有当所有 worker 都在忙、且没有正在赶来的 worker 时才新起一个，直到上限。
    已被唤醒、还没开始跑的 worker 算作"正在赶来"，所以一批连发的任务按实际忙起来的
    worker 数扩容，而不是一个任务一个。
  - 分片共享上限：上限是整个池的，不按分片均分，忙的分片可以用掉其他分片暂时不用的
    额度；没有 worker 的分片总能起第一个，保证落到它上面的任务有人执行。额度用完时，
    缺 worker 的分片会把其他分片空闲栈上的 worker 调过来（连同它占的那份下限一起），
    复用它的热栈，而不是让任务在本分片排队、别的分片的 worker 闲着。
  - 提交选分片：轮流选分片；轮到的分片有任务在排队、又没有空闲 worker 时，再随机看
    另一个分片，交给积压（排队任务数减空闲 worker 数）更少的那个，避免把任务继续
    塞给被阻塞任务占满、或队列已满的分片。
  - 取任务不加锁：队列是一个生产者（持分片锁提交）、多个消费者（worker 用 CAS 取）
    的有界环形队列，繁忙分片里的 worker 不会互相排队抢锁；锁只保护提交、空闲栈和
    队列满时的等待。
  - 缩容：后台每隔 `ShrinkInterval`（默认 1 秒）看一次上个周期里**最少**有几个
    worker 空闲，这些 worker 整个周期都没用上（就是空闲栈底部那些），从栈底退掉其中
    一半，但不低于下限。按一半
    退是为了突发过后分几个周期逐步回落，而不是把下一次突发要用的 worker 一次退光。
  - 运行中可以用 `TaskPool.Resize(min, max)` 调整上下限：调高下限立即补足 worker；
    调低上限时，空闲 worker 立即退出，忙碌的在手头任务完成后退出。
    `TaskPool.Workers()` 返回当前 worker 数（其他 Mode 调 `Resize` 返回 false）。
  - 直接使用：`taskpool.NewAdaptive(taskpool.AdaptiveConfig{Name: "jobs", MinWorkers: 16,
    MaxWorkers: 4096, QueueSize: 10000})`；`NewWithMode(name, ModeAdaptive, max, queue)`
    的下限为 0。下限为 0 时，空闲的池会把 worker 全部退掉，有任务来时再启动。在 fib 里，
    `WorkerCount` 是上限，`Config.MinWorkerCount` 是下限（默认 0）。
- 池容量按 Mode 分别给默认值，因为 `WorkerCount` 在不同 Mode 下含义不同
  （ModeAdaptive 与 ModeElastic 一样是上限，默认值也相同）：
  ModeCond 会预先创建这么多协程并让它们挂在条件变量上，这个数就是实际存在的
  协程数量，多了只是让调度器在同样的核上搬运更多协程；ModeElastic 则是按需
  fork、空闲短暂驻留后回收，这个数是上限而不是实际数量，调高在负载没到之前
  不产生开销。`DefaultPoolSizing(mode)` 返回对应 Mode 的默认值。
- `SetTaskPoolMode(mode)` 切换 Mode 时会同时把 `WorkerCount`、`MaxEvents`
  换成该 Mode 的默认值；`SetPoolSizing(workerCount, maxEvents)` 固定为自己的
  取值，之后再调 `SetTaskPoolMode` 也不会被覆盖，两者调用顺序无关。传 0 表示
  该项保持不变：

  ```go
  config := fib.DefaultConfig()
  config.SetTaskPoolMode(taskpool.ModeCond)  // 容量随之切到 cond 的默认值
  config.SetPoolSizing(500, 10000)           // 固定成自己的取值
  ```
- 背压有两道界，暂停读的原因只能是其中之一，`Engine.Stats()` 会分别计数
  （`ReadsPausedByWatermark`、`ReadsPausedByBudget`、`ReadsResumed`、
  `PendingBytes`）：
  - `WriteBufferHighWatermark` 只看这条连接自己积压了多少，是对端跟不上；
  - `MaxPendingBytes` 是整个 server 共享的总量，被别的连接耗尽时，**一条自己
    几乎没有积压的连接也会被暂停读**。排查「水位线明明远大于单条消息却触发了
    背压」时，先看这两个计数哪个在涨。
- 注意水位线要比**一轮读取**产生的回包总量大，而不只是比单条消息大：一轮读取
  是 corked 的，这一轮内所有回包先进队列、轮末一次性 flush，所以即使 TCP 发送
  缓冲区是空的，队列里也会短暂地存在这一轮的全部回包；一轮最多读
  `ReadBufferSize` 字节。严格一问一答、单条 1KiB、水位线 8KiB 这种配置有 8 倍
  余量，不会触发背压（`TestPingPongUnderWatermarkNeverPausesReads` 固定了这一点）。
- `Config.Name` 是 Engine 的名字（`Engine.Name()`），未设置时为 `fib.DefaultName`
  即 `"fib"`。它出现在 Engine 启动（`Run`）时打印的日志里，也决定了协程池的名字：
  Engine 自己的协程池叫 `<Name>-workers`，HTTP/2 与 HTTP/3 的 handler 协程池叫
  `<Name>-streams`。
- `SharedTaskPool` 默认开启；同一进程内**同名**的多个 Engine 共享 worker 和任务队列，
  避免多监听端口重复创建大量 goroutine 与队列。共享只看名字、不看配置：第一个 Engine
  按自己的配置创建协程池，之后的同名 Engine 直接使用它，其余配置不生效。需要隔离时
  可以给 Engine 起不同的名字，或把 `SharedTaskPool` 设为 `false`（仍用同名的
  `<Name>-streams`）。
- 状态日志默认关闭（`log/slog`，可用 `slog.SetDefault` 调整输出）。`config.LogStatus = true`
  时 Engine 启动后打印名字、监听地址、协程池和 poller 数；`taskpool.SetLogStatus(true)`
  之后创建的协程池在启动后打印协程池名字，以及实际运行的分片数、worker 数和队列容量等。任务 panic 且没有设置 `SetPanicHandler` 时，无论开关
  如何，都以 ERROR 级别打印协程池名字、panic 值与调用栈。
- `config.SetTaskPool(pool)` 让 Engine 使用外部提供的任务池（实现 `fib.TaskPool`
  接口，`*taskpool.TaskPool` 本身即满足）。设置后 `TaskPoolMode`、
  `WorkerCount`、`SharedTaskPool` 和池容量配置都不再生效；Engine 关闭时不会
  停止该池，由调用方在所有使用它的 Engine 关闭后自行停止。`GoTasks` 返回接受的
  前缀长度，未被接受的任务对应的连接会被关闭，所以池只应在停止时拒绝任务。
- 默认 `WriteBufferHighWatermark` 为 64 KiB，`MaxPendingBytes` 为 1 GiB。
- `Send` 先直接发送，余量复制进发送队列。默认启用自适应 writev：单缓冲走
  write，`SendParts` 的两段数据和包含多个缓冲的发送队列走 writev；仅在背压
  时复制未发送部分。
- worker 通过 command queue 与 `eventfd` 请求 event loop 刷新写关注或关闭连接。事件循环醒着时
  （从一次 wait 返回到下一次 wait 之前）入队的命令不写 `eventfd`：循环在下一次 wait 之前自己
  清空队列，以及这些命令排出的那一轮。在循环上执行的一轮关闭自己的连接时（关闭、再释放 fd），
  这样每条结束的连接就省掉两次 `eventfd` 写和两次立即返回的 wait。

## 平台支持

三个原生后端共用同一套 worker 调度、发送队列和背压逻辑，只有事件来源不同：

- Linux：edge-triggered epoll，`eventfd` 唤醒，可选 `writev`，完整对应 C 版架构。
- macOS：kqueue，所有过滤器以 `EV_CLEAR` 注册，语义与 epoll ET 一致；
  `EVFILT_USER` 唤醒；带外数据由 `EVFILT_EXCEPT`/`NOTE_OOB` 报告；`writev`
  经由 libc。暂停读取时删除读过滤器，恢复时重新添加，添加时会立即报告 socket
  里已有的数据。
- Windows：I/O 完成端口（IOCP），在其上模拟就绪模型。可读由零字节的 overlapped
  `WSARecv` 报告，worker 随后用非阻塞接收排空 socket；非阻塞发送写不完时，剩余
  数据交给 overlapped `WSASend`，它的完成就相当于可写事件。连接用 `AcceptEx`
  接入，唤醒用 `PostQueuedCompletionStatus`。`UseWritev` 对应多个 `WSABUF`
  的一次 `WSASend`。

Windows 后端不会调用 `OnPriorityData`：零字节读不报告带外数据。Windows 上
`Connection.FD()` 返回 socket handle。

三个原生后端都支持 UDP（见下文 UDP 一节）。

其他系统（如 FreeBSD）使用 Go 标准库网络轮询器的兼容后端：公共 API 相同，读取
事件仍以 connection 为单位进入 TaskPool 并保持 FIFO 串行执行，但 `Backlog`、
`UseWritev`、带外数据和背压统计不生效，`Connection.FD()` 返回 `-1`。

## API

```go
import fib "github.com/lesismal/fib"

config := fib.DefaultConfig()
// Network、Addr 与标准库 net.Listen 的参数含义一致：
// Network 取 "tcp"、"tcp4"、"tcp6"，Addr 形如 ":9000"、"127.0.0.1:9000"、"[::1]:9000"，
// 端口为 0 时由内核分配。Network 为空按 "tcp" 处理，Addr 为空按 ":0" 处理。
config.Network = "tcp"
config.Addr = "127.0.0.1:9000"
config.ReadBufferSize = 32 * 1024
server, err := fib.Bind(config, fib.HandlerFuncs{
    Data: func(c *fib.Connection, data []byte) {
        if c.Send(data) != nil { c.Close() }
    },
	PriorityData: func(c *fib.Connection, data []byte) {
		// 处理由 EPOLLPRI 触发并通过 MSG_OOB 读取的带外数据。
	},
	Close: func(c *fib.Connection, err error) {
		// 主动 Close 时 err 为 nil；对端正常关闭时为 io.EOF；
		// 网络或系统调用失败时为对应的原始错误。
	},
})
if err != nil { panic(err) }
defer server.Close()
if err := server.Run(); err != nil { panic(err) }
```

一个 server 监听多个地址时用 `Addrs`（此时 `Addr` 被忽略），它们共用同一个事件循环、
描述符表、任务池和缓冲池；`LocalAddrs()` 按配置顺序返回各监听地址，端口为 0 的会
返回内核实际分配的端口：

```go
config.Addrs = []string{"127.0.0.1:9000", "127.0.0.1:9001"}
```

### net.Conn 与 deadline

`*fib.Connection` 实现了 `net.Conn`，可以直接交给只需要拿地址、写数据、设超时、关连接
的代码。其中两个方法和阻塞 socket 的行为不同，务必注意：

- **`Read` 不阻塞**：读到什么返回什么，socket 里没有数据时返回 `fib.ErrWouldBlock`。
  数据正常是通过 `OnData` 回调交付的，`Read` 是给那种想在 `OnData` 里自己把剩下的报文
  从 socket 上读完的 handler 用的，不要把它交给 `bufio.Reader` 之类期待阻塞语义的代码。
- **deadline 不是让调用失败，而是关连接**：`Read`、`Write` 都不会等待，所以 deadline
  到了就关闭连接，`OnClose` 收到 `os.ErrDeadlineExceeded`。

```go
Data: func(c *fib.Connection, data []byte) {
    // 滚动超时：每次听到对端说话就往后推
    _ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
    ...
},
Close: func(c *fib.Connection, err error) {
    if errors.Is(err, os.ErrDeadlineExceeded) { /* 对端超时 */ }
},
```

- `SetReadDeadline(t)`：到 t 就关连接，不管这期间对端发过什么——需要滚动超时就在每次
  收到数据时重新设置。`SetWriteDeadline(t)`：到 t 时如果 `Send` 收下的数据还没全部交给
  内核才关连接，已经发完的连接不受影响。`SetDeadline(t)` 同时设置两者，零值 `time.Time{}`
  取消，已经过去的时间立即关连接。可以在任意 goroutine 调用。
- 重复设置同一个时间点不会重建定时器，所以每轮读都重申一次同样的 deadline 没有额外开销。
- `Close()` 现在返回 `error`（恒为 nil，关闭由事件循环执行，没有可报告的错误），
  `Write` 走 `Send`（拷贝并排队，不等对端），另外补上了 `LocalAddr()`。
- 关闭之后 `Read`/`Write` 返回连接关闭的原因（超时是 `os.ErrDeadlineExceeded`，
  其余是 `net.ErrClosed`），而不只是「已关闭」。
- UDP 连接没有 `Read`：数据报由事件循环读走并整包交给 `OnData`。非原生（portable）
  后端也没有 `Read`：那边由连接自己的读 goroutine 占着 socket，别处再读会把数据读走。

### 协议类型

`c.Protocol()` 返回连接的传输协议：`fib.ProtocolTCP`、`fib.ProtocolUDP`（拨出的 UDP 连接和
UDP server 的 peer）或 `fib.ProtocolUnix`，`String()` 给出 net 包的网络名 `"tcp"`、`"udp"`、
`"unix"`。判断某一种用 `c.IsTCP()`、`c.IsUDP()`、`c.IsUnix()`。协议在连接创建时确定，关闭
之后也不变。

`c.IsAccepted()` 表示连接由 Engine 的 listener 接受（accept 的 TCP、Unix 连接，以及向 UDP
server 发来第一个数据报的 peer），`c.IsDialed()` 表示由 `Dial` / `DialWithHandler` 拨出，两者
互斥，同样关闭之后也不变。同一个 handler 既服务 accept 的连接又服务拨出的连接时，可以用它区分
服务端和客户端。

### net.TCPConn 的方法

`*fib.Connection` 也有 `net.TCPConn` 在 `net.Conn` 之外的方法：`SetNoDelay`、`SetKeepAlive`、
`SetKeepAlivePeriod`、`SetKeepAliveConfig`、`SetLinger`、`SetReadBuffer`、`SetWriteBuffer`、
`CloseRead`、`CloseWrite`、`MultipathTCP`、`SyscallConn`、`File`，参数和默认值的语义与
`net.TCPConn` 一致（比如 keep-alive 时间为 0 取 15s，负数不改）。

- 对连接类型没有意义的调用什么都不做、返回 nil：Unix socket、UDP 连接上的 TCP 选项
  （`SetNoDelay`、keep-alive、`SetLinger`），UDP 连接上的 `CloseRead`/`CloseWrite`，以及
  UDP server 的 peer 上所有要动 socket 的调用（peer 和其他 peer 共用监听的那个 socket）。
  `SetReadBuffer`/`SetWriteBuffer` 对 Unix socket 和拨出的 UDP 连接照常生效。peer 的
  `File`/`SyscallConn` 返回错误，因为没有自己的 socket 可给。
- fib accept 和 dial 的 TCP 连接一开始就关闭了 Nagle（`TCP_NODELAY`），需要打开时调
  `SetNoDelay(false)`。
- `CloseWrite` 等 `Send` 已经收下的数据全部交给内核之后才 shutdown 写方向，对端先读完这些
  数据再读到 EOF；调用之后 `Send` 返回 `EPIPE`，连接照常读。它在 Layer 之下工作，不发送
  TLS 的 close_notify。
- `CloseRead` 之后不再回调 `OnData`，之后读到的 EOF（包括对端 half-close）也不再关连接，
  连接可以继续发送，直到被关闭。注意 macOS 上 shutdown 读方向之后如果对端还发数据，内核会
  reset 连接（`net.TCPConn` 也一样）。
- 这些调用都在连接的锁里执行 syscall，事件循环不会在调用中途关掉 fd 再把它分给别的连接。
  所以 `SyscallConn().Control(f)` 的 f 里不能再调这个连接的方法，否则会死锁；它的
  `Read`/`Write` 只调用一次 f，f 返回 false 时报告 `fib.ErrWouldBlock`，而不是等待就绪。
- `File` 在 Windows 上返回 `syscall.EWINDOWS`（和 `net.TCPConn` 相同）。没有 `ReadFrom`/
  `WriteTo`：`Write` 本来就不等待，而 `WriteTo` 需要阻塞读，数据却是经 `OnData` 交付的。

### HoldReads 读背压

写方向有 `WriteHighWatermark`/`MaxPendingBytes` 两级水位自动暂停读；读方向由应用自己
决定：handler 把收到的数据放进自己的缓冲区、还没来得及处理时，调用
`c.HoldReads(true)` 让连接停止读 socket，数据留在内核缓冲区里，对端被 TCP 滑动窗口
自然减速，而不是这一侧的内存无限增长；处理完再 `c.HoldReads(false)` 恢复，重新注册读
事件会把 socket 里攒下的数据直接投递上来，不需要对端再发任何字节：

```go
Data: func(c *fib.Connection, data []byte) {
    queue.push(data)
    if queue.size() >= highWater {
        c.HoldReads(true) // 在 OnData 里调用时，本轮读循环也会立即停下
    }
},
// 另一个 goroutine 消费完之后：c.HoldReads(false)
```

可以在任意 goroutine 调用，不嵌套（以最后一次调用为准），`ReadsHeld()` 返回当前状态。
UDP 连接上不生效（数据报已经被事件循环读走了）。`http` package 的流式请求 body 就是
用它做背压的。

### SendFile 零拷贝发送

`Connection.SendFile(f, offset, count)` 把文件的一段排进发送队列，与前后的 `Send` 保持
顺序。Linux 和 macOS 上由 `sendfile(2)` 直接从文件发到 socket，数据不经过用户态；
Windows 上按 socket 的发送进度每次读取一块再发送。无论哪种方式，文件都是随对端的接收
进度读取的，大文件也不会占用更多内存：

```go
f, _ := os.Open("video.mp4")
_ = c.Send(header)
_ = c.SendFile(f, 0, size) // 连接使用自己复制的描述符，f 可以立即关闭
f.Close()
```

- 带 layer 的连接（如 TLS）需要先加密，无法零拷贝：`SendFile` 会在返回前读出这段文件
  并经 layer 发送。UDP 连接返回 `ErrSendFileDatagram`。
- 文件比声明的范围短时会关闭连接，因为对端已经在等这些字节。
- `OnData` 里的发送会被暂存（cork），在 handler 返回后合并成一次写入；需要在 handler
  运行期间就把已发送的数据交给 socket 时（例如流式响应），调用 `Connection.Flush()`。

### 异步 Dial

`Engine.Dial` 发起四层（TCP）连接，立即返回，不阻塞调用方。连接建立后的 fd 与
accept 进来的连接一样由同一个事件循环管理，走同一个 handler、同一个 worker 池和
同一套背压：

```go
err := server.Dial("tcp", "127.0.0.1:9001", 3*time.Second, func(c *fib.Connection, err error) {
    if err != nil {
        // 连接失败、超时（errors.Is(err, os.ErrDeadlineExceeded)）或 Engine 已关闭
        // （errors.Is(err, net.ErrClosed)），err 为 *net.OpError。
        return
    }
    c.Send([]byte("hello"))
})
```

- network、addr 的含义与 `net.Dial` 一致；host 不是 IP 字面量时在单独的 goroutine
  里解析，调用方不会等 DNS。timeout 为 0 表示只受操作系统自身的连接超时约束。
- 原生后端由事件循环创建非阻塞 socket 并发起 `connect`，fd 随即以边沿触发注册到
  epoll/kqueue，连接结果由它的第一个可写事件报告；Windows 上用 overlapped
  `ConnectEx`，由完成端口报告结果。
- 成功时先调用 handler 的 `OnOpen`，再调用 done；失败时只调用 done（连接为
  nil），handler 不会收到任何回调。done 恰好调用一次，与 `OnOpen` 一样在事件循环
  上执行，不能阻塞；可以传 nil。
- 只有能立即判定无法发起时（未知 network、非法或无法解析的字面量地址、Engine 已
  停止），`Dial` 才直接返回错误，此时 done 不会被调用。
- Engine 关闭时，尚未完成的 Dial 都会以 `net.ErrClosed` 通知 done。
- 兼容后端（如 FreeBSD）用 `net.DialTimeout` 在 goroutine 里连接，done 在该
  goroutine 上执行。
- `DialWithHandler` 让这条连接使用自己的 handler，`OnOpen`、`OnData`、`OnClose`
  都只交给它，Engine 的 handler 收不到；同一个 Engine 因此可以同时承载 server 和
  使用不同协议的 client 连接。`Dial` 等价于 handler 传 nil，即使用 Engine 的。
- `fib.NewEngine(config, handler)` 创建不监听任何地址的 Engine，只用于 Dial 出去
  的连接；它的 `LocalAddr` 返回错误。

### Unix Socket

`Config.Network` 设为 `"unix"` 时，`Config.Addr` 是 socket 文件路径，Engine 在
Unix domain stream socket 上监听；`Dial`/`DialWithHandler` 的 network 传 `"unix"`、
addr 传路径即可拨号。连接的调度、发送队列、背压、TLS 以及 http/websocket 子
package 与 TCP 完全相同：

```go
config.Network = "unix"
config.Addr = "/tmp/fib.sock"
server, err := fib.Bind(config, handler)

err = engine.Dial("unix", "/tmp/fib.sock", 3*time.Second, done)
```

- 语义与 `net.Listen("unix", path)` 一致：路径已存在时 `Bind` 失败（不会删除别人的
  socket 文件），Engine `Close` 时删除自己创建的文件；Linux 上以 `@` 开头的路径是
  abstract socket，没有文件。
- `Connection.RemoteAddr()` 返回 `*net.UnixAddr`（对端未 bind 时名字为空）；
  `Engine.ListenAddrs()` 返回所有监听地址（TCP、Unix、UDP 均可），`LocalAddr` 只
  报告 TCP 地址。
- macOS 的 Unix socket 没有带外数据，不会调用 `OnPriorityData`；Linux 5.15 起支持。
- Windows 10 1803 起支持 AF_UNIX。由于 `ConnectEx` 只支持 TCP，Windows 上的 Unix
  拨号在 event loop 上使用普通 `connect`（与 net 包相同），本地连接会立即完成或被拒绝。
- 目前只支持 stream 类型（`"unix"`），不支持 `"unixgram"` 和 `"unixpacket"`。

### UDP

`Config.Network` 设为 `"udp"`、`"udp4"` 或 `"udp6"` 时，`Bind` 绑定 UDP socket；
`Dial`/`DialWithHandler` 的 network 传 `"udp"` 时拨号一个已 connect 的 UDP socket。
UDP 沿用同一套 Handler、worker 和 `Connection`：

```go
config.Network = "udp"
config.Addr = "127.0.0.1:9001"
server, err := fib.Bind(config, fib.HandlerFuncs{
    Data: func(c *fib.Connection, datagram []byte) { _ = c.Send(datagram) },
})
addr, _ := server.LocalUDPAddr()
```

- 监听 socket 上，每个对端地址是一条独立的 `Connection`：该地址的第一个数据报触发
  `OnOpen`，之后它的数据报都交给这条连接；`Connection.RemoteAddr()` 返回对端地址，
  `IsUDP()` 返回 true。UDP 没有关闭报文，对端静默超过 `Config.UDPIdleTimeout`
  （默认 `DefaultUDPIdleTimeout`，60 秒；负数表示不超时）后连接被关闭，`OnClose`
  收到 `ErrUDPIdleTimeout`。拨号出去的 UDP 连接不做超时。
- 数据报边界保持不变：每次 `OnData` 恰好是一个数据报，每次 `Send`/`SendParts`
  恰好发送一个数据报。
- Handler 如果实现了 `fib.DatagramsHandler`，worker 运行时连接已经排队的数据报会按到达
  顺序一次性交给 `OnDatagrams`，不再逐个调用 `OnData`；需要回应所收数据的协议（比如
  回 ACK）可以对整批数据一起回应，少发数据报。每个数据报归 handler 所有，装它们的切片
  在 `OnDatagrams` 返回后不能再持有。数据报取自 `bufferpool`，handler 用完且不再引用时
  可以用 `bufferpool.Put` 归还，供后面的数据报复用；不归还的和普通切片一样由 GC 回收。
- UDP socket 由 event loop 自己读取（level-triggered，每轮每个 socket 最多读
  256 个数据报；Linux 上用 `recvmmsg`、macOS 上用 `recvmsg_x` 一次读一批，最多 32 个，
  一批没读满就说明 socket 已空），按对端地址分发到各连接的队列，再由 worker 依次调用 `OnData`；
  每条连接最多排队 1024 个数据报，超出的直接丢弃，和内核接收缓冲满时一样。
- 发送直接写 socket，从不排队：socket 没有空间时这个数据报被丢弃，`Send` 返回错误，
  连接保持打开。因此 UDP 连接不参与写水位和背压。`SendBatch` 按顺序发送多个数据报，
  Linux 上用 `sendmmsg`、macOS 上用 `sendmsg_x`，一次系统调用发出一批。
- Windows 上监听 socket 用 overlapped `WSARecvFrom`、拨号 socket 用带缓冲区的
  overlapped `WSARecv` 接收，并关闭 `SIO_UDP_CONNRESET`，以免某个对端的 ICMP
  端口不可达导致整个监听 socket 的接收失败。兼容后端用 `net.ListenUDP` 实现同样的语义。

运行测试：

```sh
go test ./...
```

## TLS 子 package

`tls` package 以 handler 包装的方式在 Engine 的连接上提供 TLS，加解密由标准库
`crypto/tls` 完成，不需要额外依赖：

```go
import fibtls "github.com/lesismal/fib/tls"

// server：把任意 handler 包在 fibtls.NewServer 里
server, err := fib.Bind(config, fibtls.NewServer(tlsConfig, handler))

// client：config 没有 ServerName 时取 addr 的 host
err = fibtls.Dial(engine, "tcp", "example.com:443", 3*time.Second, tlsConfig, handler, done)
```

- 被包装的 handler 看到的是普通连接：`OnData` 收到的是明文，`Send`、`SendOwned`、
  `SendParts`、`CloseAfterSend` 自动加密，所以 http、websocket 子 package 不用改动
  就能跑在 TLS 上（它们的 client 通过 `ClientConfig.TLSConfig`、
  `DialerConfig.TLSConfig` 支持 https://、wss://）。`CloseAfterSend` 会先发送 close_notify。
- TCP 和 Unix socket 上都可以使用。
- `OnOpen`（以及 Dial 的 done）在连接建立后立即调用，早于 TLS 握手；此时就可以
  `Send`，数据会先缓存，握手完成后按顺序加密发出。握手失败或超时会关闭连接，
  `OnClose` 收到对应错误。
- `crypto/tls` 的握手是阻塞调用，所以每条连接的握手在单独的 goroutine 里进行，握手
  结束后 goroutine 退出；之后的记录由 worker 在 `OnData` 中非阻塞解密，跨多轮读到达
  的记录会被正确拼接。
- `Handler.HandshakeTimeout` 限制握手时长，0 表示 `DefaultHandshakeTimeout`
  （10 秒），负数表示不限制。
- `fibtls.ConnectionState(c)` 在握手完成后返回协商结果（版本、ALPN、对端证书等）。
- 这个 package 只使用 fib 的公开 API：它通过 `Connection.SetLayer` 安装一个
  `fib.Layer` 来加密发送，用 `SendRaw`、`CloseAfterSendRaw` 直接写 socket，用
  `CloseWithError` 把握手失败的原因交给 `OnClose`。其他加密或分帧协议也可以用同样
  的方式接入；本库不内置 DTLS，UDP 上需要加密时可以用这个接口接入自己选择的实现。

## HTTP 子 package

`http` package 在原始连接之上提供 HTTP/1.0、HTTP/1.1 和 HTTP/2 的增量解析和响应
处理，支持 TCP 分包/粘包、流水线请求、`Content-Length`、chunked body、trailer、
keep-alive、流式响应、sendfile 零拷贝发送文件以及请求大小限制：

```go
handler := epollhttp.NewHandler(epollhttp.HandlerFunc(
    func(c *epollhttp.Context, request *http.Request) {
        _ = c.Respond(http.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
    },
))
server, err := fib.Bind(config, handler)
```

完整示例：

```sh
go run ./examples/http/nontls/server   # 另开终端：go run ./examples/http/nontls/client
```

### HTTP/1.x：流式响应、trailer 与 sendfile

除了 `Respond`/`WriteResponse` 一次性回复，`Context` 还实现了 `http.ResponseWriter`、
`http.Flusher` 和 `io.ReaderFrom`，可以像 `net/http` 的 handler 一样边写边发，也可以直接
交给 `net/http` 的 `ServeFile`、`ServeContent`、`FileServer` 等函数：

```go
func(c *fibhttp.Context, r *http.Request) {
    switch r.URL.Path {
    case "/events":
        c.Header().Set("Trailer", "X-Count")
        for i := 0; i < 10; i++ {
            fmt.Fprintf(c, "event %d\n", i)
            c.Flush() // 立即发出这一块，不等 handler 返回
        }
        c.Header().Set("X-Count", "10") // 作为 trailer 在 body 之后发送
    case "/download":
        http.ServeFile(c, r, "big.iso") // Range、If-Modified-Since 等由 net/http 处理，文件走 sendfile
    }
}
```

- 帧格式自动选择：handler 设置了 `Content-Length`，或写的内容不超过 4KB 且在返回前写完，
  就用 `Content-Length`；否则 HTTP/1.1 用 chunked（trailer 也靠它携带），HTTP/1.0 不支持
  chunked，body 以关闭连接结束。未设置 `Content-Type` 时像 `net/http` 一样嗅探。
- trailer：在 `Trailer` 头里声明名字，写完 body 后设置对应的值；也可以在任何时候设置带
  `http.TrailerPrefix` 前缀的键。`Response.Trailer` 同样可以随 `WriteResponse` 发送。
  HTTP/1.0 客户端收不到 trailer。
- `ReadFrom`（`io.Copy`、`http.ServeContent` 都会用到）遇到普通文件，或包着文件的
  `*io.LimitedReader` 时，通过 `Connection.SendFile` 发送；TLS 连接退化为读取后加密发送。
- handler 返回时响应自动结束（写出剩余数据、结束 chunk、发送 trailer），与 `net/http`
  一致；也可以提前调用 `Context.Finish()`。只有用过这些方法的响应才会被自动结束，所以
  没有开始写的响应仍然可以之后在其他 goroutine 中用 `WriteResponse` 异步回复。
- 同一个响应不能混用两种方式：开始用 `Write`/`WriteHeader` 之后，`WriteResponse` 返回
  `ErrResponseWritten`。
- HTTP/2 和 HTTP/3 上这些方法同样可用，但响应会先缓存，handler 返回后整体发送。

服务端还会：为 HTTP/1.1 缺少 `Host` 或有多个 `Host` 的请求返回 400；不支持的
`Transfer-Encoding` 返回 501；无法满足的 `Expect` 返回 417；同时带 `Content-Length`
和 chunked 的请求按 chunked 处理并在响应后关闭连接；HTTP/1.0 请求带
`Transfer-Encoding` 时返回 400；204 和 1xx 响应不带 `Content-Length`；自动添加 `Date`。
完整的 HTTP/1.x 支持情况、限制和一致性测试见
[`docs/http1.zh-CN.md`](http1.zh-CN.md)。

### 异步响应与 body 回调（Retain / Release / OnBody）

响应由引用计数持有：处理一个请求先占一个引用，handler 返回时还回去。所以「回复完就返回」
的 handler 什么都不用改——**外层负责结束响应并回写**。要稍后再回复就多占一个引用：

```go
func(c *fibhttp.Context, r *http.Request) {
    c.Retain()                     // handler 返回后不自动回写
    go func() {
        result := doSomethingSlow()
        _ = c.Respond(200, "application/json", result)
        c.Release()                // 引用归零 → 结束响应并交给连接
    }()
}
```

- `Retain` / `Release` 成对；释放次数多于 `Retain` 会提前结束响应（因为 handler 自己的
  那一个引用也是一次释放）。`Retained()` 查当前是否被持有。
- HTTP/1 上被持有的请求会挡住排在它后面的流水线请求，等释放之后才继续解析处理，**响应
  顺序不变**。HTTP/2、HTTP/3 上 stream 之间互不影响。
- 没有任何超时约束持有时长，handler 不释放就会一直占着连接，直到对端断开或读超时。

`OnBody` 用回调接收 body，和 websocket 的 `OnFrame` 按帧交付是一个路子，回调带 `fin`。
`BodyComplete()` 判断 body 是否已经全部到达，完整的 body 可以直接从 `Request.Body` 读，
还在路上的才用 `OnBody` 异步接收：

```go
func(c *fibhttp.Context, r *http.Request) {
    f, _ := os.Create("upload.bin")
    c.OnBody(func(data []byte, fin bool, err error) { // 自动 Retain
        if err != nil {                 // 连接断了 / body 出错
            f.Close(); os.Remove("upload.bin")
            return
        }
        f.Write(data)                   // 边收边写，不缓存整个 body
        if fin {
            f.Close()
            _ = c.Respond(200, "text/plain", []byte("stored"))
        }
    }) // 最后一次回调返回后框架自动 Release，响应随即写回
}
```

- **`OnBody` 不阻塞**：它只登记回调就返回，从不在自身内部调用回调。已经到达的部分
  （`BodyComplete()` 为 true 时就是整个 body）在 handler 返回之后、在运行 handler 的
  goroutine 上交给回调；handler 返回后才调用 `OnBody` 的，则在单独的 goroutine 上交付。
- **自动 Retain / Release**：`OnBody` 自动占一个引用，最后一次回调（`fin` 或 `err`）返回
  之后框架自动释放，回调里写的响应随即回写，handler 不用自己 `Retain`/`Release`。要在别处
  （别的 goroutine）回复的，在回调里自己再 `Retain` 一次、回复后 `Release`。
- 交接之后的回调在**连接的 worker 上**执行，一次一个、保持顺序：任意大的 body 都不需要
  handler 自己的 goroutine，也不会被缓存下来，给对端限速的就是回调本身。
- `fin` 是最后一次回调；`err != nil` 表示 body 不会有后续了，同样是最后一次、且只来一次。
  `data` 只在回调期间有效，要保留先复制。
- body 只有在流式交付时才会分多次到达（看 `StreamRequestBody`）；handler 运行前
  就收全的 body 会在一次 `fin` 为 true 的回调里全部给出。
- `OnBody` 接管之后 `Request.Body` 不能再读，关闭它会让回调以 `ErrBodyAbandoned` 结束、
  剩下的 body 被丢弃。「不读 body 直接拒绝上传」就直接回复、不调用 `OnBody`。

**异常情况**：连接断开、读超时、body 收不完，都会让请求被取消——不再写出任何东西，剩余
引用全部作废，handler 只被告知一次：`OnBody` 的 `err`，以及 `Context.OnCancel(func(error))`
（给只 `Retain`、不用 `OnBody` 的异步 handler）。两者都在单独 goroutine 上执行而不是事件
循环上，拖不住服务端。`Context.Err()` 可以主动查。之后一切幂等：已经结束的请求上再调
`Retain`、`Release`、`Finish` 都是空操作，往上面写会返回那个原因而不会把连接写坏。

`Context.Flush()` 语义不变，仍然是 `http.Flusher`：把暂存的 body 立即发出去，响应不结束。

### 读超时

`Config.ReadHeaderTimeout`、`Config.ReadTimeout`、`Config.IdleTimeout` 就是
`net/http.Server` 的那三个，回退规则也一样（前两者为 0 时用 `ReadTimeout`），
全为 0 表示不限，默认不限：

```go
config := fibhttp.DefaultConfig()
config.ReadHeaderTimeout = 10 * time.Second
config.ReadTimeout       = 60 * time.Second
config.IdleTimeout       = 120 * time.Second
handler := fibhttp.NewHandlerWithConfig(config, myHandler)
```

- 连接在等第一个/下一个请求时受 `IdleTimeout` 约束；在等 header 收完时受
  `ReadHeaderTimeout` 约束；在等 body 收完（流式与否都一样）时受 `ReadTimeout` 约束，
  都从该请求的第一个字节算起。超时的连接被关闭，`OnClose` 收到 `os.ErrDeadlineExceeded`。
- **请求收全之后 deadline 会被撤掉**：比 `ReadTimeout` 慢的 handler 仍然能把响应写完。
  `ReadTimeout` 约束的是请求到达的时间，不是处理它的时间。流式 body 在 handler 运行期间
  仍在到达，所以它确实受 `ReadTimeout` 约束——慢上传占不住连接靠的就是这个。
- 变成 HTTP/2 的连接（preface 或 h2c 升级）会被解除这些超时，交给 HTTP/2 自己管。

### 流式请求 body（大 body 边收边处理）

默认情况下请求的 body 收齐之后 handler 才会被调用，上传一个 1GB 的文件就意味着先在
内存里放下 1GB。开启 `Config.StreamRequestBody`（默认关闭）之后，body 不再等待收齐：
header 解析完就调用 handler（`StreamRequestBodyThreshold` 可以让不超过它的 body 仍然
整体缓存，0 表示所有 body 都流式交付），`Request.Body` 是一个 `*fibhttp.BodyStream`，
边收边读：

```go
config := fibhttp.DefaultConfig()
config.StreamRequestBody = true             // 开启流式请求 body
config.StreamRequestBodyThreshold = 1 << 20 // 只有超过 1MB 的 body 流式交付
config.MaxStreamedBodyBytes = 4 << 30       // 流式 body 的上限，0 表示不限
config.StreamRequestBodyBuffer = 512 << 10  // 未被读走的 body 攒到这么多就停止读 socket

handler := fibhttp.NewHandlerWithConfig(config, fibhttp.HandlerFunc(
    func(c *fibhttp.Context, r *http.Request) {
        f, _ := os.Create("upload.bin")
        buf := make([]byte, 32*1024)
        for {
            n, err := r.Body.Read(buf) // 只拿已经到的，不阻塞
            f.Write(buf[:n])
            if errors.Is(err, io.EOF) { // body 读完了
                f.Close()
                _ = c.Respond(http.StatusOK, "text/plain", []byte("ok"))
                return
            }
            if errors.Is(err, fibhttp.ErrWouldBlock) { // 后面还有，异步接着收
                c.OnBody(func(data []byte, fin bool, err error) {
                    if err != nil {
                        f.Close(); os.Remove("upload.bin")
                        return
                    }
                    f.Write(data)
                    if fin {
                        f.Close()
                        _ = c.Respond(http.StatusOK, "text/plain", []byte("ok"))
                    }
                })
                return
            }
            if err != nil { // 连接断了 / 超限 / 帧错误
                f.Close(); os.Remove("upload.bin")
                _ = c.Respond(http.StatusBadRequest, "text/plain", []byte(err.Error()))
                return
            }
        }
    },
))
```

- **读取不阻塞，也不额外开 goroutine**：handler 和别的 handler 一样跑在连接的 worker
  上，worker 去等对端就是在等自己。`Read` 的返回值就是状态机：`n > 0` 是已经到的字节，
  `io.EOF` 是整个 body 读完了，`ErrWouldBlock`（就是 `fib.ErrWouldBlock`）是现在一个
  字节都没有、后续还会来，`ErrBodyAbandoned` 是 body 已被 `Close`/`OnBody` 接管或
  handler 既没 Retain 也没调 `OnBody` 就返回了，其它错误表示后续不会再来（连接断、超限、帧错误）。
- 遇到 `ErrWouldBlock`（或者一开始 `BodyComplete()` 就是 false）就用 `OnBody` 异步接管
  剩下的部分，它自动保持请求不结束。**交接不丢字节**：
  `OnBody` 拿到的正好从上一次 `Read` 停下的地方开始。body 也可能在 handler 运行时就全
  到了（和 header 在同一次读里），那就一路读到 `io.EOF` 直接回复。
- `Content-Length` 和 chunked（含 trailer，读完后在 `Request.Trailer` 里）都支持。想知道
  是不是流式的，用 `r.Body.(*fibhttp.BodyStream)` 或 `Context.RequestBody()`（返回 nil
  表示 body 已经收全了）。
- 小于阈值的 body 行为完全不变：收齐后交付，`io.ReadAll`、`json.Decoder` 照常用。
- 背压：还没被读走的 body 攒到 `StreamRequestBodyBuffer`（默认 256KB）就调用
  `Connection.HoldReads(true)` 停止读 socket，读掉一半之后恢复。上传快过 handler 处理
  速度时，减速的是对端，而不是这一侧的内存。
- HTTP/2 和 HTTP/3 同样支持（HTTP/2 用同一个 `Config.StreamRequestBody`，HTTP/3 在
  `http3.Config` 里有同名开关），handler 的写法完全一样。多路复用的连接不能为一个请求
  停止读，所以背压改由流控实现：stream 的接收窗口只在 handler 消费了 body 之后才补充，
  客户端最多领先一个窗口（1MB），同一连接上的其它请求不受影响。
- 同一连接上排在后面的流水线请求要等这个请求的响应写完之后才会被解析，响应顺序不变。
- handler 没读完就返回时：剩余不超过 256KB 就读掉丢弃、连接继续复用；更多（或者
  chunked 这种长度未知的）则响应发完后关闭连接，和 `net/http` 的做法一致。
- `Expect: 100-continue` 变成惰性的：handler 第一次要 body 时（读它，或者用 `OnBody`
  接管它）才发 100 Continue，所以可以在客户端还没开始上传时就用 413/403 拒绝掉，之后
  连接关闭。
- 连接中途断开时 `Read` 返回 `io.ErrUnexpectedEOF` 而不是 `io.EOF`，截断的上传不会被
  当成完整的；超过 `MaxStreamedBodyBytes` 时返回 `ErrBodyTooLarge`。
- 目前只有 HTTP/1.x 支持，HTTP/2 和 HTTP/3 的 body 仍然收齐后交付。

handler 收下 128MB 的上传并计数：整体缓存时堆内存峰值约 470MB，流式时约 4MB
（`StreamRequestBodyBuffer` 为默认的 256KB）。

### HTTP/2

同一个 `ServerHandler` 同时服务 HTTP/1 和 HTTP/2，不需要额外配置：连接以 HTTP/2
preface 开头时按 HTTP/2 处理，否则按 HTTP/1。这覆盖了三种场景：

- TLS + ALPN（h2）：用 `fibhttp.ConfigureTLS` 让 TLS 配置在 ALPN 中优先提供 `h2`，
  浏览器、`net/http` 等客户端就会选择 HTTP/2；不支持 h2 的客户端继续走 HTTP/1.1。
- 明文 HTTP/2（h2c，prior knowledge）：客户端直接发送 preface 即可。
- 明文 HTTP/1.1 升级（`Upgrade: h2c` + `HTTP2-Settings`，`curl --http2 http://...` 用的
  就是这种方式）：服务端回复 101，随后在 stream 1 上用 HTTP/2 回复这个请求，连接之后
  按 HTTP/2 继续。TLS 连接只通过 ALPN 切换，不接受这种升级。

```go
tlsConfig = fibhttp.ConfigureTLS(tlsConfig) // ALPN: h2, http/1.1
server, err := fib.Bind(config, fibtls.NewServer(tlsConfig, fibhttp.NewHandler(handler)))
```

- Handler 不用改：每个 stream 就是一个普通的 `*http.Request`（`Proto` 为 `HTTP/2.0`），
  用 `Context.Respond`/`WriteResponse` 回复即可，响应会被编成该 stream 的 HEADERS/DATA
  帧。HTTP/2 连接上不要直接调用 `Context.Conn.Send`。
- 多路复用：一个连接上的多个 stream 并发进行，handler 可以在其他 goroutine 中稍后
  回复（异步响应），不同 stream 的响应互不阻塞。
- handler 协程池：请求收齐后不在读取该连接的协程上执行，而是交给一个独立的协程池，
  因此同一连接上并发到达的请求是并发处理的，阻塞的 handler 只拖累它自己。协程池由
  `Config.StreamPool` 配置（`http.StreamPoolConfig`）。每个 Engine 名字有一个 handler
  协程池，名为 `<Name>-streams`（默认 `fib-streams`），由连接来自该名字 Engine 的所有
  HTTP/2 与 HTTP/3 server 共用，同名的最后一个 Engine 关闭后停止，且永远不是 Engine 的协程池：Engine 的 worker
  解析出请求后要往这个池提交，若两者是同一个池，队列满时所有 worker 都可能卡在提交上，
  没有人再取任务，连 event loop 也会卡住。它的上限是当前运行的同名 Engine 中最大协程池的
  2 倍（没有 Engine 时取 `fib.DefaultStreamPoolSizing`），下限为 0，空闲时不保留 worker。
  上限取 2 倍是因为 Engine 的 worker 只等内核，handler 还要等应用自己的 I/O：

  ```go
  httpConfig := fibhttp.DefaultConfig()
  httpConfig.StreamPool.MaxConcurrentHandlers = 8 // 单连接最多并发 8 个请求，0 表示不限
  server, err := fib.Bind(config, fibhttp.NewHandlerWithConfig(httpConfig, handler))
  ```

  `MaxConcurrentHandlers` 为 N（>0）时限制单个连接同时处理的请求数：第 N 个请求在读取
  该连接的协程上执行，在它返回前这个连接不再读取新数据，于是这个上限由对端的流控承担，
  服务端不需要排队。N 为 1（或 `StreamPool.Disable`）就是逐个执行的旧行为。
- `Response.Close` 在 HTTP/2 上优雅关闭连接：发送 GOAWAY(NO_ERROR)，不再接受新
  stream，已在处理的 stream 完成后再关闭连接。
- 流控：遵守对端的连接级与 stream 级窗口，窗口不足的响应 body 暂存，等 WINDOW_UPDATE
  后继续发送；接收方向每个 stream 窗口 1MB、连接窗口 16MB，按消费量自动补充。
- 实现了 HPACK（含 Huffman 与动态表）、CONTINUATION、trailer、多个 cookie 字段合并、
  PING、SETTINGS、RST_STREAM、GOAWAY，以及 RFC 9113 的请求合法性校验（非法请求用
  RST_STREAM 拒绝，协议错误用 GOAWAY 关闭连接）。
- `Config.MaxConcurrentStreams` 限制单连接并发 stream 数（默认 250，超出的 stream 被
  REFUSED_STREAM 拒绝）；`MaxHeaderBytes`、`MaxBodyBytes` 同样作用于 HTTP/2，超限时
  返回 431/413。`Config.DisableHTTP2` 关闭 HTTP/2，只服务 HTTP/1；`Config.HTTP2Only`
  相反，只服务 HTTP/2：连接不以 HTTP/2 preface 开头时用 GOAWAY 结束，而不是按 HTTP/1
  回复。ALPN 协商出 `h2` 的 TLS 连接一定按 HTTP/2 处理，不再嗅探。
- 通过 TLS 到达的请求（HTTP/1 和 HTTP/2）都会设置 `Request.TLS`，可以读取 ALPN 结果、
  对端证书等。

#### Server push

`Context` 实现了 `http.Pusher`：在回复之前调用 `Push`，服务端先发送 PUSH_PROMISE，
再用同一个 handler 处理被推送的请求，响应在它自己的 stream 上发送；`Push` 在被推送
请求的 handler 返回后才返回：

```go
func(c *fibhttp.Context, r *http.Request) {
    if r.URL.Path == "/index.html" {
        _ = c.Push("/style.css", nil) // 客户端不支持时返回 http.ErrNotSupported，忽略即可
    }
    _ = c.Respond(http.StatusOK, "text/html", page)
}
```

- target 是绝对路径，或 scheme 与当前请求相同的绝对 URL；`PushOptions` 可指定方法
  （GET 或 HEAD）和请求头，不能带 body 相关及连接相关的头。
- 遵守客户端的设置：`SETTINGS_ENABLE_PUSH=0` 时返回 `http.ErrNotSupported`，推送中的
  stream 数达到客户端的 `SETTINGS_MAX_CONCURRENT_STREAMS` 时返回 `ErrPushLimit`。
  客户端可以用 RST_STREAM 取消推送，不影响连接。
- HTTP/1、被推送的请求本身再调用 `Push` 时返回 `http.ErrNotSupported`。
- 主流浏览器和 Go 的 `net/http` 客户端都已关闭 push，本库的 client 同样不接收 push
  （SETTINGS_ENABLE_PUSH=0）；需要预加载时更推荐 103 Early Hints。

#### 1xx 中间响应

`Context.WriteInterim(status, header)` 在最终响应之前发送 1xx 中间响应，HTTP/1.1 和
HTTP/2 都支持，例如 103 Early Hints：

```go
_ = c.WriteInterim(http.StatusEarlyHints, http.Header{"Link": {"</style.css>; rel=preload"}})
_ = c.Respond(http.StatusOK, "text/html", page)
```

- 可以调用多次，必须在 `WriteResponse` 之前；不允许 101；HTTP/1.0 请求返回
  `http.ErrNotSupported`。
- 带 `Expect: 100-continue` 的请求会自动收到 100 Continue（HTTP/1.1 与 HTTP/2），
  因为 body 总是在 handler 运行前完整读取，客户端不必等待超时才发送 body。

#### 一致性与限制

HTTP/2 的一致性测试（h2spec 145 项全部通过，另有与 `net/http`、curl 的互通测试和原始
帧测试，CI 中运行）、当前的限制（body 整体缓存、固定的窗口参数等）、
有意未实现的功能及原因、以及待优化项见
[`docs/http2.zh-CN.md`](http2.zh-CN.md)。

### 异步 HTTP client

`http.Client` 在 Engine 上发送 HTTP/1.0、HTTP/1.1 和 HTTP/2 请求，调用方不会阻塞。响应由 Engine 的
worker 读取，body 完整缓存后再交给回调；也可以用 `Go` 拿到 Future 等待结果：

```go
engine, _ := fib.NewEngine(fib.DefaultConfig(), nil) // 或直接复用 server 的 Engine
go engine.Run()
client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())

req, _ := http.NewRequest("GET", "http://127.0.0.1:8080/hello", nil)
client.Do(req, func(resp *http.Response, err error) {
    // 响应、错误、超时或取消，恰好回调一次
})

resp, err := client.Go(req).Wait() // Future：Wait 阻塞，Done() 可用于 select
```

- 支持 `http://` 和 `https://`；https 使用 `ClientConfig.TLSConfig`（nil 表示默认
  配置，未设置 ServerName 时取 URL 的 host）。其他 scheme 返回 `ErrUnsupportedScheme`。
  http 与 https 即使地址相同也各自使用独立的连接池。
- HTTP/2：https 默认在 ALPN 中提供 `h2`、`http/1.1`（`TLSConfig.NextProtos` 已设置时
  以它为准），服务端选择 h2 时自动走 HTTP/2，一个连接并发承载多个请求（数量受服务端
  `MAX_CONCURRENT_STREAMS` 限制），对同一 host 只保持必要数量的连接。
  `ClientConfig.DisableHTTP2` 强制 HTTP/1.1；`ClientConfig.UnencryptedHTTP2` 让 http://
  以 prior knowledge 方式直接使用明文 HTTP/2（h2c）。HTTP/2 上超时或取消只会用
  RST_STREAM 结束对应 stream，不影响同连接上的其他请求；收到 GOAWAY 或
  REFUSED_STREAM 时，服务端未处理的请求会自动在新连接上重发。设置了 `Request.Trailer`
  的请求，trailer 会在 body 之后用单独的 HEADERS 帧发送（HTTP/1 上则是 chunked 的
  trailer）。
- 每个 host:port 维护连接池：keep-alive 复用，`MaxConnsPerHost` 限制同时打开或正在
  建立的连接数，超出的请求排队；`MaxIdleConnsPerHost`、`IdleConnTimeout` 控制空闲
  连接的保留。HTTP/1.1 连接同时只跑一个请求，不做 pipelining。
- 支持 `Content-Length`、chunked（含 trailer）、以关闭连接为结束的 body，HEAD、
  204、304 不读 body，1xx 中间响应自动跳过。请求的 `ContentLength` 为 -1 时 body 以
  chunked 发送，可以携带 `req.Trailer`。
- HTTP/1.0：把请求的 `Proto`、`ProtoMajor`、`ProtoMinor` 设为 `HTTP/1.0`、1、0，就会以
  HTTP/1.0 发送（body 用 `Content-Length`，不带 trailer）；请求带
  `Connection: keep-alive` 且服务端同意时连接才会复用。`MaxResponseHeaderBytes`、
  `MaxResponseBodyBytes` 限制单个响应大小。
- `Timeout` 覆盖从 `Do` 到响应完整的全过程（排队、建连、发送、读取），超时错误满足
  `errors.Is(err, os.ErrDeadlineExceeded)`；取消请求的 context 同样会结束请求。被
  放弃的请求所在的 HTTP/1.1 连接会被关闭。
- 复用的空闲连接如果已被服务端关闭、且没有收到任何响应字节，GET/HEAD/OPTIONS/TRACE
  会在新连接上自动重试一次；其他方法直接返回错误。
- 回调可能在任意 goroutine 上执行：成功的响应在读取它的 worker 上回调，超时和取消在
  定时器 goroutine 上，连接失败在单独的 goroutine 上，`Do` 立即拒绝的请求在调用方
  goroutine 上。回调不要长时间阻塞；开启 `InlineHandlers` 时成功回调会在事件循环上执行。
- `Do` 会在调用方 goroutine 里读完 `req.Body`。
- 先 `client.Close()` 再关闭 Engine：`Close` 让排队中的请求以 `ErrClientClosed` 失败，
  已发出的请求照常完成；直接关闭 Engine 不会通知 client，已发出的请求只能等超时。

### 中间件（middleware）

`middleware` package 用 `Chain` 把中间件套在 handler 外面，第一个在最外层（最先看到
请求、最后看到响应）。各子 package 用 `New(可选 Config)` 构造，HTTP/1、HTTP/2、HTTP/3
通用：

```go
handler := middleware.Chain(app,
    recover.New(),      // panic 转成 500，连接和同连接上的其他请求不受影响
    requestid.New(),    // X-Request-ID：沿用客户端的，否则生成 UUID
    logger.New(),       // 每个响应写一行，格式用 ${status}、${latency} 等标签
    responsetime.New(), // X-Response-Time
    limiter.New(limiter.Config{Max: 100, Expiration: time.Minute}), // 按 IP 限流，超出返回 429
    cors.New(cors.Config{AllowOrigins: []string{"https://app.example.com"}}),
    csrf.New(),         // double-submit cookie + net/http 的 CrossOriginProtection
    pprof.New(),        // /debug/pprof/，profile 在单独的 goroutine 里采集
    compress.New(),     // gzip / deflate
    etag.New(),         // ETag 与 If-None-Match → 304
)
server, err := fib.Bind(config, fibhttp.NewHandler(handler))
```

每个 Config 都有 `Next`，返回 true 时跳过该中间件。中间件通过 `Context` 的三个钩子改写
响应，所以 handler 无论用 `Respond`、`WriteResponse` 还是 ResponseWriter 方法写响应都适用：

- `OnHeader(fn)`：响应头定下来之前调用，拿到最终状态码，可以修改 header，不影响流式发送。
- `OnResponse(fn)`：发送前拿到完整响应（状态码、header、body、trailer）并可替换。
  注册后 ResponseWriter 方法写的响应会整体缓存到 handler 结束再发（和 HTTP/2 一样），
  HTTP/1 上 `Flush` 不再立即发送，文件也不走 sendfile；compress 和 etag 用的就是它，
  对 SSE 之类的流式响应请用 `Next` 跳过。
- `OnFinish(fn)`：响应交给连接之后调用，带状态码、header 和 body 长度。

钩子按注册的逆序执行（和 defer 一样），外层中间件看到的是内层处理完的结果。compress 要放在
etag 外面，这样 ETag 按原始 body 计算，压缩后再改成弱 ETag。

## HTTP/3 子 package

`http3` package 在 Engine 的 UDP socket 上提供 HTTP/3（RFC 9114），其下的 QUIC
（RFC 9000/9001/9002）和 QPACK（RFC 9204）都在本库内实现，TLS 1.3 握手使用标准库的
`crypto/tls.QUICConn`，不引入任何第三方依赖。Handler 与 `http` package 完全相同，
同一个 `fibhttp.Handler` 可以同时服务 HTTP/1.1、HTTP/2 和 HTTP/3：

```go
config := fib.DefaultConfig()
config.Network = "udp"
config.Addr = ":443"
server, err := fib.Bind(config, http3.NewHandler(tlsConfig, handler))
```

- 服务端就是一个 UDP Engine，handler 用 `http3.NewHandler`（或 `NewHandlerWithConfig`
  调整 `MaxHeaderBytes`、`MaxBodyBytes`、`MaxConcurrentStreams`、`MaxIdleTimeout`）。
  TLS 配置里的 ALPN 会自动加上 `h3`。
- Engine 按对端地址区分 UDP 连接，每个发来 QUIC Initial 的地址对应一个 QUIC 连接，挂在
  该地址的 `fib.Connection` 上；因此不支持连接迁移（NAT 重绑定后连接会失效），服务端在
  传输参数里声明 `disable_active_migration`。没有连接状态的短包头包会收到 stateless
  reset，让对端立即结束而不是等超时；不认识的 QUIC 版本收到 Version Negotiation。
- QUIC 的空闲超时（`MaxIdleTimeout`，默认 30 秒）应小于 Engine 的
  `Config.UDPIdleTimeout`（默认 60 秒），否则 Engine 会先把静默的对端连接关掉。
- Handler 不用改：请求的 `Proto` 为 `HTTP/3.0`，`Request.TLS` 带有 TLS 状态，用
  `Context.Respond`/`WriteResponse` 回复，响应被编成该 stream 的 HEADERS/DATA 帧；
  可以在其他 goroutine 中异步回复。`WriteInterim` 支持 1xx 中间响应（如 103 Early
  Hints），`Expect: 100-continue` 自动回复 100。`Push` 返回 `http.ErrNotSupported`：
  HTTP/3 的 push 需要客户端先发 MAX_PUSH_ID，浏览器都不这样做。
- `Response.Close` 在 HTTP/3 上优雅关闭：发送 GOAWAY，不再接受新请求，已在处理的请求
  的响应全部送达后再关闭连接。
- handler 协程池：与 HTTP/2 一样，请求收齐后交给独立的协程池（与 HTTP/2 server
  共用同名 Engine 的 `<Name>-streams`，不与 Engine 的协程池共用），同一连接上的请求并发处理；用 `Config.StreamPool` 配置，
  `StreamPool.MaxConcurrentHandlers` 限制单个连接的并发处理数，其中最后一个在读取该
  连接的协程上执行，在它返回前这个连接不再读取数据报。
- 请求 body 超过 `MaxBodyBytes` 返回 413，header 超过 `MaxHeaderBytes` 返回 431；
  非法请求（缺少伪头部、大写字段名、连接相关字段等）用 H3_MESSAGE_ERROR 重置该
  stream，协议错误（控制流不以 SETTINGS 开头、DATA 出现在 HEADERS 之前、引用动态表等）
  按 RFC 9114 的错误码关闭连接。
- 浏览器通过 HTTP/1.1 或 HTTP/2 响应里的 `Alt-Svc` 头发现 HTTP/3，`http3.AltSvc(port)`
  生成这个头的值；通常在同一端口号上同时跑 TCP 的 HTTPS 和 UDP 的 HTTP/3（见示例）。

QUIC 层的实现要点：

- 包保护支持 TLS 1.3 的三种套件：AES-128-GCM、AES-256-GCM 和
  ChaCha20-Poly1305（标准库没有导出 ChaCha20-Poly1305，本库按 RFC 8439 自行实现，
  并用 RFC 9001 附录的测试向量校验）；密钥更新（key update）双向支持：一把密钥保护的
  包数达到 RFC 9001 §6.6 的上限时主动发起，也响应对端发起的更新，解密失败次数超过
  完整性上限时关闭连接；客户端支持 Retry。不支持 0-RTT。
- 丢包检测与拥塞控制按 RFC 9002 实现：ACK 阈值和时间阈值判定丢包、PTO 探测、
  NewReno 拥塞控制；服务端在验证客户端地址前遵守 3 倍放大限制。发出的数据报固定为
  1200 字节（所有 IPv6 路径都能承载的大小），因此不需要 PMTU 探测。
- 流控：遵守对端的连接级和 stream 级限制；接收方向每个 stream 窗口 1MB、连接窗口
  16MB，按消费量自动补充；并发 stream 数用 MAX_STREAMS 动态放开。
- QPACK 两端都声明动态表容量为 0：编码只用静态表和字面量（字面量在更短时使用
  Huffman 编码），解码拒绝任何动态表引用，因此编码器流和解码器流无需内容。
- 已与 quic-go 做双向互通测试（包括 5% 随机丢包下的 20MB 双向传输），客户端也验证过
  Cloudflare、Google、nginx、Facebook、Varnish、quiche 的线上 HTTP/3 服务。

#### 一致性与限制

HTTP/3 的一致性测试（与 quic-go 客户端、服务端的双向互通，以及用手写帧构造的协议错误
用例，CI 中在三个平台运行；quic-go 放在 `http3/interop` 这个独立 module 里，fib 本身
不增加依赖）、当前的限制（按对端地址区分连接、不支持迁移、固定 1200 字节数据报、关闭时
没有 closing/draining 期等）、有意未实现的功能及原因（0-RTT、QPACK 动态表、server push
等），以及待优化项（安全加固、pacing、PMTU 探测、拥塞控制、批量收发等）见
[`docs/http3.zh-CN.md`](http3.zh-CN.md)。

### 异步 HTTP/3 client

`http3.Client` 与 `http.Client` 的用法相同：

```go
client := http3.NewClient(engine, http3.DefaultClientConfig()) // engine 可以是任意运行中的 Engine
req, _ := http.NewRequest("GET", "https://example.com/", nil)
client.Do(req, func(resp *http.Response, err error) { /* 恰好回调一次 */ })
resp, err := client.Go(req).Wait()
```

- 只支持 `https://`（端口默认 443），其他 scheme 返回 `ErrUnsupportedScheme`。
  `ClientConfig.TLSConfig` 未设置 ServerName 时取 URL 的 host，ALPN 固定为 `h3`。
- 每个 host:port 一个 QUIC 连接，所有请求各占一个 stream 并发发送，数量受服务端
  MAX_STREAMS 限制，超出的请求排队，等服务端放开 stream 后自动发送。
- 收到 GOAWAY 时，服务端未处理的请求和排队中的请求自动在新连接上发送；被服务端以
  H3_REQUEST_REJECTED 重置的请求同样重发一次。
- 支持发送请求 trailer（`req.Trailer`，名字会在 `Trailer` 头里声明）和接收响应 trailer。
- `Timeout`、请求的 context 取消只重置对应 stream（H3_REQUEST_CANCELLED），不影响
  同连接上的其他请求；`HandshakeTimeout`、`MaxIdleTimeout`、`IdleConnTimeout` 分别
  限制握手、QUIC 空闲超时和连接复用的空闲时间；`MaxResponseHeaderBytes`、
  `MaxResponseBodyBytes` 限制单个响应大小。支持 trailer，1xx 中间响应自动跳过。
- 回调可能在任意 goroutine 上执行；先 `client.Close()` 再关闭 Engine。

## WebSocket 子 package

`websocket` package 实现 RFC 6455 Upgrade 握手、增量帧解析、
分片消息重组（也可以按帧接收）、客户端掩码校验、Ping/Pong、Close 握手、子协议协商、
消息大小限制和 permessage-deflate 压缩（RFC 7692）：

```go
handler := websocket.NewHandler(websocket.HandlerFuncs{
    Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
        _ = c.WriteMessage(opcode, data)
    },
})
server, err := fib.Bind(config, handler)
```

收到的 Ping 默认自动回复同样 payload 的 Pong，收到的 Pong 默认丢弃。
`HandlerFuncs` 的 `Ping`、`Pong` 字段可以自定义这两种行为；自己实现 Handler 时，
额外实现 `PingHandler`（`OnPing`）或 `PongHandler`（`OnPong`）接口即可：

```go
handler := websocket.HandlerFuncs{
    Message: onMessage,
    // 设置 Ping 后不再自动回复，是否回 Pong 由它自己决定
    Ping: func(c *websocket.Connection, payload []byte) {
        _ = c.Pong(payload)
    },
    // 例如用于心跳检测、计算 RTT
    Pong: func(c *websocket.Connection, payload []byte) {
        lastPong.Store(time.Now().UnixNano())
    },
}
```

- 与 gorilla/websocket 的约定相同：自定义 Ping handler 会替换默认回复，需要回复时
  自己调用 `Connection.Pong`。
- payload 只在回调期间有效，需要保留时先复制；回调与 `OnMessage` 在同一个 worker
  上串行执行。server 和 client（`Dialer`）两端都支持。
- `Connection.Ping` 主动发送 Ping，payload 最多 125 字节。

运行 WebSocket echo 示例（`-compress` 开启压缩）：

```sh
go run ./examples/websocket/nontls/server   # 另开终端：go run ./examples/websocket/nontls/client
```

### 按帧接收（避免大消息占用大内存）

`OnMessage` 收到的是重组后的完整消息，分片消息要先在内存里拼起来，大 body 就意味着
大内存开销。`HandlerFuncs` 设置 `Frame` 字段（自己实现 Handler 时额外实现
`FrameHandler` 接口的 `OnFrame`）后，消息按帧交付，不再重组：

```go
handler := websocket.HandlerFuncs{
    // 设置 Frame 后 Text/Binary 不再回调 Message
    Frame: func(c *websocket.Connection, opcode websocket.Opcode, fin bool, data []byte) {
        _, _ = file.Write(data) // 边收边处理，连接不持有整条消息
        if fin {
            _ = file.Close()
        }
    },
}
```

- 每帧回调都带消息自己的 opcode（Text/Binary，不会是 Continuation）和 fin，最后一帧
  fin 为 true；未分片的消息就是一次 fin 为 true 的回调。
- 帧之间不缓存任何 payload，所以对端可以发送远大于 `MaxMessageBytes` 的消息，此时
  `MaxMessageBytes` 限制的是单帧大小而不是整条消息。
- Text 的 UTF-8 仍然增量校验：一个字符可以跨帧，但整条消息必须合法，否则以 1007 关闭。
- 压缩消息是例外：一条压缩消息的各帧是同一个 DEFLATE 流的片段，无法逐帧解压，仍然
  重组、解压后作为一次 fin 为 true 的 `OnFrame` 交付。
- server 和 client（`Dialer`）两端都支持；payload 同样只在回调期间有效，需要保留先复制。

### permessage-deflate 压缩

`Config.EnableCompression`（server）和 `DialerConfig.EnableCompression`（client）
开启 permessage-deflate，默认关闭。协商成功后 Text/Binary 消息压缩发送，控制帧不压缩；
收到的压缩消息解压后再交给 `OnMessage`，未协商时收到 RSV1 帧按协议错误以 1002 关闭。

- 发送端总是不保留上下文（server 响应里始终带 `server_no_context_takeover`，client
  offer 里带 `client_no_context_takeover`），每条消息从 `sync.Pool` 借一个
  `flate.BestSpeed` 压缩器，连接本身不持有压缩器。
- 对端保留上下文时，连接保存最近 32 KiB 解压结果作为下一条消息的字典；对端声明
  no_context_takeover 时不保存。
- 对端限制窗口（`server_max_window_bits` / `client_max_window_bits` 为 8–15）时，
  消息按窗口大小分段、各段独立压缩后拼成一个 DEFLATE 流，保证回溯距离不超过窗口。
- `MaxMessageBytes` 限制的是解压后的大小，超过时以 1009 关闭；解压失败或解压后的
  Text 不是合法 UTF-8 时以 1007 关闭。

### 异步 WebSocket client

`Dialer` 在 Engine 上发起 WebSocket 连接并完成握手，调用方不会阻塞。握手成功后，
client 连接与 server 端连接走同一套 `Handler` 回调、同一个 worker 池和同一个
`Connection` 类型：

```go
engine, _ := fib.NewEngine(fib.DefaultConfig(), nil) // 或直接复用 server 的 Engine
go engine.Run()
dialer := websocket.NewDialer(engine, websocket.DefaultDialerConfig())

handler := websocket.HandlerFuncs{
    Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
        // 服务端发来的消息；data 只在回调期间有效
    },
}
dialer.Dial("ws://127.0.0.1:8080/ws", nil, handler, func(c *websocket.Connection, resp *http.Response, err error) {
    if err != nil {
        return // 连接失败、握手被拒（errors.Is(err, websocket.ErrBadHandshake)）或超时
    }
    _ = c.WriteText("hello")
})

conn, resp, err := dialer.Go(url, header, handler).Wait() // Future 形式
```

- 支持 `ws://` 和 `wss://`；wss 使用 `DialerConfig.TLSConfig`，规则同 HTTP client。
  其他 scheme 返回 `ErrUnsupportedScheme`。
- 成功时先调用 handler 的 `OnOpen`（参数是握手请求），再调用 done；失败时只调用
  done，连接为 nil，服务端有响应时一并传入 resp（例如 403），便于查看状态码和头部。
- 握手会校验 101 状态、`Upgrade`/`Connection` 头、`Sec-WebSocket-Accept` 与本次
  key 是否匹配，以及服务端选择的子协议是否在 `Subprotocols` 里；服务端选择了未提供的
  扩展同样视为失败。
- `header` 用于附加 Origin、鉴权等头部；`Upgrade`、`Connection`、
  `Sec-WebSocket-Key`/`Version`/`Protocol`/`Extensions` 由 Dialer 设置，不能传入。
- `HandshakeTimeout` 覆盖建连和握手全过程，超时错误满足
  `errors.Is(err, os.ErrDeadlineExceeded)`。
- client 发出的每一帧都按 RFC 6455 用随机 key 掩码，需要复制一次 payload；收到
  带掩码的服务端帧按协议错误以 1002 关闭。与握手响应同一次读到的首帧也会正常交付。
- done 可能在任意 goroutine 上执行：握手成功在读取它的 worker 上，超时在定时器
  goroutine 上，连接失败在单独的 goroutine 上，URL 非法时在调用方 goroutine 上。

## bufferpool 子 package

`bufferpool` package 按 2 的幂尺寸档复用 `[]byte`，是 fib 及其各子 package 里所有
可复用 buffer 的统一来源：

```go
import "github.com/lesismal/fib/bufferpool"

buf := bufferpool.Get(n)          // len 为 n，cap 为 bufferpool.Align(n)
defer bufferpool.Put(buf)

buf = bufferpool.Append(buf, data)          // 不够时从更大的档取一块，旧的还回去
out := bufferpool.Join(nil, first, second)  // 一次取够，把几段拼成一块
```

- **为什么对齐到 2 的幂。** 一是只有对齐才谈得上复用：按请求大小裁出来的 buffer 各不相同，
  按档取整之后同一档里的任何一块都能服务这一档的任何请求，一档只需留住同时在用的那几块。
  二是底层分配没有浪费：Go 的分配器本来就要把对象向上取整到它自己的 size class，请求
  4200 字节会拿到 4864 字节的对象；而 64 字节往上的每个 2 的幂都正好是一个 size class，
  超过 32 KiB 之后分配按页走，2 的幂也正好是 8 KiB 页的整数倍。档位索引也因此只是一条
  `bits.Len`，不需要查找。
- **档位范围。** 最小 64 字节（`MinSize`），更小的对象 Go 的 tiny allocator 本来就比一次
  池操作快；最大 64 MiB（`MaxSize`），更大的直接分配、`Put` 时直接丢弃，为一次罕见的请求
  长期留住那么大一块并不划算。
- **Get/Put 不分配任何内存。** `sync.Pool` 存的是 `any`，slice header 放不进去，
  用 `*[]byte` 则每个 buffer 都要多一个 24 字节的堆对象。同一档里所有 buffer 长度相同，
  所以只存首地址（指针本身就能放进 `any`，不产生装箱分配），长度由档位算出来：
  堆上不会因为复用多出任何对象，而 `[]byte` 不含指针，GC 也从不扫描它们。
  一次 Get+Put 是 7.4ns，0 次分配。
- **working set 不会被每次 GC 清掉。** `sync.Pool` 每轮 GC 都会被清空，被清掉的正是程序的
  working set：实测 1024 块闲置的 16KiB buffer，每次 GC 之后要重新分配 16.8MB，而这些分配
  本身又会把下一次 GC 提前。所以每档在 `sync.Pool` 后面还有一层 reserve，用普通 slice 持有，
  GC 不会动它。reserve 只在「刚发生过一次 GC」之后集中补一批（GC 次数由 runtime 的 cleanup
  机制统计），平时的归还只读一个计数器就走 `sync.Pool`，锁只在这一批补充里用到：这一批的次数
  等于 reserve 里的 buffer 数，省下的正是同样数量的分配。
- **reserve 的大小是学出来的，并且有上限。** 某一档每被迫 `make` 一次，它的 target 就加一，
  所以 target 收敛到程序真正同时在用的数量；某一轮补充没用完（说明这一档不再需要那么多）就
  回落到实际留住的数量，把额度还给别的档。总量由 `SetRetainedBytes` 限定，默认
  `DefaultRetainedBytes`（32 MiB）；`SetRetainedBytes(0)` 会立刻把已经留住的内存全部释放，
  可以用来应对内存压力。`RetainedBytes()` 返回当前的额度。
- **一个全局池，不是每处一个。** 连接的读 buffer、由它拼出来的回包、一条 TLS 记录用的都是
  同一批 buffer 在各档之间流动；分成多个池只会让每个池各自留一份闲置 buffer。
- **`Get` 返回的内容是上一个使用者留下的**，和任何复用 buffer 一样，先写后读。
- **`Put` 之后不能再用这块 buffer**（包括它的任何子 slice）。小于 `MinSize` 或大于
  `MaxSize` 的会被直接丢弃，所以来路不明的 buffer 也可以直接交给 `Put`。

fib 内部用它的地方：连接的读 buffer 与发送队列缓冲、`SendFile` 的中转 buffer、UDP
拼包、TLS 的收包缓冲与记录拼接、WebSocket 跨读分帧时接管的数组、HTTP/1 与 HTTP/2 的
收包缓冲和 CONTINUATION 块、HTTP/3 的响应帧组装。

## Examples

每种协议一个目录，每个目录下分 TLS 与非 TLS 两个子目录（UDP 只有非 TLS：本库
不内置 DTLS；HTTP/3 只有 TLS：QUIC 总是加密的），各有一个 echo server 和一个 echo client：

```text
examples/
├── tcp/        nontls/{server,client}  127.0.0.1:9000    tls/{server,client}  127.0.0.1:9443
├── udp/        nontls/{server,client}  127.0.0.1:9001
├── http/       nontls/{server,client}  127.0.0.1:8080    tls/{server,client}  127.0.0.1:8443 (HTTPS)
├── http3/                                                tls/{server,client}  127.0.0.1:8445 (UDP)
└── websocket/  nontls/{server,client}  127.0.0.1:8081    tls/{server,client}  127.0.0.1:8444 (wss)
```

先启动 server，再在另一个终端运行对应的 client，例如：

```sh
go run ./examples/tcp/tls/server
go run ./examples/tcp/tls/client -n 10
```

- client 默认发送 5 条消息（`-n` 修改），逐条打印回显后退出；server 按 Ctrl-C 退出。
  地址用 `-addr`（TCP、UDP）或 `-url`（HTTP、WebSocket）修改。
- TLS server 未指定 `-cert`/`-key` 时，会为 localhost 和 127.0.0.1 签发一张自签名
  证书，并把证书（不含私钥）写到临时目录的 `fib-example-cert.pem`；TLS client 默认
  信任这个文件（`-ca` 修改），因此会像正式部署一样校验服务端证书。`-insecure` 跳过校验。
- 不同协议的 TLS 写法：TCP、HTTP、WebSocket server 用 `fibtls.NewServer` 包装原本的
  handler，client 分别用 `fibtls.Dial`、`ClientConfig.TLSConfig`、
  `DialerConfig.TLSConfig`。HTTP/3 的 TLS 由 QUIC 自己完成，server 把 TLS 配置直接
  交给 `http3.NewHandler`，client 用 `http3.ClientConfig.TLSConfig`。
- HTTP/3 server 在同一端口号上同时监听 UDP（HTTP/3）和 TCP（HTTPS，HTTP/2 与
  HTTP/1.1），TCP 上的响应带 `Alt-Svc`，浏览器据此切换到 HTTP/3。
- WebSocket server 的 `-compress` 开启 permessage-deflate，CI 用它跑 Autobahn 测试。
- HTTP server 的 `-dir` 用 `net/http` 的 `FileServer` 在 `/files/` 下提供该目录的文件
  （支持 Range、条件请求，文件走 sendfile），例如 `go run ./examples/http/nontls/server -dir .`
  后 `curl -O http://127.0.0.1:8080/files/go.mod`。

## GOMAXPROCS

工作协程在自己的协程里直接执行连接的 read/write，而协程进入系统调用时会一直占着它的 P，
直到调度器把这个 P 收回转交出去。所以 GOMAXPROCS 等于核数时，核会在等这次转交的过程中空转：
10 万连接的 echo 压测里，进程在分到的 5 个核上只用掉 2.3 个核，execution trace 显示 2 秒窗口内
有 872 秒的「已就绪但没在运行」时间，几乎全部落在被事件循环唤醒的 worker 上。

把 GOMAXPROCS 设成核数的 2 倍即可：同一份构建下 echo 从 330k/s 提升到 415k/s，建连从 55k/s
提升到 71k/s，TP99 从 145ms 降到 69ms。这是使用方的选择，库不会去改这个全局设置：

```go
runtime.GOMAXPROCS(2 * runtime.NumCPU())
```

需要注意 `runtime.NumCPU()` 取的是本进程的 CPU 亲和性掩码，被 taskset 或 cpuset 限制时它已经
是实际可用的核数。

## InlineHandlers

`Config.InlineHandlers` 让事件循环直接执行连接的这一轮处理，不再交给工作协程。省掉这次交接
在高消息速率下很可观：同一压测里 echo 446k/s 对 395k/s，建连 104k/s 对 95k/s。

代价是 handler 会阻塞它所在的整个 server —— 在它返回之前，事件循环无法收事件、无法 accept、
也无法服务这个 server 上的其他连接。只有在所有 handler 都很短且不会阻塞时才开启；会做 I/O、
抢锁或执行不定长工作的 handler 应该继续走工作协程池，这个池存在的意义正是让一条慢连接不拖住其他连接。

## IOPollers

`Config.IOPollers` 把一个 Engine 拆到多个事件循环上（仅 Linux 的 epoll 和 macOS 的 kqueue；
Windows 上忽略这个配置，保持单个循环和原来的 task pool）。`DefaultConfig()` 默认开启；
设为 `false` 时 listener 和连接都在同一个 event loop 上，每条连接的每一轮都在 worker 池上执行：

- Engine 自己的循环只负责 accept 和它的 UDP socket；每条 accept 到的连接按 `fd % pollerCount`
  交给其中一个 poller，此后由那个 poller 负责它的事件注册、读写和关闭。poller 数量由
  `Config.IOPollerCount` 指定，不大于 0（默认）时取 `runtime.NumCPU()`。
  每 CPU 一个 poller 适合连接的每一轮都在 poller 上执行（Inline）的负载；如果连接的处理都交给
  worker（比如 HTTP/1，见下），`IOPollerCount` 应该设小（1 个或几个）。这时 poller 只负责等事件、
  交给 worker，一个就够用，多出来的 poller 会和 worker 抢同样的 P：每一轮结束后 poller 都要
  让出 P 给刚唤醒的 worker，再排在它们后面等 P，于是每轮收集到的连接更少、请求被读到得更晚。
  实测 3 个 CPU、1 万连接的 HTTP/1 echo：不开 IOPollers 583k 请求/s，1 个 poller 584k，3 个
  poller 569k，p99 从 25ms 升到 37ms。
- Linux 上同时设置 `Config.ReusePort` 时，accept 也移到 poller 上：每个 poller 在 Engine 的地址上
  用 `SO_REUSEPORT` 监听一个自己的 socket，自己 accept 内核按连接地址哈希分给它的连接；
  Engine 自己的 socket 只 bind（占住地址和内核选的端口）不 listen。不设置时 Engine 自己的循环
  accept 所有连接，再逐条唤醒接手的 poller，于是 accept 的速度被这一个循环封顶，与核数无关：
  每 10 条消息就重连一次的 WebSocket echo 在 64 核上停在约 9.5 万连接/s、35 个核。代价是
  均衡：连接留在哈希选中的 poller 上，不管它多忙。Unix socket 和其他平台仍由 Engine 自己的
  循环 accept；macOS 的 `SO_REUSEPORT` 不在多个 socket 之间分摊 TCP 连接。`ReusePort` 也让
  同一用户的其他进程可以监听同一地址并分走连接。
- `Dial` / `DialWithHandler` 发起的连接（TCP、UDP、Unix socket）也同样按 fd 取模分到 poller 上。
  UDP server 的各个 peer 共用监听的那一个 socket，所以留在 Engine 自己的循环上。
- 开启后 Engine 自己的 task pool 固定为 `taskpool.ModeInline`（名为 `<Name>-inline`）：
  每个 poller 在自己的 goroutine 上直接执行它那些连接的这一轮处理，handler panic 会被
  recover 并关闭该连接。和 `InlineHandlers` 一样，阻塞的 handler 会卡住同一个 poller 上的
  所有连接。通过 `SetTaskPool` 传入的池不受影响。
- handler 可能耗时的连接调用 `Connection.SetRunOnWorkers(true)`，它的每一轮（读、`OnData`、
  `OnClose`）就改在 worker 池上执行。这个池就是 `TaskPoolMode`、`WorkerCount`、
  `SharedTaskPool` 描述的那个（`<Name>-workers`），第一条要求它的连接出现时才创建。
  Engine 本来就在 worker 上执行时这个设置不起作用；`InlineHandlers` 和 `ModeInline`
  下同样有效。
- http 子 package 的 HTTP/1 连接（服务端和客户端）一律 `SetRunOnWorkers(true)`，读、解析、
  handler 都在 worker 上完成；识别出 HTTP/2（preface、ALPN `h2`、h2c upgrade）后改回
  `false`，由 Engine 自己的池（不论是否 Inline）读和解析，请求交给共享的 stream pool
  执行。HTTP/3 同样在 Engine 自己的池上读和解析，请求走 stream pool。stream pool 仍按
  `WorkerCount` 计算大小。
- `tls` 子 package 的连接在 `OnOpen` 里、调用被包装 handler 的 `OnOpen` 之前先
  `SetRunOnWorkers(true)`：解密和加密是 TLS 连接每一轮里最贵的工作，放在 poller 上会让同一个
  poller 的连接排队逐条解密。3 个 CPU、1 万连接的 TLS echo，在 3 个 poller 上执行时 40.4 万
  echo/s、约 15% 的 CPU 空闲，改到 worker 上是 45.1 万。被包装的 handler 可以在自己的
  `OnOpen` 或之后改回 `false`，TLS 上的 HTTP/2 就是这样做的。
- WebSocket 连接不调用 `SetRunOnWorkers`，跟随 Engine 的配置：开启 IOPollers 时消息在 poller
  上用 Inline 池处理，否则用 worker 池。所以开启 IOPollers 时 WebSocket 的消息回调同样不能阻塞。
  TLS 上的 WebSocket（wss）由 `tls` 放到 worker 上。
- `MaxPendingBytes` 仍是整个 Engine（含所有 poller）的总预算；一个 poller 上的连接把预算
  释放到恢复线以下时，会唤醒其他因预算暂停了连接的 poller。`Stats()` 汇总所有 poller 的计数，
  `Connection.Engine()` 返回的仍是用户创建的那个 Engine。

```go
config := fib.DefaultConfig() // 默认已开启 IOPollers
config.IOPollerCount = 0      // runtime.NumCPU() 个
config.ReusePort = true       // Linux：每个 poller 自己 accept

single := fib.DefaultConfig()
single.IOPollers = false // 单个 event loop + worker 池
```

## OnClose 的顺序

事件循环关闭一条连接时（`Close`、deadline、UDP 空闲超时、读写出错等），连接立即停止收发，
发送队列和预算立即释放，但 `OnClose` 和 fd 的释放像 `noteEvent` 一样折进这条连接、排在它的
一轮里：如果它的一轮正在执行或已在排队，`OnClose` 等那一轮的 `OnData` 返回之后才执行，fd 也
在那之后才由事件循环关闭。所以 `OnClose` 总在之前的 `OnData` 之后，一轮还在 worker 上跑时
它读写的 fd 也不会被关闭或被别的连接复用。

相应地，在 `OnData`（例如 HTTP/1 handler）里关闭自己的连接后再等待 `OnClose` 带来的通知
（如 `Context.OnCancel`）会死锁：那个通知要等这一轮返回才会到。
