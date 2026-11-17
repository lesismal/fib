# bufferpool：自定义分配器与严格 Get/Put 配对

[English](bufferpool-allocator.md)

[`bufferpool`](../bufferpool) 保持直接返回和传递 `[]byte` 的接口，用户可以通过 `SetAllocator` 换上自己的分配器（第 1 节）。本文其余部分是一份分析，没有对应的实现。问题是：换上手动管理内存的分配器（例如用 cgo 调 jemalloc）时，能否做到：

- **Put 只接收本池取出的缓冲区**，外来的一律不收；
- **Get 与 Put 严格一对一**，不能有泄漏。

分析基于 2026-11-17 的 `b070cc6`。下文的数字都是在 Apple Silicon、14 核、Go 1.27.3 上测得的。

## 结论

1. **可插拔分配器本身没有性能代价。** `Get`/`Put` 开头判断 `atomic.Pointer` 是否为 nil，为 nil 就走内置池，实测和加入前一样快。这部分已经实现。
2. **“只收自己的缓冲区”只能由分配器判断。** 仅凭一个 `[]byte`，fib 无从知道它的来源。
3. **fib 现在做不到严格一对一。** 现有约定本来就是“任何缓冲区都可以 Put，不 Put 的交给 GC”，代码里有大量依赖这条约定的路径。要接手动管理内存的分配器，必须先改约定，再把这些路径全部修掉。
4. **用 cgo 调 jemalloc，每对 Get+Put 比现有池慢好几倍。** 它能省下的 GC 开销，现有设计大部分已经省下了。

## 1. 分配器接口：nil 回退（已实现）

```go
type Allocator interface {
    Get(size int) []byte
    Put(buf []byte)
}

// SetAllocator 应在任何组件取用缓冲区之前调用一次；nil 恢复内置池。
func SetAllocator(a Allocator)
```

实现在 [allocator.go](../bufferpool/allocator.go)：

- **默认路径**：`Get` 和 `Put` 开头各加载一次 `atomic.Pointer`，为 nil 时走内置池，其余代码不动。
- **Get**：`size <= 0` 仍然直接返回 nil，不经过分配器；其余尺寸全部交给分配器，包括超过 `MaxSize` 的。分配器返回的切片 len 必须等于 size，cap 至少为 size。
- **Put**：cap 为 0 的缓冲区直接忽略；其余全部交给分配器，由它决定如何处理不是自己发出的缓冲区（第 2、3 节）。
- **Grow、Append、Join**：通过 `Get`/`Put` 走同一个分配器；容量够用时的快路径不经过分配器。
- **Align、MinSize、MaxSize**：仍描述内置池，fib 用它们决定申请多大的缓冲区。`SetRetainedBytes`、`RetainedBytes` 只作用于内置池。
- **切换时机**：分配器应在启动前设置。换掉时还在外面的缓冲区，Put 时会交给当时的分配器。

设计时实测的 Get+Put 一对耗时：

| 方案 | 单线程 | 14 核并行 |
| --- | --- | --- |
| 原实现 | 8.15 ns | 1.02 ns |
| 包内 nil 判断，普通全局变量 | 7.74 ns | 1.12 ns |
| 包内 nil 判断，`atomic.Pointer` | 8.16 ns | 1.11 ns |
| 包级接口变量，默认实例也走接口 | 约 +1.5 ns | 约 +0.2 ns |

前三者的差距在噪声以内。最后一种多了一次间接调用，`Get` 的主体也不能再内联到调用方。因此选了 `atomic.Pointer`：速度一样，而且在 `-race` 下不会报竞争。

实现后，包内基准与加入前对比（各 8 次的平均值）：

| 基准 | 加入前 | 加入后 |
| --- | --- | --- |
| `GetPut/64B` | 7.54 ns | 7.62 ns |
| `GetPut/1500B` | 7.64 ns | 7.64 ns |
| `GetPut/16384B` | 7.58 ns | 7.70 ns |
| `GetPutParallel` | 1.03 ns | 0.86 ns |
| `JoinParts/pooled` | 24.02 ns | 24.07 ns |

## 2. Put 只接收本池的缓冲区

判断只能在分配器内部做：

- **地址登记表**：发出去的每个地址记进一张分片 map，Put 时先查，查不到的缓冲区忽略，交给 GC。能挡住外来缓冲区和重复 Put。每次 Put 多一次查表，粗估 20–50 ns，未实测。
- **地址区间**：分配器预留一段连续地址自己做分级，比如 jemalloc 的专用 arena 加自定义 extent hooks，或者自己 mmap 一块内存。Put 只需比较地址是否落在区间内，是 O(1)。
- **不能用 `malloc_usable_size` 之类的函数去判断外来指针。** 对不是 jemalloc 分配的指针调用它，属于未定义行为。
- **要拒绝指向缓冲区中间的指针**（`buf[k:]` 这类截过的切片）。free 一个中间地址会直接崩溃。这次测试中没有发现 fib 这样 Put 过。

## 3. 严格一对一：fib 当前的差距

### 测法

在 fib 的一个临时副本里给 `bufferpool` 加了配对跟踪，用环境变量开启：

- Get 时按缓冲区的基地址登记，记下调用栈；
- Put 时核销。登记表里没有的，按情况记为“外来缓冲区”“中间指针”或“重复 Put”；
- 每次新分配都挂一个 `runtime.AddCleanup`。缓冲区被 GC 回收时如果仍未 Put，记为泄漏。

然后用 `-short` 跑了全部 88 个包，全部通过。

### 结果

| 问题 | 生产代码中的位置数 | 例子 |
| --- | --- | --- |
| Get 后没有 Put，交给 GC | 48 处 | `core.(*Engine).deliverDatagram`：连接关闭时排队中的 datagram 没有还回池；`http.(*Parser).appendBuffer` |
| Put 了不是从池里取的缓冲区 | 29 处 | `http.(*h2ServerConn).sendPooledLocked`、`tls.(*layer).releaseInLocked`、`websocket.(*Parser).releaseInflated` |
| 疑似重复 Put | 2 处 | `http.(*clientRequest).releaseData`、`http3.(*clientStream).send` |
| Put 中间指针 | 0 处 | — |

泄漏是在 GC 回收时才发现的，所以只是下限。其中有一部分是测试直接丢弃连接造成的，但这些路径确实都没有配对。

那两处疑似重复 Put，同一个位置也报了外来缓冲区。推断是跟踪器的误报：GC 回收之后、清理回调运行之前，同一个地址已被新的 Go 分配用上，于是被当成重复 Put。这一点没有证实。

### 典型模式

`http` 包 `fastFramedRequest`（[client_codec.go](../http/client_codec.go)）：

1. 先 `bufferpool.Get(size)`，再用普通的 `append` 往里写；
2. `size` 没有把 URL 路径的长度算进去，路径一长，`append` 就在 Go 堆上重新分配；
3. 结果是池里取出的那块被丢给 GC，后来由 `releaseData` Put 回去的却是 Go 堆上的新内存。

在现有约定下这不是 bug。换成 jemalloc 以后，前一块是真正的内存泄漏，后一块会被 free 一个 Go 堆指针，进程崩溃。

### 要做到严格配对需要什么

1. **改约定**：`Put` 只接收本池取出、且未被替换过底层数组的缓冲区；每次 Get 在所有路径上都恰好 Put 一次，包括关闭、出错和 panic 路径。
2. **修复上面这些位置**：所有“先 Get 再 append”的写法改用 `bufferpool.Append`/`Grow`；队列和缓存在连接关闭时要释放；不是来自池的缓冲区不再 Put。全部调用点约 270 处，分布在 55 个文件里，这是一轮完整的审计。
3. **把跟踪模式做成 fib 的调试功能**（例如用 build tag 开启），在 CI 里常驻，防止以后再退化。

还有一类风险 fib 内部消除不了：交给用户回调的缓冲区（OnData 的数据、WebSocket 消息）如果被用户留到回调之外再用，现在只是读到被复用的数据；换成手动管理的内存后，就是访问已释放的内存，可能段错误或内存损坏。

## 4. cgo 与 jemalloc 的开销

本机没有装 jemalloc，用系统 malloc 代替。主要开销来自 cgo 调用本身，jemalloc 自身也是十几纳秒量级。

| | 单线程 | 14 核并行 |
| --- | --- | --- |
| cgo 空调用 | 13.8 ns | — |
| cgo malloc+free 一对 | 49.6 ns | 9.5 ns |
| bufferpool Get+Put 一对 | 约 7.5 ns | 约 1 ns |

jemalloc 能省下的只是 Go 堆上的 GC 压力，而现有设计已经省下了大部分：

- 池里存的是 `*byte`，Put 时不需要把切片头装箱，Get/Put 都是 0 次分配；
- 缓冲区不含指针，GC 不扫描它们；
- GC 清空 `sync.Pool` 之后要重新分配工作集，这部分开销由每个分级后面的 reserve 兜住。

如果目标是降低 GC 开销，更划算的做法依次是：

1. 先调 `SetRetainedBytes` 和 `GOMEMLIMIT`；
2. 在 Go 里直接 mmap 一块不在 Go 堆上的内存，自己做分级管理，不需要 cgo，前面仍然用现在的池；
3. 真要接 jemalloc，前面也要挡一层 Go 侧的每 P 缓存，用来分摊 cgo 调用的开销。

## 附：为什么不改成传递包装对象

另一个思路是参考 [valyala/bytebufferpool](https://github.com/valyala/bytebufferpool)，改成传递 `*ByteBuffer`。结论是不划算：

- **GC 方面没有收益。** bytebufferpool 要避免的是 `sync.Pool.Put([]byte)` 时切片头被装箱，fib 存 `*byte` 已经避免了。包装对象反而多一个含指针的堆对象，GC 要扫描它。
- **性能更差。** 在混合尺寸的并行负载下，按 bytebufferpool 设计重写的单池比 fib 慢 5–8 倍：共享计数器有争用，没有分级，超过 maxSize 的缓冲区会被丢弃。在 fib 的分级池外面包一层 `*Buffer`，每对 Get+Put 多约 1.4–2 ns。
- **包装对象真正的价值在于能带额外字段**：owner 指针（Put 能回到正确的分配器）、引用计数（零拷贝广播）、元数据。这些需求出现时，可以在相应路径上局部引入带 owner 的包装类型，不必改动全部调用点。
