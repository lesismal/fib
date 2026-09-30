# taskpool 性能对比

[English](README.md) | [简体中文](README.zh-CN.md)

在相同场景下对比 fib 的协程池和其他协程池。本目录是独立的 Go module
（`github.com/lesismal/fib/taskpool/benchmark`，用 `replace` 指向上两级的 fib），
所以被对比的这些库不会成为 fib 本身的依赖。

## 协程池

| 名称 | 实现 | 设置 |
|---|---|---|
| `nbio` | `github.com/lesismal/nbio/taskpool` v1.7.0 | `New(上限, 100000)` |
| `ants` | `github.com/panjf2000/ants/v2` v2.12.1 | `NewPool(上限)`，默认配置：worker 全忙时 `Submit` 阻塞 |
| `gopool` | `github.com/bytedance/gopkg/util/gopool` v0.1.4 | `NewPool(name, 上限, NewConfig())`，任务链表不限长 |
| `fnet` | `github.com/linfeip/fnet/pool` (b3e61d2) | `New(Config{MaxWorkers: 上限})`，其余默认：按核分片、队列不限长、空闲 5s 退出 |
| `fib-adaptive` | `taskpool.ModeAdaptive` | 下限 0，队列 100000 |
| `fib-adaptive-chan` | `taskpool.ModeAdaptiveChan` | 下限 0，队列 100000 |
| `fib-elastic` | `taskpool.ModeElastic` | 队列 100000 |

所有池的 worker 上限相同，都是 fib 的默认值：每个 P 1000 个。

## 场景

| 名称 | 测什么 |
|---|---|
| `Handoff` | 单个提交者，提交一个等一个：轻载服务器上的延迟 |
| `ParallelTiny` | 每个 P 同时提交空任务：池自身的争用 |
| `LoopCPUShort` | GOMAXPROCS/4 个提交者（模拟 event loop）投递约 50 轮计算的任务 |
| `LoopCPULong` | 同上，约 2000 轮计算 |
| `LoopBlocking` | 同上，任务 sleep 1ms：池扩展到大量 worker 的速度 |
| `LoopMixed` | 十分之一的任务 sleep 500µs，其余计算 500 轮 |
| `Bursts` | 每轮 256 个短任务，轮间空闲 2ms（不计时）：热 worker 的复用 |

每项 benchmark 名为 `BenchmarkPools/<场景>/<池>`，报告 `ns/op` 和 `cpu-ns/op`
（每个任务消耗的进程 CPU 时间；Linux/macOS 用 getrusage，Windows 用 GetProcessTimes）。

## 运行

Linux、macOS：

```bash
./bench.sh                                          # 全部场景和池，各跑 1 次
./bench.sh -s Handoff,Bursts -p nbio,fib-elastic -c 3
./bench.sh -cpu 4,8 -s LoopBlocking                 # 每个 GOMAXPROCS 一行
./bench.sh -docker -cpus 8                          # 在 golang:1.27 Linux 容器里跑
./bench.sh -l                                       # 列出场景和池
./bench.sh -h
```

Windows（PowerShell）：

```powershell
powershell -ExecutionPolicy Bypass -File bench.ps1
.\bench.ps1 -Scenarios Handoff,Bursts -Pools nbio,fib-elastic -Count 3
.\bench.ps1 -Cpu 4,8 -Scenarios LoopBlocking
.\bench.ps1 -List
```

`go test` 的原始输出写到 `results/<os>-<arch>-<时间>.txt`（或 `-o` / `-Out`
指定的文件），之后输出一张 Markdown 汇总表：中位数 `ns/op / cpu-ns/op`，每个场景
最快的池加粗，多次运行波动超过 1.3 倍的格子标 `*`。重新汇总已有结果：

```bash
go run ./summarize results/*.txt
```

全部场景和池各跑 1 次大约需要 3 分钟；用 `-c` / `-Count` 多跑几次，中位数和 `*`
波动标记才有意义。运行时尽量不要有其他负载，阻塞类和突发类
场景对干扰最敏感。
