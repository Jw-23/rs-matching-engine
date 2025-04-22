# Rust 撮合引擎

纯 Rust 实现的高性能限价订单簿与撮合引擎，适用于高频交易场景。

## 架构

```
┌─────────────────────────────────────────────┐
│           TcpOrderServer                    │
│   (文本协议: BUY / SELL / SUBSCRIBE)        │
└──────────────────┬──────────────────────────┘
                   │ orders
                   ▼
┌─────────────────────────────────────────────┐
│            Worker (N 线程)                   │
│  MPMC Channel → FlatOrderBook 撮合          │
└──────────────────┬──────────────────────────┘
                   │ trades
                   ▼
┌─────────────────────────────────────────────┐
│         Trade 广播 (fan-out)                 │
│   → 订阅的客户端收到 TRADE 消息              │
└─────────────────────────────────────────────┘
```

### 核心组件

- **FlatOrderBook** — 基于扁平数组的价格档位订单簿（无树/堆结构）。每个价格档位用预分配的 `OrderPool` 中双向链表存储订单。`PriceBitset` 追踪活跃档位，支持 O(1) 最优买卖价查找和快速档位遍历。
- **StandardMatchingStrategy** — taker 订单撮合逻辑，支持限价单和市价单。按价格-时间优先遍历 maker 档位，处理部分成交和多档位穿透。
- **OrderWorker** — 工作线程循环：通过 MPMC channel 接收订单，运行撮合，批量广播成交记录。
- **TcpOrderServer** — Tokio 异步 TCP 服务器，接收简单文本指令（`BUY`、`SELL`、`SUBSCRIBE`），按 `symbol_id % worker_count` 分发订单到各 worker。
- **MPMC channel** — 自研无锁多生产者多消费者 channel，零分配 `spin_loop` 轮询方式从 channel 取任务。

### 数据结构

| 组件 | 结构 | 说明 |
|---|---|---|
| 价格档位 | `Vec<FlatLevel>` | 扁平淡化数组，下标 `(price - min_price) / tick` |
| 同档位订单 | 双向链表 via `OrderPool` | FIFO 队列，`NonZeroU32` 索引 |
| 活跃价格集合 | `PriceBitset` (u64 位图) | CTZ/CLZ 指令，O(1) 查找下一个活跃档位 |
| 订单 ID 查找 | `HashMap<OrderId, OrderLocation>` | 映射订单 ID → (pool 索引, 价格) |

## 编译

```bash
cargo build --release
```

默认启用 `std-net`（标准库线程）。可切换为 `tokio-net`（异步 I/O）：

```toml
rs-matching-engine = { features = ["tokio-net"] }
```

## 运行

```bash
cargo run --release
# 服务器监听 127.0.0.1:8080
```

## 通信协议

每行为一条指令，换行符分隔，响应以 `\n` 结尾。

```
BUY   <symbol_id> <quantity> <price> [client_order_id]
SELL  <symbol_id> <quantity> <price> [client_order_id]
SUBSCRIBE <symbol_id>
```

**响应：**

```
ORDER_ACCEPTED_ASYNC <order_id>
SUBSCRIBED <symbol_id>
TRADE <symbol_id> <price> <quantity>
ERROR <message>
```

## 测试

```bash
cargo test
# 性能测试（默认跳过）：
cargo test -- --ignored
```

## 性能测试结果

测试环境：macOS，Apple Silicon（M3 Pro），release 构建。

### cpp_like_bench（最大吞吐量）

5 线程并发提交，各 1000 万订单（随机买卖方向和价格），10 轮测试：

| 轮次 |  吞吐量 (orders/s) |
|------|-------------------|
| 1 | 0.90 亿（预热） |
| 2 | 2.49 亿 |
| 3 | 2.39 亿 |
| 4 | 2.75 亿 |
| 5 | **2.92 亿** |
| 6  | 2.81 亿 |
| 7 | 2.83 亿 |
| 8  | 2.77 亿 |
| 9  | 2.87 亿 |
| 10  | 2.75 亿 |

- **平均吞吐量：2.55 亿 orders/秒**
- **最佳吞吐量：2.92 亿 orders/秒**
- **背压：0 ns/order**（channel 无阻塞）

### bench_order_book（单/多 Worker 吞吐量）

| 测试场景 | 吞吐量 |
|----------|--------|
| 100万订单 / 单品种 / 1 Worker | ~10 Mops/s |
| 50万订单 / 4品种 / 4 Worker | ~5.7 Mops/s |

单笔延迟（P50 < 1 ns，P99 ~42 ns）。

## 项目结构

```
src/
├── lib.rs          # 公开导出
├── order.rs        # Order、Side、OrderType、Price 类型
├── order_book.rs   # FlatOrderBook、OrderPool、PriceBitset、OrderBook trait
├── strategy.rs     # StandardMatchingStrategy
├── worker.rs       # OrderWorker、Worker trait
├── tcp_server.rs   # TcpOrderServer、WorkerRuntime、start_order_workers
├── server.rs       # TcpListener/TcpConnection trait（std-net / tokio-net）
├── mpmc.rs         # 无锁 MPMC channel
├── error.rs        # EgineError
├── bitmap.rs       # PriceBitset 实现
├── buffer.rs       # Buffer 工具
├── sheduler.rs    # 调度工具
├── unique_id.rs   # ID 生成
└── main.rs         # 入口，绑定 127.0.0.1:8080
```
