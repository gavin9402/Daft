# Daft × Celeborn Shuffle 集成需求文档

## 一、背景与目标

### 1.1 背景

Daft 当前的分布式 shuffle 支持两种后端：

- **Ray Object Store**：存在 pickle 序列化开销、GCS 单点元数据压力（10TB+ shuffle 导致 head 节点 OOM）、无法做流式/零拷贝优化
- **Flight Shuffle**：基于 Arrow Flight + 本地 NVMe，解决了序列化和 GCS 问题，但存在架构层面的局限性。具体实现如下：

  **Map 端（写入）**：每个 Map Task 创建一个 `InProgressShuffleCache`，内部为 N 个目标分区各启动一个独立的 IPC Writer（通过 `make_ipc_writer` 创建）。每条输入数据先按分区键 hash 拆分为 N 份，然后分别发送到对应分区的 Writer。每个 Writer 将数据以 Arrow IPC 格式写入本地磁盘的独立目录（`{shuffle_dir}/daft_shuffle/{shuffle_id}/{cache_id}/partition_{idx}/`），并按 `target_filesize`（默认 8~128MB，取决于分区数）自动切分为多个文件。

  **Reduce 端（读取）**：每个 Reduce Task 通过 Arrow Flight 协议，从所有 Worker 的本地 Flight Server 拉取属于自己分区的数据。`ShuffleReadSource` 区分本地读取（直接从 `ShuffleFlightServer` 的内存缓存读取）和远程读取（通过 `FlightClientManager` 发起 gRPC 请求），两者的结果流合并后输出。

  **Flight Shuffle 架构图示**（M=3 个 Map Task，N=4 个 Reduce 分区）：

  ```
  ┌─── Map 阶段：每个 Map Task 在本地磁盘写 N 个分区目录 ───────────────────────┐
  │                                                                              │
  │  Worker-0 (Map Task 0)              Worker-1 (Map Task 1)                    │
  │  ┌─────────────────────┐            ┌─────────────────────┐                  │
  │  │ InProgressShuffle   │            │ InProgressShuffle   │                  │
  │  │ Cache               │            │ Cache               │                  │
  │  │                     │            │                     │                  │
  │  │ input ──hash──┬──►W0│            │ input ──hash──┬──►W0│                  │
  │  │               ├──►W1│            │               ├──►W1│   Worker-2       │
  │  │               ├──►W2│            │               ├──►W2│   (Map Task 2)   │
  │  │               └──►W3│            │               └──►W3│   同样结构...    │
  │  └───────┬─────────────┘            └───────┬─────────────┘                  │
  │          ▼ 本地 NVMe                        ▼ 本地 NVMe                      │
  │  partition_0/                        partition_0/                             │
  │    ├─ 0001.ipc (≤128MB)                ├─ 0001.ipc                           │
  │    └─ 0002.ipc                         └─ 0002.ipc                           │
  │  partition_1/                        partition_1/                             │
  │    └─ 0001.ipc                         └─ 0001.ipc                           │
  │  partition_2/                        partition_2/                             │
  │    └─ 0001.ipc                         └─ 0001.ipc                           │
  │  partition_3/                        partition_3/                             │
  │    └─ 0001.ipc                         └─ 0001.ipc                           │
  │                                                                              │
  │  共 M×N = 3×4 = 12 个分区目录，每个目录下可能多个 IPC 文件                    │
  └──────────────────────────────────────────────────────────────────────────────┘

  ┌─── Reduce 阶段：每个 Reduce Task 从所有 Worker 拉取数据 ────────────────────┐
  │                                                                              │
  │  Reduce Task 0 (读 partition_0)                                              │
  │  ┌──────────────────────────────────────────────────────────┐                 │
  │  │              ┌──── gRPC ────► Worker-0 FlightServer ──► 本地 partition_0  │
  │  │  ShuffleRead ├──── gRPC ────► Worker-1 FlightServer ──► 本地 partition_0  │
  │  │  Source      └──── gRPC ────► Worker-2 FlightServer ──► 本地 partition_0  │
  │  │              合并 3 个流 ──► 输出                                          │
  │  └──────────────────────────────────────────────────────────┘                 │
  │                                                                              │
  │  Reduce Task 1 (读 partition_1)  ── 同样从 3 个 Worker 各拉一次              │
  │  Reduce Task 2 (读 partition_2)  ── 同样从 3 个 Worker 各拉一次              │
  │  Reduce Task 3 (读 partition_3)  ── 同样从 3 个 Worker 各拉一次              │
  │                                                                              │
  │  共 N×M = 4×3 = 12 个 gRPC 连接                                              │
  └──────────────────────────────────────────────────────────────────────────────┘
  ```

  **核心问题**：
  - **M×N 小文件问题**：M 个 Map Task × N 个 Reduce 分区 = M×N 个分区目录，每个目录下还可能有多个 IPC 文件。例如 1000 Map × 2000 Reduce = 200 万个目录，文件数更多，对文件系统造成巨大压力
  - **M×N 连接数问题**：Reduce 阶段每个 Reduce Task 需要从所有 M 个 Worker 的 Flight Server 拉取数据，N 个 Reduce Task × M 个 Worker = M×N 个 gRPC 连接，连接数爆炸
  - **随机读问题**：Reduce 端需要从每个 Worker 的本地磁盘上读取属于特定分区的文件，这些文件分散在不同目录下，导致大量随机 I/O
  - **计算存储耦合**：shuffle 数据存储在计算节点的本地 NVMe 上，节点故障则数据丢失
  - **无容错能力**：Task Lineage & Fault Tolerance 全部未实现，任何节点故障都需要重新计算整个 shuffle
  - **不适配云原生**：依赖本地 NVMe 磁盘，不适配无本地盘的云原生环境（如 Kubernetes spot instances）

### 1.2 目标

引入 Apache Celeborn 作为第三种 shuffle 后端，实现 **计算存储解耦、天然容错、云原生适配**，同时省去 Daft 自建 Task Lineage 容错机制的开发工作。

---

## 二、Daft Shuffle 现有接口全景

### 2.1 架构分层

```
┌─────────────────────────────────────────────────────────────┐
│                    配置层 (daft-config)                       │
│  shuffle_algorithm: "auto"|"map_reduce"|"pre_shuffle_merge"  │
│                     |"flight_shuffle"|"celeborn"(新增)        │
├─────────────────────────────────────────────────────────────┤
│                 调度层 (translate_shuffle.rs)                 │
│  gen_repartition_node() → 选择后端 + 是否预合并               │
├─────────────────────────────────────────────────────────────┤
│              分布式层 (daft-distributed)                      │
│  ShuffleBackend → build_write_stage() / emit_read_tasks()   │
├─────────────────────────────────────────────────────────────┤
│              本地执行层 (daft-local-execution)                │
│  RepartitionSink (写入) / ShuffleReadSource (读取)           │
├─────────────────────────────────────────────────────────────┤
│                    元数据层                                   │
│  ShuffleMetadata / ShufflePartitionRef                       │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Shuffle 算法决策树

```
用户设置 shuffle_algorithm
         │
         ├─ "auto" (默认)
         │     后端: Ray
         │     预合并: √(N×M) > 200 ?
         │     ├─ 是 → Child → PreShuffleMergeNode → RepartitionNode(Ray)
         │     └─ 否 → Child → RepartitionNode(Ray)
         │
         ├─ "map_reduce"
         │     后端: Ray
         │     预合并: ❌
         │     执行链: Child → RepartitionNode(Ray)
         │
         ├─ "pre_shuffle_merge"
         │     后端: Ray
         │     预合并: ✅
         │     执行链: Child → PreShuffleMergeNode → RepartitionNode(Ray)
         │
         ├─ "flight_shuffle"
         │     后端: Flight
         │     预合并: ❌
         │     执行链: Child → RepartitionNode(Flight)
         │
         └─ "celeborn" (新增)
               后端: Celeborn
               预合并: ❌ (Celeborn 服务端自带聚合)
               执行链: Child → RepartitionNode(Celeborn)
```

### 2.3 配置层接口

**现有配置项：**

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|---------|------|
| `shuffle_algorithm` | String | `"auto"` | `DAFT_SHUFFLE_ALGORITHM` | 算法选择 |
| `shuffle_aggregation_default_partitions` | usize | `200` | — | 聚合默认分区数 |
| `pre_shuffle_merge_threshold` | usize | `1GB` | — | 预合并字节阈值 |
| `pre_shuffle_merge_partition_threshold` | usize | `200` | — | 预合并分区数阈值 |
| `flight_shuffle_dirs` | Vec\<String\> | `["/tmp"]` | — | Flight 写入目录 |

**Celeborn 需新增的配置项：**

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `celeborn_master_endpoints` | Option\<String\> | None | Celeborn Master 地址，如 `"host1:9097,host2:9097"` |
| `celeborn_shuffle_partition_type` | Option\<String\> | `"hash"` | 分区类型（hash/range） |
| `celeborn_push_data_timeout_ms` | Option\<usize\> | `120000` | pushData 超时时间 |
| `celeborn_fetch_data_timeout_ms` | Option\<usize\> | `120000` | fetchData 超时时间 |
| `celeborn_compression` | Option\<String\> | `"lz4"` | 压缩算法（lz4/zstd/none） |
| `celeborn_storage_level` | Option\<String\> | `"disk"` | 存储级别（memory/disk/hdfs） |

### 2.4 分布式层接口

**DistributedShuffleBackend 枚举**（需新增 Celeborn 变体）：

```rust
// 现有
pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightShuffleBackendConfig),
}

// 新增
pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightShuffleBackendConfig),
    Celeborn(CelebornShuffleBackendConfig),  // ← 新增
}
```

**CelebornShuffleBackendConfig 需包含的字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `shuffle_id` | u64 | 本次 shuffle 的唯一标识 |
| `master_endpoints` | String | Celeborn Master 地址 |
| `compression` | Option\<String\> | 压缩算法 |
| `num_mappers` | usize | Map 任务总数 |
| `num_partitions` | usize | 目标分区数 |

**ShuffleBackend 核心方法签名**（Celeborn 需实现）：

```rust
// 构建写入阶段
pub(crate) fn build_write_stage(&self, config: ShuffleBackendWriteConfig) -> TaskBuilderStream

// 发出读取任务
pub(crate) async fn emit_read_tasks(
    &self,
    read_spec: ShuffleBackendReadSpec,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()>

// 注册清理
pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext)
```

#### 三个核心方法的职责

**`build_write_stage(config) → TaskBuilderStream`**

Shuffle 的 **Map 阶段入口**。职责是将上游子节点产出的数据流（`config.input_node`）接入一个 repartition_write 算子，使每个 Map Task 在本地执行分区 + 写入操作。具体流程：

1. 从 `config.input_node`（上游的 `TaskBuilderStream`）中取出每个 Task 的执行计划
2. 在每个 Task 的执行计划末尾追加一个 `LocalPhysicalPlan::repartition_write` 节点
3. 该节点在本地执行时会创建 `RepartitionSink`，根据 `config.repartition_spec` 对数据做 hash/random/range 分区，然后通过 `config.backend` 指定的后端写出
4. 返回新的 `TaskBuilderStream`，其中每个 Task 的输出是 `ShuffleMetadata`（各分区的行数、字节数等元信息）

对于 Celeborn 后端：`config.backend` 将是 `RepartitionWriteBackend::Celeborn { ... }`，本地执行时 `RepartitionSink` 会通过 FFI 调用 Celeborn C++ Client 的 `push_data()` 将分区数据推送到 Celeborn Worker。

**`emit_read_tasks(read_spec, node, result_tx) → DaftResult<()>`**

Shuffle 的 **Reduce 阶段入口**。职责是根据 Map 阶段产出的元数据，为每个 Reduce 分区生成一个读取任务，并通过 `result_tx` 发送出去。具体流程：

1. 从 `read_spec` 中获取 Map 阶段的输出元数据（分区位置信息）
2. 为每个目标分区创建一个 `LocalPhysicalPlan::shuffle_read` 执行计划
3. 将执行计划包装成 `SwordfishTaskBuilder`，通过 `result_tx` 发送给调度器
4. 调度器将这些 Task 分发到各 Worker 上执行

对于 Celeborn 后端：`read_spec` 将是 `ShuffleBackendReadSpec::Celeborn { shuffle_id, num_partitions }`，每个 Reduce Task 在本地执行时通过 FFI 调用 Celeborn C++ Client 的 `read_partition()` 拉取数据。

**`register_cleanup(plan_context)`**

注册 shuffle 数据的 **清理回调**。在整个查询执行完毕后，由 `PlanExecutionContext` 统一触发清理。

- Ray 后端：无需清理（Ray Object Store 自动 GC）
- Flight 后端：清理本地 NVMe 上的 shuffle 目录（`{shuffle_dir}/daft_shuffle/{shuffle_id}/`）
- Celeborn 后端（新增）：调用 `unregister_shuffle(shuffle_id)` 通知 Celeborn 释放该 shuffle 的存储资源

#### 核心类型说明

| 类型 | 定义位置 | 说明 |
|------|---------|------|
| **`TaskBuilderStream`** | `pipeline_node/mod.rs` | 对 `BoxStream<'static, SwordfishTaskBuilder>` 的封装。代表一个 **Task 构建器的异步流**——流中的每个元素是一个待构建的分布式 Task。上游节点通过 `produce_tasks()` 产出 `TaskBuilderStream`，下游节点消费它、追加算子后再产出新的 `TaskBuilderStream`，形成 pipeline 链。最终通过 `materialize()` 方法将流中的 Task 提交给调度器执行 |
| **`SwordfishTaskBuilder`** | `scheduling/task.rs` | 分布式 Task 的构建器。封装了一个 `LocalPhysicalPlanRef`（本地执行计划）+ 输入数据引用（`psets`/`inputs`）+ 调度策略（`SchedulingStrategy`）+ 上下文元数据。通过 `build()` 方法生成最终的 `SwordfishTask` 提交给调度器。可以链式调用 `.with_psets()` / `.with_flight_shuffle_reads()` 等方法附加输入数据 |
| **`PipelineNodeImpl`** | `pipeline_node/mod.rs` | 分布式 pipeline 节点的核心 trait（`Send + Sync`）。每个节点必须实现 `produce_tasks()` 方法，返回 `TaskBuilderStream`。`RepartitionNode` 就是实现了此 trait 的 shuffle 节点。`ShuffleBackend` 的方法中用 `&dyn PipelineNodeImpl` 来获取节点的配置和上下文信息 |
| **`RepartitionWriteBackend`** | `daft-local-plan/plan.rs` | 枚举，标识 **本地执行层** 使用哪种后端写入 shuffle 数据。当前有 `Ray` 和 `Flight { shuffle_id, shuffle_dirs, compression }`。它被序列化到 `LocalPhysicalPlan` 中，随 Task 发送到 Worker 上执行。Celeborn 需新增 `Celeborn { shuffle_id, master_endpoints, ... }` 变体 |
| **`RepartitionSpec`** | `daft-logical-plan` | 枚举，描述分区策略：`Hash { by: Vec<Expr> }`（按表达式 hash）、`Random { seed }`（随机分区）、`Range { by, boundaries, descending }`（范围分区）。决定 Map 端如何将数据拆分到 N 个目标分区 |
| **`Sender<SwordfishTaskBuilder>`** | `utils/channel.rs` | `tokio::sync::mpsc::Sender<SwordfishTaskBuilder>` 的类型别名。异步 channel 的发送端，用于将构建好的 Task 发送给 `RepartitionNode` 的 `execution_loop`，再由调度器分发执行 |
| **`DaftResult<()>`** | `common-error` | Daft 统一的 Result 类型（`Result<T, DaftError>`）。所有可能失败的操作都返回此类型，支持 `?` 操作符链式传播错误 |
| **`ShuffleBackendReadSpec`** | `backends/mod.rs` | 枚举，承载 Map→Reduce 之间的 **分区位置元数据**。Ray 变体包含 `Vec<Vec<PartitionRef>>`（每个 Reduce 分区对应的 Ray Object 引用列表）；Flight 变体包含 `HashMap<String, Vec<u32>>`（Worker 地址 → cache ID 列表的映射）。Celeborn 需新增变体，包含 `shuffle_id` 和 `num_partitions` |
| **`PlanExecutionContext`** | `plan.rs` | 查询执行的全局上下文，管理 Task ID 分配、调度器句柄、清理回调等。`register_cleanup` 通过它注册 shuffle 结束后的资源释放逻辑 |

#### 调用时序（以 RepartitionNode 为例）

```
RepartitionNode::produce_tasks(plan_context)
│
├─ 1. register_cleanup(plan_context)          // 注册清理回调
│
├─ 2. build_write_stage(config)               // 构建 Map 阶段
│     └─ 返回 TaskBuilderStream (Map Tasks)
│
├─ 3. execution_loop(...)                     // 异步执行循环
│     ├─ 3a. 提交 Map Tasks → 调度器 → Worker 执行
│     ├─ 3b. 收集所有 Map Task 的输出 (ShuffleMetadata)
│     ├─ 3c. 从输出中构建 ShuffleBackendReadSpec
│     └─ 3d. emit_read_tasks(read_spec, ...)  // 构建 Reduce 阶段
│           └─ 为每个 Reduce 分区发送一个 SwordfishTaskBuilder
│
└─ 4. 返回 TaskBuilderStream (Reduce Tasks)
```

**ShuffleBackendReadSpec 枚举**（需新增 Celeborn 变体）：

```rust
pub(crate) enum ShuffleBackendReadSpec {
    Ray {
        partition_groups: Vec<Vec<PartitionRef>>,
    },
    Flight {
        server_cache_mapping: HashMap<String, Vec<u32>>,
    },
    Celeborn {                              // ← 新增
        shuffle_id: u64,
        num_partitions: usize,
    },
}
```

### 2.5 写入层接口（Map 阶段）

**RepartitionSink 的 sink() 方法：**

```
输入: MicroPartition (一批表格数据)
处理: 根据 RepartitionSpec 分区
      ├─ Hash:   partition_by_hash(&bound_exprs, num_partitions)
      ├─ Random: partition_by_random(num_partitions, seed)
      └─ Range:  partition_by_range(&by, &boundaries, &descending)
输出: 分区后的 Vec<MicroPartition>，每个元素对应一个目标分区
```

**RepartitionSink 的 finalize() 方法：**

| 后端 | finalize 行为 | 输出 |
|------|-------------|------|
| **Ray** | 合并同分区数据 → `ray.put()` → 返回 ObjectRef | `ShuffleMetadata { partitions: [{ object_ref, num_rows, size_bytes }] }` |
| **Flight** | 关闭 cache → 注册到 Flight Server | `ShuffleMetadata { partitions: [{ num_rows, size_bytes }] }` |
| **Celeborn (新增)** | 调用 `mapperEnd()` 标记完成 | `ShuffleMetadata { partitions: [{ num_rows, size_bytes }] }` |

**Celeborn 写入层需实现的数据流：**

```
MicroPartition
    │
    ▼ partition_by_hash / partition_by_random
Vec<MicroPartition>  (num_partitions 个)
    │
    ▼ 对每个 SubPart_i
MicroPartition → Arrow IPC serialize → bytes
    │
    ▼ Rust FFI 调用 Celeborn C++ Client
celeborn_client.push_data(
    shuffle_id,
    map_id,
    attempt_id,
    partition_idx,
    data_ptr,       // Arrow IPC bytes 指针
    data_len,       // 数据长度
    num_mappers,
    num_partitions
)
    │
    ▼ finalize
celeborn_client.mapper_end(shuffle_id, map_id, attempt_id)
    │
    ▼ 返回
ShuffleMetadata { partitions: [...] }
```

### 2.6 读取层接口（Reduce 阶段）

**现有 ShuffleReadSource 字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `receiver` | UnboundedReceiver | 接收读取任务 |
| `shuffle_id` | u64 | shuffle 标识 |
| `local_cache_ids` | Option\<Vec\<u32\>\> | 本地缓存 ID（Flight 专用） |
| `remote_cache_mapping` | HashMap\<String, Vec\<u32\>\> | 远程服务器→缓存映射（Flight 专用） |
| `local_server` | Arc\<ShuffleFlightServer\> | 本地 Flight Server（Flight 专用） |
| `schema` | SchemaRef | 数据 schema |
| `num_parallel_tasks` | usize | 并行读取任务数 |

**Celeborn 读取层需实现的数据流：**

```
celeborn_client.read_partition(shuffle_id, partition_idx)  // Rust FFI → C++ Client
    │
    ▼ 返回 bytes 缓冲区（零拷贝）
bytes → Arrow IPC deserialize → RecordBatch
    │
    ▼ 封装
MicroPartition::new_loaded(schema, vec![record_batch], None)
    │
    ▼ 发送到下游
sender.send(PipelineMessage::Morsel { input_id, partition })
```

**Celeborn 读取层调用时序（对比 Flight 现有实现）：**

Flight 现有实现中，`ShuffleReadSource` 通过 `spawn_flight_shuffle_processor` 启动一个异步处理循环，
从 `receiver` 接收读取任务，然后并行地从本地/远程 FlightServer 拉取数据。Celeborn 需实现类似的模式，
但数据源从 FlightServer 替换为 Celeborn C++ Client：

```
┌─── Flight 现有读取时序 ──────────────────────────────────────────────────────┐
│                                                                              │
│  ShuffleReadSource::get_data()                                               │
│  │                                                                           │
│  ├─ create_channel(output_sender, output_receiver)                           │
│  │                                                                           │
│  ├─ spawn_flight_shuffle_processor(output_sender)  // io_runtime 上的异步任务 │
│  │   │                                                                       │
│  │   ├─ 创建 FlightClientManager (管理 gRPC 连接池)                          │
│  │   │                                                                       │
│  │   ├─ loop: 从 receiver 接收 (input_id, FlightShuffleReadInput)            │
│  │   │   │                                                                   │
│  │   │   ├─ get_partition_stream(partition_idx)                               │
│  │   │   │   ├─ 本地: local_server.get_partition_local(shuffle_id, idx, ids)  │
│  │   │   │   └─ 远程: client_manager.fetch_partition(shuffle_id, idx, map)   │
│  │   │   │         └─ gRPC → 远端 Worker FlightServer → 读本地 IPC 文件      │
│  │   │   │                                                                   │
│  │   │   └─ spawn forward_partition_stream(stream, sender, input_id)         │
│  │   │       └─ loop: stream.next() → RecordBatch                            │
│  │   │           → MicroPartition::new_loaded(schema, vec![batch], None)      │
│  │   │           → sender.send(PipelineMessage::Morsel { input_id, mp })     │
│  │   │                                                                       │
│  │   └─ 所有 partition 完成后发送 PipelineMessage::Flush(input_id)           │
│  │                                                                           │
│  └─ 返回 output_receiver.into_stream() 作为 SourceStream                     │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌─── Celeborn 读取时序（需新增实现）────────────────────────────────────────────┐
│                                                                              │
│  CelebornShuffleReadSource::get_data()                                       │
│  │                                                                           │
│  ├─ create_channel(output_sender, output_receiver)                           │
│  │                                                                           │
│  ├─ spawn_celeborn_shuffle_processor(output_sender)  // io_runtime 异步任务  │
│  │   │                                                                       │
│  │   ├─ 创建/获取 CelebornShuffleClient (FFI → C++ Client, 连接 Celeborn)   │
│  │   │                                                                       │
│  │   ├─ loop: 从 receiver 接收 (input_id, CelebornShuffleReadInput)          │
│  │   │   │                                                                   │
│  │   │   ├─ celeborn_client.read_partition(shuffle_id, partition_idx)         │
│  │   │   │   └─ Rust FFI → C++ Client → Celeborn Worker                     │
│  │   │   │       └─ 返回该 partition 的全部数据 (bytes)                       │
│  │   │   │           (Celeborn 服务端已将 M 个 Map 的数据聚合为一份)          │
│  │   │   │                                                                   │
│  │   │   ├─ Arrow IPC deserialize: bytes → Vec<RecordBatch>                  │
│  │   │   │                                                                   │
│  │   │   ├─ for each batch:                                                  │
│  │   │   │   MicroPartition::new_loaded(schema, vec![batch], None)            │
│  │   │   │   → sender.send(PipelineMessage::Morsel { input_id, mp })         │
│  │   │   │                                                                   │
│  │   │   └─ sender.send(PipelineMessage::Flush(input_id))                    │
│  │   │                                                                       │
│  │   └─ 所有 partition 完成                                                  │
│  │                                                                           │
│  └─ 返回 output_receiver.into_stream() 作为 SourceStream                     │
│                                                                              │
│  关键差异:                                                                    │
│  ┌────────────────────────────────────────────────────────────────────┐       │
│  │ Flight: 每个 Reduce 需连接 M 个 Worker → M 个 gRPC 连接          │       │
│  │ Celeborn: 每个 Reduce 只需连接 Celeborn → 1 个连接               │       │
│  │                                                                    │       │
│  │ Flight: 数据分散在 M 个 Worker 本地磁盘 → 随机读                 │       │
│  │ Celeborn: 数据已在服务端按 partition 聚合 → 顺序读               │       │
│  │                                                                    │       │
│  │ Flight: Worker 挂掉 → 数据丢失 → 整个 shuffle 重算              │       │
│  │ Celeborn: Worker 挂掉 → 数据在 Celeborn → 不受影响              │       │
│  └────────────────────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.7 元数据层接口

**ShufflePartitionRef 枚举**（需新增 Celeborn 变体）：

```rust
pub(crate) enum ShufflePartitionRef {
    Ray(PartitionRef),
    Flight(FlightShufflePartitionRef),
    Celeborn(CelebornShufflePartitionRef),  // ← 新增
}
```

**CelebornShufflePartitionRef 需包含的字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `shuffle_id` | u64 | shuffle 标识 |
| `partition_id` | usize | 分区索引 |
| `map_id` | usize | 产生该分区的 map 任务 ID |
| `attempt_id` | usize | 尝试次数 |
| `num_rows` | usize | 行数 |
| `size_bytes` | usize | 字节数 |

**需新增的分组函数：**

```rust
// partition_groups.rs
pub(crate) fn celeborn_read_spec_from_outputs(
    outputs: Vec<TaskOutput>,
    shuffle_id: u64,
    num_partitions: usize,
) -> DaftResult<ShuffleBackendReadSpec>
```

### 2.8 RepartitionSpec（分区策略）

Celeborn 需支持的分区策略：

| 策略 | 参数 | Celeborn 支持 |
|------|------|-------------|
| **Hash** | `num_partitions: Option<usize>`, `by: Vec<ExprRef>` | ✅ |
| **Random** | `num_partitions: Option<usize>`, `seed: Option<u64>` | ✅ |
| **Range** | `num_partitions`, `boundaries`, `by`, `descending` | ❌ 不支持（与 Flight 一致） |

---

## 三、Celeborn 侧需要对接的 API

### 3.1 写入 API

```cpp
// 核心写入接口（Celeborn C++ Client）
int CelebornClient::pushData(
    int shuffleId,          // shuffle 标识
    int mapId,              // map 任务 ID
    int attemptId,          // 尝试次数
    int partitionId,        // 目标分区 ID
    const char* data,       // 数据指针（Arrow IPC bytes）
    int length,             // 数据长度
    int numMappers,         // map 任务总数
    int numPartitions       // 目标分区总数
);

// 标记 mapper 完成
void CelebornClient::mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers);
```

### 3.2 读取 API

```cpp
// 读取分区数据（返回字节缓冲区）
std::unique_ptr<CelebornReadBuffer> CelebornClient::readPartition(
    int shuffleId,      // shuffle 标识
    int partitionId,    // 分区 ID
    int attemptId       // 尝试次数
);

// CelebornReadBuffer 提供数据访问
class CelebornReadBuffer {
    const char* data();     // 数据指针
    int size();             // 数据长度
};
```

### 3.3 生命周期 API

```cpp
// 创建 CelebornClient（每个 worker 一个，内部管理与 Master 的连接）
std::unique_ptr<CelebornClient> CelebornClient::create(
    const std::string& masterEndpoints,   // Master 地址列表，如 "host1:port1,host2:port2"
    const std::string& appId,             // 应用唯一标识
    const CelebornConf& conf              // 配置（压缩方式、存储级别等）
);

// 注册 shuffle（获取分区位置信息）
void CelebornClient::registerShuffle(
    int shuffleId,        // shuffle 标识
    int numMappers,       // map 任务总数
    int numPartitions     // 目标分区总数
);

// 清理 shuffle 数据
void CelebornClient::unregisterShuffle(int shuffleId);

// 关闭客户端
void CelebornClient::shutdown();
```

---

## 四、集成方案：C++ 客户端 + Rust FFI

### 4.1 方案选型

Celeborn 提供了 **Java 客户端** 和 **C++ 客户端**（Gluten/Velox 集成使用，已在生产环境验证）。

| 方案 | 路径 | 性能 | 工作量 | 可行性 |
|------|------|------|--------|--------|
| **A. Python/Java 桥接** | Rust → PyO3 → Python → Py4J → JVM → Celeborn | 🔴 差（三层跨语言） | 🟢 低 | ✅ 最稳妥 |
| **B. C++ 客户端 + FFI** ⭐ | Rust → FFI → Celeborn C++ Client → Celeborn | 🟢 好（一层 FFI） | 🟡 中 | ✅ C++ 客户端已存在 |
| **C. Rust 重写 Netty 协议** | Rust → 自实现 Netty 协议 → Celeborn | 🟢 最好（纯 Rust） | 🔴 极高 | ⚠️ 维护成本大 |

**推荐方案 B**，理由：
- Celeborn C++ 客户端已在 Gluten + Velox 生产环境中验证（参考 CELEBORN-2269）
- Rust 调用 C++ 通过 FFI（`cxx` crate 或 `extern "C"`）是成熟方案
- **完全绕过 Python 和 JVM**，消除跨语言桥接的性能损耗
- Daft 本身就是 Rust 项目，C++ FFI 比 Python/Java 桥接更自然
- 数据可以通过指针直接传递，实现真正的零拷贝

### 4.2 Rust FFI 层设计

**模块结构：**

```
src/daft-shuffles/src/
├── client/
│   ├── flight_client.rs        # 现有 Flight 客户端
│   └── celeborn_client.rs      # 新增：Celeborn C++ 客户端的 Rust FFI 封装
├── server/
│   └── flight_server.rs        # 现有 Flight 服务端
├── celeborn_ffi.rs             # 新增：C++ FFI 绑定定义
└── ...
```

**FFI 绑定接口（使用 `cxx` crate）：**

```rust
// src/daft-shuffles/src/celeborn_ffi.rs

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("celeborn/CelebornClient.h");

        type CelebornClient;

        // 创建客户端
        fn new_celeborn_client(
            master_endpoints: &str,
            app_id: &str,
            compression: &str,
        ) -> UniquePtr<CelebornClient>;

        // 推送分区数据
        fn push_data(
            self: &CelebornClient,
            shuffle_id: i64,
            map_id: i32,
            attempt_id: i32,
            partition_id: i32,
            data: &[u8],           // Arrow IPC bytes，零拷贝传递
            num_mappers: i32,
            num_partitions: i32,
        ) -> Result<()>;

        // 标记 mapper 完成
        fn mapper_end(
            self: &CelebornClient,
            shuffle_id: i64,
            map_id: i32,
            attempt_id: i32,
        ) -> Result<()>;

        // 读取分区数据
        fn read_partition(
            self: &CelebornClient,
            shuffle_id: i64,
            partition_id: i32,
        ) -> Result<Vec<u8>>;     // 返回 Arrow IPC bytes

        // 清理 shuffle 数据
        fn unregister_shuffle(
            self: &CelebornClient,
            shuffle_id: i64,
        ) -> Result<()>;

        // 关闭客户端
        fn shutdown(self: &CelebornClient) -> Result<()>;
    }
}
```

**Rust 安全封装：**

```rust
// src/daft-shuffles/src/client/celeborn_client.rs

pub struct CelebornShuffleClient {
    inner: cxx::UniquePtr<ffi::CelebornClient>,
}

impl CelebornShuffleClient {
    pub fn new(master_endpoints: &str, app_id: &str, compression: &str) -> DaftResult<Self> { ... }
    pub async fn push_data(&self, shuffle_id: u64, map_id: usize, ..., data: &[u8]) -> DaftResult<()> { ... }
    pub async fn mapper_end(&self, shuffle_id: u64, map_id: usize, attempt_id: usize) -> DaftResult<()> { ... }
    pub async fn read_partition(&self, shuffle_id: u64, partition_id: usize) -> DaftResult<Vec<u8>> { ... }
    pub fn unregister_shuffle(&self, shuffle_id: u64) -> DaftResult<()> { ... }
}
```

### 4.3 数据序列化

数据在 Rust 层直接完成 Arrow IPC 序列化/反序列化，**不经过 Python**：

```rust
// 写入：MicroPartition → Arrow IPC bytes
let ipc_bytes = micro_partition.to_ipc_bytes()?;  // Rust 原生 Arrow IPC 序列化
celeborn_client.push_data(shuffle_id, map_id, ..., &ipc_bytes)?;

// 读取：Arrow IPC bytes → RecordBatch
let ipc_bytes = celeborn_client.read_partition(shuffle_id, partition_id)?;
let record_batch = RecordBatch::from_ipc_bytes(&ipc_bytes, schema)?;
let mp = MicroPartition::new_loaded(schema, vec![record_batch], None);
```

---

## 五、完整数据流

```
═══ Map 阶段 ═══════════════════════════════════════════════

Worker A (Map Task 0)
┌──────────────────────────────────────────────────────┐
│ Input MicroPartition                                  │
│   ↓ partition_by_hash(exprs, num_partitions)          │
│ [SubPart_0, SubPart_1, ..., SubPart_M]               │
│   ↓ 对每个 SubPart_i                                  │
│ SubPart_i.to_ipc_bytes() → bytes  (Rust 原生序列化)   │
│   ↓ Rust FFI → C++ Client                            │
│ celeborn_client.push_data(                            │
│     shuffle_id, map_id=0, attempt_id=0,               │
│     partition_id=i, data=&bytes,                      │
│     num_mappers=N, num_partitions=M                   │
│ )                                                     │
│   ↓ finalize                                          │
│ celeborn_client.mapper_end(shuffle_id, map_id=0, 0)   │
│   ↓ 返回                                              │
│ ShuffleMetadata { partitions: [...] }                 │
└──────────────────────────────────────────────────────┘
         │
         ▼
═══ Celeborn Service ═══════════════════════════════════

┌──────────────────────────────────────────────────────┐
│ Celeborn Worker (ESS)                                 │
│ ┌─────────────────────────────────────────────┐      │
│ │ Partition 0: [Map0_data, Map1_data, ...]    │      │
│ │ Partition 1: [Map0_data, Map1_data, ...]    │      │
│ │ ...                                          │      │
│ │ Partition M: [Map0_data, Map1_data, ...]    │      │
│ └─────────────────────────────────────────────┘      │
│ 服务端按分区聚合，多副本容错                            │
└──────────────────────────────────────────────────────┘
         │
         ▼
═══ Reduce 阶段 ════════════════════════════════════════

Worker B (Reduce Task i)
┌──────────────────────────────────────────────────────┐
│ celeborn_client.read_partition(shuffle_id, i) → bytes │
│   ↓                                                   │
│ ipc_bytes_to_record_batches(bytes, schema)            │
│   ↓                                                   │
│ MicroPartition::new_loaded(schema, batches, None)     │
│   ↓                                                   │
│ 下游算子（GroupBy / Join / Sort ...）                  │
└──────────────────────────────────────────────────────┘
```

---

## 六、需修改的文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `src/common/daft-config/src/lib.rs` | 修改 | 新增 Celeborn 配置字段和默认值 |
| `src/common/daft-config/src/python.rs` | 修改 | 新增 `"celeborn"` 验证和 Celeborn 配置参数 |
| `src/daft-distributed/src/pipeline_node/shuffles/backends/mod.rs` | 修改 | 扩展 `DistributedShuffleBackend`、`ShuffleBackendReadSpec` 枚举 |
| `src/daft-distributed/src/pipeline_node/shuffles/backends/celeborn.rs` | **新建** | Celeborn 后端实现 |
| `src/daft-distributed/src/pipeline_node/shuffles/translate_shuffle.rs` | 修改 | 新增 `"celeborn"` 分支 |
| `src/daft-distributed/src/pipeline_node/shuffles/partition_groups.rs` | 修改 | 新增 `celeborn_read_spec_from_outputs()` |
| `src/daft-distributed/src/pipeline_node/mod.rs` | 修改 | 新增 `CelebornShufflePartitionRef`、扩展 `ShufflePartitionRef` |
| `src/daft-local-execution/src/sinks/repartition.rs` | 修改 | 扩展 `RepartitionBackend`、`RepartitionState`，实现 sink/finalize |
| `src/daft-local-execution/src/sources/shuffle_read.rs` | 修改 | 新增 Celeborn 读取分支 |
| `src/daft-local-plan/src/plan.rs` | 修改 | 扩展 `RepartitionWriteBackend` |
| `daft/shuffle/celeborn_client.py` | **新建** | Python Celeborn 客户端封装 |
| `daft/shuffle/celeborn_serde.py` | **新建** | Arrow IPC 序列化层 |
| `pyproject.toml` | 修改 | 新增 Celeborn 可选依赖 |

---

## 七、开发任务分解

### 第一阶段：Python 基础设施（2-3 周）

1. 封装 Celeborn Java 客户端（通过 Py4J/JPype 桥接），实现 pushData、readPartition、mapperEnd、lifecycle 管理等核心 API
2. 实现 MicroPartition ↔ Arrow IPC bytes 的序列化/反序列化转换层，避免 pickle 开销

### 第二阶段：Rust 核心扩展（2-3 周）

3. 配置层：在 daft-config 中新增 Celeborn 相关配置项，shuffle_algorithm 新增 `"celeborn"` 选项
4. 分布式层：扩展 `DistributedShuffleBackend` 枚举，新建 `backends/celeborn.rs`
5. 写入层（Map 阶段）：扩展 `RepartitionBackend` 枚举，实现 sink() 和 finalize()
6. 元数据层：扩展 `ShufflePartitionRef`，新增分组函数
7. 读取层（Reduce 阶段）：在 `ShuffleReadSource` 中新增 Celeborn 读取分支
8. 调度层：扩展 `translate_shuffle.rs` 中的算法选择逻辑

### 第三阶段：集成与测试（1-2 周）

9. 依赖管理：pyproject.toml 新增 Celeborn 可选依赖
10. 集成测试：本地 Celeborn 集群搭建、TPCH 正确性测试、性能对比

---

## 八、技术风险与约束

| 风险 | 等级 | 缓解措施 |
|------|------|---------|
| Rust→Python→Java 三层桥接性能 | 🔴 高 | 批量 pushData、异步调用、减少跨语言调用次数 |
| Arrow IPC 序列化开销 | 🟡 中 | Arrow IPC 本身接近零拷贝，远优于 pickle |
| JVM 生命周期管理 | 🟡 中 | 使用 JPype 管理 JVM，进程退出时自动清理 |
| Tokio 与 Netty 异步模型冲突 | 🟡 中 | 在 `spawn_blocking` 中调用 Python/Java，避免阻塞 Tokio |
| Celeborn 版本兼容 | 🟢 低 | 锁定特定版本，做好兼容层 |
| Range 分区不支持 | 🟢 低 | 与 Flight 后端一致，Range 分区场景较少 |

---

## 九、三种 Shuffle 后端对比

| 维度 | Ray Object Store | Flight Shuffle | Celeborn |
|------|-----------------|----------------|----------|
| **存储位置** | Ray 共享内存 | 计算节点本地 NVMe | 独立 ESS 节点 |
| **序列化** | pickle（慢） | Arrow IPC（快） | Arrow IPC（快） |
| **元数据管理** | GCS 集中式（单点瓶颈） | 各 Worker 本地 | Celeborn Master |
| **容错** | 有（但 GCS OOM） | ❌ 无 | ✅ 多副本/EC |
| **计算存储耦合** | 耦合 | 耦合 | 解耦 |
| **云原生适配** | 依赖 Ray 集群 | 需要本地 NVMe | ✅ 无本地盘要求 |
| **弹性伸缩** | 受限 | 受限 | ✅ 独立扩缩容 |
| **网络开销** | 中（跨节点 Object Store） | 低（P2P + 本地读取） | 中（多一跳） |
| **运维复杂度** | 低（Ray 内置） | 低（无额外组件） | 中（需部署 Celeborn） |
