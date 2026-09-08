# 🌌 12306 性能基线报告、亿级超高并发容量规划与硬件选型白皮书

> **最高学术与工程准则**：本白皮书针对 12306 系统在 **1亿级 (100M RPS)** 极限吞吐配置下的物理机房硬件、多中心网络互联、高吞吐 CDN 及顶级 SRE 运维中心，进行了严密、精确的底层计算机物理事实推导，完成了对存储、网络带宽、计算节点 and 消息中间件的物理容量（Sizing）估算与硬件选型规划，并详述了读写两阶段架构的演进大坝。全篇采用纯 ASCII 排版。

---

## 1. 业务基准假设 (Sizing Baseline & Hypotheses)

为了进行精确的数学计算，我们设定以下符合中国铁路局真实开行方案的商业基准（相比 50M，运力与流量双倍扩容）：

```text
  [ 每日开行列车 (Trains/Day) ] ──────► 2,000 趟列车
  [ 每趟列车平均站数 (Stations) ] ─────► 15 个经停站 (产生 14 个物理段/Segment)
  [ 每趟列车平均席位 (Seats) ] ────────► 1,000 个物理座位 (如商务舱/一等/二等总和)
  [ 每日售出车票 (Tickets/Day) ] ──────► 100,000,000 张 (一亿张票)
  [ 高并发峰值总流量 (Peak QPS) ] ──────► 100,000,000 次交互/秒 (99% 读, 1% 写)
```

---

## 2. 1亿级 (100M) 极限流量目标拆解 (The 100M RPS Deconstruction)

**1亿次/秒 (100,000,000 QPS/TPS)** 是人类史诗级的极值灾难并发。在真实的 12306 场景中，绝大多数流量来自于用户的疯狂刷新（读）。根据二八定律与实际购票漏斗，我们按照 **99% 读流量与 1% 写流量** 进行物理拆解：

*   **总峰值吞吐**：100,000,000 RPS (Requests per Second)
*   **读峰值 (Query)**：`99,000,000 QPS` (9900 万次查询/秒)
*   **写峰值 (Command)**：`1,000,000 TPS` (100 万次下单预占/秒)

要支撑这一量级，单中心静态配置必定崩溃，必须引入 **异地多活 (Active-Active Multi-Region)** 与 **边缘计算极光防御** 体系。

---

## 3. 数据库与分布式缓存存储容量估算 (Storage Sizing)

### A. 每日 MySQL 基础配席数据（静态与段锁数据）
每日开行 2000 趟车，每趟 1000 个座席，产生 2,000,000 个物理 Seat 记录。由于列车包含 15 个站，产生 14 个物理区间段（Segment Locks）。

#### ① 单条 SeatSegment 记录存储开销：
```text
  字段名称 (Field)       类型 (Type)          物理大小 (Bytes)
  id                     INT (Primary Key)    4
  schedule_id            INT (Foreign Key)    4
  seat_id                INT (Foreign Key)    4
  segment_no             TINYINT              1
  state                  VARCHAR(10)          10
  version                INT                  4
  -----------------------------------------------------------
  物理小计 (Row Size)                         ~27 字节 (考虑 InnoDB 页头与指针折合为 64 字节/行)
```

#### ② 每日段锁表记录数及存储体积：
```text
  [ 每日记录数 ] = 2,000 趟 * 1,000 席 * 14 段 = 28,000,000 行/天 (两千八百万行)
  [ 每日数据量 ] = 28,000,000 行 * 64 字节 = 1,792,000,000 字节 ≈ 1.79 GB/天
  [ 每日索引与元数据开销 (100% 冗余) ] ───────► 1.79 GB * 2 = 3.58 GB/天
  [ 年度物理存储总量 (Yearly Metadata) ] ─────► 3.58 GB * 365 ≈ 1.3 TB/年
```

---

### B. 每日购票事务数据（订单、票扣、发件箱）
每日成功售出 100,000,000 张车票（即 Command 事务）。产生 1亿个 Ticket 记录、约 5000 万个 Order 订单记录（假设一次订单平均买 2 张票）以及 1亿个 Outbox 发件箱事件记录。

#### ① 单条记录物理大小估算：
*   `Ticket` 行大小：约 **128 字节**（含车票号、车次、座位 ID、乘车人 ID、票价、状态）。
*   `Order` 行大小：约 **256 字节**（含订单号、支付流水号、总价、状态、创建时间）。
*   `Outbox_event` 行大小：约 **512 字节**（含事件全局唯一 ID、类型、聚合 ID、状态、JSON payload 契约体）。

#### ② 每日交易数据存储增量：
```text
  [ Ticket 每日大小 ] = 100,000,000 * 128 字节 = 12.8 GB/天
  [ Order 每日大小 ]  = 50,000,000 * 256 字节 = 12.8 GB/天
  [ Outbox 每日大小 ] = 100,000,000 * 512 字节 = 51.2 GB/天
  -------------------------------------------------------------
  [ 纯数据每日小计 ]                         = 76.8 GB/天
  [ 索引、冗余与事务日志 (MySQL Binlog/Undo, 150% 冗余) ] ─► 76.8 * 2.5 ≈ 192 GB/天
  [ 年度事务数据总量 (Yearly Transactions) ] ──► 192 GB * 365 ≈ 70,080 GB ≈ 70.08 TB/年
```

> **MySQL 存储选型建议**：
> 每日 192 GB 写入，一年沉淀 **70 TB** 强事务数据。
> 1. 采用 **MySQL 512个物理分片集群（Sharding）**，单物理分片承载 `70 TB / 512 ≈ 136 GB/年` 的轻量级存储，完美发挥 PCIe-4.0 NVMe SSD 的极致 IOPS 优势。
> 2. 引入冷热数据隔离，主库只保全近 30 天的最热在线高并发活跃数据，主库磁盘水位长效锁定在 **5 TB** 以内。

---

### C. 分布式缓存内存估算 (Redis Memory Sizing)
全量最热列车的区间二进制占用位掩码（Bitmask）与快速可用余票统计，100% 缓存在 Redis Cluster 中。
*   保持最近 15 天开行的所有车次完全在 Redis 内存中。
*   活跃车次总数 = 2000 趟/天 * 15 天 = 30,000 趟列车。

#### ① 席位区间位掩码哈希（r:{sched_id}:seat:{id}）
30,000 趟车 * 1000 个物理席位 = 30,000,000 个活跃席位 Key。
每个 Key 值为二进制整数，由于 Redis 内存分配器的内存对齐 Overhead，折合消耗约 **150 字节/Key**：
```text
  [ 席位位掩码内存总量 ] = 30,000,000 * 150 字节 = 4,500,000,000 字节 ≈ 4.5 GB
```

#### ② 快速查询余票 Hash（q:availability:{sched_id}）
30,000 趟车 * 6 种座位席别 = 180,000 个余票缓存 Key。每个 Hash 约消耗 **12 KB**：
```text
  [ 快速余票 Hash 内存总量 ] = 180,000 * 12 KB ≈ 2,160,000 KB ≈ 2.16 GB
```

> **Redis 内存选型结论**：
> 即使在 1 亿并发架构下，全系统最核心的抢票和余票读缓存在 Redis 内仅消耗 `4.5 + 2.16 ≈ 6.66 GB` 内存！瓶颈非内存容量，而是 CPU 主频计算位掩码的速度。

---

### D. 有状态数据库物理 I/O（IOPS ＆ 读写吞吐）需求精确估算方案

当写并发达到峰值 **1,000,000 TPS** 且全量事务（1亿笔购票/天）穿透排队机落盘时，512 个 MySQL 物理分片（Sharding）将承担极其残酷的有状态磁盘物理 I/O 考验。以下从 MySQL InnoDB 存储引擎的底层落盘原理出发，定量推导单数据库节点与全局系统的 I/O 需求。

#### ① 单个购票写事务（Command）的物理 I/O 放大因子
在 12306 业务中，一个标准的扣票写事务包含：
1.  **写订单表（Order）**：新增 1 行记录，修改 1 处索引页。
2.  **写车票表（Ticket）**：新增 2 行记录（假设平均每单 2 张票），修改 2 处索引页。
3.  **写发件箱表（Outbox Event）**：新增 1 行记录，修改 1 处索引页。
4.  **更新区间段锁表（SeatSegment）**：更新列车座位区间状态，产生 1 行记录修改。
5.  **元数据落盘放大**：
    *   **Undo Log**：生成事务回滚段数据页。
    *   **Redo Log**：顺序写入事务恢复日志页。
    *   **Binlog**：顺序写入复制日志。
    *   **Doublewrite Buffer（双写缓冲区）**：防止页损坏，所有脏页需先顺序双写物理落盘，再随机刷新回原数据文件。

#### ② 峰值写 IOPS 估算模型
在峰值 $1,000,000 \text{ TPS}$ 下，512 个 MySQL 分片平均承担的负载：

$$TPS_{shard} = \frac{1,000,000 \text{ TPS}}{512 \text{ Shards}} \approx 1,953 \text{ TPS/Shard}$$

在 InnoDB Group Commit（组提交）机制下，高并发写时的顺序 Redo Log 与 Binlog 刷盘发生合并，设平均合并因子 $G = 10$，则日志刷盘 IOPS 为 $2 / 10 = 0.2 \text{ IOPS/Transaction}$。
而脏页（Data Page）的随机写入在双写机制下，每个事务需要：
*   **Doublewrite 顺序刷盘**：$5 \text{ Pages} \times 1 \text{ IOPS} = 5 \text{ IOPS}$。
*   **Data File 随机脏刷（Checkpointed）**：$5 \text{ Pages} \times 1 \text{ IOPS} = 5 \text{ IOPS}$。

单分片核心磁盘峰值物理写入 IOPS 计算公式如下：

$$IOPS_{write\_shard} = TPS_{shard} \times \left( \frac{2}{G} + Pages_{modified} \times 2 \right) = 1,953 \times (0.2 + 5 \times 2) \approx 19,920 \text{ IOPS/Shard}$$

*   **安全冗余系数**：引入 $S = 2.0$ 的 SRE 突发流量冗余系数，以抵御数据库物理 Checkpoint 时的突发 I/O 潮汐。
*   **单分片目标写入 IOPS 需求**：$$19,920 \times 2.0 = 39,840 \approx 40,000 \text{ IOPS}$$
*   **全网 512 分片总写入 IOPS 需求**：$$40,000 \text{ IOPS} \times 512 \approx 20,480,000 \text{ IOPS}$$ (超 2000 万全局 IOPS)。

#### ③ 峰值写 I/O 吞吐量（Throughput）估算模型
InnoDB 标准数据页（Page Size）大小为 $16 \text{ KB}$。一个事务平均修改 5 个物理数据页，双写机制下，物理总写入量计算如下：

$$Size_{write\_tx} = (Redo + Binlog \approx 2 \text{ KB}) + (5 \text{ Pages} \times 16 \text{ KB} \times 2) = 162 \text{ KB/Transaction}$$

单分片峰值物理写吞吐带宽（Write Throughput）计算公式：

$$Throughput_{write\_shard} = TPS_{shard} \times Size_{write\_tx} = 1,953 \text{ TPS} \times 162 \text{ KB} \approx 316.4 \text{ MB/s (单分片)}$$

*   **安全冗余系数**：同样引入 $S = 2.0$ 的弹性冗余。
*   **单分片目标物理写吞吐需求**：$$316.4 \text{ MB/s} \times 2.0 = 632.8 \text{ MB/s} \approx 640 \text{ MB/s}$$
*   **全网 512 分片总写吞吐带宽需求**：$$640 \text{ MB/s} \times 512 \approx 327.68 \text{ GB/s}$$ (极其暴力的全局有状态落盘物理带宽)。

#### ④ 存储硬件（PCIe NVMe SSD）物理匹配论证
对标我们在 **`docs/05_01`（本地 IDC Spine-Leaf 网络架构）** 中为 512 组 MySQL 主备节点配置的顶级存储选型：
*   **物理配置**：每个分片节点配备 **4 x 3.2TB Enterprise PCIe 5.0 NVMe SSD** 物理卡做硬件 **RAID-10**。
*   **硬件物理极限**：单块 Enterprise PCIe 5.0 盘提供 **1,500,000 (150万) 随机写 IOPS** 且顺序写带宽达 **14,000 MB/s**。
*   **冗余容错安全度**：单机 RAID-10 物理写入 IOPS 保守估计为 **1,500,000+ IOPS**，其物理吞吐处理能力（150万 IOPS）是单分片峰值最高需求（4万 IOPS）的 **`37.5 倍`**，物理写吞吐带宽是最高需求（640 MB/s）的 **`20 倍`**。
*   **SRE 选型结论**：通过超高密度 SSD 的物理冗余，确保 SSD 始终在寿命最长、温度最低且 **I/O 响应延迟低于 0.1ms** 的“物理黄金甜区”中运行，100% 杜绝因物理写队列积压导致的底层购票事务级雪崩。

---

### E. 基于 PostgreSQL 集群的高并发 I/O 需求估算方案 (TeX 公式版)

本方案针对采用 **PostgreSQL 高可用集群（含主从物理复制与分片）** 部署下的有状态存储，建立定量的物理 I/O 估算数学模型。本篇全面采用标准 $\text{\LaTeX}$（TeX）公式进行学术级推导，深度剖析 PostgreSQL 独有的多版本并发控制（MVCC）写放大、全页写入（Full Page Writes - FPW）以及自动垃圾回收（Autovacuum）机制对磁盘 IOPS 和吞吐带宽的物理吞噬，为底层存储硬件选型提供最硬核的定量支撑。

#### ① 单分片写吞吐率计算
单个 PostgreSQL 分片在峰值时分配承载的事务率 $TPS_{shard}$ 如下：

$$TPS_{shard} = \frac{1,000,000 \text{ TPS}}{512 \text{ Shards}} \approx 1,953.125 \text{ TPS/Shard}$$

在每个标准的购票事务中，PostgreSQL 必须执行如下 DML 写入：
1. `INSERT INTO orders` （产生 1 个新元组）
2. `INSERT INTO tickets` （产生 2 个新元组，假设平均一单购 2 张票）
3. `INSERT INTO outbox_events` （产生 1 个新元组，用于 EDA 消息分发）
4. `UPDATE seat_segments` （更新 1 个现有的座位区间二进制状态，触发 MVCC 原理）

#### ② PostgreSQL 独有 I/O 放大因子推导
PostgreSQL 的物理写入开销与 MySQL InnoDB 存在本质差异，主要体现在以下两个物理底层机制上。
*   **MVCC 引发的写放大**：
    PostgreSQL 采用 **追加式 MVCC**。每次 `UPDATE` 时，PostgreSQL 并不修改原有行，而是在堆页中追加写入一个全新的元组，并将旧元组标记为 Dead。这会引发显著的写放大。我们定义 PostgreSQL 的 MVCC 写入放大因子为 $\beta$：
    $$\beta \approx 1.8 \quad (\text{在存在多索引及高频更新状态下})$$
*   **Full Page Writes（全页写入 - FPW）引发的 WAL 暴增**：
    在每次 Checkpoint（检查点）之后，当某个数据页（8 KB）被第一次修改时，必须将整个数据页的内容完整写入 WAL（预写日志）。高负载下，FPW 会导致 WAL 带宽产生瞬时喷涌。我们定义 FPW 的 WAL 物理放大因子为 $\alpha$：
    $$\alpha \approx 3.0 \quad (\text{在主库高频 Checkpoint 刷新期间})$$

#### ③ 磁盘物理写入吞吐量（Write Throughput）数学估算
PostgreSQL 的物理写入由顺序写入的 WAL 日志，以及由 `bgwriter`/`checkpointer` 进程刷出的随机 8 KB 数据脏页组成。
*   **WAL 顺序写吞吐量模型 ($Throughput_{WAL}$)**：
    一个标准扣票事务产生的裸 WAL 日志增量约为 $1.5 \text{ KB}$。在 FPW 放大下，单事务实际产生的 WAL 物理写入量 $Size_{WAL\_tx}$ 如下：
    $$Size_{WAL\_tx} = 1.5 \text{ KB} \times \alpha = 1.5 \text{ KB} \times 3.0 = 4.5 \text{ KB}$$
    单分片在峰值吞吐下的 WAL 物理顺序写吞吐量 $Throughput_{WAL}$ 公式为：
    $$Throughput_{WAL} = TPS_{shard} \times Size_{WAL\_tx} = 1,953 \text{ TPS} \times 4.5 \text{ KB} \approx 8.79 \text{ MB/s}$$
*   **脏数据页随机写吞吐量模型 ($Throughput_{Data}$)**：
    单事务所修改的逻辑物理页数基准值为 $5$ 页。PostgreSQL 默认数据页大小为 $8 \text{ KB}$。代入 MVCC 放大因子 $\beta = 1.8$ 后，单事务实际需要写入的物理脏页数 $Pages_{modified}$ 为：
    $$Pages_{modified} = 5 \text{ Pages} \times \beta = 5 \times 1.8 = 9 \text{ Pages}$$
    单分片由脏页刷盘产生的随机写吞吐量 $Throughput_{Data}$ 如下：
    $$Throughput_{Data} = TPS_{shard} \times Pages_{modified} \times 8 \text{ KB} = 1,953 \text{ TPS} \times 9 \times 8 \text{ KB} \approx 140.61 \text{ MB/s}$$
*   **Autovacuum 垃圾清理吞吐量模型 ($Throughput_{Vacuum}$)**：
    Autovacuum 产生的额外磁盘写吞吐量设为事务写入量的 $5\%$ 左右：
    $$Throughput_{Vacuum} = Throughput_{Data} \times 5\% \approx 140.61 \text{ MB/s} \times 0.05 \approx 7.03 \text{ MB/s}$$
*   **单分片综合写入吞吐量需求测算**：
    单分片在峰值状态下的综合磁盘写入吞吐量 $Throughput_{Total}$ 为上述三者之和：
    $$Throughput_{Total} = Throughput_{WAL} + Throughput_{Data} + Throughput_{Vacuum} \approx 8.79 + 140.61 + 7.03 = 156.43 \text{ MB/s}$$
    引入 SRE 冗余系数 $S = 2.0$ 后单分片目标物理写吞吐需求：
    $$Throughput_{Target} = Throughput_{Total} \times S = 156.43 \text{ MB/s} \times 2.0 \approx 312.86 \text{ MB/s}$$
    512 分片集群全局总吞吐带宽需求：
    $$Throughput_{Global} = 312.86 \text{ MB/s} \times 512 \approx 160.18 \text{ GB/s}$$

#### ④ 磁盘物理 IOPS 需求数学估算
与吞吐量同样关键的是物理磁盘的 IOPS（每秒 I/O 操作次数），特别是高频 8 KB 随机读写下的磁盘负荷。
*   **WAL 日志顺序写入 IOPS ($IOPS_{WAL}$)**：
    在高并发下，物理层通过 `Group Commit` 合并。设平均每 $10$ 个事务合并为一次物理磁盘写入：
    $$IOPS_{WAL} = \frac{TPS_{shard}}{10} = \frac{1,953}{10} \approx 195.3 \text{ IOPS}$$
*   **脏数据页刷盘 IOPS ($IOPS_{Data}$)**：
    随机 8 KB 脏页写入数据文件。在缓存合并机制下设物理页合并率（页重用率）为 $50\%$：
    $$IOPS_{Data} = TPS_{shard} \times Pages_{modified} \times (1 - 50\%) = 1,953 \times 9 \times 0.5 \approx 8,788.5 \text{ IOPS}$$
*   **Autovacuum 扫描及整理产生的随机 I/O ($IOPS_{Vacuum}$)**：
    Autovacuum 随机读写产生的额外写 IOPS 如下：
    $$IOPS_{Vacuum\_Write} \approx 800 \text{ IOPS}$$
*   **单分片综合写入 IOPS 需求测算**：
    单分片在最热购票突发时刻的磁盘写入总 IOPS 需求 $IOPS_{Total}$ 如下：
    $$IOPS_{Total} = IOPS_{WAL} + IOPS_{Data} + IOPS_{Vacuum\_Write} \approx 195.3 + 8,788.5 + 800 \approx 9,783.8 \text{ IOPS}$$
    引入 SRE 冗余系数 $S = 2.0$ 后的单分片目标 I/O IOPS：
    $$IOPS_{Target} = IOPS_{Total} \times S = 9,783.8 \times 2.0 \approx 20,000 \text{ IOPS}$$
    512 分片集群全局总写入 IOPS 需求：
    $$IOPS_{Global} = 20,000 \text{ IOPS} \times 512 \approx 10.24 \text{ Million IOPS}$$

---

## 4. 读写双侧架构演进与扩展规划 (Architectural Evolution)

### A. 读侧架构演进：9900 万 QPS 的三级缓存防御圈
必须将计算与缓存推向边缘与进程本地。

```text
  [ 99,000,000 QPS 狂暴读流量 ]
             │
             ▼ 1. 边缘节点防御 (CDN / Anycast 智能网关) ──────► 拦截 60% (约 59.4M QPS)
             │    - 返回前一秒的短时效静态余票快照。
             │
             ▼ 2. 本地内存网关 (L1 Cache: Nginx Dict / 进程内存) ─► 拦截 30% (约 29.7M QPS)
             │    - 部署万级无状态网关节点，接收 Kafka Projector 的 Pub/Sub 余票广播。
             │
             ▼ 3. 分布式缓存中心 (L2 Cache: Redis Cluster) ─────► 最终承接 10% (约 9.9M QPS)
                  - 物理拆分成 200~256 个分片 (Shards)。单 Shard 承担 40,000 QPS。
```

### B. 写侧架构演进：100 万 TPS 的异步排队与数据分片
100 万 TPS 的物理落盘必须采用 **排队削峰 + Lua分片扣减 + MySQL 细胞化分片**：

#### ① 极速排队机与写流控 (Virtual Waiting Room)
将超出 100 万 TPS 的海量无效恶意点击拦截在门外，对合法请求下发 Token 排队号，削峰填谷。

#### ② Redis 分片预占拦截 (Lua Bitmask Sharding)
*   升级为 Redis Cluster **512 个 Shard** 分片集群。单台 Redis 处理 2,000 TPS 的 Lua 位掩码，512 台完美吞下 1,000,000 TPS 峰值扣减。

#### ③ MySQL 细胞化分表落盘 (Cellular Database Sharding)
*   **分库分表维度**：采用 `Hash(schedule_id) % 512` 拆分出 **512 个物理 Database 集群**。
*   每个主库仅需承担 `1,000,000 / 512 ≈ 1,953 TPS`，行锁升序（ASC）机制依然 100% 生效，死锁率为 0！

---

## 5. 网络带宽与消息队列物理容量估算 (Network & Kafka Sizing)

### A. 网络物理带宽估算
*   **读侧 QPS 带宽**：在 99,000,000 QPS 读峰值下，包大小按 200 字节算：
    $$Bandwidth_{read} = 99,000,000 \text{ QPS} \times 200 \text{ Bytes} \times 8 \text{ bits/Byte} = 158.4 \text{ Gbps}$$
    *SRE 减压策略*：拦截 $90\%$ 后，穿透至机房核心的带宽稳压在 $15.84 \text{ Gbps}$。
*   **写侧 TPS 带宽**：在 1,000,000 TPS 下，事务包大小 1000 字节：
    $$Bandwidth_{write} = 1,000,000 \text{ TPS} \times 1,000 \text{ Bytes} \times 8 \text{ bits/Byte} = 8.0 \text{ Gbps}$$

### B. 消息队列 (Kafka Partition Sizing)
所有的交易事务（100,000,000 次/天）投递到 Kafka 主题 `ticket_events` 中。
*   **分发保序**：物理硬锁 Kafka 拥有 **64 个 Partitions**（对齐 KEDA 消费副本扩容）。
*   **积压容量（7天保留期）**：
    $$Capacity_{backlog} = 100,000,000 \text{ msgs/day} \times 512 \text{ Bytes/msg} \times 7 \text{ days} \approx 358.4 \text{ GB}$$

---

## 6. SRE 计算核心与服务器配置规格表 (Spec Sheet & HW Matrix)

为了稳托 1亿级 RPS，Python+MySQL 基线下的硬件选型规格扩展如下：

```text
  ┌───────────────────────┬──────────┬────────────────────────────────────────────────────────┐
  │ 部署服务名称          │ 预估副本 │ 单台服务器硬件配置推荐 (Bare-Metal / Pod Spec)         │
  ├───────────────────────┼──────────┼────────────────────────────────────────────────────────┤
  │ 1. API无状态核心      │ 10000+   │ - CPU: 4 Cores (Intel Ice Lake @ 2.8GHz+)              │
  │    (ticketing-web)    │ 个 Pod   │ - RAM: 8 GB                                            │
  ├───────────────────────┼──────────┼────────────────────────────────────────────────────────┤
  │ 2. Redis 内存缓存群   │ 256主    │ - CPU: 16 Cores AMD EPYC (高主频 4.0GHz+, 极速单线程)  │
  │    (Redis Shard master)│ 256从    │ - RAM: 64 GB                                           │
  ├───────────────────────┼──────────┼────────────────────────────────────────────────────────┤
  │ 3. MySQL 数据库分片   │ 512组    │ - CPU: 32 Cores AMD EPYC @ 3.0GHz+                     │
  │    (MySQL Shards)     │ (一主)   │ - RAM: 256 GB (保全全库 100% 数据索引完全在内存 Buffer) │
  │                       │ (一备)   │ - 磁盘: 4 x 3.2TB Enterprise PCIe-4.0 NVMe SSD (RAID10)│
  ├───────────────────────┼──────────┼────────────────────────────────────────────────────────┤
  │ 4. Kafka 分布式中枢   │ 32台     │ - CPU: 16 Cores Intel Xeon @ 2.6GHz+                   │
  │    (Kafka Broker)     │ 物理机   │ - RAM: 128 GB (预留充足 of OS Page Cache 提升读写吞吐)   │
  └───────────────────────┴──────────┴────────────────────────────────────────────────────────┘
```
---

## 7. SRE 物理数据中心备件库存与硬件折损替代评估模型 (Spare Parts Sizing & MTBF Model)

一个超高并发、超高通量的数据中心要达成 $99.999\%$ 的系统级可用度，不仅需要在软件架构上做双活冗余，还必须在物理层面确保故障发生时，备件库中具有 100% 可就地（On-Site）替换的备品。
本模型采用 泊松分布（Poisson Distribution）概率数学模型，科学测算在常态 3 年物理运行周期内，为了保障 $99.9\%$（三个九）备件瞬时可得率 时的安全备件仓储规模。

### A. 备件仓储数学估算公式
对于保有量为 $N$ 的同种硬件组件，设其单器件平均无故障工作时间为 $MTBF$（小时），在观测时间 $T$（小时，此处设定为自建机房首期 3 年正常运行周期，即 $3 \times 365 \times 24 = 26,280$ 小时）内：

*   **单器件故障率（$\lambda$）**：
    $$\lambda = \frac{1}{MTBF}$$
*   **物理组件总期望故障数（$\mu$）**：
    $$\mu = N \times \lambda \times T = \frac{N \times T}{MTBF}$$
*   **安全备件库存规模（$S$）判定条件**：
    为了在 99.9% 概率（信心水平）下，发生的真实故障次数不超过备存数量 $S$（即不发生因等待厂商 RMA 邮寄导致的断件宕机灾难）：
    $$\sum_{k=0}^{S} \frac{e^{-\mu} \mu^k}{k!} \ge 99.9\%$$

由于 $N$ 较大时期望故障数 $\mu > 30$，根据拉普拉斯-李雅普诺夫大数中心极限定理，泊松分布可完美近似为正态分布。则安全备件数量 $S$ 的简化推演公式为：

$$S \approx \mu + 3.09 \times \sqrt{\mu} \quad (\text{基于正态单尾分布，z-score} = 3.09 \text{ 完美覆盖 } 99.9\% \text{ 的右侧单尾随机故障边界})$$

---

### B. 4 类核心关键物理硬件备品测算

#### ① 核心计算处理器（AMD EPYC 9654 CPU）
*   **保有总量（$N$）**：1152 台服务器 $\times$ 双路处理器 = 2,304 颗。
*   **平均 MTBF**：180,000 小时。
*   **3年期望故障数推演**：
    $$\mu = \frac{2304 \times 26,280}{180,000} \approx 336.38 \text{ 颗}$$
*   **99.9% 安全备件库存规模（$S_{CPU}$）**：
    $$S_{CPU} \approx 336.38 + 3.09 \times \sqrt{336.38} \approx 336.38 + 3.09 \times 18.34 \approx 336.38 + 56.67 \approx 393 \text{ 颗}$$
*   **备存比率**：$393 / 2304 \approx 17.06\%$（推荐备存 393 颗）。

#### ② 数据库核心闪存盘（Enterprise PCIe 5.0 3.2TB NVMe SSD）
*   **保有总量（$N$）**：512 台数据库主库 $\times$ 4 盘硬 RAID-10 = 2,048 块。
*   **平均 MTBF**：120,000 小时。
*   **3年期望故障数推演**：
    $$\mu = \frac{2048 \times 26,280}{120,000} \approx 448.51 \text{ 块}$$
*   **99.9% 安全备件库存规模（$S_{SSD}$）**：
    $$S_{SSD} \approx 448.51 + 3.09 \times \sqrt{448.51} \approx 448.51 + 3.09 \times 21.18 \approx 448.51 + 65.45 \approx 514 \text{ 块}$$
*   **备存比率**：$514 / 2048 \approx 25.10\%$（推荐备存 514 块，高频落盘事务损耗较大）。

#### ③ 核心万兆网卡（Mellanox ConnectX-6 Dx 100GbE NIC）
*   **保有总量（$N$）**：1152 台核心计算服务器 $\times 1$ 网卡/台 = 1,152 块。
*   **平均 MTBF**：150,000 小时。
*   **3年期望故障数推演**：
    $$\mu = \frac{1152 \times 26,280}{150,000} \approx 201.83 \text{ 块}$$
*   **99.9% 安全备件库存规模（$S_{NIC}$）**：
    $$S_{NIC} \approx 201.83 + 3.09 \times \sqrt{201.83} \approx 201.83 + 3.09 \times 14.21 \approx 201.83 + 43.91 \approx 246 \text{ 块}$$
*   **备存比率**：$246 / 1152 \approx 21.35\%$（推荐备存 246 块）。

#### ④ TOR 接入交换机（Arista DCS-7050SX3 Leaf Switch）
*   **保有总量（$N$）**：48 台 Leaf 交换机。
*   **平均 MTBF**：100,000 小时。
*   **3年期望故障数推演**：
    $$\mu = \frac{48 \times 26,280}{100,000} \approx 12.61 \text{ 台}$$
*   **99.9% 安全备件库存规模（$S_{Leaf}$）**：
    由于样本数过小，不能完全采用正态近似。通过精确 Poisson 累计分布函数推算，在 $\mu = 12.61$ 时：
    $$\sum_{k=0}^{24} \frac{e^{-12.61} 12.61^k}{k!} \approx 99.85\%$$
    $$\sum_{k=0}^{25} \frac{e^{-12.61} 12.61^k}{k!} \approx 99.93\% \ge 99.9\%$$
*   **安全交换机库存规模**：推荐备存 25 台。
*   **备存比率**：$25 / 48 \approx 52.08\%$（样本小且处于东西向转发必经要道，备存比高，提供绝对冗余保障）。


