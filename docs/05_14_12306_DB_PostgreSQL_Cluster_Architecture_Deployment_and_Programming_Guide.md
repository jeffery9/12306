# 🌌 12306 高并发票务分配系统 — 生产级 PostgreSQL 高可用集群架构、性能部署与极致编程白皮书

在 12306 核心交易链路中，**PostgreSQL** 是写模型（Command Path）的强一致性基石，承载着核心物理座席悲观互斥锁、订单事务以及 Transactional Outbox 消息的持久化。

作为承载国家级出票吞吐量的核心数据库，本白皮书从 **架构设计篇（HA Cluster Architecture）、生产部署篇（Production Tuning）与高并发编程篇（Advanced Programming）** 三个物理维度进行全方位深度规约。

---

## 第一篇：架构设计篇 — 物理无单点 Patroni 数据库集群拓扑

在生产环境，单节点 PostgreSQL 是绝对禁止的。12306 底层采用基于 **Patroni + etcd + PgBouncer + HAProxy** 的高可用分布式强一致集群架构：

```text
========================================================================================================
                              POSTGRESQL HIGH-AVAILABILITY CLUSTER ARCHITECTURE
========================================================================================================

                                [ Client Applications / DB Pool ]
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │    HAProxy / Keepalived│ (Virtual VIP Router)
                                    └───────────┬───────────┘
                                                │ (Port 5432 / 6432)
                                                ▼
                                    ┌───────────────────────┐
                                    │   PgBouncer Poolers   │ (Middleware Connection Pooler)
                                    └───────────┬───────────┘
                                                │
                       ┌────────────────────────┼────────────────────────┐
                       ▼ (Primary Write/Read)   ▼ (Replica 1 Read)       ▼ (Replica 2 Read)
             ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
             │   PG Master       │    │   PG Replica 1    │    │   PG Replica 2    │
             │   (Patroni Agent) │    │   (Patroni Agent) │    │   (Patroni Agent) │
             └─────────┬─────────┘    └─────────┬─────────┘    └─────────┬─────────┘
                       │                        │                        │
                       │                        ├────────────────────────┘
                       │ (Synchronous / Async   │ (Streaming Replication)
                       │  Log Shipping)         │
                       ▼                        ▼
             ┌─────────────────────────────────────────────────────────────┐
             │            Consul / etcd Distributed DCS                    │ (Leader Election &
             │            (3-Node Consensus Cluster)                       │  TTL Heartbeat)
             └─────────────────────────────────────────────────────────────┘
========================================================================================================
```

### 1. 核心高可用组件职责
*   **Patroni Agent**: 作为 PG 的守护进程，通过 REST API 实时监控本地实例状态，并与 etcd 保持 TTL 心跳。一旦 Master 崩溃，Patroni 协同 etcd 在 **sub-10s 内自动触发无损主从竞选（Auto-failover）**。
*   **etcd DCS**: 提供分布式一致性协调，用于存储集群的元数据（Leader Key, Replica States）。
*   **PgBouncer**: 轻量级连接池，作为中间件拦截万级并发微服务连接，将其转化为数十个高复用物理连接，阻断 PostgreSQL 的 `backend process` 内存暴涨。
*   **Streaming Replication**: 采用**物理流复制**。为了保证零数据丢失（Zero RPO），采用 **1 主从同步（Synchronous） + 1 从异步（Asynchronous）** 拓扑。

---

## 第二篇：生产部署篇 — 硬件调优与 postgresql.conf 核心参数调优

### 1. 物理存储与挂载调优 (Storage Tuning)
数据库所在存储阵列强制采用 **PCIe 5.0 NVMe Enterprise SSD（RAID 10）**。
*   **文件系统文件挂载**：选用 `XFS` 或 `ext4`，在 `/etc/fstab` 挂载参数中强制加入 `noatime,nodiratime,nobarrier`。
    *   `noatime`: 禁用读取时更新文件访问时间，磁盘 IOPS 直接提升 10%。
    *   `nobarrier`: 在带备用电池（BBU）的硬件 RAID 缓存卡保护下，禁用写屏障，大幅降低 WAL 落盘时延。

### 2. 内核大页 (Huge Pages) 启用
在 Linux 宿主机中强制启用透明大页（Transparent Huge Pages, THP）的禁用，改用**物理预留大页（Huge Pages）**。
*   **原因**：PostgreSQL 的 `shared_buffers` 较大时（如 32GB+），使用默认 4KB 页会导致 CPU TLB（Translation Lookaside Buffer） Miss 飙升，启用 2MB 物理大页可降低 CPU 系统调用消耗 15%+。

### 3. `postgresql.conf` 高并发极限优化配置 (32C 64GB RAM 规格)
```ini
# ─────────── 核心连接与并发 ───────────
max_connections = 500                  # PgBouncer 接管下，最大物理进程连接数
superuser_reserved_connections = 10    # 预留给管理员/SRE On-Call 诊断的物理连接

# ─────────── 内存分配规约 ───────────
shared_buffers = 16GB                  # 内存的 25%，用于 PG 共享高速缓冲区
huge_pages = try                       # 开启物理大页
work_mem = 64MB                        # 每一个 backend sort/hash 独占内存，防磁盘溢出
maintenance_work_mem = 2GB             # 索引创建/Vacuum 独占内存
effective_cache_size = 48GB            # 内存的 75%，提供给 Planner 的 OS 物理缓存预估

# ─────────── 事务发件箱与 WAL 优化 ───────────
wal_level = replica                    # 开启物理流复制
synchronous_commit = off               # 核心：高并发秒杀下，允许 WAL 异步组提交 (RPO ~ 10ms，吞吐提升 3 倍)
wal_buffers = 64MB                     # 缓冲区大小
checkpoint_completion_target = 0.9     # 平滑 Checkpoint，避免 I/O 剧烈抖动
max_wal_size = 16GB                    # 最大 WAL 阈值
min_wal_size = 2GB                     # 最小 WAL 阈值

# ─────────── 悲观行锁死锁检测 ───────────
deadlock_timeout = 100ms               # 缩短死锁判定时耗（默认 1s），快速中断冲突事务并释放资源

# ─────────── MVCC 自动垃圾回收 (Transactional Outbox 核心) ───────────
autovacuum = on                        # 必须开启
autovacuum_max_workers = 8             # 增加 Vacuum 工作线程数
autovacuum_vacuum_scale_factor = 0.05  # 表中 5% 记录被更新/删除时，立即触发 Vacuum (防止死元组堆积)
autovacuum_vacuum_cost_limit = 2000    # 调高限制，加速清理
```

---

## 第三篇：高并发编程篇 — PostgreSQL 极致开发实践

### 1. lexicographical 升序加锁规约 (lexicographical Lock Ordering) — 彻底免疫死锁
在秒杀并发中，多笔事务可能同时试图锁住同一批物理座席的 `seat_segment` 记录。如果加锁顺序不一致（如进程 A 锁 1->2，进程 B 锁 2->1），会瞬间发生 **死锁（Deadlock）**。
*   **规约**：应用代码中，对 `seat_segment` 的 `FOR UPDATE` 锁定，**必须保证 `seat_id` 且 `segment_no` 满足绝对的数学升序**。
*   **SQL 实现**：
    ```sql
    -- 🔍 强一致升序锁定
    SELECT id, state 
    FROM seat_segment 
    WHERE schedule_id = $1 
      AND seat_id = $2 
      AND segment_no >= $3 
      AND segment_no < $4 
      AND state = 'AVAILABLE' 
    ORDER BY seat_id ASC, segment_no ASC 
    FOR UPDATE;
    ```

### 2. 原子 CTE 合并写 (`WITH ... INSERT ... ON CONFLICT`) — 减半网络往返
在列车、排班车次及站点的 TRS 权威数据导入中，需要执行“若存在则返回 id，不存在则插入”的语义。如果先 Select 再 Insert，不仅有并发冲突，更有 2 次网络往返（RTT）。
*   **优化**：采用 PostgreSQL 原生的 **CTE（Common Table Expressions） 与 `ON CONFLICT DO NOTHING RETURNING`** 融合写入：
    ```sql
    -- 🌌 Go / Rust / Java 共享的单 RTT 幂等导入语句
    WITH s AS (
        SELECT id FROM train WHERE code = $1
    ),
    i AS (
        INSERT INTO train (code) 
        VALUES ($1) 
        ON CONFLICT (code) DO NOTHING 
        RETURNING id
    )
    SELECT id FROM i 
    UNION ALL 
    SELECT id FROM s 
    LIMIT 1;
    ```

### 3. Transactional Outbox 极致消费：`SKIP LOCKED` 无锁并发捞取
Outbox Poller 协程需要从 `outbox_event` 表中频繁获取 `status = 'NEW'` 的消息进行广播发布。如果简单执行 `SELECT ... FOR UPDATE`，多个微服务实例的 Poller 会互相阻塞，吞吐量沦为单机。
*   **优化**：采用 **`FOR UPDATE SKIP LOCKED`**。这能让当前 Poller 瞬间跳过已被其他实例锁定的行，无锁、并行、安全地捕获属于自己的 100 条未处理事件，消费吞吐提升 **30 倍以上**：
    ```sql
    -- ⚡ 高能 Poller 专用：无锁并发拉取
    SELECT id, payload 
    FROM outbox_event 
    WHERE status = 'NEW' 
    ORDER BY id ASC 
    LIMIT 100 
    FOR UPDATE SKIP LOCKED;
    ```

### 4. 动态范围分区表设计 (Range Partitioning)
12306 的车票订单（`orders`）和锁座记录（`reservation`）带有极强的**时间亲和性**（主要针对特定乘车日期的车票）。若所有历史数据全部堆积在单张表中，B+ Tree 索引深度将呈指数级增加，导致读写性能急剧衰退。
*   **设计**：采用基于 `service_date` 的 **PostgreSQL 原生范围分区（PARTITION BY RANGE）**。
*   **SQL 实现**：
    ```sql
    -- 创建主表
    CREATE TABLE reservation (
        id VARCHAR(64) NOT NULL,
        schedule_id INT NOT NULL,
        service_date DATE NOT NULL,
        seat_id INT NOT NULL,
        state VARCHAR(20) NOT NULL,
        PRIMARY KEY (id, service_date) -- 分区键必须是主键的一部分
    ) PARTITION BY RANGE (service_date);

    -- 创建具体天数的分区子表 (物理隔离存储)
    CREATE TABLE reservation_y2026m09d08 PARTITION OF reservation
        FOR VALUES FROM ('2026-09-08') TO ('2026-09-09');

    CREATE TABLE reservation_y2026m09d09 PARTITION OF reservation
        FOR VALUES FROM ('2026-09-09') TO ('2026-09-10');
    ```
*   **优势**：
    1.  **查询修剪（Partition Pruning）**：当查询特定乘车日的记录时，优化器会直接物理排除其余数百张无关的分区表，扫描开销直接压缩。
    2.  **极速历史清理**：对于已经过期数月的历史订单，SRE 无需执行高 IO、产生大量 MVCC 碎片的 `DELETE` 动作，只需简单执行 `DROP TABLE reservation_y2026m06...` 即可实现毫秒级物理空间安全回收，**写放大系数为零**！

---

## 总结 (Summary)

数据库是高并发架构的最后防线。通过部署 **Patroni + etcd** 消除物理单点（SPOF），配合 SSD XFS 的 `nobarrier` 与 `postgresql.conf` 的 autovacuum 加平滑 checkpoint 优化，保障了物理 I/O 带宽的高稳定输入；同时，在代码端全面推行 **“绝对升序加锁规约”、“单 RTT CTE 幂等合并写”、“SKIP LOCKED 无锁事件发布” 与 “动态范围分区表”** 编程四大律令，直接从微观语法层压扁了事务耗时和锁排队，共同筑起 12306 强一致性交易引擎的钢铁大坝！
