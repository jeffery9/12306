# 🌌 12306 高并发票务系统 — 事务性发件箱与事件驱动架构 (EDA) 深度方案

> **最高设计原则**：本项目采用事件驱动架构（Event-Driven Architecture, EDA）与读写分离（CQRS）并驱。由于 12306 面对的写碰撞与读吞吐量均是天级数十亿次，通过异步事件流、发件箱保序、Kafka 高并发分区以及读侧幂等投影，能够将笨重的物理数据库盘级读写完全解耦为内存级飞速读缓存，保障系统不溃。

---

## 1. 架构拓扑大纲 (Architecture Topology)

系统严格实施“发件箱模式”（Transactional Outbox Pattern），在单次 MySQL 业务事务中建立业务数据的同时，原子写入一条代表状态机变更的事件记录，杜绝了“业务落库成功但向 Kafka 发布失败（或者反之）”的分布式双写一致性漏洞。

```text
  [ 交易服务端 / Command Service ] 
               │
               ▼ (在单 async_session 数据库事务中原子写入)
  ┌────────────────────────────────────────────────────────┐
  │                   MySQL 数据库 (Shard)                  │
  ├────────────────────────────┬───────────────────────────┤
  │    A. 业务表 (Business)     │    B. 发件箱表 (Outbox)    │
  │ (Reservation/Orders 状态行) │ (保存未发布的 state 变更事件)│
  └────────────┬───────────────┴────────────┬──────────────┘
               │                            │ (State: UNPUBLISHED)
               │                            ▼
               │                  [ MySQL 磁盘物理持久化 ] (Commit)
               │                            │
               │                            ▼ (SELECT FOR UPDATE SKIP LOCKED 捞取)
               │                  [ 发件箱守护进程 Outbox Publisher ]
               │                            │
               │                            ▼ (高并发顺序投递)
               │                     [ Kafka 消息中枢 ] ◄── (Topic: ticket_events)
               │                            │
               │                            ▼ (Async Consumer Group)
               │                  [ 异步投影器 Projector ]
               │                            │
               │                            ▼ (幂等重算位掩码并覆盖写入)
               └───────────────────────► [ Redis 高速缓存 ] ◄── [ 乘客快速区间查询 (Query) ]
```

---

## 2. 核心组件交互详解 (Core Components Lifecycle)

### A. 事务性发件箱落库 (Transactional Outbox Table Schema)

当用户预占座位、订单生成、完成支付、订单超时失效时，均不直接往 Kafka 物理投递事件。而是随同业务变更，在同一数据库连接、同一事务内，原子写入 `outbox_events` 表：

```sql
CREATE TABLE outbox_events (
    id VARCHAR(36) PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL, -- 聚合根类型（如 RESERVATION, ORDER）
    aggregate_id VARCHAR(50) NOT NULL,   -- 聚合根ID（如订票ID, 订单ID）
    event_type VARCHAR(100) NOT NULL,    -- 事件类型（如 RESERVATION_HELD, ORDER_PAID）
    payload JSON NOT NULL,                -- 契约体 JSON 报文
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed INT DEFAULT 0,              -- 0: 未发送; 1: 已成功投递且提交
    version INT DEFAULT 0
);
```

> **一致性定理**：
> `[ MySQL Transaction ] = [ Create Reservation/Order ] + [ Insert OutboxEvent ]`
> 通过本地数据库的 ACID 强持久化保障，在本地事务提交成功那一瞬间，事件有且仅有一次被 100% 物理保存在了磁盘上，杜绝分布式脏态。

### B. 保序且高并发发布（Outbox Publisher with `SKIP LOCKED`）

为了实现超高吞吐，发布器采用异步并发轮询。然而，多个发布器实例同时轮询 `outbox_events` 会导致重复捞取或行锁碰撞。本项目采用现代化数据库 **`FOR UPDATE SKIP LOCKED`（行级悲观锁且自动跳过被锁行）** 特性实现了无锁冲突并行发布：

```python
# OutboxPublisher.py
stmt = (
    select(OutboxEvent)
    .where(OutboxEvent.processed == 0)
    .order_by(OutboxEvent.created_at.asc())
    .limit(100)
    .with_for_update(skip_locked=True) # 100% 规约分布式冲突
)
```

> **无锁并行吞吐（SKIP LOCKED）机制**：
>
> ```text
>   [ Publisher 1 ] ──► select limit 100 ──► 锁住 001~100 行 ──► 并发投递 Kafka (001~100)
>   [ Publisher 2 ] ──► select limit 100 (自动跳过 001~100) ──► 锁住 101~200 行 ──► 并发投递 (101~200)
> ```
> 各发布器彼此不产生排队等待（零阻塞），达成极致的线性吞吐扩张！

### C. 读侧自愈幂等投影器 (Idempotent Projector Consumer)

这是实现 **C（Command）与 Q（Query）** 最终一致性的最末一级。由于 Kafka 仅保证 **At-least-once（最少一次投递）**，网络波动可能导致事件被 Projector 消费多次。
本项目引入了基于 **`processed_events` 幂等去重表** 的一站式**精确一次（Exactly-once）** 状态拦截器：

```sql
CREATE TABLE processed_events (
    event_id VARCHAR(36) PRIMARY KEY,   -- Kafka 中提取的全局唯一 Event ID
    consumer_name VARCHAR(100) NOT NULL, -- 消费端标识符（如 Projector）
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```python
# Projector.py
async def process_event(db_session, event: dict) -> bool:
    event_id = event["event_id"]
    # 物理尝试向幂等表抢占式写入
    stmt = insert(ProcessedEvent).values(event_id=event_id, consumer_name="projector")
    try:
        await db_session.execute(stmt)
        await db_session.flush() # 抢占成功，说明是首次处理，继续执行
    except IntegrityError:
        return False # 抢占失败（唯一索引冲突），说明已处理过该事件，直接幂等丢弃

    # 1. 提取事件 Payload，重新查询最新的物理 SeatSegments
    # 2. 调用 Projector.recalculate_and_project() 重算区间二进制掩码与可用余票
    # 3. 强力原子性 HSET 覆写 Redis，读模型无缝自愈！
```

---

## 3. 时序协同规约 (Event Timing Sequence Diagram)

以下是全生命周期各组件的时序交互流：

```text
  [Client]    [API Service]    [MySQL DB]    [Outbox Pub]    [Kafka Bus]    [Projector]    [Redis Cache]
     │              │              │              │               │              │               │
     │─1. 购票占位 ─►│              │              │               │              │               │
     │              │──2. 写入业务 ─►│              │               │              │               │
     │              │──3. 写入事件 ─►│              │               │              │               │
     │              │◄──4. Commit ──│              │               │              │               │
     │◄──5. ID 确认──│              │              │               │              │               │
     │              │              │              │               │              │               │
     │              │              │──6. 轮询锁 ─►│               │              │               │
     │              │              │◄──7. 未发行──│               │              │               │
     │              │              │              │──8. 并发投递 ─►│              │               │
     │              │              │              │               │──9. 消费通知 ─►│               │
     │              │              │              │               │              │──10. 幂等拦截 ─► [抢占表]
     │              │              │              │               │              │◄──11. 成功 ───┤
     │              │              │              │               │              │               │
     │              │              │              │               │              │──12. 重算掩码───► [MySQL]
     │              │              │              │               │              │◄──13. 新数据 ───┤
     │              │              │              │               │              │               │
     │              │              │              │               │              │──14. HSET 覆写─► [Redis]
     │              │              │──15. 标记 ──►│               │              │               │
     │              │              │   (processed=1)              │              │               │
     ▼              ▼              ▼              ▼               ▼              ▼               ▼
```

---

## 4. 极致高并发场景下的削峰与防护行为 (High-Load Protection)

> *   **物理削峰**：高并发的查询请求（占系统 90%+ 流量）全部被截流在 Redis [D. EPIC-09] 建立的区间可用缓存上。写请求（预占）也仅在 Redis Lua 快速位运算后放行至后台数据库，保护物理 MySQL 不发生雪崩。
> *   **事件压缩重算（Event Deduplication Strategy）**：若瞬间有 10000 个退票/占票事件向 Kafka 倾泻，Projector 不必傻傻重算 10000 次数据库。因为 Projector 采用“基于车次（`schedule_id`）”的最新状态覆盖写机制。在消费时，通过车次级局部限流重算，可以将冗余的事件中间投影在内存中合并，达成极致的计算自愈效能！

---
**EDA Spec Verified | Outbox Standard Active | Consistency Guranteed**
