# 🌌 12306 高并发票务分配系统 — 敏捷 Epic / User Story 登记与 BDD 验收规约

本文件详细记录了 12306 高并发区间票务分配系统（Python + Vue 3）的敏捷产品契约，作为项目进行需求迭代、工程对齐、功能审计与行为驱动测试（BDD）的全局权威参考。

---

## 1. 敏捷架构大纲 (Agile Hierarchy)

本项目核心交易与查询生命周期被划分为 **4 大史诗特性（Epics）**：

```text
  [ EPIC-01: 读写分离冷热查询 ]
     ├── US-1.1: 快速区间余票查询 (CORS Cross-Origin Query)
     └── US-1.2: 冷启动读视图自愈投影 (Cache-Miss Self-Healing)

  [ EPIC-02: 区间位掩码高并发锁座 ]
     ├── US-2.1: 内存级 Lua 原子锁定 (Redis Bitmask Pre-Lock)
     └── US-2.2: 升序行锁物理落库防重 (MySQL ASC Ordered Segment Lock)

  [ EPIC-03: 交易状态机与自愈回收 ]
     ├── US-3.1: 超时未支付占位自动回收 (Background Cron Reclamation)
     └── US-3.2: 模拟在线支付与票据核销 (Atomic Payment Settle)

  [ EPIC-04: 发件箱事件最终一致性 ]
     ├── US-4.1: 事务性发件箱保序投递 (SKIP LOCKED Outbox Publisher)
     └── US-4.2: 读模型异步投影器 (Idempotent Projector Consumer)
```

---

## 2. Epics & User Stories 深度梳理登记

---

### 🌌 EPIC-01: 读写分离冷热查询 (Read-Write Separation Query)

**业务价值 (Value Proposition)**：将超高频的余票冷查询与底层的写模型（Order/MySQL）进行绝对物理隔离。用户查询不触碰核心写入，从而避免因高频检索耗尽数据库连接池，保障购票通道的畅通。

#### 👤 US-1.1: 快速区间余票查询

- **用户故事**：作为一名准备乘车出行的**旅客**，我希望能够**输入出发站、到达站并快速查询当前是否有商务舱车票**，以便我合理安排我的出行计划。
- **验收标准 (AC-1)**：
  - **Given (前提)**：旅客打开了部署在 Port `8080` 的 Vue 3 响应式 Web 仪表盘，且后端 API 服务在 Port `8000` 正常运行。
  - **When (触发动作)**：旅客在下拉选择框中选择“北京 (Seq 1)”到“上海 (Seq 4)”，并点击“实时余票查询”按钮。
  - **Then (预期结果)**：前端发送 GET 请求至 `/api/v1/query`，余票状态框瞬间渲染并显示当前精准的可用车票数。
- **工程追溯**：
  - **前端 (Vue 3)**：`src/app/static/index.html` -> `queryAvailability()`
  - **后端 (FastAPI)**：`src/app/main.py` -> `@app.get("/api/v1/query")`

#### 👤 US-1.2: 冷启动读视图自愈投影

- **用户故事**：作为一名**系统架构师**，我希望**在 Redis 读模型缓存不存在（冷数据）时，系统能够自动穿透回源并对缓存实施重建投影**，以便高频冷热数据能够无缝过渡，免去人工刷温的运维负担。
- **验收标准 (AC-2)**：
  - **Given (前提)**：Redis 缓存处于冷启动状态，键 `q:availability:1:BUSINESS` 不存在。
  - **When (触发动作)**：旅客发起首次北京->济南的查询请求。
  - **Then (预期结果)**：查询引擎捕捉到 Cache Miss 异常，立刻在后台调用 `Projector.recalculate_and_project`，通过 SQL 查询重算该车次的所有子区间占用位掩码，完成 Redis Hash 覆盖重建，随后秒级返回最新余票数。
- **工程追溯**：
  - **后端**：`src/app/main.py` -> `query_availability()` 里的 `val is None` 拦截自愈分支
  - **组件**：`src/app/projector.py` -> `recalculate_and_project()`

---

### 🌌 EPIC-02: 区间位掩码高并发锁座 (Bitmask Pre-lock)

**业务价值 (Value Proposition)**：支持中国铁路独有的“一车多站、区间动态分配”模式，利用超高性能的二进制位图算法（Bitmask）替代低效的行扫描。在高并发下保障绝对不超卖、不错卖，同时避免数据库死锁。

#### 👤 US-2.1: 内存级 Lua 原子锁定

- **用户故事**：作为一名**高并发购票系统旅客**，我希望在点击抢票时**系统能在内存中进行纳秒级冲突过滤并对区间进行占位**，以便我在抢票高峰期能够获得无延迟的原子级别响应反馈。
- **验收标准 (AC-1)**：
  - **Given (前提)**：当前座位占用值为 `occupied = 0`（全线未售）。
  - **When (触发动作)**：旅客预定天津 (Seq 2) 到济南 (Seq 3) 的车票（对应掩码 Mask = 2，即二进制 `010`），系统执行 Redis Lua 原子预占。
  - **Then (预期结果)**：因为 `(0 & 2) == 0`，锁定成功，占用值更新为 `0 | 2 = 2`。此时，另一个试图抢购北京 (Seq 1) 到上海 (Seq 4)（对应掩码 Mask = 7，二进制 `111`）的旅客触发预占，因 `(2 & 7) != 0`，其预占被毫秒级原子拦截拒绝，提示“余票不足”，保障零超卖。
- **工程追溯**：
  - **Lua 脚本**：`src/app/redis_client.py` -> `reserve_seat.lua`
  - **服务**：`src/app/reservation_service.py` -> `reserve_ticket()` 里的 `get_redis().eval`

#### 👤 US-2.2: 升序行锁物理落库防重

- **用户故事**：作为一名**票务系统管理员**，我希望在物理落库扣减席位区间时**对涉及的段行锁执行固定的自增升序排序加锁**，以便多路高并发线程在争夺同一批席位时永远不会产生底层数据库死锁（Deadlock）而导致系统崩溃。
- **验收标准 (AC-2)**：
  - **Given (前提)**：旅客 A 与旅客 B 在同一时刻点击抢夺同一个物理座位的不同子区间（例如 A 抢 1->2，B 抢 2->3）。
  - **When (触发动作)**：写事务同时对目标 `SeatSegment` 执行 `SELECT ... FOR UPDATE`。
  - **Then (预期结果)**：加锁排序器强制让请求 A 按 `id` 升序对行排序，请求 B 也按相同顺序排序，行锁排队顺控，二者并行分配成功而没有发生任何相互环形加锁阻塞（零死锁），座位利用率完美翻倍。
- **工程追溯**：
  - **服务**：`src/app/reservation_service.py` -> `order_by(SeatSegment.seat_id.asc(), SeatSegment.segment_no.asc())`

---

### 🌌 EPIC-03: 交易状态机与自愈回收 (Transaction State Machine)

**业务价值 (Value Proposition)**：规范购票订单生命周期的单向状态流转，通过强一致的写状态机配合异步定时 Worker 进行过期倒带清理，确保因放弃支付流失的座席资源能迅速流转回公共销售池。

#### 👤 US-3.1: 超时未支付占位自动回收

- **用户故事**：作为一名**希望购票的其他乘车人**，我希望**那些占了座位却在 60 秒内（MVP 超期）没有付款的订单能够被系统自动倒带回收**，以便我有机会买到这些流失回库的有效车票。
- **验收标准 (AC-1)**：
  - **Given (前提)**：旅客 A 成功预占了北京 (Seq 1) 到天津 (Seq 2) 的车票，生成 HELD 状态占位，但未进行支付，超过了 60 秒。
  - **When (触发动作)**：系统管理员点击前端“触发超时释放”按钮，或后台 Cron 调度器被拉起。
  - **Then (预期结果)**：超时回收事务启动，旅客 A 的占位状态强转为 `RELEASED`，对应的 `SeatSegment` 状态还原为 `AVAILABLE`，同时向 Redis 发送 `release_seat.lua` 倒带清除该区间的二进制位（bit 0 变回 0）。旅客 B 再次查询该区间，余票数瞬间重置自愈，可重新被抢购。
- **工程追溯**：
  - **后端**：`src/app/order_service.py` -> `release_expired_reservations()`
  - **端点**：`src/app/main.py` -> `@app.post("/api/v1/cron/release")`

#### 👤 US-3.2: 模拟在线支付与票据核销

- **用户故事**：作为一名**已经锁定席位的购票人**，我希望在锁定时间内**一键确认支付并扣款核销**，以便我获得一张合法的乘车纸质/电子票据。
- **验收标准 (AC-2)**：
  - **Given (前提)**：旅客已处于 Reservation `HELD` 占位状态，且倒计时未结束。
  - **When (触发动作)**：旅客在 Vue 3 前端一键点击“确认模拟支付”按钮。
  - **Then (预期结果)**：前端链式调用 `/api/v1/order` 生成待付单（`WAITING_PAYMENT`），随后调用 `/api/v1/pay` 执行扣款核销，状态机扭转为终态 `CONFIRMED`，倒计时动画完美停止，前端渲染展示包含车厢、席位号和金额的精美虚拟车票。
- **工程追溯**：
  - **服务**：`src/app/order_service.py` -> `create_order()` 与 `pay_order()`
  - **前端**：`src/app/static/index.html` -> `payOrder()`

---

### 🌌 EPIC-04: 发件箱事件最终一致性 (Transactional Outbox CQRS)

**业务价值 (Value Proposition)**：高并发下的分布式通信难点在于：如何确保“数据库状态更新”与“MQ 消息发送”百分之百同时成功或同时失败。通过事务性发件箱（Transactional Outbox）保障事件投递与读模型投影的 100% 最终一致性。

#### 👤 US-4.1: 事务性发件箱保序投递

- **用户故事**：作为一名**系统消息总线 Worker**，我希望在拉取最新未发送事件时**利用物理行锁过滤已经被其他 Worker 消费的记录**，以便在高负载集群下事件能被绝无重合、绝无冲突、严格保序地发布至 Kafka。
- **验收标准 (AC-1)**：
  - **Given (前提)**：MySQL 的发件箱 `outbox_event` 表中堆积了多笔 `NEW` 状态的订单支付成功事件。
  - **When (触发动作)**：后台多路 Outbox Publisher 定时 Worker 同时扫表。
  - **Then (预期结果)**：扫表查询使用 `FOR UPDATE SKIP LOCKED`。Worker 1 锁定的行将被 Worker 2 物理跳过，各路 Worker 互不干扰，将数据安全投递至 Kafka，并将 `status` 变更为 `PUBLISHED`，提交事务。
- **工程追溯**：
  - **后台进程**：`src/tests/test_outbox_publisher.py` (采用 `SKIP LOCKED` 实现)

#### 👤 US-4.2: 读模型异步幂等投影器

- **用户故事**：作为一名**读模型数据投影服务**，我希望在消费 Kafka 的购票/支付事件时**能够拦截一切网络抖动导致的重复投递**，以便读侧 Redis 的余票高速缓存不会因为重复计算而产生混乱。
- **验收标准 (AC-2)**：
  - **Given (前提)**：Kafka 消费者收到了一笔 `ORDER_PAID` 事件（携带唯一标识 `event_id`）。
  - **When (触发动作)**：投影处理器执行数据更新。
  - **Then (预期结果)**：投影器首先在单事务内向 `processed_event` 表中插入 `(consumer_group, event_id)` 唯一索引记录。如果该事件由于网络重发再次投递，唯一约束触发将导致其被直接抛弃，实现完美幂等（Idempotency），随后安全重算 Redis 可售数量，读侧获得最终一致。
- **工程追溯**：
  - **投影逻辑**：`src/app/projector.py` -> `process_event_idempotent()`

---

## 3. BDD Gherkin 契约对齐文件

上述 User Stories 与底层业务规则已被 100% 集成、固化至项目根目录的 BDD 特征文件 **`src/tests/features/ticketing.feature`** 中。
我们通过 **`pytest-bdd`** 自动化行为测试框架，对这些验收契约进行了无死角绿灯核销：

```gherkin
Feature: 12306 High-Concurrency Ticketing MVP BDD Acceptance
  As a railway passenger
  I want to query and book sub-route train tickets safely
  So that seat inventory remains strictly consistent without over-selling or deadlocks

  # 对应 US-2.1 与 US-2.2：区间预占与升序行锁原子保障
  Scenario: Successful sub-route seat reservation
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    When passenger requests to reserve a ticket from sequence 1 to 2
    Then the system should grant a reservation ID
    And the MySQL seat segment 1 should be marked as "HELD"
    And the Redis seat mask should reflect the reservation

  # 对应 US-2.1 的区间冲突验证：重叠区段买断，非重叠区段并行占用
  Scenario: Reject overlapping sub-route booking
    Given a passenger has already reserved a ticket from sequence 1 to 2
    When another passenger attempts to reserve a ticket from sequence 2 to 3
    And another passenger attempts to reserve an overlapping ticket from sequence 1 to 3
    Then the non-overlapping booking should succeed
    And the overlapping booking should be rejected as "No seats available"

  # 对应 US-3.2 与 US-4.2：模拟支付状态机与 EDA 最终一致性投影自愈
  Scenario: Payment confirmation triggers eventual consistency
    Given a passenger has successfully reserved a ticket from sequence 1 to 2
    And they have established an order for that reservation
    When they complete payment for the order
    And the background event processor consumes the "ORDER_PAID" event
    Then the MySQL seat segment 1 should be "CONFIRMED"
    And the Redis query model for route 1 to 3 should return 0 available seats
```

---

## 4. 生产级运维支撑与可观测性 (DevOps & SRE Ops Support)

为了将上述史诗特性（Epics）和行为契约（BDD）无缝护航部署，本项目配备了标准的 **DevOps 自动化与可观测性套件**：

### 4.1 SRE 级微服务健康探针 (GET /api/v1/ops/health)

本系统在交易网关中内置了高标准的存活与就绪性检测探针，用于支持 Kubernetes/Docker 容器集群的自愈调度：

- **MySQL 探测**：在数据库连接池中执行极速的 `SELECT 1` 心跳嗅探。
- **Redis 探测**：在锁内存中执行 `PING-PONG` 极速网络和可用性握手。
- **响应格式**：
  ```json
  {
    "status": "UP",
    "components": {
      "mysql": "OK",
      "redis": "OK"
    }
  }
  ```

### 4.2 自动化配席重组工具 (ops/seed_db.py)

用于支持每日“列车运行计划发布”时的数据库配席和余票初始化：

1. **DDL 原子重建**：一键清除历史冗余脏数据，重构最新的物理表约束。
2. **高速缓存清刷**：执行 `redis.flushdb()` 清除无效的旧版读视图。
3. **主数据与区间席位填充**：原子注册 `G888` 次列车计划，并对席位依次分配 1-2、2-3、3-4 物理占用区间。
4. **读缓存预热 (Pre-heating)**：直接触发 Projector 预算，将最新余票投影至 Redis Hash 中。

### 4.3 极简运维控制台一键脚本 (ops.sh)

为了大幅降低开发与运维门槛，项目根目录集成了免配置的 `./ops.sh` 脚本工具：

- **`./ops.sh seed`**：触发自动数据配席初始化，重建数据库。
- **`./ops.sh health`**：调用后台探针返回最精确的系统可用性 JSON 指标报告。
- **`./ops.sh status`**：一键并行感知后端（Port 8000）与 Vue 3 前端（Port 8080）的运行状态。
- **`./ops.sh test`**：拉起完整全量测试流水线，一键核销 100% 绿灯指标。
