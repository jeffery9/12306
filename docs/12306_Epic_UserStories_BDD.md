# 🌌 12306 高并发票务分配系统 — 敏捷 Epic / User Story 登记与 BDD 验收规约

本文件详细记录了 12306 高并发区间票务分配系统（Python + Vue 3）的敏捷产品契约，作为项目进行需求迭代、工程对齐、功能审计与行为驱动测试（BDD）的全局权威参考。

---

## 1. 敏捷架构大纲 (Agile Hierarchy)

本项目核心交易与查询生命周期被划分为 **5 大史诗特性（Epics）**：

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

  [ EPIC-05: 铁路局端运营调度后台 ]
     ├── US-5.1: 动态配票定价与里程费率配置 (Dynamic Revenue Pricing)
     ├── US-5.2: 可视化席位区间占用热力图 (Visual Live Heatmap)
     └── US-5.3: 应急席位划拨与官方硬锁隔离 (Emergency Requisition Block)

  [ EPIC-06: 智能化座席管理与选座编排 ]
     ├── US-6.1: 多人邻座自动分配与席位偏好 (Adjacent Allocation & Preference)
     ├── US-6.2: 同车中途断配智能换座推荐 (Split-Seat Smart Recomposition)
     └── US-6.3: 车厢席位平面图与点击选座 (Interactive Carriage 2D Seatmap)

  [ EPIC-07: 票额分配控制与动态票池管理 ]
     ├── US-7.1: 分段阶梯限售与长途优先保障池隔离 (Long-Distance Quota Isolation)
     ├── US-7.2: 临售时限未售配额动态解锁与共享 (Dynamic Quota Sharing & Auto-Release)
     └── US-7.3: 弹性安全库存缓冲与候补溢出超配 (Elastic Safety Buffer & Standby Queue)
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

### 🌌 EPIC-05: 铁路局端运营调度后台 (Railway Bureau Operations)

**业务价值 (Value Proposition)**：为铁路局端（12306 运营调度、收益管控、应急防务部门）提供底层车次生命周期控制。支持动态区间收益阶梯调价、席位段物理状态的可视化热力透视、以及物理区间的“应急官方硬锁隔离”，从而实现列车在安全与收益层面的精细化编排。

#### 👤 US-5.1: 动态配票定价与里程费率配置

- **用户故事**：作为一名**铁路局收益管理员**，我希望能够**动态调整特定车次和席别在不同运行区间、不同旺季的每里程基准票价单价**，以便精准实现收益最大化（Revenue Management）并响应市场供需。
- **验收标准 (AC-1)**：
  - **Given (前提)**：铁路局调度系统发布了 G888 次商务舱的基准费率（如每公里 1.2 元）。
  - **When (触发动作)**：收益管理员将该费率上调至 1.5 元。
  - **Then (预期结果)**：此后，当旅客对“北京-天津”和“天津-济南”段购票时，订单服务核心自动根据最新费率重算该区间的物理公里单价（如北京-天津 120km = 180元），保证费率变更对未锁定订单秒级生效。

#### 👤 US-5.2: 可视化席位区间占用热力图

- **用户故事**：作为一名**铁路局列车运营调度员**，我希望能够**在一个大屏上可视化查看指定日期车次每个物理座席在全部区间上的状态热力矩阵**，以便我实时掌握列车席位段的周转利用率（Seat Turn Rate）并决策是否增开。
- **验收标准 (AC-2)**：
  - **Given (前提)**：G888 次车 01 车厢 01A 座位，北京-天津段已被购买，济南-上海段被预占 HELD，天津-济南段闲置 AVAILABLE。
  - **When (触发动作)**：调度员输入车次、发车日查询席位热力大屏。
  - **Then (预期结果)**：系统返回一个二维状态矩阵（Seat-Segment Matrix），清晰将 01A 座在 bit 0（段 1）标记为 CONFIRMED（红色），bit 1（段 2）标记为 AVAILABLE（绿色），bit 2（段 3）标记为 HELD（黄色），热力分布毫秒级透视。

#### 👤 US-5.3: 应急席位划拨与官方硬锁隔离

- **用户故事**：作为一名**铁路局安全防务协调员**，我希望能够**强制硬性锁定列车的特定席位及对应区间段（标记为官方隔离或应急预备）**，以便保障列车运行保障人员、技术抢修员的硬性占座，防止其被公众网络票流抢占。
- **验收标准 (AC-3)**：
  - **Given (前提)**：列车 01 车厢 02A 座位全段状态为空闲 AVAILABLE。
  - **When (触发动作)**：安全员在局端调度台对 02A 的“北京-天津”（段 1）执行“应急官方隔离锁定”（更新 MySQL 记录为 `BLOCKED`，并将 Redis 对应位掩码 bit 0 开关硬性设为 1，所有权人标识为 `OFFICIAL_EMERGENCY_REQUISITION`）。
  - **Then (预期结果)**：当普通公众购票人试图在北京-天津段抢占 02A 座时，Redis 内存位图过滤和 MySQL 区间排他行锁直接返回 `Seat Segment Blocked` 异常拒绝订购，确保国家级应急指挥/抢修通道 100% 绝对畅通。

---

### 🌌 EPIC-06: 智能化座席管理与选座编排 (Smart Seat Allocation)

**业务价值 (Value Proposition)**：将基础的资源扣减提升为高水准的人性化、智能化服务。通过多人邻座自动锁定技术、同车断配自愈推荐算法以及 2D 车厢动态图谱，大幅提升客流成行率与车席周转收益。

#### 👤 US-6.1: 多人邻座自动分配与席位偏好

- **用户故事**：作为一名与家人共同出行的**旅客**，我希望在**一次提交多人订单时系统能够自动寻找并锁定物理上相邻的座位，或者在单人出行时能自选“靠窗/过道”**，以便我们能舒适、贴近地共度旅途。
- **验收标准 (AC-1)**：
  - **Given (前提)**：G888 次车 01 车厢有 01A-01C（邻座对）空闲，02A（靠窗，无邻座）空闲。
  - **When (触发动作)**：旅客 A 与 B 选择共同购票。
  - **Then (预期结果)**：算法检测到这是一个双人组合，自动过滤单空座位，优先匹配并在单事务中锁死 01A、01C 的行锁并返回。若是单人购票并勾选“靠窗（Window）”，则算法自动挑选 02A 并加锁，完美照顾个性化偏好。

#### 👤 US-6.2: 同车中途断配智能换座推荐

- **用户故事**：作为一名**没买到北京到上海直达车票的急切旅客**，我希望**系统能在全车没有一个座位是全程空闲时，智能组装并推荐一个同车中途换座的拼座位方案**，以便我依然可以搭乘这一班次回家。
- **验收标准 (AC-2)**：
  - **Given (前提)**：G888 次车“北京->上海”直达可售票数为 0。但 01A 座位在段 1（北京-天津）空闲，02C 座位在段 2-3（天津-上海）空闲。
  - **When (触发动作)**：旅客发起“北京->上海”的购票查询。
  - **Then (预期结果)**：常规查询返回 0。但智能推荐引擎（Recombinator）自动识别到同车断配重组可能性，向旅客弹窗推荐：“为您找到同车换座方案：北京-天津(01A) + 天津-上海(02C)”。旅客点击确认后，系统在单事务内同时完成这两个席位不同物理段的 Redis + MySQL 原子预占。

#### 👤 US-6.3: 车厢席位平面图与点击选座

- **用户故事**：作为一名**对座位有强迫症的极客乘客**，我希望能够**在前端看到当前车厢的 2D 座位布局和每个座位的区间空闲热力，并可以直接点击某个空窗座位进行锁座**，以便我获得 100% 的自主掌控权。
- **验收标准 (AC-3)**：
  - **Given (前提)**：前端 Vue 3 Web App 中加载了 2D 车厢席位大图。
  - **When (触发动作)**：旅客点击选中 01F 席位（其在所选区间上显示为绿色空闲）。
  - **Then (预期结果)**：前端发送携带 `seat_id=01F` 强主键绑定的 `/reserve` 锁座呼叫。后端锁座逻辑跳过任意分配（Random Allocation），直接定向对 01F 执行排他加锁事务，抢占成功后将该 2D 坐标变为黄色/红色。

---

### 🌌 EPIC-07: 票额分配控制与动态票池管理 (Ticket Pool & Quotas)

**业务价值 (Value Proposition)**：精细化调配长短途客流，最大化单车公里总收益。通过长途物理保障池在初期的硬隔离，杜绝短途旅客“切碎”长途票源；并在邻近开车时通过自动定时策略解锁回拢配额，注入共享票池，实现空座率为零的完美周转。

#### 👤 US-7.1: 分段阶梯限售与长途优先保障池隔离

- **用户故事**：作为一名**铁路局票额分配总监**，我希望能够**将列车指定车次的一定比例席位划入“长途专售票池”中（初始仅允许北京->上海全线购买，禁止中途站区间预订）**，以便最大化客单价和铁路里程收益。
- **验收标准 (AC-1)**：
  - **Given (前提)**：01车厢 01A、01C 被划入“长途专售票池”。
  - **When (触发动作)**：普通公众购买人试图买北京->天津段（段 1）的 01A、01C。
  - **Then (预期结果)**：预占引擎检测到这些席位正处于长途限售保障期内（未到解锁阈值），立即返回 `Quota Restricted` 错误拒绝。只有当旅客买北京->上海（段 1-3 全线）时，才允许锁座，完美保全高价值全线票流。

#### 👤 US-7.2: 临售时限未售配额动态解锁与共享复用

- **用户故事**：作为一名**平时买不到车票的短途出行旅客**，我希望在**开车前 24 小时（或模拟时限阈值内），系统能自动将长途池中未售出的剩余座位合并至“共享票池”**，以便我能捡漏抢到这些放开限制的短途车票。
- **验收标准 (AC-2)**：
  - **Given (前提)**：开往上海的 G888 次列车还有 12 小时发车，长途专售池中 01A、01C 座位仍旧未售出。
  - **When (触发动作)**：后台动态解锁定时器（Quota Releaser）或管理员触发配额回拢。
  - **Then (预期结果)**：系统运行 `ops/release_quotas` 自动解锁事务，将 01A、01C 移出隔离专售池，并将其二进制锁掩码对公众放开。此时短途旅客查询北京-天津（段 1）或天津-济南（段 2），原先显示为 0 的余票数瞬间变为 2，实现配额的自愈共享，提高车辆载客率。

#### 👤 US-7.3: 弹性安全库存缓冲与候补溢出超配

- **用户故事**：作为一名**购票高峰期候补旅客**，我希望在**普通共享票池显示售罄时，系统能够允许我提交“候补占位（Standby Queue）”订单并锁定高优先级退票**，以便我在车票倒带释放的第一时间自动排队补位，无需肉眼刷票。
- **验收标准 (AC-3)**：
  - **Given (前提)**：列车普通票池余票显示为 0。系统配置了 2 张座位的“弹性超配/候补安全缓冲区”。
  - **When (触发动作)**：旅客点击提交候补订单。
  - **Then (预期结果)**：预占事务并不返回 Sold Out，而是提示“候补成功，处于排队第 1 位”，状态显示为 `WAITING_FOR_STANDBY`。一旦系统有人未支付超时释放车票（US-3.1 触发），或有人在线退票，后台保序发布器（Outbox）事件自动触发投影重算，该空余座位按 FIFO 原则秒级自动划拨给候补排队旅客，核销生成 CONFIRMED 实体车票。

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

  # 对应 US-5.3：官方官方应急锁段，拒绝普通客流预占
  Scenario: Railway bureau coordinator blocks a seat segment for emergency crew use, rejecting passenger booking
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    When the railway bureau coordinator issues an emergency block on segment 1 for official crew reservation
    Then the MySQL seat segment 1 should be marked as "BLOCKED"
    And the Redis seat mask should reflect the official requisition block
    When a regular passenger attempts to reserve a ticket from sequence 1 to 2
    Then their booking request should be rejected as "Seat segment blocked for official use"

  # 对应 US-5.1：收益定价调整，后续结算金额更新
  Scenario: Revenue manager adjusts dynamic pricing rate and updates passenger billing amount
    Given the dynamic base tariff rate for "BUSINESS" is set to 1.2 yuan per km
    When the revenue manager increases the dynamic base tariff rate to 1.5 yuan per km
    And a passenger creates an order for route sequence 1 to 2 (120 km)
    Then the order payment amount should reflect the updated pricing tariff of 180.00 yuan

  # 对应 US-6.1：多人出行邻座自动分配
  Scenario: Traveling group of two passengers requests booking, receiving adjacent physical seats automatically
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And adjacent seats "01A" (Window) and "01C" (Aisle) in Carriage 1 are fully available
    When a traveling group of 2 passengers requests to reserve seats from sequence 1 to 3
    Then the system adjacent seat locator should lock both seats "01A" and "01C" in Carriage 1
    And both passengers should receive unified booking details on the same order

  # 对应 US-6.2：同车断配拼座换座自愈推荐
  Scenario: No direct single seat available from start to end, system recomposes a split-seat route for the passenger
    Given the direct ticket availability for train "G666" from sequence 1 to 4 is fully sold out
    And Seat "01A" is available only for segment 1 to 2 (北京-天津)
    And Seat "02C" is available only for segment 2 to 4 (天津-上海)
    When a passenger queries tickets from sequence 1 to 4
    Then the smart recomposition engine should propose a split-seat itinerary "Seat 01A (Seg 1-2) + Seat 02C (Seg 2-4)"
    When the passenger confirms the split-seat itinerary
    Then the system should atomic-reserve segment 1-2 on Seat 01A and segment 2-4 on Seat 02C in a single transaction

  # 对应 US-7.1：长途专售隔离，限制短途购票
  Scenario: Long-distance safeguard pool restricts short-distance booking to preserve full-journey ticket assets
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And Seat "01A" is allocated in the long-distance safeguard pool (Sequence 1 to 3 exclusive)
    When a passenger attempts to reserve Seat "01A" for short-distance from sequence 1 to 2
    Then the reservation engine should reject the booking as "Quota restricted"
    When another passenger attempts to reserve Seat "01A" for full-journey from sequence 1 to 3
    Then the reservation should succeed with a valid reservation ID

  # 对应 US-7.2：临离发车配额自动释放共享
  Scenario: Unsold long-distance quotas are auto-released near departure time, enabling short-distance bookings
    Given Seat "01A" was locked in the long-distance safeguard pool for full-journey sequence 1 to 3
    And the time to departure is within 24 hours threshold
    When the automatic quota releaser triggers allocation merger
    Then the long-distance isolation lock on Seat "01A" should be dynamic-released
    And the short-distance queries for sequence 1 to 2 should now return 1 available seat
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
