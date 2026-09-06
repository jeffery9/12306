# 12306 高并发票务系统 Python MVP 技术设计文档

**文档日期：2026-09-06**  
**技术栈：Python 3.11 + FastAPI + MySQL 8 + Redis 7 + Kafka + SQLAlchemy (Async)**

---

## 1. 总体架构设计 (CQRS + Outbox)

系统在架构上保证：
1. **Query Service**：仅访问 Redis 读缓存与 Local Cache，绝对不回源 MySQL，允许短暂的最终一致性（秒级延迟）。
2. **Command Service**：负责核心预占与写盘，直接对 Redis Command 层进行排他锁控制，持久化至 MySQL，并通过 **Transactional Outbox** 发送事件。
3. **事件驱动流向**：
   `Command 写盘 + Outbox 落地 (同一本地事务)` -> `Outbox Publisher (后台线程/独立进程) 轮询发至 Kafka` -> `Query Projector 消费 Kafka` -> `更新 Query Redis 余票计数`。

---

## 2. 数据库与缓存数据模型 (第一阶段)

### 2.1 MySQL 关系模型

```sql
-- 1. 车次表
CREATE TABLE train (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(16) NOT NULL UNIQUE,  -- 例如 G123
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 车站与经停顺序表
CREATE TABLE station (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    train_id BIGINT NOT NULL,
    name VARCHAR(64) NOT NULL,
    sequence INT NOT NULL,              -- 站序：北京=0, 天津=1, 济南=2...
    FOREIGN KEY (train_id) REFERENCES train(id),
    UNIQUE KEY uk_train_station (train_id, name),
    UNIQUE KEY uk_train_sequence (train_id, sequence)
);

-- 3. 日期车次实例表 (Schedule)
CREATE TABLE train_schedule (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    train_id BIGINT NOT NULL,
    service_date DATE NOT NULL,         -- 运行日期：2026-09-06
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    FOREIGN KEY (train_id) REFERENCES train(id),
    UNIQUE KEY uk_train_date (train_id, service_date)
);

-- 4. 物理座位表
CREATE TABLE seat (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    schedule_id BIGINT NOT NULL,
    carriage_no VARCHAR(16) NOT NULL,   -- 车厢号，例如 "02"
    seat_no VARCHAR(16) NOT NULL,       -- 座位号，例如 "08A"
    seat_class VARCHAR(32) NOT NULL,    -- 二等座/一等座/商务座
    FOREIGN KEY (schedule_id) REFERENCES train_schedule(id),
    UNIQUE KEY uk_schedule_seat (schedule_id, carriage_no, seat_no)
);

-- 5. 区间座位占用明细表 [CRITICAL - 核心防超卖底线]
CREATE TABLE seat_segment (
    schedule_id BIGINT NOT NULL,
    seat_id BIGINT NOT NULL,
    segment_no INT NOT NULL,            -- 区间序号，0表示S0(站0->站1)，1表示S1...
    reservation_id VARCHAR(64) NULL,    -- 锁定此区间的 Reservation ID (NULL 表示空闲)
    state VARCHAR(16) NOT NULL DEFAULT 'AVAILABLE', -- AVAILABLE, HELD, SOLD
    version BIGINT NOT NULL DEFAULT 0,  -- 乐观锁版本号
    PRIMARY KEY (schedule_id, seat_id, segment_no),
    FOREIGN KEY (seat_id) REFERENCES seat(id)
);

-- 6. 预占事务记录表 (Reservation)
CREATE TABLE reservation (
    id VARCHAR(64) PRIMARY KEY,         -- R 开头的唯一预占 ID
    request_id VARCHAR(64) NOT NULL,    -- 客户端唯一幂等 Key (UUID)
    schedule_id BIGINT NOT NULL,
    seat_id BIGINT NOT NULL,
    from_segment INT NOT NULL,          -- 开始区间序
    to_segment INT NOT NULL,            -- 结束区间序
    state VARCHAR(32) NOT NULL,         -- HELD, CONFIRMED, RELEASED
    expires_at TIMESTAMP NOT NULL,      -- 锁票过期时间 (例如创建后 15 分钟)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_request (request_id),
    FOREIGN KEY (schedule_id) REFERENCES train_schedule(id),
    FOREIGN KEY (seat_id) REFERENCES seat(id)
);

-- 7. 订单主表
CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,         -- O 开头的订单 ID
    request_id VARCHAR(64) NOT NULL,    -- 客户端订单创建幂等 Key
    reservation_id VARCHAR(64) NOT NULL, -- 关联的预占 ID
    state VARCHAR(32) NOT NULL,         -- WAITING_PAYMENT, CONFIRMED, EXPIRED, CANCELLED
    total_amount DECIMAL(18,2) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_request (request_id),
    UNIQUE KEY uk_reservation (reservation_id),
    FOREIGN KEY (reservation_id) REFERENCES reservation(id)
);

-- 8. 事务性发件箱 (Transactional Outbox)
CREATE TABLE outbox_event (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id CHAR(36) NOT NULL,         -- UUID，事件唯一标志
    aggregate_type VARCHAR(64) NOT NULL,-- 聚合类型 (Order, Reservation)
    aggregate_id VARCHAR(64) NOT NULL,  -- 聚合根 ID
    event_type VARCHAR(128) NOT NULL,   -- 事件类型 (OrderCreated, TicketIssued)
    payload JSON NOT NULL,              -- 结构化事件内容
    status VARCHAR(16) NOT NULL DEFAULT 'NEW', -- NEW, PUBLISHED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP NULL,
    UNIQUE KEY uk_event (event_id)
);

-- 9. 已处理事件幂等表 (消费端防重复消费)
CREATE TABLE processed_event (
    consumer_name VARCHAR(128) NOT NULL,
    event_id CHAR(36) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, event_id)
);
```

### 2.2 Redis 键值设计

#### A. Command Redis (预占库)
*   **座位二进制占用位图**: `r:{schedule_id}:seat:{seat_id}` -> 整数形式 (如 `6` 代表 `0110`)
*   **预占单 Hash 元数据**: `r:{schedule_id}:reservation:{reservation_id}` -> Hash `{seat_id, mask, state, expires_at}`

#### B. Query Redis (余票查询缓存库)
*   **区间余票缓存**: `q:availability:{schedule_id}:{from_station_name}:{to_station_name}:{seat_class}` -> 整数 (如 `"45"`)

---

## 3. 原子预占引擎与核心业务流程 (第二阶段)

### 3.1 Redis Lua 原子预占脚本 (r_reserve_seat.lua)

抢票时，利用位图 AND 运算判断是否冲突。如果无冲突，执行 OR 运算更新位图，并原子保存预占单，防止多线程 TOCTOU（Time-of-Check to Time-of-Use）竞争。

```lua
-- KEYS[1]: seat occupancy key (r:{schedule_id}:seat:{seat_id})
-- KEYS[2]: reservation metadata key (r:{schedule_id}:reservation:{reservation_id})
-- ARGV[1]: request_mask (e.g. 6 for 0110)
-- ARGV[2]: reservation_id
-- ARGV[3]: ttl_seconds (e.g. 900 for 15 min)

local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]
local ttl = tonumber(ARGV[3])

-- 1. 获取当前座位区间占用位图
local occupied = redis.call("GET", seat_key)
if occupied == false then
    occupied = 0
else
    occupied = tonumber(occupied)
end

-- 2. 判断请求区间是否已被重叠占用 (按位与操作)
-- 在 Lua 5.1/Redis 中，我们使用 bit.band
local bit = require("bit")
if bit.band(occupied, mask) ~= 0 then
    return 0 -- 已经被占用，锁票失败
end

-- 3. 原子标记占用 (按位或操作)
local new_occupied = bit.bor(occupied, mask)
redis.call("SET", seat_key, new_occupied)

-- 4. 写入预占详情，设置 TTL (自动超时释放)
redis.call("HSET", res_key, "reservation_id", res_id, "mask", mask, "state", "HELD")
redis.call("EXPIRE", res_key, ttl)

return 1 -- 锁定成功
```

### 3.2 Redis Lua 原子释放脚本 (r_release_seat.lua)

在支付超时或用户主动取消预占时，必须释放座位占用位图。为了安全，必须校验预占单的存在性与所有权，防止误释放其他订单的锁。

```lua
-- KEYS[1]: seat occupancy key (r:{schedule_id}:seat:{seat_id})
-- KEYS[2]: reservation metadata key (r:{schedule_id}:reservation:{reservation_id})
-- ARGV[1]: request_mask (e.g. 6 for 0110)
-- ARGV[2]: reservation_id

local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]

-- 1. 验证预占单是否存在且 ID 是否匹配
local stored_res_id = redis.call("HGET", res_key, "reservation_id")
if stored_res_id ~= res_id then
    return 0 -- 不属于此 reservation，或者已被释放，直接返回
end

-- 2. 获取当前座位区间占用位图
local occupied = redis.call("GET", seat_key)
if occupied == false then
    occupied = 0
else
    occupied = tonumber(occupied)
end

-- 3. 按位清除指定区间 (new = occupied & ~mask)
local bit = require("bit")
local clean_mask = bit.bnot(mask)
local new_occupied = bit.band(occupied, clean_mask)

-- 4. 更新座位位图，删除预占单
redis.call("SET", seat_key, new_occupied)
redis.call("DEL", res_key)

return 1 -- 释放成功
```

---

### 3.3 核心 Command 业务流程设计

#### A. 购票核心：创建预占 (POST /api/v1/reservations)
1. **网关层限流**：校验 `Idempotency-Key`。若已处理，直接返回历史结果。
2. **逻辑校验**：查询 `train_schedule`、`station` 获取乘车站序，转换起止区间并计算 **request_mask**。
   * 例如：北京(0) -> 济南(2)，占用 S0, S1 两个区间。
   * `from_seq = 0`, `to_seq = 2`。
   * `mask = rangeMask(0, 2) = 3` (二进制 `0011`)。
3. **选择座位**：
   * 在 MySQL/Redis 缓存中找出符合当前车次和席别的所有物理座位 `seat_id` 列表。
   * 对每个座位，顺序或并发调用 Redis Lua 预占脚本，直到某一个座位返回 `1` (预占成功)。
   * 如果所有座位均返回 `0`，则表明当前席别无可用座位，直接抛出 `400`（余票不足），**拒绝后续所有物理数据库开销**。
4. **生成物理事务**：
   * 启动数据库事务。
   * 锁住 `reservation` 表（幂等校验）。
   * 对 `seat_segment` 进行强校验更新：
     ```sql
     UPDATE seat_segment 
     SET reservation_id = :res_id, state = 'HELD', version = version + 1
     WHERE schedule_id = :sched_id AND seat_id = :seat_id 
       AND segment_no >= :from_seg AND segment_no < :to_seg
       AND state = 'AVAILABLE' AND reservation_id IS NULL;
     ```
   * 校验 `affected_rows == (to_segment - from_segment)`。如果不相等，抛出并发冲突，**回滚事务**（保障不超卖）。
   * 插入 `reservation` 表，状态设为 `HELD`，到期时间 `expires_at = now() + 15 minutes`。
   * 写入 `outbox_event`：包含事件 `ReservationHeld`（包含车次、席别、锁定的具体座位和起止站信息）。
   * 提交事务。返回 `201 Created` 与 `reservation_id`。

#### B. 确认订单：生成订单并等待支付 (POST /api/v1/orders)
1. **基础校验**：传入 `reservation_id`。
2. **校验预占状态**：从数据库读取对应的 `reservation`，确认其 `state = 'HELD'` 且未过期。
3. **物理事务落库**：
   * 开启事务。
   * 插入 `orders` 表，状态为 `WAITING_PAYMENT`，到期时间同步 `reservation.expires_at`。
   * 写入 `outbox_event` 表，事件为 `OrderCreated`。
   * 提交事务。

#### C. 支付确认：确认购票 (POST /api/v1/orders/{order_id}/pay)
1. **开启本地事务**。
2. **状态变迁**：
   * 将 `orders` 状态修改为 `CONFIRMED`（前置状态必须为 `WAITING_PAYMENT`，状态冲突则返回 0 affected_rows 抛错）。
   * 将对应的 `reservation` 状态修改为 `CONFIRMED`。
   * 将 `seat_segment` 中对应 `reservation_id` 的状态修改为 `SOLD`。
   * 写入 `outbox_event` 表，事件为 `OrderPaid` (包含 `InventoryChanged` 余票变动逻辑)。
3. **Redis 同步确认**：
   * 将 Redis 中该 `reservation_id` 的元数据 `state` 原子更新为 `CONFIRMED` 并移除 TTL。
4. **提交事务**。返回支付成功。

#### D. 超时回收：定时任务释放
由于客户端或支付回调可能丢失，我们配置一个后台线程定时查询过期未支付的订单：
1. **扫描未支付超时预占**：
   ```sql
   SELECT id, seat_id, from_segment, to_segment FROM reservation 
   WHERE state = 'HELD' AND expires_at < NOW() 
   FOR UPDATE SKIP LOCKED LIMIT 100;
   ```
2. **物理原子释放事务**：
   * 开启事务。
   * 将 `reservation` 状态设为 `RELEASED`。
   * 将 `seat_segment` 对应行重置 `reservation_id = NULL, state = 'AVAILABLE'`。
   * 如果该预占有关联的 orders 表记录，将 `orders` 状态修改为 `EXPIRED`。
   * 写入 `outbox_event` 表，事件为 `ReservationReleased`。
   * 提交事务。
3. **Redis 同步释放**：
   * 调用 r_release_seat.lua 释放 Redis 占用位图。

   ---

   ## 4. 异步事件驱动与最终一致性投影 (第三阶段)

   ### 4.1 Transactional Outbox 发行器 (Outbox Publisher)

   为了保证“本地事务写盘”与“发布事件到 Kafka”的原子性，Command 业务流程只写入 `outbox_event` 表。独立运行的 `OutboxPublisher` 后台协程（或独立后台进程）负责持续轮询并可靠推送：

   1. **高可靠轮询查询**：
      ```sql
      SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload 
      FROM outbox_event 
      WHERE status = 'NEW' 
      ORDER BY id ASC 
      FOR UPDATE SKIP LOCKED 
      LIMIT 100;
      ```
   2. **投递到 Kafka**：
      * 采用统一的 Kafka Topic：`ticket_events`。
      * **分区键 (Partition Key)** 设为 `schedule_id` 的字符串形式。
      * *设计考量*：同一列车实例（schedule）的所有预占、支付、释放、订单事件将投递到 Kafka 的同一个 Partition，从而保证**同一车次事件的局部严格顺序消费**，彻底消除多分区并发导致的读写乱序。
   3. **状态确认更新**：
      * 投递成功后，在事务中更新：`UPDATE outbox_event SET status = 'PUBLISHED', published_at = NOW() WHERE id = :id;`。
      * 如果 Kafka 宕机或网络失败，事件状态保持为 `NEW`，并在网络恢复后自动重试，保障 **At-Least-Once (至少投递一次)**。

   ---

   ### 4.2 Query Projector (读模型投影器)

   读模型投影器作为 Kafka 的 Consumer 组（Group ID: `query_projector_group`），专门监听 `ticket_events` Topic：

   1. **消费幂等防御 (Consumer Idempotency)**：
      * 消费端由于网络抖动极易收到重复消息。投影器消费事件前，首先尝试向 MySQL 的 `processed_event` 表中插入一行 `(consumer_name, event_id)`。
      * 如果插入由于唯一约束冲突（Duplicate Key）失败，说明该事件已成功消费，**直接确认消息并忽略，绝对不重复更新缓存**。
   2. **读模型重建与计算逻辑 (Projection Rebuild)**：
      * 为防止增量更新（例如 count--、count++）因极低概率的系统异常产生漂移，投影器采用**“权威状态重算更新”**机制。
      * 监听到 `ReservationHeld`、`ReservationReleased`、`OrderPaid` 等事件时，投影器向 MySQL 数据库发起一次轻量聚合：
        ```sql
        SELECT seat_id, segment_no, reservation_id, state, seat.seat_class
        FROM seat_segment
        JOIN seat ON seat.id = seat_segment.seat_id
        WHERE seat_segment.schedule_id = :schedule_id;
        ```
      * 在内存中将该列车实例的所有座位区间状态恢复为 Bitmap，并对所有可能的起止区间（对于 5 站，共 10 种起止组合）执行：
        `bitmap & request_mask == 0` 的过滤。
      * 分别统计各区间的可用余票数。
      * **更新 Redis 读缓存**：
        调用 Pipeline 批量重写 `q:availability:{schedule_id}:{from_station_name}:{to_station_name}:{seat_class}`。
      * *特点*：极具鲁棒性，具备“自动纠偏”能力。即使前置事件丢失，下一次事件到来时也会从底层权威数据自动重算，完全保证强一致。

   ---

   ## 5. API 契约、容器编排与质量验证方案 (第四阶段)

   ### 5.1 API 契约设计

   #### A. 查询车次余票 (GET /api/v1/query/availability)
   *   **Query 参数**：`schedule_id` (int), `from_station` (str), `to_station` (str), `seat_class` (str)
   *   **响应示例 (200 OK)**：
       ```json
       {
         "schedule_id": 1,
         "from_station": "BEIJING",
         "to_station": "SHANGHAI",
         "seat_class": "SECOND",
         "available_count": 45,
         "updated_at": "2026-09-06T15:30:22Z"
       }
       ```

   #### B. 创建座位预占 (POST /api/v1/command/reservations)
   *   **请求 Header**：`Idempotency-Key` (str, UUID)
   *   **请求 Body**：
       ```json
       {
         "schedule_id": 1,
         "from_station": "BEIJING",
         "to_station": "SHANGHAI",
         "seat_class": "SECOND"
       }
       ```
   *   **响应示例 (201 Created)**：
       ```json
       {
         "reservation_id": "R20260906000001",
         "schedule_id": 1,
         "seat_id": 102,
         "carriage_no": "02",
         "seat_no": "08A",
         "state": "HELD",
         "expires_at": "2026-09-06T15:45:00Z"
       }
       ```

   #### C. 确认生成订单 (POST /api/v1/command/orders)
   *   **请求 Body**：
       ```json
       {
         "reservation_id": "R20260906000001"
       }
       ```
   *   **响应示例 (201 Created)**：
       ```json
       {
         "order_id": "O20260906000001",
         "reservation_id": "R20260906000001",
         "state": "WAITING_PAYMENT",
         "total_amount": 553.00,
         "expires_at": "2026-09-06T15:45:00Z"
       }
       ```

   #### D. 模拟支付订单 (POST /api/v1/command/orders/{order_id}/pay)
   *   **响应示例 (200 OK)**：
       ```json
       {
         "order_id": "O20260906000001",
         "state": "CONFIRMED",
         "message": "Payment confirmed, ticket successfully issued."
       }
       ```

   ---

   ### 5.2 Docker Compose 编排配置 (docker-compose.yml)

   我们将打包所有的核心依赖：MySQL、Redis、Kafka、Zookeeper 及我们即将开发的 FastAPI 应用，形成一键运行的环境：

   ```yaml
   version: '3.8'

   services:
     mysql:
       image: mysql:8.0
       container_name: ticketing_mysql
       environment:
         MYSQL_ROOT_PASSWORD: root
         MYSQL_DATABASE: ticketing_db
       ports:
         - "3306:3306"
       volumes:
         - mysql_data:/var/lib/mysql
       healthcheck:
         test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-proot"]
         interval: 5s
         timeout: 5s
         retries: 5

     redis:
       image: redis:7.0-alpine
       container_name: ticketing_redis
       ports:
         - "6379:6379"
       command: redis-server --appendonly yes
       healthcheck:
         test: ["CMD", "redis-cli", "ping"]
         interval: 5s
         timeout: 5s
         retries: 5

     zookeeper:
       image: confluentinc/cp-zookeeper:7.3.0
       container_name: ticketing_zookeeper
       environment:
         ZOOKEEPER_CLIENT_PORT: 2181
         ZOOKEEPER_TICK_TIME: 2000

     kafka:
       image: confluentinc/cp-kafka:7.3.0
       container_name: ticketing_kafka
       depends_on:
         - zookeeper
       ports:
         - "9092:9092"
       environment:
         KAFKA_BROKER_ID: 1
         KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
         KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
         KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
       healthcheck:
         test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
         interval: 10s
         timeout: 5s
         retries: 5

     web:
       build: .
       container_name: ticketing_web
       ports:
         - "8000:8000"
       depends_on:
         mysql:
           condition: service_healthy
         redis:
           condition: service_healthy
         kafka:
           condition: service_healthy
       environment:
         DATABASE_URL: mysql+aiomysql://root:root@mysql:3306/ticketing_db
         REDIS_URL: redis://redis:6379/0
         KAFKA_BOOTSTRAP_SERVERS: kafka:29092

   volumes:
     mysql_data:

---

## 6. BDD & TDD 行为驱动开发与测试规范 (第五阶段)

本 MVP 严格落地 **BDD (Behavior-Driven Development)** 与 **TDD (Test-Driven Development)** 双轮驱动，以此构建高质量、强一致的代码根基。

### 6.1 BDD / TDD 技术选型

1. **测试运行器**：`pytest`
2. **BDD 插件**：`pytest-bdd`
3. **测试组件设计**：
   * Gherkin 特性描述文件：`src/tests/features/ticketing.feature`
   * BDD 步骤绑定与执行代码：`src/tests/step_defs/test_ticketing.py`
   * 高并发多线程 TDD 压测脚本：`src/tests/test_concurrency.py`

---

### 6.2 Gherkin 核心行为契约 (`src/tests/features/ticketing.feature`)

在编写任何业务代码前，我们先定义以下 Gherkin 用户故事和功能规范。它们是人机协作、开发和验收的唯一标准：

```gherkin
Feature: 12306 High-Concurrency Interval Ticketing System

  Scenario: Successfully book a sub-route ticket when seat is available
    Given a train schedule "G123" on "2026-09-06" with stations "BEIJING, TIANJIN, JINAN, NANJING, SHANGHAI"
    And the seat "08A" in carriage "02" of class "SECOND" is completely "AVAILABLE"
    When passenger Alice requests to reserve a ticket from "BEIJING" to "JINAN" of class "SECOND" with idempotency key "alice-req-1"
    Then the reservation should be successful with state "HELD"
    And the seat bitmap in Redis for seat "08A" should record the segments as "0011" (binary)
    And a MySQL reservation record should exist with state "HELD" for Alice

  Scenario: Fail to book when requested segments overlap with existing booking
    Given a train schedule "G123" on "2026-09-06" with stations "BEIJING, TIANJIN, JINAN, NANJING, SHANGHAI"
    And the seat "08A" is already booked from "BEIJING" to "TIANJIN" (mask "0001") by Bob
    When passenger Alice requests to reserve a ticket from "BEIJING" to "JINAN" of class "SECOND" with idempotency key "alice-req-2"
    Then the reservation should fail with status "Oversold/No Ticket available"
    And the seat bitmap in Redis for seat "08A" should remain "0001" (binary)
    And Alice should not have any successful reservation records in MySQL

  Scenario: Complete booking after successful payment and verify eventual consistency
    Given a train schedule "G123" on "2026-09-06" with stations "BEIJING, TIANJIN, JINAN, NANJING, SHANGHAI"
    And passenger Alice has a successful reservation "R20260906000001" with state "HELD"
    When Alice pays for her order associated with reservation "R20260906000001"
    Then the order state should transition to "CONFIRMED" in MySQL
    And the associated seat_segment state should transition to "SOLD" in MySQL
    And the Redis read-model query for "BEIJING" to "JINAN" should eventually decrease by 1
```

---

### 6.3 TDD 铁律与开发闭环 (Red-Green-Refactor)

在开始每一阶段的具体实现时，我们必须严格按以下三个步骤演进：

1. **RED（红灯阶段）**：
   * 在 `src/tests/` 下编写一个针对底层具体方法的单体测试（如位掩码生成、Redis Lua 脚本调用），或者执行上述 BDD Scenarios。
   * **必须执行测试并确认失败**（Failure），同时在终端中捕获失败信息。
2. **GREEN（绿灯阶段）**：
   * 编写**恰好**能够让该测试通过的最简业务实现代码。
   * 严禁在未经测试覆盖的情况下编写“防御性/假设性”逻辑。
   * 运行测试，确认输出通过（Pass）。
3. **REFACTOR（重构阶段）**：
   * 在测试通过（保持绿灯）的前提下，消除代码重复、规范命名、解耦类/模块。
   * 确保测试套件没有被破坏，再次运行以确认状态安全。


