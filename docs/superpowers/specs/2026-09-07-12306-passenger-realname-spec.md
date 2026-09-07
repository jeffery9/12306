# 🌌 12306 乘客实名制与同车防重购冲突架构设计规格书 (Passenger & Real-Name Collision Spec)

本设计规格书详细规定了 12306 乘客乘车实名校验、一人一票、同车时空重购冲突阻断及车票（Ticket）明细的一对多高内聚隔离架构。

本方案在写侧交易（MySQL）层面筑起刚性防重壁垒，彻底阻断黄牛同号多抢或多渠道投机。

---

## 1. 物理数据模型演进 (Physical Relational Schema)

为了实现“多人出行、单人定座、退改签分离以及学生/儿童优惠票价计算”，我们将系统模型演进为以下三表级联的高凝聚实体结构：

```text
  ┌───────────────────────────────────┐
  │         Passenger (乘车人档案)      │ ──► (乘车人档案主表，预先录入，高频读/低频写)
  ├───────────────────────────────────┤
  │ - id: VARCHAR(64) [PK]            │ ──► (前缀: PSG_XXXX)
  │ - name: VARCHAR(64)               │
  │ - id_no: VARCHAR(64) [Unique]     │ ──► [实名认证身份证，全局唯一，建有 UK 索引]
  │ - passenger_type: VARCHAR(32)     │ ──► [ADULT / STUDENT / CHILD]
  │ - created_at: TIMESTAMP           │
  └──────────────────┬────────────────┘
                     │
                     │ (1) : (N) Ticket.passenger_id
                     ▼
  ┌───────────────────────────────────┐           (N) : (1)           ┌───────────────────────────────────┐
  │         Ticket (车票子实体)         │ ────────────────────────────► │        Reservation (预订主订单)     │
  ├───────────────────────────────────┤                               ├───────────────────────────────────┤
  │ - id: VARCHAR(64) [PK]            │ ──► (前缀: TCK_XXXX)           │ - id: VARCHAR(64) [PK]            │ ──► (前缀: RES_XXXX)
  │ - reservation_id: VARCHAR(64) [FK]│                               │ - request_id: VARCHAR(64) [Unique]│
  │ - passenger_id: VARCHAR(64) [FK]  │                               │ - schedule_id: INT [FK]           │
  │ - seat_id: INT [FK]               │ ──► [精细化绑定到具体的物理座席]│ - seat_class: VARCHAR(32)         │
  │ - price: NUMERIC(18, 2)           │                               │ - state: VARCHAR(32) (HELD...)    │
  │ - created_at: TIMESTAMP           │                               └───────────────────────────────────┘
  └───────────────────────────────────┘
```

### 1.1 SQLAlchemy 模型实体声明 (Model Attribute Bindings)

*   **`Passenger`**：存储实名乘客详情。`id_no` 字段建有全局唯一索引。
*   **`Ticket`**：作为 `Reservation` 的子实体，一个 Reservation 含有 1 到多个 Ticket。Ticket 在订位锁座时创建，分别绑定不同的座位（`seat_id`），并映射其对应的 `passenger_id`。
*   **`Reservation` 与 `Orders` 联动**：
    `Orders` 依然一对一关联 `Reservation`，订单的总金额为该 Reservation 旗下所有 Ticket 的票价总和。

---

## 2. 实名防重购碰撞校验算法 (`Collision-Guard Algorithm`)

在进行极速占座或候补提报时，系统必须阻止同一个身份证在同一 `schedule_id` 下同时抢票或处于有效的占座状态（HELD 或 CONFIRMED）。

### 2.1 阻断拦截流图 (Collision Block Flow)

```text
       [ 抢票请求: 乘车人 id_list = ["PSG_A", "PSG_B"] ]
                        │
                        ▼
         [ 12306 碰撞阻断检查引擎 ] (MySQL 行锁区)
                        │
                        ├──────► (检查 ticket 和 reservation 级联数据) ──────►
                        │
                        ▼ [ 碰撞扫描 SQL 执行 ]
          SELECT p.name, p.id_no 
          FROM ticket t
          JOIN reservation r ON t.reservation_id = r.id
          JOIN passenger p ON t.passenger_id = p.id
          WHERE r.schedule_id = :schedule_id
            AND t.passenger_id IN (:id_list)
            AND r.state IN ('HELD', 'CONFIRMED')
                        │
                        ▼
                /───────────────\
               <   是否检索到记录?  >
                \───────────────/
                        │
                        ├─── [YES (发现碰撞)] ──► 1. 抛出异常: "乘客 [张三] 已拥有本车次有效车票/占位，请勿重复抢票！"
                        │                        2. 强行阻断核心事务，退出预占锁
                        │
                        └─── [NO (安全通过)] ──► 1. 继续执行核心 Lua 席位预占。
                                                 2. 在同一个事务中，批量向 ticket 表写入 Ticket 实名映射。
```

---

## 3. 业务层与 API 接口升级规范 (API Request Schema Updates)

### 3.1 购票接口 (`POST /api/v1/reserve`)

*   **请求体格式更改**：
    移除旧有的无名 `passenger_count` 参数，强行替换为具体的 `passenger_ids: List[str]` 数组。乘客的人数根据该数组的长度计算：`passenger_count = len(passenger_ids)`。

```json
{
  "request_id": "REQ_RES_10019238",
  "schedule_id": 4,
  "from_station_seq": 1,
  "to_station_seq": 4,
  "seat_class": "BUSINESS",
  "passenger_ids": [
    "PSG_001",
    "PSG_002"
  ]
}
```

### 3.2 候补接口 (`POST /api/v1/waitlist`)

*   **请求体格式更改**：
    由于候补队列也需要支持多张候补，或者单张候补绑定到确切的常用乘车人。同样的，移除 `passenger_count`，替换为 `passenger_ids: List[str]`。

```json
{
  "request_id": "REQ_WL_9081239",
  "schedule_id": 4,
  "from_station_seq": 1,
  "to_station_seq": 2,
  "seat_class": "BUSINESS",
  "passenger_ids": [
    "PSG_001"
  ]
}
```

*   **候补自动兑现联动**：
    候补在后台 `auto_fulfill_waitlist` 兑现时，也会直接在子事务中对 `passenger_ids` 进行同车实名校验，并自动落票（创建 Ticket），保障实名制规则在整个生命周期的连贯。

---

## 4. 单元与集成测试套件规格 (Verification & Test Spec)

新特性的正确性需由以下新单元/集成测试用例进行全面担保：

1.  **乘客常用联系人注册**：
    验证乘客档案（`Passenger`）实体录入、身份证全局 Unique 验证。
2.  **实名防冲突阻断 (Collision Guard Tests)**：
    *   **测试 A**：乘客 `PSG_001` 已买到了 1->4 票。另一个请求帮 `PSG_001` 买同一车次的 2->3 段，系统必须断言拦截，抛出 `passenger has conflicting booking`。
    *   **测试 B**：两人同行购票（`PSG_001` + `PSG_002`），在同一列车中分别绑定相邻的 `SeatA` 和 `SeatB`，生成 2 个 Ticket 实体，验证票价、物理座席与身份证的一一绑定无误。
3.  **超时释放与物理退票**：
    验证订单未付过期释放（`release_expired_reservations`）时，级联将关联的所有 `Ticket` 实体安全注销，且其锁定的物理座席百分之百归还公池。
