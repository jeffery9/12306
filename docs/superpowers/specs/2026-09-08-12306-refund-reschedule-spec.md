# 🌌 12306 阶梯手续费部分退票与“退旧买新”原子改签设计规格书 (Refund & Atomic Reschedule Spec)

本设计规格书详细规定了 12306 核心售票网关在“一单多票”级联模型下，进行阶梯退票费收取、部分乘车人退票销单，以及基于数据库级原子嵌套保存点（Savepoint）的“退旧买新”原子无损改签的物理算法与接口标准。

---

## 1. 物理业务与数据模型对齐 (Schema Alignment)

系统已具备以下关系链条：

- **`Orders` (1) : (1) `Reservation`**：订单一对一关联预定信息。
- **`Reservation` (1) : (N) `Ticket`**：一笔订单内含多名同行乘客的 Ticket 车票子实体。
- **`Ticket` (1) : (1) `Passenger`**：每张车票精准强校验绑定一名实名乘车人。
- **`Ticket` (1) : (1) `Seat`**：每张车票精准分配绑定物理座席，在底层的 `SeatSegment` 区间段中进行时空占用。

---

## 2. 阶梯退票费与部分退票算法 (`Active Partial Refund Guard`)

### 2.1 阶梯退票费率公式 (Sliding Window Fees)

根据当前退票动作日期距离列车发车日期（`TrainSchedule.service_date`）的差值，实施阶梯费率：

$$
\text{Handling Fee Rate} = \begin{cases} 
0\% & \text{Diff} \ge 15 \text{ days} \\
5\% & 2 \text{ days} \le \text{Diff} < 15 \text{ days} \\
10\% & 1 \text{ day} \le \text{Diff} < 2 \text{ days} \\
20\% & \text{Diff} < 1 \text{ day}
\end{cases}
$$

使用纯文本 ASCII 表达如下：
```text
[ Days to Departure ] >= 15 Days       ──►  Fee: 0%  (全额退)
2 Days <= [ Days to Departure ] < 15   ──►  Fee: 5%  (收5%手续费)
1 Day  <= [ Days to Departure ] < 2    ──►  Fee: 10% (收10%手续费)
[ Days to Departure ] < 1 Day          ──►  Fee: 20% (收20%手续费)
```

### 2.2 部分退票流程 (Partial Ticket Release)

支持针对单一 Reservation 下多乘客的某一个特定 `passenger_id` 进行部分清退：

1. **精准加锁**：行级锁（`FOR UPDATE`）锁定订单及关联的 Reservation。
2. **检索并删除 Ticket**：
   - 查询该 Reservation 下指定的 `passenger_id` 所持有的 `Ticket`。
   - 获取其对应的座位 `seat_id`、原车次 `schedule_id` 及起止区间 `from_segment` 和 `to_segment`。
   - 物理物理删除或标记注销该 `Ticket` 记录。
3. **物理释放座席**：
   - 锁定并更新 `SeatSegment`，将该座位对应的区间状态重置为 `AVAILABLE`，`reservation_id` 设为 `NULL`。
   - 利用 Redis Lua `release_seat_lua` 原子清除该席位在该区间的二进制占用位掩码。
4. **结算价差**：
   - 根据退票费公式计算实扣手续费 `fee = ticket.price * rate`，实退金额 `refund_cash = ticket.price - fee`。
   - 订单的总金额 `Orders.total_amount` 扣减该车票的原价，完成支付退款核销。
5. **最终一致性自愈**：
   - 写入 `ORDER_REFUNDED` 发件箱事件，包含具体的释放席位与变更退款。
   - Projector 重投影，秒级回填公网抢票池，并即刻拉起候补队列自愈兑现。

---

## 3. “退旧买新”原子改签算法 (`Atomic Reschedule engine`)

改签核心要求是：**高并发锁座失败时，原车票绝对完好无损保留，拒绝“两头落空”**。

### 3.1 原子事务流控制 (Savepoint Orchestration)

所有的库级与缓存修改必须被包裹在同一个数据库物理事务中，并在尝试锁定新车票前建立**保存点（Savepoint）**：

```text
       [ 开始 DB 事务 (db.Begin) ]
                    │
                    ▼
          [ 建立嵌套保存点 (Savepoint) ]
                    │
                    ▼
    [ 1. 目标新车次实名重购校验 (Collision Guard) ]
                    │
                    ▼
      [ 2. 新车次尝试预占锁座 (reserve_ticket) ]
         /                           \
    (占座成功)                     (占座失败/售罄)
       /                               \
      ▼                                 ▼
[ 3. 释放 A 车次旧车票 ]          [ 嵌套回滚 (Rollback to Savepoint) ]
[ 4. 释放旧座位段并回刷位图 ]     [ 5. 保留 A 车次车票 100% 有效 ]
[ 5. 更新 Ticket 指向新座位 ]     [ 6. 返回 "No seats available on G888" ]
[ 6. 结算差额 (多退少补) ]                       │
[ 7. 提交整个事务 (Commit) ]                     ▼
                    │                  [ 结束 (无损熔断) ]
                    ▼
            [ 结束 (改签成功) ]
```

### 3.2 差额多退少补规则 (Price Difference Settlement)

- **`new_price` == `old_price`**：订单总价不变，修改车票物理关联。
- **`new_price` > `old_price`**：计算差额 `diff = new_price - old_price`，订单的总金额 `Orders.total_amount` 增加 `diff`，向网关申请追加扣款。
- **`new_price` < `old_price`**：计算差额 `diff = old_price - new_price`，订单的总金额扣减 `diff`，差额直接退还给用户。

---

## 4. 接口契约定义 (API Contract)

### 4.1 部分/全额退票接口

- **Endpoint**: `POST /api/v1/refund`
- **Request Body**:
  ```json
  {
    "order_id": "ORD_885FA2E4DF25",
    "passenger_id": "PSG_001" 
  }
  ```
  *(注：若 `passenger_id` 缺失，则默认为整单全额清退)*
- **Response**:
  ```json
  {
    "success": true,
    "refund_amount": 95.00,
    "handling_fee": 5.00,
    "remaining_order_amount": 0.00
  }
  ```

### 4.2 最终原子改签接口

- **Endpoint**: `POST /api/v1/reschedule`
- **Request Body**:
  ```json
  {
    "ticket_id": "TCK_AA214B88D",
    "new_schedule_id": 102,
    "new_seat_class": "BUSINESS"
  }
  ```
- **Response**:
  ```json
  {
    "success": true,
    "new_ticket_id": "TCK_AA214B88D_R",
    "new_seat_no": "01C",
    "price_difference": 50.00,
    "action": "PAY_DIFFERENCE"
  }
  ```
