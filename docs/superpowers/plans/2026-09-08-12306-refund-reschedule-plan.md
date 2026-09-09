# 🌌 12306 阶梯手续费退票与原子事务改签系统开发规划书 (Refund & Atomic Reschedule Plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement full-fidelity tier-based refund (including partial refund) and "refund old & buy new" atomic rescheduling with price adjustment under a single database savepoint transaction.

**Architecture:** Utilize database nested savepoints (`db_session.begin_nested()`) to guarantee transaction atomicity during rescheduling, with fallback rollback. Calculate dynamic refund fees relative to departure dates. Use Redis Lua Script commands for atomic bitmask rollback.

**Tech Stack:** Python, SQLAlchemy, Redis (Lua Scripting), FastAPI, Pytest, Pytest-BDD

**Spec:** docs/superpowers/specs/2026-09-08-12306-refund-reschedule-spec.md

## Global Constraints

- SQLAlchemy ORM Active Record mappings: Orders, Reservation, Ticket, Passenger, SeatSegment.
- Atomic row locking via SELECT ... FOR UPDATE (ASC ordered on seat_id and segment_no to prevent deadlocks).
- Redis bitmask reclamation via release_seat_lua.
- Transactional Event Outbox: logging 'ORDER_REFUNDED' and 'TICKET_RESCHEDULED' events.

---

## 🛠️ Tasks Decomposition

### Task 1: 阶梯手续费部分退票引擎开发 (Tier-based & Partial Refund)

**Files:**
- Modify: `src/app/order_service.py` (升级并实现 `refund_order`)
- Test: `tests/test_order_service.py` (编写退票费阶梯算法及部分退票单元测试)

**Interfaces:**
- Consumes: `Passenger`, `Ticket`, `Reservation`, `Orders` SQLAlchemy models from `src/app/models.py`.
- Produces: `OrderService.refund_order(db_session, order_id: str, passenger_id: str = None) -> Dict[str, Any]` returning `{"success": true, "refund_amount": float, "handling_fee": float, "remaining_order_amount": float}`.

- [ ] **Step 1: Write the failing tests**

Add these test cases in `tests/test_order_service.py`:
```python
async def test_order_partial_refund_and_tier_handling_fee(db_session, event_loop):
    # 1. Seed two passenger tickets (PSG_001, PSG_002) in one Reservation
    # 2. Complete payment -> CONFIRMED
    # 3. Request partial refund for PSG_001 with TrainSchedule departure set to 48 hours later (5% fee)
    # 4. Assert PSG_001 ticket is deleted, SeatSegment for PSG_001 is set to AVAILABLE
    # 5. Assert 5.00 yuan handling fee and 95.00 yuan refund cash
    # 6. Assert PSG_002's ticket remains intact and CONFIRMED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_order_service.py -v`
Expected: FAIL with "AttributeError" or test assertions failing.

- [ ] **Step 3: Implement step-wise refund code in order_service.py**

Modify `OrderService.refund_order` to:
- Lock order and reservation.
- Check time diff to `TrainSchedule.service_date`:
  - $\ge 15$ days: rate = 0.0
  - $\ge 2$ days: rate = 0.05
  - $\ge 1$ day: rate = 0.10
  - else: rate = 0.20
- If `passenger_id` is provided, find and delete only that `Ticket` record. Atomic lock & release its specific seat segments. Decrement order total amount.
- If `passenger_id` is None, perform full release of all tickets.
- Insert `ORDER_REFUNDED` outbox event, trigger recalculate projector & auto_fulfill_waitlist.
- Commit and return details.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_order_service.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/app/order_service.py tests/test_order_service.py
git commit -m "feat: implement partial refund with dynamic tier-based handling fees"
```

---

### Task 2: “退旧买新”原子保存点改签引擎开发 (Atomic Reschedule Engine)

**Files:**
- Modify: `src/app/order_service.py` (新增 `reschedule_ticket` 静态方法)
- Test: `tests/test_order_service.py` (编写改签成功价差结算与改签失败无损回滚测试)

**Interfaces:**
- Consumes: `ReservationService.reserve_ticket`, `release_seat_lua`
- Produces: `OrderService.reschedule_ticket(db_session, ticket_id: str, new_schedule_id: int, new_seat_class: str) -> Dict[str, Any]` returning `{"success": true, "new_ticket_id": str, "new_seat_no": str, "price_difference": float, "action": str}`.

- [ ] **Step 1: Write the failing tests**

Add these test cases in `tests/test_order_service.py`:
```python
async def test_atomic_reschedule_success_and_soldout_rollback(db_session, event_loop):
    # 1. Seed seat A on train G666 (Seq 1->3) as CONFIRMED for PSG_001
    # 2. Seed seat B on train G888 (Seq 1->3) as AVAILABLE
    # 3. Test Success: reschedule G666 to G888, assert G666 is released, G888 is locked, ticket updated
    # 4. Test Rollback: try reschedule to G777 which is fully sold out, assert Exception raised and G888 ticket remains 100% CONFIRMED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_order_service.py -v`
Expected: FAIL with "AttributeError: type object 'OrderService' has no attribute 'reschedule_ticket'".

- [ ] **Step 3: Implement reschedule_ticket in order_service.py**

Implement `reschedule_ticket` utilizing SQLAlchemy savepoint:
```python
async with db_session.begin_nested():
    # 1. Fetch old Ticket & Passenger details
    # 2. Check Real-name collision on new_schedule_id
    # 3. Call ReservationService.reserve_ticket on new train (allocates new seat, creates new Reservation & Ticket HELD)
    # 4. Commit new reserve & release old seat segments (set SeatSegments to AVAILABLE, update Redis Bitmask)
    # 5. Delete old Ticket and transition old Reservation state to RELEASED
    # 6. Settle price differences: update old Order total amount, transition new Reservation to CONFIRMED
    # 7. Write TRANSACTIONAL TICKET_RESCHEDULED outbox event
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_order_service.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/app/order_service.py tests/test_order_service.py
git commit -m "feat: implement atomic reschedule utilizing nested database savepoints"
```

---

### Task 3: REST API 网关路由层暴漏 (REST HTTP Endpoints Expose)

**Files:**
- Modify: `src/app/main.py` (注册退票与改签控制器端点)
- Test: `tests/test_api_endpoints.py` (对齐物理端点并核销 HTTP 接口)

**Interfaces:**
- Consumes: `OrderService.refund_order`, `OrderService.reschedule_ticket`
- Produces: `POST /api/v1/refund`, `POST /api/v1/reschedule`

- [ ] **Step 1: Write the failing API integration tests**

Add these test cases in `tests/test_api_endpoints.py`:
```python
async def test_refund_and_reschedule_api_endpoints(client, db_session, event_loop):
    # 1. Call POST /api/v1/refund with valid order_id, assert 200 OK
    # 2. Call POST /api/v1/reschedule with valid ticket_id, assert 200 OK
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_api_endpoints.py -v`
Expected: FAIL with "404 Not Found".

- [ ] **Step 3: Expose POST routes in src/app/main.py**

Define standard Pydantic request models:
```python
class RefundRequest(BaseModel):
    order_id: str
    passenger_id: Optional[str] = None

class RescheduleRequest(BaseModel):
    ticket_id: str
    new_schedule_id: int
    new_seat_class: str
```
Mount endpoints calling `OrderService.refund_order` and `OrderService.reschedule_ticket`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_api_endpoints.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/app/main.py tests/test_api_endpoints.py
git commit -m "feat: expose refund and reschedule REST API endpoints"
```

---

### Task 4: BDD Gherkin 验收契约测试全链核销 (BDD Gherkin Verification)

**Files:**
- Modify: `tests/test_bdd_ticketing.py` (绑定两个全新 Scenario 并提供 Python Step 定义)
- Test: Run BDD test suite

- [ ] **Step 1: Declare scenario binds in test_bdd_ticketing.py**

Append bindings:
```python
@scenario("features/ticketing.feature", "Process active refund with dynamic tier-based handling fees")
def test_active_refund_handling_fees():
    pass

@scenario("features/ticketing.feature", "Atomic rescheduling of ticket to a new train schedule with price adjustment")
def test_atomic_rescheduling_price_adjustment():
    pass
```

- [ ] **Step 2: Implement Step Definitions**

Write the matching `@given`, `@when`, `@then` steps for:
- "Given a passenger "{pid}" has a paid confirmed ticket on "{code}" from sequence {f:d} to {t:d}"
- "And the departure date is set to "{date_str}""
- "When passenger "{pid}" requests a refund {hours:d} hours before departure"
- "Then the system should approve the refund with a {pct:d}% handling fee applied"
- "And passenger "{pid}" holds a paid confirmed ticket on train "{code}"..."
- "When passenger "{pid}" requests to reschedule their ticket to "{new_code}""
- "Then the system should atomically reserve the new seat..."

- [ ] **Step 3: Run the BDD tests to verify Gherkin contract is 100% green**

Run: `python3 -m pytest tests/test_bdd_ticketing.py -v`
Expected: PASS for all BDD scenarios.

- [ ] **Step 4: Commit**

```bash
git add tests/test_bdd_ticketing.py
git commit -m "test: align and verify new refund & reschedule BDD scenarios"
```
