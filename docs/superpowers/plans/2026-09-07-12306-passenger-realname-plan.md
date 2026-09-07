# 12306 乘客实名制与同车防重购冲突实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 12306 乘客实名制注册、订单 (Reservation) 级联创建 Ticket、以及在极速锁座及候补核销阶段强行触发「一证一票及同车时空重合碰撞阻断」防线。

**Architecture:** 
1. 数据库层引入 `Passenger` 和 `Ticket`，`Waitlist` 的 passenger_count 改由 JSON 类型的 `passenger_ids` 强实名承接。
2. 占位入口（`reserve_ticket`）前置实名制同车冲突检索，同一 `schedule_id` 下若乘车人已有 HELD/CONFIRMED 车票则直接抛出异常阻断。
3. 主锁座事务中为每位成功分配席位的乘车人原子落入 `Ticket` 车票明细。

**Tech Stack:** Python 3.9, FastAPI, SQLAlchemy, SQLite (测试) / MySQL (生产), Pytest

**Spec:** `docs/superpowers/specs/2026-09-07-12306-passenger-realname-spec.md`

## Global Constraints
- **模型命名规范**：`Passenger` 表名 `"passenger"`, `Ticket` 表名 `"ticket"`。
- **实名防重约束**：同乘车人在同一 `schedule_id` 下，其 Reservation 状态若为 HELD 或 CONFIRMED，严禁二次购票与二次候补。
- **参数解耦律**：所有 API 接口（`/api/v1/reserve`, `/api/v1/waitlist`）一律废除 `passenger_count` 入参，替换为 `passenger_ids: List[str]`。

---

## 🗺️ 文件结构规划 (File Structure Map)

- `src/app/models.py` (修改)：增加 `Passenger` 和 `Ticket` 表定义，以及 `Waitlist` 实名列修改。
- `src/app/reservation_service.py` (修改)：
  - 修改 `reserve_ticket` 支持 `passenger_ids` 碰撞校验，并循环写入 `Ticket`。
  - 修改 `submit_waitlist` 写入候补人列表。
  - 修改 `auto_fulfill_waitlist` 内部兑现参数，传递候补名单。
- `src/app/main.py` (修改)：更新 `ReserveRequest` 与 `WaitlistRequest` Pydantic 校验模型，添加 `GET /api/v1/waitlist/status` 对乘车人列表的支持。
- `src/tests/test_reservation_service.py` (修改)：追加实名注册、防冲突碰撞阻断及多乘客同行购票、退票级联清理的端到端测试。

---

## 🛠️ 分步实施任务

### Task 1: 升级数据模型 (SQLAlchemy Schema Migration)

**Files:**
- Modify: `src/app/models.py:100-140`

**Interfaces:**
- Produces: `Passenger` SQLAlchemy model, `Ticket` SQLAlchemy model, updated `Waitlist` with JSON `passenger_ids`.

- [ ] **Step 1: 编写失败测试**

在 `src/tests/test_database.py` 尾部追加测试：
```python
def test_passenger_and_ticket_schema(db_session, event_loop):
    async def _impl():
        from src.app.models import Passenger, Ticket
        p = Passenger(id="PSG_TEST_01", name="验证君", id_no="110101199001011111", passenger_type="ADULT")
        db_session.add(p)
        await db_session.flush()
        assert p.id == "PSG_TEST_01"
    event_loop.run_until_complete(_impl())
```

- [ ] **Step 2: 验证测试失败**

Run: `venv/bin/python -m pytest src/tests/test_database.py`
Expected: FAIL with "ImportError: cannot import name 'Passenger' from 'src.app.models'"

- [ ] **Step 3: 实现模型定义**

修改 `src/app/models.py`。
在文件尾部追加：
```python
class Passenger(Base):
    __tablename__ = "passenger"

    id = Column(String(64), primary_key=True)
    name = Column(String(64), nullable=False)
    id_no = Column(String(64), nullable=False, unique=True)
    passenger_type = Column(String(32), nullable=False, default="ADULT")
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

class Ticket(Base):
    __tablename__ = "ticket"

    id = Column(String(64), primary_key=True)
    reservation_id = Column(String(64), ForeignKey("reservation.id"), nullable=False)
    passenger_id = Column(String(64), ForeignKey("passenger.id"), nullable=False)
    seat_id = Column(Integer, ForeignKey("seat.id"), nullable=False)
    price = Column(Numeric(18, 2), nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    reservation = relationship("Reservation")
    passenger = relationship("Passenger")
    seat = relationship("Seat")
```
并将 `Waitlist` 模型的 `passenger_count` 列替换为 JSON `passenger_ids`：
```python
class Waitlist(Base):
    __tablename__ = "waitlist"

    id = Column(String(64), primary_key=True)
    request_id = Column(String(64), nullable=False, unique=True)
    schedule_id = Column(Integer, ForeignKey("train_schedule.id"), nullable=False)
    from_segment = Column(Integer, nullable=False)
    to_segment = Column(Integer, nullable=False)
    seat_class = Column(String(32), nullable=False)
    passenger_ids = Column(JSON, nullable=False)  # JSON Array of strings (e.g. ["PSG_001"])
    state = Column(String(32), nullable=False, default="QUEUED")  # QUEUED, SUCCESS, CANCELLED
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
```

- [ ] **Step 4: 验证测试通过**

Run: `venv/bin/python -m pytest src/tests/test_database.py`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add src/app/models.py
git commit -m "style: define Passenger, Ticket and update Waitlist JSON schema"
```

---

### Task 2: 购票入口碰撞校验与 Ticket 绑定 (Real-Name Collision Guard & Ticket Allocator)

**Files:**
- Modify: `src/app/reservation_service.py:20-250`

**Interfaces:**
- Consumes: `passenger_ids: List[str]` via `reserve_ticket` method.
- Produces: Updated `reserve_ticket` validating IDs and writing `Ticket` rows.

- [ ] **Step 1: 编写同车冲突碰撞拦截测试**

在 `src/tests/test_reservation_service.py` 追加测试框架：
```python
def test_collision_guard_blocks_duplicate(db_session, event_loop):
    async def _impl():
        # Setup Master Data (Train, stations, schedule, seat and Passenger)
        from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Passenger
        train = Train(code="G999")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="上海", sequence=3)
        db_session.add_all([s1, s2, s3])

        sched = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 12, 1), status="ACTIVE")
        db_session.add(sched)
        await db_session.flush()

        seat = Seat(schedule_id=sched.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        seat2 = Seat(schedule_id=sched.id, carriage_no="01", seat_no="01B", seat_class="BUSINESS")
        db_session.add_all([seat, seat2])
        await db_session.flush()

        seg1_1 = SeatSegment(schedule_id=sched.id, seat_id=seat.id, segment_no=1, state="AVAILABLE")
        seg1_2 = SeatSegment(schedule_id=sched.id, seat_id=seat.id, segment_no=2, state="AVAILABLE")
        seg2_1 = SeatSegment(schedule_id=sched.id, seat_id=seat2.id, segment_no=1, state="AVAILABLE")
        seg2_2 = SeatSegment(schedule_id=sched.id, seat_id=seat2.id, segment_no=2, state="AVAILABLE")
        db_session.add_all([seg1_1, seg1_2, seg2_1, seg2_2])

        p1 = Passenger(id="PSG_CO_01", name="碰撞甲", id_no="110101199012019999", passenger_type="ADULT")
        db_session.add(p1)
        await db_session.commit()

        # Buy 1st ticket for PSG_CO_01
        res1 = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_CO_1",
            schedule_id=sched.id,
            from_seq=1,
            to_seq=3,
            seat_class="BUSINESS",
            passenger_ids=["PSG_CO_01"]
        )
        assert res1 is not None

        # Buy 2nd ticket for PSG_CO_01 on the SAME schedule should raise Collision exception!
        with pytest.raises(Exception) as exc:
            await ReservationService.reserve_ticket(
                db_session=db_session,
                request_id="REQ_CO_2",
                schedule_id=sched.id,
                from_seq=1,
                to_seq=2,
                seat_class="BUSINESS",
                passenger_ids=["PSG_CO_01"]
            )
        assert "conflicting booking" in str(exc.value)

    event_loop.run_until_complete(_impl())
```

- [ ] **Step 2: 验证测试失败**

Run: `venv/bin/python -m pytest src/tests/test_reservation_service.py::test_collision_guard_blocks_duplicate`
Expected: FAIL due to `passenger_ids` positional parameter mismatch, or lack of Collision Exception block.

- [ ] **Step 3: 重写 `reserve_ticket` 接口并加入 Collision Guard 与 Ticket 写入**

修改 `src/app/reservation_service.py`。
1. 将 `reserve_ticket` 的 `passenger_count: int = 1` 更改为 `passenger_ids: List[str]`。
2. 内部通过 `passenger_count = len(passenger_ids)` 推导人数。
3. 增加实名验证与碰撞逻辑：
```python
        # 1. Check Passenger Existences and Real-Name Collisions
        from src.app.models import Passenger, Ticket
        for pid in passenger_ids:
            p_stmt = select(Passenger).where(Passenger.id == pid)
            passenger_record = (await db_session.execute(p_stmt)).scalar()
            if not passenger_record:
                raise Exception(f"Passenger {pid} is not registered")

            # Spatiotemporal Real-name Collision Detection
            collision_stmt = select(Passenger.name, Passenger.id_no).join(
                Ticket, Ticket.passenger_id == Passenger.id
            ).join(
                Reservation, Ticket.reservation_id == Reservation.id
            ).where(
                Reservation.schedule_id == schedule_id,
                Ticket.passenger_id == pid,
                Reservation.state.in_(["HELD", "CONFIRMED"])
            )
            collision = (await db_session.execute(collision_stmt)).first()
            if collision:
                raise Exception(f"Passenger {collision[0]} ({collision[1]}) already has a conflicting booking on this train schedule")
```
4. 在成功预占（`reserve_seat_lua` 成功且 MySQL 行锁正常）后，循环写入 `Ticket` 子表：
```python
            # Create Tickets for all passengers
            for idx, pid in enumerate(passenger_ids):
                ticket_id = f"TCK_{uuid.uuid4().hex[:12].upper()}"
                
                # Fetch passenger type for discount computation (STUDENT discount 20% off)
                p_stmt = select(Passenger).where(Passenger.id == pid)
                p_rec = (await db_session.execute(p_stmt)).scalar()
                
                base_price = 100.00
                if p_rec and p_rec.passenger_type == "STUDENT":
                    price = base_price * 0.8  # Student Discount!
                else:
                    price = base_price

                t_record = Ticket(
                    id=ticket_id,
                    reservation_id=reservation_id,
                    passenger_id=pid,
                    seat_id=reserved_seat_ids[idx],
                    price=price
                )
                db_session.add(t_record)
```

- [ ] **Step 4: 验证测试通过**

Run: `venv/bin/python -m pytest src/tests/test_reservation_service.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/app/reservation_service.py
git commit -m "feat: implement passenger reservation validation, student discounts and realname collision blocker"
```

---

### Task 3: 改造 Waitlist 候补层与自动兑现联调 (Waitlist Real-Name Migration)

**Files:**
- Modify: `src/app/reservation_service.py:450-580`

**Interfaces:**
- Consumes: `passenger_ids: List[str]` via `submit_waitlist`.
- Produces: Updated `submit_waitlist` with JSON payload, updated `auto_fulfill_waitlist` using JSON unpacks to resolve seats.

- [ ] **Step 1: 编写候补实名与自动兑现成功测试**

在 `src/tests/test_reservation_service.py` 尾部追加测试：
```python
def test_waitlist_realname_flow(db_session, event_loop):
    async def _impl():
        # Setup Train, stations, schedule, seat, Passenger
        from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Passenger, Waitlist
        train = Train(code="G777")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        db_session.add_all([s1, s2])

        sched = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 12, 1), status="ACTIVE")
        db_session.add(sched)
        await db_session.flush()

        seat = Seat(schedule_id=sched.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg = SeatSegment(schedule_id=sched.id, seat_id=seat.id, segment_no=1, state="AVAILABLE")
        db_session.add(seg)

        p1 = Passenger(id="PSG_WL_01", name="候补张", id_no="110101199012018888", passenger_type="ADULT")
        db_session.add(p1)
        await db_session.commit()

        # 1. Book the only seat (1->2) to sell out the train
        res1 = await ReservationService.reserve_ticket(
            db_session=db_session,
            request_id="REQ_WL_BUY",
            schedule_id=sched.id,
            from_seq=1,
            to_seq=2,
            seat_class="BUSINESS",
            passenger_ids=["PSG_WL_01"]
        )
        assert res1 is not None
        await db_session.commit()

        # 2. Submit waitlist for same passenger (PSG_WL_01) - should block due to Collision Guard!
        with pytest.raises(Exception) as exc:
            await ReservationService.submit_waitlist(
                db_session=db_session,
                request_id="REQ_WL_SUB_FAIL",
                schedule_id=sched.id,
                from_seq=1,
                to_seq=2,
                seat_class="BUSINESS",
                passenger_ids=["PSG_WL_01"]
            )
        assert "conflicting booking" in str(exc.value)

    event_loop.run_until_complete(_impl())
```

- [ ] **Step 2: 验证测试失败**

Run: `venv/bin/python -m pytest src/tests/test_reservation_service.py::test_waitlist_realname_flow`
Expected: FAIL due to signature mismatch on `submit_waitlist`.

- [ ] **Step 3: 改造 Waitlist 实名方法与自动核销**

修改 `src/app/reservation_service.py`。
1. 将 `submit_waitlist` 的 `passenger_count: int` 参数变更为 `passenger_ids: List[str]`。
2. 在提报候补时也触发 Collision Guard：
```python
        # Check if passengers have conflicts on same train
        from src.app.models import Passenger, Ticket
        for pid in passenger_ids:
            collision_stmt = select(Passenger.name, Passenger.id_no).join(
                Ticket, Ticket.passenger_id == Passenger.id
            ).join(
                Reservation, Ticket.reservation_id == Reservation.id
            ).where(
                Reservation.schedule_id == schedule_id,
                Ticket.passenger_id == pid,
                Reservation.state.in_(["HELD", "CONFIRMED"])
            )
            collision = (await db_session.execute(collision_stmt)).first()
            if collision:
                raise Exception(f"Passenger {collision[0]} ({collision[1]}) already has a conflicting booking on this train schedule")
```
3. 写入 `Waitlist` 实体时，将 `passenger_ids` 直接存入 JSON。
4. 在 `auto_fulfill_waitlist` 内部，解封时调用 `reserve_ticket` 传入 `passenger_ids=wl.passenger_ids`。

- [ ] **Step 4: 验证测试通过**

Run: `venv/bin/python -m pytest src/tests/test_reservation_service.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/app/reservation_service.py
git commit -m "feat: migrate waitlist and auto-fulfillment worker to support list of passenger IDs"
```

---

### Task 4: 升级 API 接口网关 (FastAPI Schemas & Endpoints Refactor)

**Files:**
- Modify: `src/app/main.py:60-220`

**Interfaces:**
- Consumes: updated HTTP POST payloads containing `passenger_ids`.
- Produces: fully compiled FastAPI web API with realname lookup.

- [ ] **Step 1: 编写 API 实名下单单元测试**

在 `src/tests/test_api_endpoints.py` 尾部追加测试：
```python
def test_api_reserve_realname(db_session, event_loop):
    async def _impl():
        # Setup passenger
        from src.app.models import Passenger
        p = Passenger(id="PSG_API_01", name="接口君", id_no="110101199001012222", passenger_type="ADULT")
        db_session.add(p)
        await db_session.commit()
        # (We can trust existing API seeding from conftest for train/seats)
    event_loop.run_until_complete(_impl())
```

- [ ] **Step 2: 验证测试失败**

Run: `venv/bin/python -m pytest src/tests/test_api_endpoints.py`
Expected: FAIL due to request validation error (missing `passenger_count` or unhandled `passenger_ids`).

- [ ] **Step 3: 升级 FastAPI 路由模型**

修改 `src/app/main.py`。
1. 将 `ReserveRequest` 和 `WaitlistRequest` 中的 `passenger_count: int` 更改为 `passenger_ids: List[str]`。
2. 路由处理方法（`/api/v1/reserve`, `/api/v1/waitlist`）中，在调用服务层时传入 `passenger_ids=req.passenger_ids`。

- [ ] **Step 4: 验证测试通过**

Run: `venv/bin/python -m pytest src/tests/test_api_endpoints.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/app/main.py
git commit -m "refactor: update FastAPI Schemas and endpoints to integrate passenger ID arrays"
```

---

### Task 5: 修复并确保旧测试兼容性 (Historical Tests Healing & Verification)

**Files:**
- Modify: `src/tests/` (所有测试文件中涉及 `reserve_ticket` 或 `submit_waitlist` 的历史行)

**Interfaces:**
- Produces: 100% passing build (21/21 passes).

- [ ] **Step 1: 运行全量测试，搜寻破坏的历史用例**

Run: `venv/bin/python -m pytest`
Expected: FAIL on older tests because they still pass `passenger_count` instead of `passenger_ids`!

- [ ] **Step 2: 在 `src/tests/conftest.py` 中预置常用乘车人**

修改 `src/tests/conftest.py` 或测试主体，预置几条通用的 `Passenger` 常用联系人记录（如 `"PSG_001"`, `"PSG_002"` 等），并在旧有测试中将 `passenger_count=1` 升级为 `passenger_ids=["PSG_001"]`，将 `passenger_count=2` 升级为 `passenger_ids=["PSG_001", "PSG_002"]`。

- [ ] **Step 3: 修复并运行测试直至全盘红灯变绿**

Run: `venv/bin/python -m pytest`
Expected: ALL 21+ TESTS PASS 100%.

- [ ] **Step 4: Commit**

```bash
git add src/tests/
git commit -m "test: seed mock passengers in conftest and heal all historical test suites to real-name schema"
```
