# 12306 High-Concurrency Interval Ticketing System MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a robust, containerized Python MVP for the 12306 ticketing system demonstrating CQRS, Redis Bitmaps, Transactional Outbox, Kafka pipelines, and dual-layer database safety against over-selling.

**Architecture:** 
- Command requests verify concurrency on Redis Bitmaps via Lua scripts, commit seat segment rows to MySQL (with row-locked segment counts), and write to an Outbox table.
- A background Outbox Publisher sends events to Kafka keyed by `schedule_id`.
- A Query Projector consumes events, checks `processed_event` for idempotency, recalculates segment availabilities from MySQL, and updates the Redis Query cache.

**Tech Stack:** Python 3.11+, FastAPI, SQLAlchemy Async, aiomysql, redis-py, aiokafka, pytest, pytest-bdd.

**Spec:** `docs/superpowers/specs/2026-09-06-12306-ticketing-mvp-design.md`

## Global Constraints
- **Python Version**: Python 3.11+
- **Docker Compose Dependencies**: MySQL 8.0, Redis 7.0, Confluent cp-kafka/cp-zookeeper 7.3
- **Test Command**: `pytest src/tests -v`
- **TDD Rule**: No production code may be written unless accompanied by a failing test first.

---

## Workspace File Structure

We will implement the project inside the following focused directory structure:

```text
/Users/jeffery/Downloads/12306/
├── docs/
│   └── superpowers/
│       └── specs/
│           └── 2026-09-06-12306-ticketing-mvp-design.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── src/
    ├── app/
    │   ├── __init__.py
    │   ├── main.py
    │   ├── config.py
    │   ├── database.py
    │   ├── models.py
    │   ├── redis_client.py
    │   ├── kafka_client.py
    │   ├── routers/
    │   │   ├── __init__.py
    │   │   ├── command.py
    │   │   └── query.py
    │   └── services/
    │       ├── __init__.py
    │       ├── reservation.py
    │       ├── order.py
    │       ├── outbox_publisher.py
    │       └── projector.py
    └── tests/
        ├── __init__.py
        ├── conftest.py
        ├── features/
        │   └── ticketing.feature
        ├── step_defs/
        │   ├── __init__.py
        │   └── test_ticketing.py
        └── test_concurrency.py
```

---

## Implementation Tasks

### Task 1: Environment Scaffolding & Requirements

**Files:**
- Create: `requirements.txt`
- Create: `docker-compose.yml`
- Create: `Dockerfile`

**Interfaces:**
- Consumes: None
- Produces: Dockerized MySQL, Redis, and Kafka infrastructure.

- [ ] **Step 1: Write requirements.txt with absolute pinned versions**
Write `requirements.txt`:
```text
fastapi>=0.100.0
uvicorn>=0.22.0
pydantic-settings>=2.0.0
sqlalchemy[asyncio]>=2.0.0
aiomysql>=0.2.0
redis>=5.0.0
aiokafka>=0.8.1
pytest>=7.3.1
pytest-asyncio>=0.21.0
pytest-bdd>=6.1.1
httpx>=0.24.1
cryptography>=41.0.0
```

- [ ] **Step 2: Create Docker Compose configuration**
Write `docker-compose.yml` as defined in Section 5.2 of the design document.

- [ ] **Step 3: Create FastAPI App Dockerfile**
Write `Dockerfile`:
```dockerfile
FROM python:3.11-slim
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/workspace/src
CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 4: Spin up infrastructure and verify they are healthy**
Run:
```bash
docker compose up -d mysql redis kafka
```
Expected: Containers spin up and report healthy.

- [ ] **Step 5: Commit scaffolding**
```bash
git add requirements.txt docker-compose.yml Dockerfile
git commit -m "chore: scaffold project structure and docker composition"
```

---

### Task 2: Pydantic Config, Async DB Client & Models

**Files:**
- Create: `src/app/config.py`
- Create: `src/app/database.py`
- Create: `src/app/models.py`
- Create: `src/tests/test_database.py`

**Interfaces:**
- Consumes: Database URLs from Config
- Produces: `async_session` generator, SQLAlchemy declarative class `Base`, and database entity models.

- [ ] **Step 1: Write a failing test for Database Connectivity**
Write `src/tests/test_database.py`:
```python
import pytest
from sqlalchemy import select
from src.app.database import get_db
from src.app.config import settings

@pytest.mark.asyncio
async def test_db_connectivity():
    async for session in get_db():
        result = await session.execute(select(1))
        assert result.scalar() == 1
```

- [ ] **Step 2: Run test to verify it fails**
Run: `pytest src/tests/test_database.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.app.config'`

- [ ] **Step 3: Implement Config & Async Database Engine**
Write `src/app/config.py`:
```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "mysql+aiomysql://root:root@localhost:3306/ticketing_db"
    REDIS_URL: str = "redis://localhost:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    RESERVE_TTL: int = 900

settings = Settings()
```

Write `src/app/database.py`:
```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from src.app.config import settings

engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with async_session() as session:
        yield session
```

- [ ] **Step 4: Implement Models in `src/app/models.py`**
Write all model entities (Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, Orders, OutboxEvent, ProcessedEvent) using SQLAlchemy 2.0 Declarative mapping to match the database schemas specified in Section 2.1 of the design doc.

- [ ] **Step 5: Run db test to verify it passes**
Run: `pytest src/tests/test_database.py -v`
Expected: PASS

- [ ] **Step 6: Commit DB layer**
```bash
git add src/app/config.py src/app/database.py src/app/models.py src/tests/test_database.py
git commit -m "feat: implement configuration, async database client, and models"
```

---

### Task 3: Redis Client & Lua Script Loader

**Files:**
- Create: `src/app/redis_client.py`
- Create: `src/tests/test_redis_lua.py`

**Interfaces:**
- Consumes: Redis URL from Config
- Produces: `get_redis()`, `reserve_seat_lua(seat_key, res_key, mask, res_id, ttl)`, and `release_seat_lua(seat_key, res_key, mask, res_id)`.

- [ ] **Step 1: Write a failing test for Lua Seat Reservation**
Write `src/tests/test_redis_lua.py`:
```python
import pytest
from src.app.redis_client import get_redis, reserve_seat_lua, release_seat_lua

@pytest.mark.asyncio
async def test_lua_reservation_flow():
    redis_conn = get_redis()
    seat_key = "r:1:seat:101"
    res_key = "r:1:reservation:R1"
    
    # Clean keys
    await redis_conn.delete(seat_key, res_key)
    
    # 1. First reservation should succeed for mask 3 (segments 0 and 1)
    res = await reserve_seat_lua(redis_conn, seat_key, res_key, mask=3, res_id="R1", ttl=60)
    assert res == 1
    
    # 2. Overlapping reservation for mask 2 (segment 1) should fail
    res2 = await reserve_seat_lua(redis_conn, seat_key, "r:1:reservation:R2", mask=2, res_id="R2", ttl=60)
    assert res2 == 0
```

- [ ] **Step 2: Run test to verify failure**
Run: `pytest src/tests/test_redis_lua.py -v`
Expected: FAIL with `ImportError` or `AttributeError` on Lua functions.

- [ ] **Step 3: Implement Redis Connection & Lua Loader**
Write `src/app/redis_client.py` using `redis.asyncio`:
```python
import redis.asyncio as aioredis
from src.app.config import settings

_redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

def get_redis():
    return _redis_client

LUA_RESERVE = """
local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]
local ttl = tonumber(ARGV[3])

local occupied = redis.call("GET", seat_key)
occupied = occupied and tonumber(occupied) or 0

local bit = require("bit")
if bit.band(occupied, mask) ~= 0 then
    return 0
end

local new_occupied = bit.bor(occupied, mask)
redis.call("SET", seat_key, new_occupied)
redis.call("HSET", res_key, "reservation_id", res_id, "mask", mask, "state", "HELD")
redis.call("EXPIRE", res_key, ttl)
return 1
"""

LUA_RELEASE = """
local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]

local stored_res_id = redis.call("HGET", res_key, "reservation_id")
if stored_res_id ~= res_id then
    return 0
end

local occupied = redis.call("GET", seat_key)
occupied = occupied and tonumber(occupied) or 0

local bit = require("bit")
local clean_mask = bit.bnot(mask)
local new_occupied = bit.band(occupied, clean_mask)

redis.call("SET", seat_key, new_occupied)
redis.call("DEL", res_key)
return 1
"""

async def reserve_seat_lua(redis_conn, seat_key: str, res_key: str, mask: int, res_id: str, ttl: int) -> int:
    script = redis_conn.register_script(LUA_RESERVE)
    return await script(keys=[seat_key, res_key], args=[mask, res_id, ttl])

async def release_seat_lua(redis_conn, seat_key: str, res_key: str, mask: int, res_id: str) -> int:
    script = redis_conn.register_script(LUA_RELEASE)
    return await script(keys=[seat_key, res_key], args=[mask, res_id])
```

- [ ] **Step 4: Run test to verify it passes**
Run: `pytest src/tests/test_redis_lua.py -v`
Expected: PASS

- [ ] **Step 5: Commit Redis layer**
```bash
git add src/app/redis_client.py src/tests/test_redis_lua.py
git commit -m "feat: implement async redis client and atomic lua lock scripts"
```

---

### Task 4: Core Command - Seat Reservation Logic & MySQL Segment Locking

**Files:**
- Create: `src/app/services/reservation.py`
- Create: `src/tests/test_reservation_service.py`

**Interfaces:**
- Consumes: Redis and DB sessions.
- Produces: `ReservationService.reserve_ticket(schedule_id, from_station, to_station, seat_class, request_id)` -> returns DB `Reservation` model.

- [ ] **Step 1: Write a failing test for ticket reservation**
Write `src/tests/test_reservation_service.py`:
```python
import pytest
from src.app.services.reservation import ReservationService

@pytest.mark.asyncio
async def test_reserve_ticket_success(db_session, clean_redis):
    # Setup G123 seed data (train, stations, schedule, seats, seat_segments) in db_session
    # Run reservation
    service = ReservationService(db_session)
    res = await service.reserve_ticket(
        schedule_id=1,
        from_station="BEIJING",
        to_station="JINAN",
        seat_class="SECOND",
        request_id="alice-test-uuid"
    )
    assert res is not None
    assert res.state == "HELD"
```

- [ ] **Step 2: Run test to verify it fails**
Run: `pytest src/tests/test_reservation_service.py -v`
Expected: FAIL with `ImportError` on `ReservationService`.

- [ ] **Step 3: Implement Range Masking Helper & ReservationService**
Write segment range masking in `src/app/services/reservation.py`:
```python
def calculate_segment_mask(from_seq: int, to_seq: int) -> int:
    # Generates a bitmask representing occupied sections.
    # from_seq = 0, to_seq = 2 -> 3 (binary 0011, representing segments 0 and 1)
    length = to_seq - from_seq
    mask = (1 << length) - 1
    return mask << from_seq
```
Implement the `ReservationService` including:
1. Translating station names to sequences and validating `from_seq < to_seq`.
2. Retrieving all seat IDs for the given schedule and class.
3. For each candidate seat, executing `reserve_seat_lua` in Redis.
4. Once locked in Redis, executing MySQL Transaction:
   - Perform pessimistic lock verify: `SELECT ... FROM seat_segment WHERE schedule_id = :sched_id AND seat_id = :seat_id AND segment_no >= :from_seq AND segment_no < :to_seq FOR UPDATE;`
   - Assert all are `AVAILABLE` and `reservation_id` is null.
   - Run: `UPDATE seat_segment SET reservation_id = :res_id, state = 'HELD', version = version + 1 WHERE ...` and assert `affected_rows == segment_count` (the MySQL final bulletproof constraint).
   - Create and insert `reservation` row.
   - Create and insert `outbox_event` row with type `ReservationHeld`.
   - Commit. If any SQL lock fail/rollback happens, call `release_seat_lua` in Redis and raise error.

- [ ] **Step 4: Run test to verify it passes**
Run: `pytest src/tests/test_reservation_service.py -v`
Expected: PASS

- [ ] **Step 5: Commit Reservation Service**
```bash
git add src/app/services/reservation.py src/tests/test_reservation_service.py
git commit -m "feat: implement reservation service with double-layer safety"
```

---

### Task 5: Order Service & Timeout Release Worker

**Files:**
- Create: `src/app/services/order.py`
- Create: `src/tests/test_order_service.py`

**Interfaces:**
- Consumes: `ReservationService` and DB entities.
- Produces: `OrderService.create_order(reservation_id)`, `OrderService.pay_order(order_id)`, and `OrderService.release_expired_reservations()`.

- [ ] **Step 1: Write a failing test for Order Flow & Timeout**
Write `src/tests/test_order_service.py`:
```python
import pytest
from src.app.services.order import OrderService

@pytest.mark.asyncio
async def test_order_creation_and_payment(db_session, clean_redis):
    # Setup HELD reservation
    service = OrderService(db_session)
    order = await service.create_order("R20260906000001")
    assert order.state == "WAITING_PAYMENT"
    
    paid_order = await service.pay_order(order.id)
    assert paid_order.state == "CONFIRMED"
```

- [ ] **Step 2: Run test to verify failure**
Run: `pytest src/tests/test_order_service.py -v`
Expected: FAIL with `ImportError` on `OrderService`.

- [ ] **Step 3: Implement OrderService & Timeout poller**
Write `src/app/services/order.py`.
Implement:
1. `create_order`: queries the database `reservation`, asserts state `HELD`, and inserts an `orders` record and an `outbox_event` with type `OrderCreated`.
2. `pay_order`: within a transaction, updates `orders` and `reservation` status to `CONFIRMED`, updates `seat_segment` states to `SOLD`, updates Redis reservation state to `CONFIRMED` (and persistent key), and inserts `outbox_event` with type `OrderPaid`.
3. `release_expired_reservations`:
   - Scans database for: `SELECT * FROM reservation WHERE state = 'HELD' AND expires_at < NOW() FOR UPDATE SKIP LOCKED LIMIT 100`.
   - For each: updates database states (`reservation` -> `RELEASED`, `orders` -> `EXPIRED`, `seat_segment` -> `AVAILABLE`/null).
   - Inserts `outbox_event` with type `ReservationReleased`.
   - Calls Redis Lua release script `release_seat_lua` to restore Redis bitmap.
   - Commits.

- [ ] **Step 4: Run tests to verify they pass**
Run: `pytest src/tests/test_order_service.py -v`
Expected: PASS

- [ ] **Step 5: Commit Order Service**
```bash
git add src/app/services/order.py src/tests/test_order_service.py
git commit -m "feat: implement order service and automatic timeout release logic"
```

---

### Task 6: Kafka Client & Outbox Event Publisher

**Files:**
- Create: `src/app/kafka_client.py`
- Create: `src/app/services/outbox_publisher.py`
- Create: `src/tests/test_outbox_publisher.py`

**Interfaces:**
- Consumes: Kafka bootstrap servers, `outbox_event` table.
- Produces: `KafkaProducer` and background loop `publish_pending_events()`.

- [ ] **Step 1: Write a failing test for Outbox Publisher**
Write `src/tests/test_outbox_publisher.py`:
```python
import pytest
from src.app.services.outbox_publisher import OutboxPublisher

@pytest.mark.asyncio
async def test_publish_events(db_session, kafka_producer):
    # Insert a dummy Outbox Event into MySQL
    # Start publisher poll
    publisher = OutboxPublisher(db_session)
    published_count = await publisher.publish_pending_events()
    assert published_count >= 1
```

- [ ] **Step 2: Run test to verify failure**
Run: `pytest src/tests/test_outbox_publisher.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement aiokafka wrapper & Outbox Publisher Service**
Write `src/app/kafka_client.py` to wrap `aiokafka.AIOKafkaProducer` and `aiokafka.AIOKafkaConsumer`.
Write `src/app/services/outbox_publisher.py` with an async polling routine that queries MySQL:
```sql
SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload 
FROM outbox_event 
WHERE status = 'NEW' 
ORDER BY id ASC 
FOR UPDATE SKIP LOCKED 
LIMIT 100;
```
For each event:
- Extract `schedule_id` from payload as partition key (converted to string).
- Send to Kafka topic `ticket_events`.
- Update event row `status = 'PUBLISHED', published_at = NOW()`.
- Commit.

- [ ] **Step 4: Run test to verify it passes**
Run: `pytest src/tests/test_outbox_publisher.py -v`
Expected: PASS (requires running Kafka container)

- [ ] **Step 5: Commit Outbox Publisher**
```bash
git add src/app/kafka_client.py src/app/services/outbox_publisher.py src/tests/test_outbox_publisher.py
git commit -m "feat: implement async kafka client and transactional outbox event publisher"
```

---

### Task 7: Event-Driven Projector (最終一致)

**Files:**
- Create: `src/app/services/projector.py`
- Create: `src/tests/test_projector.py`

**Interfaces:**
- Consumes: Kafka events from `ticket_events` topic.
- Produces: Updates Redis Query Cache keys `q:availability:*`.

- [ ] **Step 1: Write a failing test for Query Projector**
Write `src/tests/test_projector.py`:
```python
import pytest
from src.app.services.projector import QueryProjector

@pytest.mark.asyncio
async def test_projection_rebuild(db_session, clean_redis):
    # Seed train data in DB, lock 1 seat, run projector on schedule 1
    projector = QueryProjector(db_session)
    await projector.rebuild_projection(schedule_id=1)
    
    # Verify Redis Query cache
    redis_conn = clean_redis
    count = await redis_conn.get("q:availability:1:BEIJING:SHANGHAI:SECOND")
    assert count is not None
```

- [ ] **Step 2: Run test to verify failure**
Run: `pytest src/tests/test_projector.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement QueryProjector & DB State Aggregator**
Write `src/app/services/projector.py`.
Implement:
1. `rebuild_projection(schedule_id)`:
   - Run query to fetch all active seat segment rows:
     `SELECT seat_id, segment_no, reservation_id, seat.seat_class FROM seat_segment JOIN seat ON seat.id = seat_segment.seat_id WHERE seat_segment.schedule_id = :schedule_id;`
   - Retrieve station routes to compute all 10 possible sub-route masks.
   - For each sub-route + class, count seats having `bitmap & mask == 0`.
   - Update Redis keys `q:availability:{schedule_id}:{from}:{to}:{seat_class}` with final counts.
2. `start_consumer_loop()`:
   - Run `AIOKafkaConsumer` subscribing to `ticket_events`.
   - For each message, check and insert `(consumer_name, event_id)` in `processed_event` for deduplication.
   - Extract `schedule_id` from payload, trigger `rebuild_projection(schedule_id)`.
   - Commit / confirm offset.

- [ ] **Step 4: Run test to verify it passes**
Run: `pytest src/tests/test_projector.py -v`
Expected: PASS

- [ ] **Step 5: Commit Projector Service**
```bash
git add src/app/services/projector.py src/tests/test_projector.py
git commit -m "feat: implement event-driven projector to rebuild and refresh query redis"
```

---

### Task 8: FastAPI HTTP API Endpoints

**Files:**
- Create: `src/app/routers/query.py`
- Create: `src/app/routers/command.py`
- Create: `src/app/main.py`
- Create: `src/tests/test_api.py`

**Interfaces:**
- Consumes: FastAPI routers.
- Produces: REST endpoints for ticketing.

- [ ] **Step 1: Write failing integration test for endpoints**
Write `src/tests/test_api.py`:
```python
import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_rest_api_ticketing_workflow():
    async with AsyncClient(base_url="http://localhost:8000") as ac:
        res = await ac.get("/api/v1/query/availability?schedule_id=1&from_station=BEIJING&to_station=SHANGHAI&seat_class=SECOND")
        assert res.status_code == 200
```

- [ ] **Step 2: Run test to verify failure**
Run: `pytest src/tests/test_api.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement Routers & main.py entrypoint**
Write FastAPI routers:
1. `src/app/routers/query.py`:
   - `GET /api/v1/query/availability`: Checks local cache or Redis Query key. If missing, returns degraded 503 response, **never** queries MySQL directly.
2. `src/app/routers/command.py`:
   - `POST /api/v1/command/reservations`: Accepts schedule/route. Reads `Idempotency-Key` from header. Executes reservation service lock. Returns reservation metadata (or 400 if oversold).
   - `POST /api/v1/command/orders`: Accepts `reservation_id`. Generates order.
   - `POST /api/v1/command/orders/{order_id}/pay`: Pays order and issues tickets.
3. `src/app/main.py`:
   - Setup FastAPI app and mount both routers.
   - Setup async context manager `lifespan` to handle startup and shutdown of Redis client and Kafka producer/consumers.

- [ ] **Step 4: Run test to verify it passes**
Run: `pytest src/tests/test_api.py -v`
Expected: PASS

- [ ] **Step 5: Commit API layer**
```bash
git add src/app/routers/query.py src/app/routers/command.py src/app/main.py src/tests/test_api.py
git commit -m "feat: implement fastapi routers and main app with lifespan hooks"
```

---

### Task 9: BDD Functional Acceptance Testing (pytest-bdd)

**Files:**
- Create: `src/tests/features/ticketing.feature`
- Create: `src/tests/step_defs/test_ticketing.py`

**Interfaces:**
- Consumes: Gherkin specs and API endpoints.
- Produces: High-fidelity BDD acceptance test suite.

- [ ] **Step 1: Write features/ticketing.feature file**
Create directory `src/tests/features/` and write the `.feature` file exactly as specified in Section 6.2 of the design doc.

- [ ] **Step 2: Write failing BDD step definitions**
Write `src/tests/step_defs/test_ticketing.py` with Gherkin bindings using `@given`, `@when`, `@then` and run them before setting up DB test fixtures.
Run: `pytest src/tests/step_defs/test_ticketing.py -v`
Expected: FAIL (with step definition missing or DB fixture errors).

- [ ] **Step 3: Wire BDD Steps & DB/Redis State verification**
Write step bindings in `src/tests/step_defs/test_ticketing.py` using FastAPI `TestClient` or direct service class invocations to mimic Alice and Bob's user journeys, state transitions, and eventual consistency counts.

- [ ] **Step 4: Run tests to watch them pass**
Run: `pytest src/tests/step_defs/test_ticketing.py -v`
Expected: PASS

- [ ] **Step 5: Commit BDD tests**
```bash
git add src/tests/features/ticketing.feature src/tests/step_defs/test_ticketing.py
git commit -m "test: implement bdd scenarios using pytest-bdd for functional validation"
```

---

### Task 10: Multi-Threaded High-Concurrency Test (TDD Stress Test)

**Files:**
- Create: `src/tests/test_concurrency.py`

**Interfaces:**
- Consumes: Live FastAPI endpoints.
- Produces: Stress test reporting.

- [ ] **Step 1: Write a high-concurrency test script**
Write `src/tests/test_concurrency.py` simulating 1000 parallel clients attempting to reserve the *exact same* single-seat train route simultaneously using async `asyncio.gather` and `httpx.AsyncClient`:
```python
import pytest
import asyncio
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_high_concurrency_single_seat():
    # 1. Ensure only 1 seat is available
    # 2. Fire 1000 simultaneous POST /api/v1/command/reservations requests using different UUIDs
    # 3. Assert exactly ONE request returns 201 Created
    # 4. Assert 999 requests return 400 (Oversold)
    # 5. Assert MySQL reservation has exactly one matching record
```

- [ ] **Step 2: Run test to verify oversell immunity**
Run: `pytest src/tests/test_concurrency.py -v`
Expected: PASS (with 0 over-selling, proving perfect thread-safety and lock robustness).

- [ ] **Step 3: Commit Concurrency Stress Test**
```bash
git add src/tests/test_concurrency.py
git commit -m "test: add high-concurrency single seat stress test to verify zero overselling"
```
