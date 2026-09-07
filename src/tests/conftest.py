import pytest
import asyncio
from src.app.database import Base, engine, async_session
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, Orders, OutboxEvent, ProcessedEvent
from src.app.redis_client import get_redis

@pytest.fixture(scope="session")
def event_loop():
    """Ensure a single persistent event loop for the entire pytest session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="function", autouse=True)
def setup_db(event_loop):
    """Synchronous fixture running DB migrations within the single session event loop."""
    async def _setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        # Flush Redis to guarantee perfect test isolation
        redis_client = get_redis()
        await redis_client.flushdb()

        # Seed default passengers for easy testing
        from src.app.models import Passenger
        async with async_session() as session:
            p1 = Passenger(id="PSG_001", name="张三", id_no="110101199001019999", passenger_type="ADULT")
            p2 = Passenger(id="PSG_002", name="李四", id_no="110101199001018888", passenger_type="ADULT")
            session.add_all([p1, p2])
            await session.commit()
    
    event_loop.run_until_complete(_setup())
    
    yield
    
    async def _teardown():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            
    event_loop.run_until_complete(_teardown())

@pytest.fixture(scope="function")
def db_session(event_loop):
    """Synchronous fixture yielding AsyncSession bound strictly to the session event loop."""
    session = async_session()
    
    yield session
    
    # Gracefully close session inside the session-scoped loop
    event_loop.run_until_complete(session.close())
