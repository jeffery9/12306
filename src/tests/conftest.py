import pytest_asyncio
import pytest
from src.app.database import Base, engine, async_session
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment, Reservation, Orders, OutboxEvent, ProcessedEvent

@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_db():
    # Asynchronously create all tables defined in models.py in the target MySQL database
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest_asyncio.fixture
async def db_session():
    async with async_session() as session:
        yield session
