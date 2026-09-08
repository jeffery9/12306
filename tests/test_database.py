import pytest
from sqlalchemy import select
from src.app.database import get_db

def test_db_connectivity(event_loop):
    """Verify core MySQL database connectivity and simple query execution."""
    async def _impl():
        async for session in get_db():
            result = await session.execute(select(1))
            assert result.scalar() == 1
            
    event_loop.run_until_complete(_impl())

def test_passenger_and_ticket_schema(db_session, event_loop):
    async def _impl():
        from src.app.models import Passenger, Ticket
        p = Passenger(id="PSG_TEST_01", name="验证君", id_no="110101199001011111", passenger_type="ADULT")
        db_session.add(p)
        await db_session.flush()
        assert p.id == "PSG_TEST_01"
    event_loop.run_until_complete(_impl())

