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
