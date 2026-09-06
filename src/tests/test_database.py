import pytest
from sqlalchemy import select
from src.app.database import get_db

@pytest.mark.asyncio
async def test_db_connectivity():
    # Attempt to retrieve a database session and execute a simple ping query (SELECT 1)
    async for session in get_db():
        result = await session.execute(select(1))
        assert result.scalar() == 1
