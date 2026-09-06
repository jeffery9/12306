from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from src.app.config import settings

# Create standard SQLAlchemy Async engine
engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)

# Build a thread-safe Async session maker
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Shared base class for SQLAlchemy entities
Base = declarative_base()

async def get_db():
    async with async_session() as session:
        yield session
