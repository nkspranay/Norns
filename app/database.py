from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

# Create the async engine — this is the core connection to PostgreSQL
engine = create_async_engine(
    settings.database_url,
    echo=True,          # logs every SQL query to terminal — very useful during development
    pool_size=10,       # keep 10 connections open and ready
    max_overflow=20,    # allow up to 20 extra connections under heavy load
    pool_pre_ping=True, # test connections before using them — prevents stale connection errors
)

# Session factory — creates new database sessions
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,  # keep objects accessible after commit without re-querying
)

# Base class for all database models
class Base(DeclarativeBase):
    pass

# Dependency — used in FastAPI endpoints to get a database session
async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()