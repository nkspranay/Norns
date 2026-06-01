import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import NullPool
from sqlalchemy import text

from app.main import app
from app.database import Base, get_db
from app.config import settings

TEST_DATABASE_URL = settings.database_url.replace(
    "/scheduler_db", "/scheduler_test_db"
)

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    poolclass=NullPool,
)

TestSessionLocal = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@pytest_asyncio.fixture(scope="session")
async def setup_database():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(setup_database):
    yield
    async with test_engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE TABLE executions, jobs, workers RESTART IDENTITY CASCADE")
        )


@pytest_asyncio.fixture(scope="session")
async def db_session(setup_database):
    """Session-scoped DB session for direct assertions."""
    async with TestSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def client(setup_database):
    async def override_get_db():
        async with TestSessionLocal() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac

    app.dependency_overrides.clear()