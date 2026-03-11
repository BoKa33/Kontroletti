from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

# Use SQLite by default for portability
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./kontroletti.db")

engine = create_async_engine(DATABASE_URL, echo=False)  # Turn off echo for cleaner logs
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

async def get_db():
    async with async_session() as session:
        yield session

async def init_db():
    from .models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
