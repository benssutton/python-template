from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from core.settings import Settings

_settings = Settings()

engine = create_async_engine(
    _settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
