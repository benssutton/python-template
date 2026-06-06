from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from core.settings import get_settings

_settings = get_settings()

engine = create_async_engine(
    _settings.database_url,
    pool_pre_ping=True,
    pool_size=_settings.db_pool_size,
    max_overflow=_settings.db_pool_max_overflow,
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
