from sqlalchemy.orm import Mapped, mapped_column

from persistence.transaction_store.postgres.postgres_base import PostgresBase


class Configuration(PostgresBase):
    __tablename__ = "configuration"

    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(nullable=False)
