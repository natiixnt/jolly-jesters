import logging
import os
import time
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from sqlalchemy.exc import OperationalError
from alembic import context

from app.database import Base  # Base = declarative_base() z twoich modeli
import app.models as models    # import wszystkich modeli, żeby Base.metadata miał pełen schemat
from app.config import DATABASE_URL

# ustawiamy metadata do autogenerate
target_metadata = Base.metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")

# Zawsze wymuszamy użycie tej samej zmiennej DATABASE_URL, której używa aplikacja
config.set_main_option("sqlalchemy.url", DATABASE_URL)

ALEMBIC_MAX_RETRIES = max(1, int(os.getenv("ALEMBIC_MAX_RETRIES", "5")))
ALEMBIC_RETRY_DELAY = max(0.5, float(os.getenv("ALEMBIC_RETRY_DELAY", "2.0")))


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connect_kwargs = dict(config.get_section(config.config_ini_section, {}) or {})
    connect_kwargs["sqlalchemy.url"] = config.get_main_option("sqlalchemy.url")

    for attempt in range(1, ALEMBIC_MAX_RETRIES + 1):
        connectable = engine_from_config(
            connect_kwargs,
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )

        try:
            with connectable.connect() as connection:
                context.configure(connection=connection, target_metadata=target_metadata)

                with context.begin_transaction():
                    context.run_migrations()

            logger.info("Migrations applied successfully on attempt %s", attempt)
            break
        except OperationalError as exc:
            if attempt >= ALEMBIC_MAX_RETRIES:
                logger.error("Database connection failed after %s attempts", attempt)
                raise

            wait_time = ALEMBIC_RETRY_DELAY * attempt
            logger.warning(
                "Database not ready (attempt %s/%s): %s. Retrying in %.1f seconds...",
                attempt,
                ALEMBIC_MAX_RETRIES,
                exc,
                wait_time,
            )
            time.sleep(wait_time)
        finally:
            connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
