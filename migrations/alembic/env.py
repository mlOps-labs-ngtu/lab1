"""Alembic env: синхронный движок; URL — из TS_DATABASE_URL (или sqlalchemy.url в ini)."""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine

# корень проекта в sys.path, чтобы импортировать dags.ts_etl.db независимо от CWD
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dags.ts_etl.db import metadata

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

url = os.environ.get("TS_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
if not isinstance(url, str):
    raise SystemExit("TS_DATABASE_URL не задан")
config.set_main_option("sqlalchemy.url", url)


def run_migrations() -> None:
    if not isinstance(url, str):
        raise SystemExit("TS_DATABASE_URL не задан")
    engine = create_engine(url)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=url,
        target_metadata=metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    run_migrations()
