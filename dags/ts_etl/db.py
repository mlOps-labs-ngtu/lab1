"""SQLAlchemy Core-таблицы БД `ts` и хелперы записи.

`metadata` — источник правды для Alembic-миграций (migrations/alembic/versions);
схема БД `ts` создаётся `migrations/create_ts_db.sh` и применяется Alembic
(DAG `ts_migrations` + таска `ensure_schema` ETL-дага).
Типы универсальные (JSON/Double/Text) и SQLite-совместимые: server_default
только через text("CURRENT_TIMESTAMP") — func.now() в DDL ломает CREATE TABLE
на SQLite (тесты).
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    JSON, Boolean, Column, DateTime, Double, Integer, MetaData, Numeric,
    String, Table, delete, insert, select, text, update,
)

metadata = MetaData()

raw_datasets = Table(
    "raw_datasets", metadata,
    Column("ds_id", String, primary_key=True),
    Column("fetched_at", DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")),
    Column("title", String),
    Column("unit", String),
    Column("freq", String),
    Column("n_series", Integer),
    Column("n_periods", Integer),
    Column("payload", JSON, nullable=False),
)

series_config = Table(
    "series_config", metadata,
    Column("series_id", String, primary_key=True),
    Column("title", String),
    Column("unit", String),
    Column("freq", String),
    Column("source_url", String),
    Column("model", String(16), nullable=False, server_default=text("'auto'")),
    Column("trend", String(8)),
    Column("seasonal", String(8)),
    Column("alpha", Numeric(8, 6)),
    Column("horizon", Integer, nullable=False, server_default=text("12")),
    Column("last_seen_at", DateTime(timezone=True)),
    Column("n_points", Integer),
)

series_long = Table(
    "series_long", metadata,
    Column("ds_id", String, nullable=False, primary_key=True),
    Column("series_label", String, nullable=False, primary_key=True),
    Column("period_order", Integer, nullable=False, primary_key=True),
    Column("period", String, nullable=False),
    Column("value", Double, nullable=False),
)

rolling_means = Table(
    "rolling_means", metadata,
    Column("ds_id", String, nullable=False, primary_key=True),
    Column("series_label", String, nullable=False, primary_key=True),
    Column("period_order", Integer, nullable=False, primary_key=True),
    Column("window", Integer, nullable=False, primary_key=True),
    Column("value", Double, nullable=False),
)

decomposition = Table(
    "decomposition", metadata,
    Column("ds_id", String, nullable=False, primary_key=True),
    Column("series_label", String, nullable=False, primary_key=True),
    Column("period_order", Integer, nullable=False, primary_key=True),
    Column("component", String, nullable=False, primary_key=True),
    Column("value", Double),
)

stationarity = Table(
    "stationarity", metadata,
    Column("ds_id", String, nullable=False, primary_key=True),
    Column("series_label", String, nullable=False, primary_key=True),
    Column("statistic", Double),
    Column("p_value", Double),
    Column("usedlag", Integer),
    Column("nobs", Integer),
    Column("crit_1pct", Double),
    Column("crit_5pct", Double),
    Column("crit_10pct", Double),
    Column("is_stationary", Boolean),
)

acf_values = Table(
    "acf_values", metadata,
    Column("ds_id", String, nullable=False, primary_key=True),
    Column("series_label", String, nullable=False, primary_key=True),
    Column("lag", Integer, nullable=False, primary_key=True),
    Column("acf", Double, nullable=False),
    Column("ci_lower", Double),
    Column("ci_upper", Double),
)

forecasts = Table(
    "forecasts", metadata,
    Column("ds_id", String, nullable=False, primary_key=True),
    Column("series_label", String, nullable=False, primary_key=True),
    Column("model", String, nullable=False, primary_key=True),
    Column("step", Integer, nullable=False, primary_key=True),
    Column("model_used", String, nullable=False),
    Column("horizon", Integer, nullable=False),
    Column("value", Double, nullable=False),
    Column("params", JSON),
    Column("created_at", DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")),
)


def get_ts_database_url() -> str:
    """URL БД `ts` (пользователь/пароль совпадают с compose.yaml + secrets/db_password)."""
    return "postgresql+psycopg2://airflow:db-password@postgres:5432/ts"


def upsert_raw_dataset(con, ds: dict) -> None:
    """Upsert по ds_id, переписывает payload целиком."""
    values = dict(
        ds_id=ds["id"], title=ds.get("title"), unit=ds.get("unit"),
        freq=ds.get("freq"), n_series=ds.get("n_series"),
        n_periods=ds.get("n_periods"), payload=ds,
    )
    exists = con.execute(
        select(raw_datasets.c.ds_id).where(raw_datasets.c.ds_id == ds["id"])
    ).scalar_one_or_none()
    if exists is None:
        con.execute(insert(raw_datasets).values(**values))
    else:
        con.execute(
            update(raw_datasets)
            .where(raw_datasets.c.ds_id == ds["id"])
            .values({k: v for k, v in values.items() if k != "ds_id"})
        )


def upsert_series_config(
    con, *, series_id: str, title: str | None, unit: str | None, freq: str | None,
    source_url: str | None, n_points: int,
) -> None:
    """Новый ряд — insert с дефолтами; существующий — ТОЛЬКО авто-колонки.

    Пользовательские (model/trend/seasonal/alpha/horizon) не затрагиваются:
    ручной тюнинг аналитика не затирается повторными запусками extract.
    """
    exists = con.execute(
        select(series_config.c.series_id).where(series_config.c.series_id == series_id)
    ).scalar_one_or_none()
    if exists is None:
        con.execute(
            insert(series_config).values(
                series_id=series_id, title=title, unit=unit, freq=freq,
                source_url=source_url, n_points=n_points,
                last_seen_at=text("CURRENT_TIMESTAMP"),
            )
        )
    else:
        con.execute(
            update(series_config)
            .where(series_config.c.series_id == series_id)
            .values(
                title=title, unit=unit, freq=freq,
                source_url=source_url, n_points=n_points,
                last_seen_at=text("CURRENT_TIMESTAMP"),
            )
        )


def replace_rows(con, table, ds_id: str, rows: list[dict]) -> int:
    """DELETE по ds_id + INSERT в рамках текущей транзакции. Идемпотентно."""
    con.execute(delete(table).where(table.c.ds_id == ds_id))
    if rows:
        con.execute(insert(table), rows)
    return len(rows)


def run_ts_migrations(database_url: str) -> None:
    """Применить все миграции БД `ts` к `database_url` (alembic upgrade head, идемпотентно).

    Используется таской `ensure_schema` ETL-дага — гарантия актуальной схемы
    до любой записи. Корень проекта (migrations/alembic.ini) — parents[2] от
    этого файла; в контейнере это /opt/airflow (смонтированы dags/ и migrations/).
    """
    import os

    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parents[2]
    cfg = Config(str(root / "migrations" / "alembic.ini"))
    os.environ["TS_DATABASE_URL"] = database_url
    command.upgrade(cfg, "head")
