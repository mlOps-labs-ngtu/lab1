"""Тесты Alembic-миграций схемы БД `ts`.

Миграции применяются к временной SQLite-БД (TS_DATABASE_URL) — PostgreSQL
не нужен. Проверки: baseline создаёт все 8 таблиц из dags.ts_etl.db.metadata,
колонки совпадают с metadata, upgrade идемпотентен.
"""
import os
import pathlib
import subprocess
import sys

from sqlalchemy import MetaData, create_engine, inspect

from dags.ts_etl import db

EXPECTED_TABLES = {
    "raw_datasets", "series_config", "series_long", "rolling_means",
    "decomposition", "stationarity", "acf_values", "forecasts",
}
# alembic_version — служебная таблица версий Alembic, тоже создаётся при upgrade
EXPECTED_TABLES_WITH_ALEMBIC = EXPECTED_TABLES | {"alembic_version"}

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _url(tmp_path):
    return f"sqlite:///{tmp_path / 'ts_test.db'}"


def _run_alembic_upgrade(url: str) -> None:
    env = {**os.environ, "TS_DATABASE_URL": url}
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini",
         "upgrade", "head"],
        check=True, env=env, cwd=str(PROJECT_ROOT),
    )


def test_baseline_migration_creates_all_tables(tmp_path):
    # миграции применяются к пустой БД
    url = _url(tmp_path)
    _run_alembic_upgrade(url)
    assert set(inspect(create_engine(url)).get_table_names()) == EXPECTED_TABLES_WITH_ALEMBIC


def test_migration_matches_metadata(tmp_path):
    # ревьюшн должно давать ту же схему, что metadata из dags.ts_etl.db
    url = _url(tmp_path)
    _run_alembic_upgrade(url)
    insp = inspect(create_engine(url))
    for name, table in db.metadata.tables.items():
        db_cols = {c.name for c in table.columns}
        real_cols = {c["name"] for c in insp.get_columns(name)}
        assert db_cols == real_cols, f"{name}: {db_cols ^ real_cols}"


def test_upgrade_is_idempotent(tmp_path):
    url = _url(tmp_path)
    for _ in range(2):
        _run_alembic_upgrade(url)
    assert set(inspect(create_engine(url)).get_table_names()) == EXPECTED_TABLES_WITH_ALEMBIC
