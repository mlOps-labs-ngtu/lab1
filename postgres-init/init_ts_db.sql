-- БД `ts` пайплайна временных рядов (rosstat_ts_etl).
-- Выполняется при первом старте postgres (volume postgres_vol пуст).
-- Создаётся ТОЛЬКО база данных; таблицы схемы `ts` — Alembic
-- (migrations/alembic, baseline из dags/ts_etl/db.metadata):
--   вручную: uv run airflow dags trigger ts_migrations
--   или:     таска ensure_schema ETL-дага rosstat_ts_etl
-- Разовое создание БД вне первого старта: migrations/create_ts_db.sh.

CREATE DATABASE ts;
