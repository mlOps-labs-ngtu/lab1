"""DAG-миграций: применяет Alembic-миграции схемы БД `ts`.

Ручная точка: первое развёртывание, изменение схемы. Повторный запуск — no-op
(alembic upgrade head идемпотентен). ETL-даг также сам применяет миграции
идемпотентной таской ensure_schema — обе точки безопасны одновременно.

BashOperator запускается в /opt/airflow (WORKDIR Dockerfile); migrations/
смонтан в контейнер (compose.airflow.yaml), alembic — в образе.
URL БД задаётся env-переменной TS_DATABASE_URL прямо в bash_command.
"""

from datetime import UTC, datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

TS_MIGRATION_ENV = (
    "TS_DATABASE_URL=postgresql+psycopg2://airflow:db-password@postgres:5432/ts "
    "alembic -c migrations/alembic.ini upgrade head"
)
DAG_DIR = Path(__file__).resolve().parent
MIGRATIONS_CWD = DAG_DIR.parent


@dag(
    dag_id="ts_migrations",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["lab", "migrations"],
)
def ts_migrations_dag() -> None:
    _ = BashOperator(
        task_id="run_migrations", bash_command=TS_MIGRATION_ENV, cwd=str(MIGRATIONS_CWD)
    )


ts_migrations_dag()
