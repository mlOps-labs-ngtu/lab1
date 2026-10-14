import logging
import random
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task
from faker import Faker
from sqlalchemy import insert

from dags.schemas import source

logger = logging.getLogger(__name__)
fake = Faker("ru_RU")


# === Таска генерации фейковых данных ===
@task
def generate_fake_data(target_date: str) -> list[dict[str, Any]]:
    n_rows = 10
    logger.info(f"Generating {n_rows} fake rows for date {target_date}")

    # Парсим целевую дату
    target_dt = datetime.fromisoformat(target_date).replace()

    rows = []
    # Распределяем 10 записей равномерно в течение дня (каждые ~2.4 часа)
    time_interval = timedelta(hours=24) / n_rows

    for i in range(n_rows):
        timestamp = target_dt + (time_interval * i)
        rows.append(
            {
                "ts": timestamp,
                "column1": fake.word(),
                "i_column2": random.randint(10, 200),
                "column3": fake.company(),
                "i_column4": random.randint(500, 800),
                "column5": fake.city(),
                "i_column6": random.randint(-200, -10),
            }
        )
    return rows


# === Таска записи в PostgreSQL ===
@task
def load_to_postgres(fake_rows: Any):
    logger.info(f"Loading {len(fake_rows)} rows into 'source' table")
    hook = PostgresHook(postgres_conn_id="backend_conn_id")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        _ = conn.execute(insert(source), fake_rows)

    logger.info("Data successfully loaded into PostgreSQL")


# === Определение DAG ===
@dag(
    dag_id="fake_source_loader",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "fake_data", "postgres"],
)
def fake_source_loader_dag():
    data = generate_fake_data(target_date="{{ ds }}")
    _ = load_to_postgres(data)


dag_instance = fake_source_loader_dag()
