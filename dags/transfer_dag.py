import logging
from datetime import UTC, datetime
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import (
    Variable,
    dag,  # pyright: ignore[reportUnknownVariableType]
    task,
)
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.exceptions import AirflowNotFoundException
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy import and_, insert, select

from dags.schemas import Target2Src, metadata_table, source, target
from plugins.transfer_manager import TransferDateTimeManager

logger = logging.getLogger(__name__)


@task(task_id="load_transfer_maping")
def load_transfer_maping() -> list[Target2Src]:
    logger.info("Start task get transfer columns")
    hook = PostgresHook(postgres_conn_id="metadata_conn_id")
    engine = hook.get_sqlalchemy_engine()
    ta = TypeAdapter(list[Target2Src])

    with engine.begin() as con:
        try:
            result = con.execute(metadata_table.select())
            mapings_rows = result.mappings().all()
            mapings_list = ta.validate_python(mapings_rows)

        except ValidationError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e

    if not len(mapings_list):
        logger.warning("Empty Transfer Table")
        raise AirflowNotFoundException("No columns to transfer")

    result = ta.dump_python(mapings_list, mode="json")
    return result


@task(task_id="get_last_update", multiple_outputs=True)
def get_last_update():
    init_date = Variable.get(key="init_date", default="1970-01-01 00:00:00")
    transfer_table = Variable.get(
        key="metadata_transfer_table", default="transfer_max_dates"
    )
    manager = TransferDateTimeManager(transfer_table, init_date=init_date)
    starttime = manager.get_max_date()
    return {"starttime": starttime}


@task(task_id="get_latest_row_in_src")
def get_latest_row_time() -> str:
    logger.info("Start task get_latest_row_time")
    hook = PostgresHook(postgres_conn_id="backend_conn_id")
    engine = hook.get_sqlalchemy_engine()

    class TsDict(BaseModel):
        ts: datetime

    with engine.begin() as con:
        try:
            result = con.execute(select(source.c.ts).order_by(source.c.ts.desc()))
            mapings_rows = result.mappings().first()
            if not mapings_rows:
                raise AirflowNotFoundException("Empty source table")
            logger.info(mapings_rows)
            max_row_time = TsDict.model_validate(mapings_rows)

        except ValidationError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e

    return max_row_time.ts.isoformat(sep=" ")


@task(task_id="transfer_data")
def transfer_data(starttime: Any, endtime: Any, src2target_list: Any):
    logger.info(starttime)
    ta = TypeAdapter(list[Target2Src])
    src2target = ta.validate_python(src2target_list)
    init_date = Variable.get(key="init_date", default="1970-01-01 00:00:00")
    transfer_table = Variable.get(
        key="metadata_transfer_table", default="transfer_max_dates"
    )
    hook = PostgresHook(postgres_conn_id="backend_conn_id")
    engine = hook.get_sqlalchemy_engine()

    # Сформировали колонки для переноса
    try:
        source_cols_with_map = []
        target_cols: list[str] = []
        for maping in src2target:
            col_instance = getattr(source.c, maping.source_name)
            source_cols_with_map.append(col_instance.label(maping.target_name))
            _ = getattr(target.c, maping.target_name)
            target_cols.append(maping.target_name)
    except Exception as e:
        logger.error("Error at map cols")
        logger.error(e)
        logger.debug("Source cols")
        logger.debug(source.columns)
        logger.debug("Target cols")
        logger.debug(target.columns)
        raise e

    # формируем запросы на выполнение
    stmt_select = select(*source_cols_with_map).where(
        and_(source.c.ts >= starttime, source.c.ts < endtime)
    )
    stmt_insert = insert(target).from_select(target_cols, stmt_select)
    # выполняем перенос сразу по месту без загрузки данных в программу
    manager = TransferDateTimeManager(transfer_table, init_date=init_date)
    with engine.begin() as con:
        try:
            result = con.execute(stmt_insert)
            logger.info(f"Insert in target table {result.rowcount} rows")

            # записываем сразу метаданные в этой транзакции
            manager.update_max_date(date_value=endtime)
            logger.info(f"Max date in meta table updated to {endtime}")

        except Exception as e:
            logger.error(e)
            raise e


@dag(
    dag_id="lab3-dag",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["debug"],
)
def debug_psql_dag():
    src2target = load_transfer_maping()
    work_dates = get_last_update()
    max_src_time = get_latest_row_time()
    if not isinstance(work_dates, PlainXComArg):
        raise TypeError("Bad arg type")
    transfer_res = transfer_data(
        starttime=work_dates["starttime"],
        endtime=max_src_time,
        src2target_list=src2target,
    )
    transfer_res.set_upstream([src2target, work_dates, max_src_time])


debug_psql = debug_psql_dag()
