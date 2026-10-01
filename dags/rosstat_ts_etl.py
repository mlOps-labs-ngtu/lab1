"""DAG: временные ряды Росстата (roschart.ru) -> Postgres (БД `ts`) + аналитика.

extract (raw wide + config) → dedup/clean → long
→ 5 аналитических тасок параллельно:
скользящие средние, разложение, ADF, ACF, прогноз (автоселектор SES/Holt/HW).

Схема БД `ts` — Alembic (baseline в migrations/alembic/versions); применяется:
- вручную: `uv run airflow dags trigger ts_migrations` (BashOperator);
- автоматически: таской `ensure_schema` ETL-дага (in-process, no-op при актуальной схеме).
Подключение `ts_etl_conn_id` — из env `AIRFLOW_CONN_TS_ETL_CONN_ID` в compose.
"""
from __future__ import annotations

import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task
from sqlalchemy import select

from dags.ts_etl.analytics import (
    SEASONAL_BY_FREQ, acf_series, decompose_series, forecast_series, rolling_means,
    select_model, stationarity_adf, wide_to_long,
)
from dags.ts_etl.client import RosstatClient
from dags.ts_etl.db import (
    acf_values, decomposition, forecasts, raw_datasets,
    replace_rows, rolling_means as t_rolling_means,
    series_config, series_long, stationarity, upsert_raw_dataset, upsert_series_config,
)

logger = logging.getLogger(__name__)

CONN_ID = "ts_etl_conn_id"
ROLLING_WINDOWS = (3, 6, 12)


def _engine():
    return PostgresHook(CONN_ID).get_sqlalchemy_engine()


@task(task_id="extract_dataset")
def extract_dataset(ds_id: str) -> dict:
    """1) GET /api/dataset/{ds_id}; 2) upsert raw_datasets + series_config."""
    client = RosstatClient(api_key=Variable.get("rosstat_api_key", default=None))
    try:
        ds = client.dataset(ds_id)
    except Exception:
        logger.exception("Не удалось получить датасет «%s» из roschart.ru", ds_id)
        raise
    n_points = sum(sum(v is not None for v in s["values"]) for s in ds["series"])
    with _engine().begin() as con:
        upsert_raw_dataset(con, ds)
        upsert_series_config(
            con, series_id=ds_id, title=ds.get("title"), unit=ds.get("unit"),
            freq=ds.get("freq"), source_url=ds.get("source_url"), n_points=n_points,
        )
    logger.info("Датасет %s: series=%s periods=%s", ds_id, ds["n_series"], ds["n_periods"])
    return {"ds_id": ds_id, "freq": ds.get("freq"), "title": ds.get("title")}


@task(task_id="dedup_clean_to_long")
def dedup_clean_to_long(meta: dict) -> int:
    """Свежий payload из raw_datasets → wide_to_long (dedup + NaN) → series_long."""
    ds_id = meta["ds_id"]
    with _engine().connect() as con:
        raw = con.execute(
            select(raw_datasets.c.payload).where(raw_datasets.c.ds_id == ds_id)
        ).scalar_one()
    df = wide_to_long(raw)
    rows = df.assign(ds_id=ds_id)[
        ["ds_id", "series_label", "period", "period_order", "value"]
    ].to_dict("records")
    with _engine().begin() as con:
        replace_rows(con, series_long, ds_id, rows)
    logger.info("series_long: %s строк (ds_id=%s)", len(rows), ds_id)
    return len(rows)


def _series_frames(ds_id: str) -> pd.DataFrame:
    """series_long по ds_id -> [series_label, period, period_order, value]."""
    with _engine().connect() as con:
        rows = con.execute(
            select(
                series_long.c.series_label, series_long.c.period,
                series_long.c.period_order, series_long.c.value,
            )
            .where(series_long.c.ds_id == ds_id)
            .order_by(series_long.c.series_label, series_long.c.period_order)
        ).all()
    return pd.DataFrame(rows, columns=["series_label", "period", "period_order", "value"])


def _frames_by_series(df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    """Группы по series_label; ряды короче 2 точек пропускаются с warning."""
    out = []
    for label, g in df.groupby("series_label"):
        if len(g) < 2:
            logger.warning("Ряд «%s» короче 2 точек — пропускаем", label)
            continue
        out.append((label, g.sort_values("period_order")))
    return out


def _per_series(meta: dict, table, build) -> int:
    """Читает series_long, применяет build(ds_id, label, g) -> list[dict] к каждому ряду,
    переписывает таблицу по ds_id (DELETE + INSERT в одной транзакции)."""
    ds_id = meta["ds_id"]
    rows: list[dict] = []
    for label, g in _frames_by_series(_series_frames(ds_id)):
        rows.extend(build(ds_id, label, g))
    with _engine().begin() as con:
        replace_rows(con, table, ds_id, rows)
    logger.info("%s: %s строк (ds_id=%s)", table.name, len(rows), ds_id)
    return len(rows)


@task(task_id="compute_rolling")
def compute_rolling(meta: dict) -> int:
    def build(ds_id, label, g):
        rm = rolling_means(g, windows=ROLLING_WINDOWS)
        return [
            {"ds_id": ds_id, "series_label": label, "period_order": int(r.period_order),
             "window": int(r.window), "value": float(r.value)}
            for r in rm.itertuples()
        ]
    return _per_series(meta, t_rolling_means, build)


@task(task_id="decompose_series")
def decompose_series_task(meta: dict) -> int:
    sp = SEASONAL_BY_FREQ.get(meta.get("freq"))

    def build(ds_id, label, g):
        values = g.set_index("period_order")["value"]
        comps = decompose_series(values, seasonal_period=sp)
        out = []
        for comp, s in comps.items():
            for order, v in s.items():
                if pd.notna(v):
                    out.append({"ds_id": ds_id, "series_label": label,
                                "period_order": int(order), "component": comp,
                                "value": float(v)})
        return out
    return _per_series(meta, decomposition, build)


@task(task_id="compute_stationarity")
def compute_stationarity(meta: dict) -> int:
    def build(ds_id, label, g):
        r = stationarity_adf(g.set_index("period_order")["value"])
        return [{"ds_id": ds_id, "series_label": label, **r}]
    return _per_series(meta, stationarity, build)


@task(task_id="compute_acf")
def compute_acf(meta: dict) -> int:
    def build(ds_id, label, g):
        df = acf_series(g.set_index("period_order")["value"])
        return [
            {"ds_id": ds_id, "series_label": label, "lag": int(r.lag),
             "acf": float(r.acf), "ci_lower": float(r.ci_lower),
             "ci_upper": float(r.ci_upper)}
            for r in df.itertuples()
        ]
    return _per_series(meta, acf_values, build)


@task(task_id="forecast_series")
def forecast_series_task(meta: dict, context: dict | None = None) -> int:
    """Прогноз по каждому ряду. horizon: run_conf > series_config > 12.
    model: series_config.model; 'auto' → analytics.select_model."""
    ds_id = meta["ds_id"]
    run_conf = (context or {}).get("run_conf") or {}
    sp = SEASONAL_BY_FREQ.get(meta.get("freq"))
    with _engine().connect() as con:
        cfg_row = con.execute(
            select(series_config).where(series_config.c.series_id == ds_id)
        ).mappings().first()
    rows: list[dict] = []
    for label, g in _frames_by_series(_series_frames(ds_id)):
        values = g.set_index("period_order")["value"]
        cfg = {
            "model": cfg_row["model"] if cfg_row else "auto",
            "trend": cfg_row["trend"] if cfg_row else None,
            "seasonal": cfg_row["seasonal"] if cfg_row else None,
            "alpha": float(cfg_row["alpha"]) if cfg_row and cfg_row["alpha"] is not None else None,
            "seasonal_period": sp,
        }
        horizon = int(run_conf.get("horizon")
                      or (cfg_row["horizon"] if cfg_row else None) or 12)
        model_used = cfg["model"] if cfg["model"] != "auto" else select_model(values, sp)
        cfg["model"] = model_used
        fc, params = forecast_series(values, cfg, horizon)
        for r in fc.itertuples():
            rows.append({"ds_id": ds_id, "series_label": label,
                         "model": cfg["model"], "model_used": model_used,
                         "horizon": horizon, "step": int(r.step),
                         "value": float(r.value), "params": params})
        logger.info("%s: model=%s horizon=%s", label, model_used, horizon)
    with _engine().begin() as con:
        replace_rows(con, forecasts, ds_id, rows)
    logger.info("forecasts: %s строк (ds_id=%s)", len(rows), ds_id)
    return len(rows)


@dag(
    dag_id="rosstat_ts_etl", schedule=None,
    start_date=pd.Timestamp("2026-01-01").to_pydatetime(),
    catchup=False, tags=["lab", "rosstat"], default_args={"retries": 2},
)
def rosstat_ts_etl(ds_id: str = "vvp-kvartal-s-1995-2026-valovoy-vnutrenniy-produkt") -> None:
    meta = extract_dataset(ds_id=ds_id)
    dedup_clean_to_long(meta)
    compute_rolling(meta)
    decompose_series_task(meta)
    compute_stationarity(meta)
    compute_acf(meta)
    forecast_series_task(meta)


rosstat_ts_etl_dag = rosstat_ts_etl()
