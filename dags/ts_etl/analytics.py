"""Чистые функции аналитики временных рядов: без PG, без сети.

Вход/выход — pandas. Метки периодов намеренно НЕ парсятся в даты
(в источнике встречаются «Январь-июнь 2014» и т.п.) — упор на
``period TEXT + period_order INT``.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict, cast

import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import acf as sm_acf
from statsmodels.tsa.stattools import adfuller

SEASONAL_BY_FREQ: dict[str, int | None] = {
    "monthly": 12,
    "quarterly": 4,
    "annual": None,
}


def wide_to_long(dataset: dict[Any, Any]) -> pd.DataFrame:
    """Сырой dataset API -> long-формат.

    ``dataset``: {"periods": [...], "series": [{"label":..., "values":[...]}], ...}
    Результат: [series_label, period, period_order, value];
    null/NaN удалены, дубли по (series_label, period_order) удалены.
    """
    periods = dataset["periods"]
    rows = []
    for s in dataset["series"]:
        for order, (period, value) in enumerate(zip(periods, s["values"])):
            if value is None:
                continue
            value = float(value)
            if np.isnan(value):
                continue
            rows.append((s["label"], period, order, value))
    df = pd.DataFrame(rows, columns=["series_label", "period", "period_order", "value"])
    return df.drop_duplicates(subset=["series_label", "period_order"]).reset_index(
        drop=True
    )


def rolling_means(
    series: pd.DataFrame, windows: tuple[int, ...] = (3, 6, 12)
) -> pd.DataFrame:
    """Скользящее среднее ряда по 3 окнам.

    ``series``: long-таблица одного ряда [series_label, period, period_order, value].
    Результат: [period_order, window, value]; NaN-хвосты (первые window-1 точек) отброшены.
    """
    values = series.set_index("period_order")["value"].sort_index()
    parts = [
        pd.DataFrame({"window": w, "value": values.rolling(window=w).mean()})
        for w in windows
    ]
    out = pd.concat(parts).reset_index()
    return (
        out.dropna(subset=["value"])
        .sort_values(["window", "period_order"])
        .reset_index(drop=True)
    )


def decompose_series(
    values: pd.Series, seasonal_period: int | None, model: str = "add"
) -> dict[str, pd.Series]:
    """Разложение на observed/trend/seasonal/resid (``values`` — индекс period_order).

    При отсутствии сезона или ряду короче 2 циклов — fallback:
    тренд = центрированное скользящее 3, seasonal = NaN, resid = observed − trend.
    """
    if seasonal_period and len(values) >= 2 * seasonal_period:
        d = seasonal_decompose(
            values,
            period=seasonal_period,
            model=model,
            extrapolate_trend=seasonal_period - 1,
        )
        return {k: getattr(d, k) for k in ("observed", "trend", "seasonal", "resid")}
    trend = values.rolling(window=3, center=True, min_periods=1).mean()
    return {
        "observed": values,
        "trend": trend,
        "seasonal": pd.Series(np.nan, index=values.index),
        "resid": values - trend,
    }


def stationarity_adf(values: pd.Series) -> dict[str, float | bool]:
    """ADF-тест; is_stationary = p < 0.05."""
    stat, p, usedlag, nobs, crit, _ = cast(
        tuple[float, float, int, int, dict[str, float], int],
        adfuller(values.dropna(), autolag="AIC"),
    )
    return {
        "statistic": float(stat),
        "p_value": float(p),
        "usedlag": int(usedlag),
        "nobs": int(nobs),
        "crit_1pct": float(crit["1%"]),
        "crit_5pct": float(crit["5%"]),
        "crit_10pct": float(crit["10%"]),
        "is_stationary": bool(p < 0.05),
    }


def acf_series(values: pd.Series, nlags: int = 40, alpha: float = 0.05) -> pd.DataFrame:
    """ACF с доверительным коридором: [lag, acf, ci_lower, ci_upper]."""
    s = values.dropna()
    nlags = min(nlags, len(s) - 1)
    vals, ci = cast(
        "tuple[np.ndarray[Any, Any], np.ndarray[Any, Any]]",
        sm_acf(s, nlags=nlags, alpha=alpha),
    )
    df = pd.DataFrame({"lag": np.arange(len(vals)), "acf": vals})
    df["ci_lower"] = ci[:, 0]
    df["ci_upper"] = ci[:, 1]
    return df


def has_trend(values: pd.Series, alpha: float = 0.05) -> bool:
    return adfuller(values.dropna(), autolag="AIC")[1] > alpha


def has_seasonality(values: pd.Series, seasonal_period: int) -> bool:
    """Сезонность ищем по дедрендированному ряду: у чистого тренда ACF высока
    на всех лагах и даст ложный сигнал на лаге сезона."""
    w = min(seasonal_period, 5)
    detrended = values - values.rolling(window=w, center=True, min_periods=1).mean()
    df = acf_series(detrended, nlags=max(40, 2 * seasonal_period))
    row = df[df["lag"] == seasonal_period]
    if row.empty:
        return False
    a = float(row["acf"].iloc[0])
    return bool(a > 0.3 and a > float(row["ci_upper"].iloc[0]))


def select_model(
    values: pd.Series, seasonal_period: int | None
) -> Literal["ses", "holt", "hw"]:
    """Автоселектор: hw (тренд+сезон) > holt (тренд) > ses."""
    if seasonal_period and len(values) >= 2 * seasonal_period:
        if has_seasonality(values, seasonal_period):
            return "hw"
    if has_trend(values):
        return "holt"
    return "ses"


class _ForecastConfigRequired(TypedDict):
    """Обязательные поля :class:`ForecastConfig`."""

    model: Literal["ses", "holt", "hw"]


class ForecastConfig(_ForecastConfigRequired, total=False):
    """Конфиг модели прогноза (см. :func:`forecast_series`)."""

    trend: Literal["add", "mul"] | None
    seasonal: Literal["add", "mul"] | None
    alpha: float | None
    seasonal_period: int | None


def forecast_series(
    values: pd.Series, cfg: ForecastConfig, horizon: int
) -> tuple[pd.DataFrame, dict[str, float | list[float] | None]]:
    """Прогноз SES / Holt / Holt-Winters на ``horizon`` шагов вперёд.

    ``values`` индексирован по period_order. ``cfg``:
    {"model": "ses"|"holt"|"hw", "trend": "add"|"mul"|None,
     "seasonal": "add"|"mul"|None, "alpha": float|None, "seasonal_period": int|None}.
    Возвращает: (DataFrame [step, value], dict параметров модели).
    """
    v = values.dropna()
    model = cfg["model"]
    trend = cfg.get("trend") or ("add" if model in ("holt", "hw") else None)
    seasonal = cfg.get("seasonal") or ("add" if model == "hw" else None)
    seasonal_periods = cfg.get("seasonal_period") if model == "hw" else None
    es = ExponentialSmoothing(
        v,
        trend=trend,
        seasonal=seasonal,
        seasonal_periods=seasonal_periods,
        initialization_method="estimated",
    )
    alpha = cfg.get("alpha")
    fitted = es.fit(smoothing_level=alpha) if alpha is not None else es.fit()
    fc = fitted.forecast(steps=horizon)
    out = pd.DataFrame(
        {"step": np.arange(1, horizon + 1), "value": np.asarray(fc, dtype=float)}
    )
    params: dict[str, float | list[float] | None] = {}
    for k, x in dict(fitted.params).items():
        if x is None:
            params[k] = None
        elif isinstance(x, np.ndarray):
            params[k] = [float(v) for v in np.atleast_1d(x)]
        else:
            params[k] = float(x)
    return out, params
