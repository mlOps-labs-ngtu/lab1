"""HTTP-клиент JSON-API roschart.ru (данные официальных сборников Росстата).

Документация: https://roschart.ru/docs
Базовые запросы работают без ключа; для полного каталога — заголовок
``X-API-Key`` (Airflow Variable ``rosstat_api_key``).
"""

from __future__ import annotations

from typing import Self, cast

import httpx

ROSCHART_API = "https://roschart.ru/api"
REQUEST_TIMEOUT = 60


class RosstatClient:
    _client: httpx.Client

    def __init__(
        self, api_key: str | None = None, base_url: str = ROSCHART_API
    ) -> None:
        headers = {"X-API-Key": api_key} if api_key else {}
        # завершающий "/" обязателен: httpx склеивает base_url и путь
        # как бинарные префиксы (иначе "/api" + "/meta" -> "/apimeta")
        self._client = httpx.Client(
            base_url=base_url.rstrip("/") + "/",
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

    def _get(self, path: str, **params: str | float | bool) -> dict[str, object]:
        resp = self._client.get(path, params=params)
        return cast("dict[str, object]", resp.raise_for_status().json())

    def meta(self) -> dict[str, object]:
        """Состояние БД источника: n_datasets, n_values, upload_id."""
        return self._get("/meta")

    def search(
        self, query: str, kind: str = "timeseries", limit: int = 10
    ) -> list[dict[str, object]]:
        """Поиск наборов; элемент: {"ds_id": ..., "title": ..., ...}."""
        items = self._get("/catalog", q=query, kind=kind, limit=limit).get("items", [])
        return cast("list[dict[str, object]]", items)

    def dataset(self, ds_id: str) -> dict[str, object]:
        """Полный набор. Ответ API: {"ok": ..., "dataset": {...}} — возвращаем "dataset"."""
        return cast("dict[str, object]", self._get(f"/dataset/{ds_id}")["dataset"])

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
