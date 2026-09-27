# ==========================================
# Stage 1: Экспорт зависимостей через Poetry
# ==========================================
FROM python:3.13-slim-bookworm AS builder

WORKDIR /build

ARG POETRY_VERSION=2.5.1
ARG POETRY_EXPORT_VER=1.10.1
# Ставим Poetry во временный образ
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}" "poetry-plugin-export==${POETRY_EXPORT_VER}"

COPY pyproject.toml poetry.lock ./

# Экспортируем главные зависимости, airflow extra + опциональные EXTRAS
ARG EXTRAS=""
RUN poetry export -f requirements.txt \
  --extras "airflow ${EXTRAS}" \
  --without-hashes \
  -o /build/requirements.txt

# ==========================================
# Stage 2: Финальный образ Airflow
# ==========================================
ARG AIRFLOW_VERSION=3.3.2

FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.13

ARG AIRFLOW_VERSION
# 1. Системные зависимости (под root)
# USER root
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#         tesseract-ocr \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# 2. Установка Python-зависимостей (под airflow)
USER airflow
WORKDIR /opt/airflow

# Забираем чистый requirements.txt из этапа builder
COPY --from=builder /build/requirements.txt /tmp/requirements.txt

ARG PYTHON_VERSION=3.13
# Устанавливаем зависимости с учетом constraints Airflow
# (гарантирует совместимость версий зависимостей с предустановленным Airflow)
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
  --constraint "${CONSTRAINT_URL}" \
  -r /tmp/requirements.txt \
  && rm /tmp/requirements.txt
