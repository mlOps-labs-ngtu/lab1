# ----------------------------------------------------
# 1. Глобальные аргументы (до первого FROM)
# ----------------------------------------------------
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.13

FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root pyproject.toml uv.lock ./

ARG EXTRAS=""
# Монтируем бинарник uv прямо из его образа на время выполнения шага
RUN --mount=from=ghcr.io/astral-sh/uv:latest,source=/uv,target=/bin/uv \
  uv export --no-dev --no-cache --frozen --no-hashes \
  --extra airflow \
  ${EXTRAS:+--extra $EXTRAS} \
  -o /tmp/requirements.txt \
  && pip install --no-cache-dir -r /tmp/requirements.txt \
  && rm /tmp/requirements.txt
