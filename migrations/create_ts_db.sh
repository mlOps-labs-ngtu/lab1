#!/usr/bin/env bash
# Создание БД `ts` (идемпотентно). Разовый шаг развёртывания.
# PostgreSQL не поддерживает CREATE DATABASE IF NOT EXISTS.
#
# Запуск (локально, из контейнера postgres):
#   docker compose -f compose.yaml run --rm --entrypoint bash postgres \
#     -c 'sh -c "cat > /tmp/create_ts_db.sh && sh /tmp/create_ts_db.sh" < migrations/create_ts_db.sh'
# Если контейнер postgres не запущен — сначала `docker compose -f compose.yaml up -d postgres`.
set -euo pipefail
DB_USER="${POSTGRES_USER:-postgres}"
if psql -U "$DB_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='ts'" | grep -q 1; then
  echo "БД ts уже существует — пропускаю"
  exit 0
fi
psql -U "$DB_USER" -c "CREATE DATABASE ts"
echo "БД ts создана"
