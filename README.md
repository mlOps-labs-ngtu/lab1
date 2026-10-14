# Airflow Metadata lab

Так стартует весь проект:

```bash
docker compose up -d
```

так можно дебагинг стартануть

```bash
docker compose --profile debug up airflow-debug-worker
```

```sql
-- Наполнение таблицы transfer_metadata маппингом колонок source -> target
INSERT INTO transfer_metadata (id, source_name, target_name)
VALUES
    (1, 'column1',   'column7'),
    (2, 'i_column2', 'i_column8'),
    (3, 'column3',   'column9'),
    (4, 'i_column4', 'i_column10'),
    (5, 'column5',   'column11'),
    (6, 'i_column6', 'i_column12')
ON CONFLICT (id) DO UPDATE SET
    source_name = EXCLUDED.source_name,
    target_name = EXCLUDED.target_name;
```
