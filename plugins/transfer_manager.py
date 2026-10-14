from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook


class TransferDateTimeManager:
    def __init__(
        self,
        table_name: str,
        init_date: str,
        postgres_conn_id: str = "metadata_conn_id",
    ):
        self.table_name = table_name
        self.init_date = init_date
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def get_max_date(self) -> datetime:
        sql = """
            SELECT COALESCE(MAX(max_date), %s)
            FROM transfer_max_dates
            WHERE table_name = %s
        """
        result = self.hook.get_first(sql, parameters=(self.init_date, self.table_name))
        max_date = result[0]
        if not isinstance(max_date, datetime):
            raise TypeError("Bad datetime")
        return max_date

    def update_max_date(
        self, date_value: datetime, updated_at: datetime | None = None
    ) -> None:
        if not isinstance(updated_at, datetime):
            updated_at = datetime.now()
        sql = """
            INSERT INTO transfer_max_dates (table_name, max_date, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE
            SET max_date = EXCLUDED.max_date,
                updated_at = EXCLUDED.updated_at
        """
        self.hook.run(sql, parameters=(self.table_name, date_value, updated_at))
