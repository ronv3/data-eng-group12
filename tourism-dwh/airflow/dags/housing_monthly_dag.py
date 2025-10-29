from __future__ import annotations
import os, pendulum
from datetime import date
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from clickhouse_connect.driver import Client as _Client

# Import our ingestion code
from src.ingestion.housing import load_month
from src.ingestion.common import compute_month_start, ch_client

HOUSING_URL = "https://andmed.eesti.ee/api/datasets/f3324f95-e672-4041-804f-edf8b7083c43/files/701fb53b-73ee-4c67-b438-e5fed9e5429a/download-s3"

@dag(
    dag_id="housing_monthly",
    schedule="0 2 1 * *",     # 02:00 on the 1st of each month
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    default_args={"owner": "data-eng"},
    max_active_runs=1,
    tags=["bronze","housing"]
)
def housing_monthly():
    @task
    def ensure_clickhouse_objects():
        # Run our DDL file (idempotent) to make sure DB/Tables exist
        client = ch_client("default")
        ddl_path = "/opt/airflow/include/clickhouse_ddl.sql"
        with open(ddl_path, "r", encoding="utf-8") as f:
            sql = f.read()
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            client.command(stmt)

    @task
    def extract_and_load(execution_date_str: str) -> int:
        # Airflow passes logical date in UTC
        logical = pendulum.parse(execution_date_str).date()
        period_date: date = compute_month_start(logical)
        rowcount = load_month(period_date=period_date, housing_url=HOUSING_URL)
        return rowcount

    @task
    def dq_bronze(execution_date_str: str, inserted: int):
        logical = pendulum.parse(execution_date_str).date()
        period_date: date = compute_month_start(logical)
        client = ch_client("bronze")
        # 1) count rows for this period
        cnt = client.query(
            "SELECT count() FROM bronze.housing_raw WHERE period_date = %(p)s",
            parameters={"p": period_date.isoformat()}
        ).first_item
        if cnt < 1 or cnt < inserted:
            raise AirflowFailException(f"DQ failed: expected >= {inserted} rows, found {cnt}")

        # 2) duplicates by record_hash in this partition
        dup = client.query("""
            SELECT sum(c)-count() AS dup
            FROM (
              SELECT record_hash, count() AS c
              FROM bronze.housing_raw
              WHERE period_date = %(p)s
              GROUP BY record_hash
            )
        """, parameters={"p": period_date.isoformat()}).first_item
        if dup and dup > 0:
            raise AirflowFailException(f"DQ failed: found {dup} duplicate hashes in bronze.housing_raw for {period_date}")

housing_monthly()
