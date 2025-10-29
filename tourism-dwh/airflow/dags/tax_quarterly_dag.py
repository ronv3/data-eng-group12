from __future__ import annotations
import os, pendulum
from datetime import date
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from src.ingestion.tax import load_quarter
from src.ingestion.common import compute_quarter_start, ch_client

TAX_BASE_URL = "https://www.emta.ee/en/business-client/board-news-and-contact/news-press-information-statistics/statistics-and-open-data"

@dag(
    dag_id="tax_quarterly",
    schedule="0 3 1 1,4,7,10 *",   # 03:00 on the 1st of Jan/Apr/Jul/Oct
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    default_args={"owner": "data-eng"},
    max_active_runs=1,
    tags=["bronze","tax"]
)
def tax_quarterly():
    @task
    def ensure_clickhouse_objects():
        client = ch_client("default")
        ddl_path = "/opt/airflow/include/clickhouse_ddl.sql"
        with open(ddl_path, "r", encoding="utf-8") as f:
            sql = f.read()
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            client.command(stmt)

    @task
    def extract_and_load(execution_date_str: str, which: str = "latest") -> int:
        logical = pendulum.parse(execution_date_str).date()
        period_quarter: date = compute_quarter_start(logical)
        return load_quarter(period_quarter=period_quarter, base_url=TAX_BASE_URL, which=which)

    @task
    def dq_bronze(execution_date_str: str, inserted: int):
        logical = pendulum.parse(execution_date_str).date()
        period_quarter: date = compute_quarter_start(logical)
        client = ch_client("bronze")
        cnt = client.query(
            "SELECT count() FROM bronze.tax_raw WHERE period_quarter = %(p)s",
            parameters={"p": period_quarter.isoformat()}
        ).first_item
        if cnt < 1 or cnt < inserted:
            raise AirflowFailException(f"Tax DQ failed: expected >= {inserted} rows, found {cnt}")

        dup = client.query("""
            SELECT sum(c)-count() AS dup
            FROM (
              SELECT record_hash, count() AS c
              FROM bronze.tax_raw
              WHERE period_quarter = %(p)s
              GROUP BY record_hash
            )
        """, parameters={"p": period_quarter.isoformat()}).first_item
        if dup and dup > 0:
            raise AirflowFailException(f"Tax DQ failed: found {dup} duplicate hashes in bronze.tax_raw for {period_quarter}")

tax_quarterly()
