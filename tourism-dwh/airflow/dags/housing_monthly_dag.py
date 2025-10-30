from __future__ import annotations
import pendulum
from datetime import date
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from src.ingestion.housing import load_month
from src.ingestion.common import compute_month_start, ch_client
from airflow import Dataset
BRONZE_HOUSING_DS = Dataset("clickhouse://bronze/housing_raw")
BRONZE_TAX_DS     = Dataset("clickhouse://bronze/tax_raw")


HOUSING_URL = "https://andmed.eesti.ee/api/datasets/f3324f95-e672-4041-804f-edf8b7083c43/files/701fb53b-73ee-4c67-b438-e5fed9e5429a/download-s3"

@dag(
    dag_id="housing_monthly",
    schedule="0 2 1 * *",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "data-eng"},
    max_active_runs=1,
    tags=["bronze","housing"]
)
def housing_monthly():
    @task
    def ensure_clickhouse_objects():
        client = ch_client("default")  # create DBs/tables from default
        ddl_path = "/opt/airflow/include/clickhouse_ddl.sql"
        with open(ddl_path, "r", encoding="utf-8") as f:
            # strip '--' comments before splitting
            lines = [ln for ln in f.readlines() if not ln.strip().startswith("--")]
            sql = "".join(lines)
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            client.command(stmt)

    @task
    def extract_and_load(execution_date_str: str) -> int:
        logical = pendulum.parse(execution_date_str).date()
        period_date: date = compute_month_start(logical)  # label snapshot by month start
        rowcount = load_month(period_date=period_date, housing_url=HOUSING_URL)
        return rowcount

    @task(outlets=[BRONZE_HOUSING_DS])
    def dq_bronze(execution_date_str: str, inserted: int):
        import math
        logical = pendulum.parse(execution_date_str).date()
        period_date: date = compute_month_start(logical)
        client = ch_client("bronze")

        def _scalar(res):
            # Prefer tuple-based first_row; fall back to dict/first_item
            if hasattr(res, "first_row") and res.first_row is not None:
                return res.first_row[0]
            itm = res.first_item
            if isinstance(itm, dict):
                return next(iter(itm.values()))
            return itm

        # 1) count rows for this period (alias ensures stable key)
        res_cnt = client.query(
            "SELECT count() AS cnt FROM bronze.housing_raw WHERE period_date = %(p)s",
            parameters={"p": period_date.isoformat()}
        )
        cnt = int(_scalar(res_cnt) or 0)

        # 2) duplicates by record_hash in this partition (alias dup; COALESCE to 0)
        res_dup = client.query("""
                               SELECT coalesce(sum(c) - count(), 0) AS dup
                               FROM (SELECT record_hash, count() AS c
                                     FROM bronze.housing_raw
                                     WHERE period_date = %(p)s
                                     GROUP BY record_hash)
                               """, parameters={"p": period_date.isoformat()})
        dup = int(_scalar(res_dup) or 0)

        ins = int(inserted or 0)
        if cnt < 1 or cnt < ins:
            raise AirflowFailException(f"Housing DQ failed: expected >= {ins} rows, found {cnt} for {period_date}")
        if dup > 0:
            raise AirflowFailException(f"Housing DQ failed: found {dup} duplicate hashes for {period_date}")


    # INSTANTIATE + CHAIN TASKS
    ddl = ensure_clickhouse_objects()
    rows = extract_and_load(execution_date_str="{{ ds }}")
    dq   = dq_bronze(execution_date_str="{{ ds }}", inserted=rows)
    ddl >> rows >> dq

housing_monthly()
