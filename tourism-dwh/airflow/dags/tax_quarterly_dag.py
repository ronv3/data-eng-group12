from __future__ import annotations
import pendulum
from datetime import date
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from src.ingestion.tax import load_quarter
from src.ingestion.common import compute_quarter_start, ch_client
from airflow import Dataset
BRONZE_HOUSING_DS = Dataset("clickhouse://bronze/housing_raw")
BRONZE_TAX_DS     = Dataset("clickhouse://bronze/tax_raw")



TAX_BASE_URL = "https://www.emta.ee/en/business-client/board-news-and-contact/news-press-information-statistics/statistics-and-open-data"

@dag(
    dag_id="tax_quarterly",
    schedule="0 3 1 1,4,7,10 *",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    catchup=False,
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
            lines = [ln for ln in f.readlines() if not ln.strip().startswith("--")]
            sql = "".join(lines)
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            client.command(stmt)

    @task
    def extract_and_load(execution_date_str: str, which: str = "latest") -> int:
        logical = pendulum.parse(execution_date_str).date()
        period_quarter: date = compute_quarter_start(logical)
        return load_quarter(period_quarter=period_quarter, base_url=TAX_BASE_URL, which=which)

    @task(outlets=[BRONZE_TAX_DS])
    def dq_bronze(execution_date_str: str, inserted: int):
        import math
        logical = pendulum.parse(execution_date_str).date()
        period_quarter: date = compute_quarter_start(logical)
        client = ch_client("bronze")

        def _scalar(res):
            if hasattr(res, "first_row") and res.first_row is not None:
                return res.first_row[0]
            itm = res.first_item
            if isinstance(itm, dict):
                return next(iter(itm.values()))
            return itm

        res_cnt = client.query(
            "SELECT count() AS cnt FROM bronze.tax_raw WHERE period_quarter = %(p)s",
            parameters={"p": period_quarter.isoformat()}
        )
        cnt = int(_scalar(res_cnt) or 0)

        res_dup = client.query("""
                               SELECT coalesce(sum(c) - count(), 0) AS dup
                               FROM (SELECT record_hash, count() AS c
                                     FROM bronze.tax_raw
                                     WHERE period_quarter = %(p)s
                                     GROUP BY record_hash)
                               """, parameters={"p": period_quarter.isoformat()})
        dup = int(_scalar(res_dup) or 0)

        ins = int(inserted or 0)
        if cnt < 1 or cnt < ins:
            raise AirflowFailException(f"Tax DQ failed: expected >= {ins} rows, found {cnt} for {period_quarter}")
        if dup > 0:
            raise AirflowFailException(f"Tax DQ failed: found {dup} duplicate hashes for {period_quarter}")

    ddl = ensure_clickhouse_objects()
    rows = extract_and_load(execution_date_str="{{ ds }}", which="latest")
    dq   = dq_bronze(execution_date_str="{{ ds }}", inserted=rows)
    ddl >> rows >> dq

tax_quarterly()
