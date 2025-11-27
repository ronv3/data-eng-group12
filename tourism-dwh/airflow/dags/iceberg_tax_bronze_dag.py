from __future__ import annotations

import os
import time
from datetime import date

import pandas as pd
import pendulum
import pyarrow as pa
import requests
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from src.ingestion.common import compute_quarter_start, ch_client

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, DateType, StringType, TimestampType

# Dataset produced by the bronze tax loader DAG
BRONZE_TAX_DS = Dataset("clickhouse://bronze/tax_raw")

# Iceberg namespace.table
ICE_NS = os.environ.get("ICEBERG_NAMESPACE", "bronze")
ICE_TABLE = os.environ.get("ICEBERG_TAX_TABLE", "tax_raw")
ICE_FULL_NAME = f"{ICE_NS}.{ICE_TABLE}"


@dag(
    dag_id="iceberg_bronze_tax",
    schedule=[BRONZE_TAX_DS],
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Tallinn"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "iceberg", "tax"],
)
def iceberg_bronze_tax():
    @task
    def wait_for_iceberg_rest():
        """
        Ping the Iceberg REST catalog until it's reachable.
        """
        base = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg_rest:8181").rstrip("/")
        url = f"{base}/v1/config"
        last_status, last_exc = None, None
        for _ in range(60):  # ~2 minutes
            try:
                r = requests.get(url, timeout=3)
                last_status = r.status_code
                if 200 <= r.status_code < 300:
                    return
            except Exception as e:
                last_exc = e
            time.sleep(2)
        raise AirflowFailException(
            f"Iceberg REST not reachable. uri={base} last_status={last_status} last_exc={last_exc}"
        )

    @task
    def ensure_namespace_and_table() -> str:
        """
        Ensure the namespace and table exist in the REST catalog (idempotent).
        """
        catalog = load_catalog("rest")  # reads PYICEBERG_CATALOG__REST__* envs

        # Namespace
        try:
            catalog.create_namespace(ICE_NS)
        except Exception:
            pass  # already exists

        # Table: try to load, else create (unpartitioned; simple for PyIceberg 0.6)
        try:
            catalog.load_table(ICE_FULL_NAME)
        except Exception:
            schema = Schema(
                NestedField(1, "period_quarter", DateType(), required=False),
                NestedField(2, "which",          StringType(), required=False),
                NestedField(3, "source_url",     StringType(), required=False),
                NestedField(4, "source_file",    StringType(), required=False),
                NestedField(5, "raw_json",       StringType(), required=False),
                NestedField(6, "record_hash",    StringType(), required=False),
                NestedField(7, "ingested_at",    TimestampType(), required=False),
            )
            catalog.create_table(
                ICE_FULL_NAME,
                schema=schema,
                properties={"format-version": "2"},
            )
        return ICE_FULL_NAME

    @task
    def write_quarter(execution_date_str: str, table_name: str) -> int:
        """
        Read the exact quarter slice from ClickHouse bronze and append only NEW rows
        (dedup on record_hash). Idempotent without needing Iceberg delete/overwrite APIs.
        Also forces Arrow schema types to match Iceberg (ingested_at -> timestamp[us]).
        """
        # Use built-in datetime.date to avoid Pendulum Date literal issues
        logical = date.fromisoformat(execution_date_str)  # "YYYY-MM-DD" -> datetime.date
        period_quarter: date = compute_quarter_start(logical)

        # Read from bronze (ClickHouse)
        client = ch_client("bronze")
        df: pd.DataFrame = client.query_df(
            """
            SELECT period_quarter, which, source_url, source_file, raw_json, record_hash, ingested_at
            FROM bronze.tax_raw
            WHERE period_quarter = %(p)s
            """,
            parameters={"p": period_quarter.isoformat()},
        )
        if df.empty:
            return 0

        # Normalize types for Arrow -> Iceberg
        df["period_quarter"] = pd.to_datetime(df["period_quarter"]).dt.date  # -> date32
        # Make naive (no tz) and later cast to microseconds
        df["ingested_at"] = pd.to_datetime(df["ingested_at"]).dt.tz_localize(None)

        # Build Arrow table with explicit schema (force timestamp[us])
        arrow_schema = pa.schema([
            pa.field("period_quarter", pa.date32()),
            pa.field("which",          pa.string()),
            pa.field("source_url",     pa.string()),
            pa.field("source_file",    pa.string()),
            pa.field("raw_json",       pa.string()),
            pa.field("record_hash",    pa.string()),
            pa.field("ingested_at",    pa.timestamp("us")),  # <- critical
        ])
        arrow_tbl: pa.Table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False, safe=False)

        # Load Iceberg table
        catalog = load_catalog("rest")
        tbl = catalog.load_table(table_name)

        # Fetch existing hashes to make append idempotent (small table -> OK to scan)
        existing_hashes = set()
        existing = tbl.scan().to_arrow()
        if existing is not None and existing.num_rows > 0:
            existing_df = existing.to_pandas(types_mapper=None)
            if "record_hash" in existing_df.columns:
                existing_hashes = set(existing_df["record_hash"].dropna().astype(str).unique())

        new_df = df[~df["record_hash"].astype(str).isin(existing_hashes)]
        if new_df.empty:
            return 0

        # Rebuild Arrow table for the new rows with the same schema
        arrow_new: pa.Table = pa.Table.from_pandas(new_df, schema=arrow_schema, preserve_index=False, safe=False)

        # Simple append
        tbl.append(arrow_new)
        return len(new_df)

    rest_ok = wait_for_iceberg_rest()
    table_ok = ensure_namespace_and_table()
    written = write_quarter(execution_date_str="{{ ds }}", table_name=table_ok)

    rest_ok >> table_ok >> written


iceberg_bronze_tax()
