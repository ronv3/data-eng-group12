from typing import Iterable, Tuple, Dict, Any
from datetime import date
from .common import ch_client, record_sha256

def replace_partition_housing(period_date: date):
    """
    Idempotency: delete the target month before insert.
    """
    client = ch_client("bronze")
    client.command(
        "ALTER TABLE bronze.housing_raw DELETE WHERE period_date = %(p)s",
        parameters={"p": period_date.isoformat()}
    )

def replace_partition_tax(period_quarter: date):
    client = ch_client("bronze")
    client.command(
        "ALTER TABLE bronze.tax_raw DELETE WHERE period_quarter = %(p)s",
        parameters={"p": period_quarter.isoformat()}
    )

def insert_housing_rows(period_date: date,
                        source_url: str,
                        source_file: str,
                        raw_json_rows: Iterable[str]):
    client = ch_client("bronze")
    # compute hashes once to avoid CH computing
    rows = []
    for r in raw_json_rows:
        rows.append((
            period_date,
            source_url,
            source_file,
            r,
            record_sha256(r),
        ))
    if rows:
        client.insert(
            "bronze.housing_raw",
            rows,
            column_names=["period_date", "source_url", "source_file", "raw_json", "record_hash"]
        )

def insert_tax_rows(period_quarter: date,
                    which: str,
                    source_url: str,
                    source_file: str,
                    raw_json_rows: Iterable[str]):
    client = ch_client("bronze")
    rows = []
    for r in raw_json_rows:
        rows.append((
            period_quarter,
            which,
            source_url,
            source_file,
            r,
            record_sha256(r),
        ))
    if rows:
        client.insert(
            "bronze.tax_raw",
            rows,
            column_names=["period_quarter", "which", "source_url", "source_file", "raw_json", "record_hash"]
        )