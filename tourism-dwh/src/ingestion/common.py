import os, json, hashlib
from typing import Iterable, Dict, Any, Optional
from datetime import date
import clickhouse_connect

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
DB_BRONZE = os.environ.get("CLICKHOUSE_DB_BRONZE", "bronze")

def ch_client(database: Optional[str] = None):
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=database or DB_BRONZE,
        secure=False
    )

def normalize_json_record(row: Dict[str, Any]) -> str:
    """
    Deterministic JSON for hashing.
    """
    return json.dumps(row, sort_keys=True, ensure_ascii=False)

def record_sha256(raw_json: str) -> str:
    return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

def to_json_rows(df) -> Iterable[str]:
    for rec in df.to_dict(orient="records"):
        yield normalize_json_record(rec)

def ensure_non_negative(df, numeric_cols):
    for c in numeric_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)
            df.loc[df[c] < 0, c] = 0
    return df

def compute_month_start(d: date) -> date:
    return d.replace(day=1)

def compute_quarter_start(d: date) -> date:
    q = (d.month - 1) // 3  # 0..3
    first_month = q * 3 + 1
    return d.replace(month=first_month, day=1)
