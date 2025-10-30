import io, requests, pandas as pd
from tqdm import tqdm
from datetime import date
from typing import Optional
from .common import to_json_rows, ensure_non_negative
from .loaders import replace_partition_housing, insert_housing_rows

NUMERIC_COLS = [
    'Voodikohad',
    'Tubade arv kokku',
    'Voodikohtade arv kõrghooajal',
    'Voodikohtade arv madalhooajal',
    'Haagissuvila kohtade arv',
    'Telkimiskohtade arv'
]

def _download_stream(url: str, chunk_size: int = 1 << 15) -> Optional[io.BytesIO]:
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    buf = io.BytesIO()
    with tqdm(total=total if total else None, unit='B', unit_scale=True, desc="housing") as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                buf.write(chunk); pbar.update(len(chunk))
    buf.seek(0)
    return buf

def _parse_excel_or_csv(buff: io.BytesIO) -> pd.DataFrame:
    # Peek signature to decide parser
    sig = buff.read(4)
    buff.seek(0)

    # XLSX files are ZIPs -> start with PK\x03\x04
    if sig.startswith(b'PK\x03\x04'):
        dfs = pd.read_excel(buff, sheet_name=None, engine="openpyxl")
        first = next(iter(dfs))
        return dfs[first]

    # Otherwise assume CSV; try utf-8 then a permissive fallback
    try:
        return pd.read_csv(buff)
    except UnicodeDecodeError:
        buff.seek(0)
        return pd.read_csv(buff, encoding="latin-1")


def fetch_and_clean(housing_url: str) -> pd.DataFrame:
    buff = _download_stream(housing_url)
    if buff is None:
        raise RuntimeError("Failed to download housing file")
    df = _parse_excel_or_csv(buff)
    # basic fixes
    df = ensure_non_negative(df, NUMERIC_COLS)
    # opening/seasonal columns can be left as-is or normalized in Silver
    return df

def load_month(period_date: date, housing_url: str, source_file: str = "housing_download") -> int:
    """
    Idempotent monthly load:
      - delete target month
      - insert rows with hashes
      - return inserted rowcount
    """
    df = fetch_and_clean(housing_url)

    # DQ in-batch: drop exact dup rows
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        print(f"DQ: dropped {dropped} duplicate rows (batch-level).")

    # write to ClickHouse
    replace_partition_housing(period_date)
    insert_housing_rows(
        period_date,
        source_url=housing_url,
        source_file=source_file,
        raw_json_rows=to_json_rows(df),
    )
    return len(df)
