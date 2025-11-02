# housing.py
import io, requests, pandas as pd, unicodedata
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

# --- NEW: header normalization and aliasing ---
COLUMN_ALIASES = {
    # variants -> canonical
    "ettevõtte registrikood": "Ettevõtte registrikood",
    "ettev\u00f5tte registrikood": "Ettevõtte registrikood",
    "ettevotte registrikood": "Ettevõtte registrikood",
    "ettevõtte regnr": "Ettevõtte registrikood",
    "ettevõtte reg nr": "Ettevõtte registrikood",
    "registrikood": "Ettevõtte registrikood",
    "reg nr": "Ettevõtte registrikood",
}

def _canonicalize_cols(cols):
    normed = []
    for c in cols:
        s = unicodedata.normalize("NFKC", str(c))  # normalize accents/width
        s = s.replace("\u00A0", " ")               # NBSP -> space
        s = s.strip()
        normed.append(s)
    return normed

def _apply_aliases(cols):
    out = []
    for c in cols:
        out.append(COLUMN_ALIASES.get(c.lower(), c))
    return out
# --- end new helpers ---

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
    sig = buff.read(4); buff.seek(0)
    if sig.startswith(b'PK\x03\x04'):
        dfs = pd.read_excel(buff, sheet_name=None, engine="openpyxl")
        first = next(iter(dfs))
        return dfs[first]
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

    # --- NEW: stabilize headers so JSON keys are stable going forward ---
    df.columns = _canonicalize_cols(df.columns)
    df.columns = _apply_aliases(df.columns)

    df = ensure_non_negative(df, NUMERIC_COLS)
    df = df.where(pd.notna(df), None)  # pandas dumps None -> JSON null
    return df

def load_month(period_date: date, housing_url: str, source_file: str = "housing_download") -> int:
    df = fetch_and_clean(housing_url)

    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        print(f"DQ: dropped {dropped} duplicate rows (batch-level).")

    replace_partition_housing(period_date)
    insert_housing_rows(
        period_date,
        source_url=housing_url,
        source_file=source_file,
        raw_json_rows=to_json_rows(df),
    )
    return len(df)