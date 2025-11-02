import io, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from typing import Literal
from .common import to_json_rows
from .loaders import replace_partition_tax, insert_tax_rows

LATEST_TITLE = "Data for the current and the previous year (CSV)"
HISTORICAL_TITLE = "Data from previous years (CSV)"

def _resolve_url(base_url: str, title: str) -> str:
    resp = requests.get(base_url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    for a in soup.find_all('a', href=True, title=True):
        if a['title'].strip() == title:
            return a['href']
    raise RuntimeError(f"Could not find link with title: {title}")

def _download_csv(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))

def fetch(which: Literal['latest','historical','both'], base_url: str) -> pd.DataFrame:
    if which == 'latest':
        url = _resolve_url(base_url, LATEST_TITLE)
        return _download_csv(url)
    elif which == 'historical':
        url = _resolve_url(base_url, HISTORICAL_TITLE)
        return _download_csv(url)
    else:
        url_latest = _resolve_url(base_url, LATEST_TITLE)
        url_hist = _resolve_url(base_url, HISTORICAL_TITLE)
        return pd.concat([_download_csv(url_latest), _download_csv(url_hist)], ignore_index=True)

def load_quarter(period_quarter: date, base_url: str, which: str = "latest", source_file: str = "tax_download"):
    """
    Idempotent quarterly load.
    """
    df = fetch(which, base_url)
    df = df.where(pd.notna(df), None)
    # DQ in-batch
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        print(f"DQ: dropped {dropped} duplicate rows (batch-level).")

    replace_partition_tax(period_quarter)
    insert_tax_rows(period_quarter, which, base_url, source_file, to_json_rows(df))
    return len(df)
