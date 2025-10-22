import pandas as pd
import requests
import io
from tqdm import tqdm
from urllib.parse import urlparse
import re
from typing import Union

def download_file(url: str, chunk_size: int = 1024 * 32) -> Union[io.BytesIO, None]:
    """Download a file (Excel/CSV/ZIP/etc.) from the given URL into memory."""
    print(f"Downloading from {url}")
    try:
        resp = requests.get(url, stream=True, allow_redirects=True, timeout=60)
        resp.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        total = int(resp.headers.get("content-length", 0))
        data = io.BytesIO()
        with tqdm(total=total if total else None, unit='B', unit_scale=True, desc="Downloading") as pbar:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    data.write(chunk)
                    pbar.update(len(chunk))
        data.seek(0)
        print(f"Download complete, {len(data.getvalue())/1024:.1f} KB")
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during download: {e}")
        return None

def parse_file(file_bytes: io.BytesIO) -> pd.DataFrame:
    """Try to parse the downloaded file as Excel or CSV."""
    try:
        dfs = pd.read_excel(file_bytes, sheet_name=None)
        sheet_names = list(dfs.keys())
        print(f"Excel workbook with sheets: {sheet_names}")
        # For now, return the first sheet
        return dfs[sheet_names[0]]
    except Exception as e:
        print("Excel parsing failed, trying CSV...", e)
        file_bytes.seek(0)
        df = pd.read_csv(file_bytes)
        return df

def download_files(base_url: str) -> pd.DataFrame | None:
    """
    Entry-level logic to download one or more datasets.
    For now, this expects `base_url` to be a direct link to a file.
    Future versions could support multiple datasets ('latest', 'historical', etc.)
    """
    file_bytes = download_file(base_url)
    if file_bytes:
        df = parse_file(file_bytes)
        return df
    else:
        return None