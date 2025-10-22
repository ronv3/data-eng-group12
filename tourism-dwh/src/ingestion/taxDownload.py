import requests
import pandas as pd
from bs4 import BeautifulSoup
import io

def download_file(url: str) -> pd.DataFrame:
    print(f"Downloading data from {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    return df

def get_url(base_url: str, title: str) -> str:
    print(f"Fetching file list from: {base_url}")
    resp = requests.get(base_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    for a in soup.find_all('a', href=True, title=True):
        if a['title'].strip() == title:
            return a['href']

    raise RuntimeError(f"Could not find link with title: {title}")

def download_files(base_url: str, which: str = 'both') -> pd.DataFrame:
    latest_file_title = "Data for the current and the previous year (CSV)"
    historical_file_title = "Data from previous years (CSV)"

    if which == 'latest':
        url_latest = get_url(base_url, latest_file_title)
        df = download_file(url_latest)
    elif which == 'historical':
        url_earlier = get_url(base_url, historical_file_title)
        df = download_file(url_earlier)
    elif which == 'both':
        url_latest = get_url(base_url, latest_file_title)
        url_earlier = get_url(base_url, historical_file_title)
        df_latest = download_file(url_latest)
        df_earlier = download_file(url_earlier)
        df = pd.concat([df_latest, df_earlier], ignore_index=True, sort=False)
    else:
        raise ValueError("Invalid value for 'which'. Use 'latest', 'historical', or 'both'.")

    return df