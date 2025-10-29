import os
import pandas as pd
from housingDownload import download_files as download_housing_files
from taxDownload import download_files as download_tax_files

housing_url = 'https://andmed.eesti.ee/api/datasets/f3324f95-e672-4041-804f-edf8b7083c43/files/701fb53b-73ee-4c67-b438-e5fed9e5429a/download-s3'
tax_url = "https://www.emta.ee/en/business-client/board-news-and-contact/news-press-information-statistics/statistics-and-open-data"


def main():
    # Ensure the data folder exists
    data_folder = '/opt/airflow/data'
    os.makedirs(data_folder, exist_ok=True)

    # Housing data download
    print("Starting housing data download...")
    housing_df = download_housing_files(housing_url)
    if housing_df is not None:
        print("Housing data download complete.")
        print(housing_df.head())
        housing_csv_path = os.path.join(data_folder, 'housing_data.csv')
        housing_df.to_csv(housing_csv_path, index=False)
        print(f"Housing data saved to {housing_csv_path}")
    else:
        print("Failed to download housing data.")

    # Tax data download - Latest
    print("Starting latest tax data download...")
    try:
        latest_tax_df = download_tax_files(tax_url, which='latest')
        print("Latest tax data download complete.")
        print(latest_tax_df.head())
        latest_tax_csv_path = os.path.join(data_folder, 'latest_tax_data.csv')
        latest_tax_df.to_csv(latest_tax_csv_path, index=False)
        print(f"Latest tax data saved to {latest_tax_csv_path}")
    except Exception as e:
        print(f"Failed to download latest tax data: {e}")

    # Tax data download - Historical
    print("Starting historical tax data download...")
    try:
        historical_tax_df = download_tax_files(tax_url, which='historical')
        print("Historical tax data download complete.")
        print(historical_tax_df.head())
        historical_tax_csv_path = os.path.join(data_folder, 'historical_tax_data.csv')
        historical_tax_df.to_csv(historical_tax_csv_path, index=False)
        print(f"Historical tax data saved to {historical_tax_csv_path}")
    except Exception as e:
        print(f"Failed to download historical tax data: {e}")

if __name__ == "__main__":
    main()
"""
#remove python images
docker images
find python image and remove
docker rmi f5ae923e6a20
"""
