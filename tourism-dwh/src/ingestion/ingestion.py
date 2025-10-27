import pandas as pd
from housingDownload import download_files as download_housing_files
from taxDownload import download_files as download_tax_files

### vars ###
## using google docks link to avoid scraping issues with legality of it on the site
## downloading manually and using it is OK. will figure out permanent solution
housing_url = 'https://docs.google.com/spreadsheets/d/1m5zfF6zGz8itskGpKsNU3sFNvCPdLRTK/export?format=xlsx'
tax_url = "https://www.emta.ee/en/business-client/board-news-and-contact/news-press-information-statistics/statistics-and-open-data"


def main():
    # Housing data download
    print("Starting housing data download...")
    housing_df = download_housing_files(housing_url)
    if housing_df is not None:
        print("Housing data download complete.")
        print(housing_df.head())
    else:
        print("Failed to download housing data.")

    # Tax data download
    print("Starting tax data download...")
    try:
        tax_df = download_tax_files(tax_url, which='both')
        print("Tax data download complete.")
        print(tax_df.head())
    except Exception as e:
        print(f"Failed to download tax data: {e}")

if __name__ == "__main__":
    main()


"""
#remove images
docker images
docker rmi $(docker images -q python-app)

#chekk running containers
docker ps -q | xargs docker stop
#remove stopped containers
docker ps -a -q | xargs docker rm

#remove unused images
docker image prune -a

#build and run python app
docker build -t python-app .
docker run --rm python-app
"""
