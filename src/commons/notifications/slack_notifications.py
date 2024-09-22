import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import schedule
from dotenv import load_dotenv
from gcs_module import gcsModule
from requests.adapters import HTTPAdapter
from slack_package import init_slack, get_slack_decorators
from tqdm import tqdm
from urllib3.util.retry import Retry

from config import BUCKET_NAME

# Load environment variables from .env file (for BUCKET_NAME, BASE_URL, SLACK_WEBHOOK_URL)
load_dotenv()

# Initialize Slack
webhook_url = os.getenv("SLACK_WEBHOOK_URL")
slack_channel = init_slack(webhook_url)
slack_decorators = get_slack_decorators()

# Configure request session with retries and increased connection pool size
session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
session.mount("https://", adapter)

REQUEST_LIMIT = 5
REQUEST_INTERVAL = 1  # seconds

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize GCS Module using BUCKET_NAME from the .env file
gcs_module = gcsModule(bucket_name=BUCKET_NAME)


def send_start_message(symbols, start_date, end_date):
    message = (
        f"*Crypto Processing Started* :rocket:\n"
        f"*Number of cryptocurrencies*: {len(symbols)}\n"
        f"*Date Range*: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n"
        f"*Cryptocurrencies being processed*:\n"
    )
    for symbol in symbols:
        message += f"- {symbol}\n"
    message += "Processing has begun. Stay tuned for updates and completion details."
    slack_channel.send_message("Crypto Processing Started :rocket:", message, "#36a64f")


def send_end_message(start_time, total_files, total_existing_files, total_size):
    end_time = time.time()
    elapsed_time = end_time - start_time
    message = (
        f"*Crypto Processing Completed* :tada:\n"
        f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
        f"*Total Files Generated*: {total_files}\n"
        f"*Total Existing Files Skipped*: {total_existing_files}\n"
        f"*Total Size of Files*: {total_size / (1024 * 1024 * 1024):.2f} GB\n"
        "Excellent work team! All files have been successfully processed and uploaded."
    )
    slack_channel.send_message("Crypto Processing Completed :tada:", message, "#36a64f")


@slack_decorators.notify_with_files(header="Crypto File Processing")
def download_and_process_data(start_date=None, end_date=None):
    """Download and process data for given symbols and manage local parquet files."""

    symbols = os.getenv("SYMBOL_LIST").split(",")
    expected_columns = os.getenv("COLUMN_LIST").split(",")
    data_name = os.getenv("DATANAME")

    base_url = os.getenv("BASE_URL")

    if start_date is None:
        start_date = datetime.today() - timedelta(days=1)
    if end_date is None:
        end_date = datetime.today() - timedelta(days=1)

    dates = pd.date_range(start_date, end_date, freq="D")
    current_year = datetime.now().year
    current_month = datetime.now().month
    existing_files_count = 0
    total_size = 0  # To accumulate the size of files

    send_start_message(symbols, start_date, end_date)
    start_time = time.time()  # Start the timer

    file_info_list = []

    for symbol in symbols:
        monthly_data_frames = {}

        for date in tqdm(dates):
            formatted_date = date.strftime("%Y-%m-%d")
            year = date.year
            month = date.month

            gcs_key = f"Raw/Binance/{symbol.replace('/', '')}/{data_name}/{year}/{month:02d}/data.parquet"

            if not (year == current_year and month == current_month):
                if gcs_module.check_gcs_object_exists(gcs_module.bucket, gcs_key):
                    logging.info(
                        f"File {gcs_key} already exists in GCS. Skipping download and upload."
                    )
                    existing_files_count += 1
                    continue

            url = f"{base_url}/{data_name}/{symbol.upper()}/{symbol.upper()}-{data_name}-{formatted_date}.zip"
            logging.info(
                f"Downloading {symbol} {data_name} for {formatted_date}: {url}"
            )

            df = download_and_extract(url, formatted_date, expected_columns)
            if df is not None:
                key = (year, month)
                if key not in monthly_data_frames:
                    monthly_data_frames[key] = []
                monthly_data_frames[key].append(df)

            time.sleep(REQUEST_INTERVAL / REQUEST_LIMIT)

        for (year, month), dfs in monthly_data_frames.items():
            valid_dfs = [df for df in dfs if df is not None and not df.empty]
            if len(valid_dfs) == 0:
                logging.warning(f"No valid data for {year}-{month}. Skipping.")
                continue

            month_df = pd.concat(valid_dfs, ignore_index=True)
            logging.info(
                f"Concatenated DataFrame for {year}-{month} has {len(month_df)} rows."
            )

            dir_path = f"{year}/parquet"
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            parquet_file_path = os.path.join(
                dir_path, f"{symbol.lower()}_{year}_{month:02d}.parquet"
            )
            month_df.to_parquet(parquet_file_path)
            logging.info(f"Exported to {parquet_file_path} with {len(month_df)} rows.")

            gcs_module.upload_to_gcs(
                month_df, symbol, year, month, gcs_module.bucket, data_name
            )
            logging.info(
                f"Uploaded {parquet_file_path} to Google Cloud Storage at {gcs_key}."
            )

            total_size += os.path.getsize(parquet_file_path)

            if os.getenv("KEEP_LOCAL", "False").lower() == "false":
                os.remove(parquet_file_path)
                logging.info(f"Deleted local file {parquet_file_path}.")

            file_info_list.append(
                {"url": gcs_key, "size": os.path.getsize(parquet_file_path)}
            )

    send_end_message(start_time, len(file_info_list), existing_files_count, total_size)
    return (
        file_info_list,
        symbols,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )


def schedule_job():
    schedule.every().day.at("10:00").do(download_and_process_data)

    logging.info("Scheduled job to run every day at 10:00 AM.")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    initial_start_date = datetime(2024, 3, 1)
    initial_end_date = datetime.today() - timedelta(days=1)

    logging.info(
        f"Running initial data processing from {initial_start_date} to {initial_end_date}."
    )
    download_and_process_data(start_date=initial_start_date, end_date=initial_end_date)

    schedule_job()
