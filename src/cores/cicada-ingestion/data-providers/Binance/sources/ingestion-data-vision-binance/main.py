import itertools
import logging
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import requests
import schedule
from dotenv import load_dotenv
from gcs_module import gcsModule
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from config import BUCKET_NAME
from src.libs.slack import init_slack, get_slack_decorators

TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "D"]

# Load environment variables from .env file
load_dotenv()

# Initialize Slack
webhook_url = os.getenv("WEBHOOK_URL")
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


def send_start_message(symbols, start_date, end_date, data_name):
    message = (
        f"*{data_name.capitalize()} Processing Started* :rocket:\n"
        f"*Number of cryptocurrencies*: {len(symbols)}\n"
        f"*Date Range*: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n"
        f"*Cryptocurrencies being processed*:\n"
    )
    for symbol in symbols:
        message += f"- {symbol}\n"
    message += "Processing has begun. Stay tuned for updates and completion details."
    slack_channel.send_message("Crypto Processing Started :rocket:", message, "#36a64f")


def send_end_message(
    start_time, total_files, total_existing_files, total_size, data_name
):
    end_time = time.time()
    elapsed_time = end_time - start_time
    message = (
        f"*{data_name} Processing Completed* :tada:\n"
        f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
        f"*Total Files Generated*: {total_files}\n"
        f"*Total Existing Files Skipped*: {total_existing_files}\n"
        f"*Total Size of Files*: {total_size / (1024 * 1024 * 1024):.2f} GB\n"
        "Excellent work team! All files have been successfully processed and uploaded."
    )
    slack_channel.send_message("Crypto Processing Completed :tada:", message, "#36a64f")


def download_and_extract(url, date, expected_columns):
    """
    Download and extract a ZIP file from the given URL.
    Returns a DataFrame containing the data with the expected columns.
    """
    try:
        response = session.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with zipfile.ZipFile(BytesIO(response.content)) as z:
            with z.open(z.namelist()[0]) as file:
                df = pd.read_csv(file)

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")
        return None

    except zipfile.BadZipFile as e:
        logging.error(f"Error extracting ZIP file from {url}: {e}")
        return None

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return None


def process_tasks(tasks, data_name, expected_columns):
    existing_files_count = 0
    total_size = 0
    file_info_list = []

    for task in tqdm(tasks):
        try:
            if data_name == "klines":
                base_url, symbol, timeframe, date = task
            else:
                base_url, symbol, date = task
                timeframe = ""  # No timeframe needed for non-klines data

            formatted_date = date.strftime("%Y-%m-%d")
            year = date.year
            month = date.month
            day = date.day

            base_type = "futures" if "futures" in base_url else "spot"
            gcs_key = (
                f"Raw/Binance-data-vision/historical/{symbol.replace('/', '')}/"
                f"{base_type}/{data_name}/{timeframe}/{year}/{month:02d}/{day:02d}/data.parquet"
            )

            # Check if the file already exists in GCS
            if gcs_module.check_gcs_object_exists(gcs_module.bucket, gcs_key):
                logging.info(
                    f"File {gcs_key} already exists in GCS. Skipping download and upload."
                )
                existing_files_count += 1
                continue  # Skip to the next date

            if data_name == "klines":
                url = f"{base_url}/{symbol.upper()}/{timeframe}/{symbol.upper()}-{timeframe}-{formatted_date}.zip"
            else:
                url = f"{base_url}/{data_name}/{symbol.upper()}/{symbol.upper()}-{data_name}-{formatted_date}.zip"

            logging.info(
                f"Downloading {symbol} {data_name} for {formatted_date} with {timeframe}: {url}"
            )

            df = download_and_extract(url, formatted_date, expected_columns)
            if df is not None:
                daily_dir_path = f"{year}/{month:02d}/{day:02d}"
                if not os.path.exists(daily_dir_path):
                    os.makedirs(daily_dir_path)
                daily_parquet_file_path = os.path.join(daily_dir_path, "data.parquet")
                df.to_parquet(daily_parquet_file_path)

                # Verify file creation
                if os.path.exists(daily_parquet_file_path):
                    logging.info(f"Parquet file created at {daily_parquet_file_path}.")
                    try:
                        file_size = os.path.getsize(daily_parquet_file_path)
                        total_size += file_size
                    except FileNotFoundError:
                        logging.error(
                            f"File {daily_parquet_file_path} not found. Skipping size calculation."
                        )
                        continue
                else:
                    logging.error(
                        f"Failed to create parquet file at {daily_parquet_file_path}."
                    )
                    continue

                gcs_module.upload_to_gcs(
                    pd.read_parquet(daily_parquet_file_path),
                    symbol,
                    year,
                    month,
                    gcs_module.bucket,
                    gcs_key,
                )
                logging.info(
                    f"Uploaded {symbol} {data_name} for {formatted_date} with {timeframe} directly to GCS at {gcs_key}."
                )

                file_info_list.append({"url": gcs_key, "size": file_size})

            time.sleep(REQUEST_INTERVAL / REQUEST_LIMIT)

        except Exception as e:
            logging.error(f"Error processing {symbol} on {formatted_date}: {e}")
            continue  # Skip to the next task

    return file_info_list, existing_files_count, total_size


def download_and_process_data(start_date=None, end_date=None):
    symbols = os.getenv("SYMBOL_LIST").split(",")
    expected_columns = os.getenv("COLUMN_LIST").split(",")
    data_name = os.getenv("DATANAME")

    if data_name == "aggTrades" or data_name == "trades":
        base_urls = [
            "https://data.binance.vision/data/spot/daily",
            "https://data.binance.vision/data/futures/um/daily",
        ]
    elif data_name == "klines":
        base_urls = [
            "https://data.binance.vision/data/futures/um/daily/klines",
            "https://data.binance.vision/data/spot/um/daily/klines",
        ]
    else:
        base_urls = [
            "https://data.binance.vision/data/futures/um/daily",
        ]

    if start_date is None:
        start_date = datetime.today() - timedelta(days=1)
    if end_date is None:
        end_date = datetime.today() - timedelta(days=1)

    dates = pd.date_range(start_date, end_date, freq="D")
    send_start_message(symbols, start_date, end_date, data_name)
    start_time = time.time()  # Start the timer

    # Organize tasks by year for threading
    tasks_by_year = {}
    for date in dates:
        year = date.year
        if year not in tasks_by_year:
            tasks_by_year[year] = []
        if data_name == "klines":
            tasks_by_year[year].extend(
                itertools.product(base_urls, symbols, TIMEFRAMES, [date])
            )
        else:
            tasks_by_year[year].extend(itertools.product(base_urls, symbols, [date]))

    logging.info(
        f"{sum(len(tasks) for tasks in tasks_by_year.values())} tasks have been generated."
    )

    # Use ThreadPoolExecutor to process each year's data in parallel
    with ThreadPoolExecutor(
        max_workers=min(len(tasks_by_year), os.cpu_count())
    ) as executor:
        futures = {
            executor.submit(process_tasks, tasks, data_name, expected_columns): year
            for year, tasks in tasks_by_year.items()
        }

        total_files = 0
        total_existing_files = 0
        total_size = 0

        for future in as_completed(futures):
            year = futures[future]
            try:
                file_info_list, existing_files_count, year_total_size = future.result()
                total_files += len(file_info_list)
                total_existing_files += existing_files_count
                total_size += year_total_size
                logging.info(f"Year {year} processed successfully.")
            except Exception as e:
                logging.error(f"Error processing data for year {year}: {e}")

    send_end_message(
        start_time, total_files, total_existing_files, total_size, data_name
    )
    return total_files, total_existing_files, total_size


def schedule_job():
    schedule.every().day.at("10:00").do(download_and_process_data)

    logging.info("Scheduled job to run every day at 10:00 AM.")

    while True:
        schedule.run_pending()
        print(time.time())
        time.sleep(30 * 5)


if __name__ == "__main__":
    initial_start_date = datetime(2017, 8, 17)
    initial_end_date = datetime.today()

    logging.info(
        f"Running initial data processing from {initial_start_date} to {initial_end_date}."
    )
    download_and_process_data(start_date=initial_start_date, end_date=initial_end_date)

    schedule_job()
