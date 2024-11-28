import time
from datetime import datetime, timedelta

from config import s3_client, BUCKET_NAME
from config.slack_config import slack_manager
from data_processing import (
    download_and_extract,
    aggregate_data,
    upload_to_s3,  # Make sure this upload_to_s3 function is updated to use Parquet
    download_and_extract_monthly,
)
from log.logging_config import log_safe
from utils.common_utils import check_s3_object_exists


def process_symbol(exchange, symbol, start_date, end_date):
    log_safe(f"Processing {exchange} {symbol}...")
    start_time = time.time()

    start_year = start_date.year
    end_year = end_date.year
    exported_files = []
    existing_files_count = 0
    total_files = 0
    total_size = 0

    current_year = datetime.now().year
    current_month = datetime.now().month

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_date.year and month > end_date.month:
                break

            s3_key = f"cicada-data/HistoricalTradeAggregator/binance-futures/{symbol.replace('/', '')}/{year}/{month:02d}.parquet"

            # Check if the file exists in S3 only for past months and years
            if not (year == current_year and month == current_month):
                if check_s3_object_exists(s3_client, BUCKET_NAME, s3_key):
                    log_safe(
                        f"File {s3_key} already exists in S3. Skipping download and upload."
                    )
                    existing_files_count += 1
                    continue

            if year == current_year and month == current_month:
                all_files_contents = []
                d = datetime(year, month, 1)
                while d.month == month and d.date() < datetime.now().date():
                    daily_content = download_and_extract(
                        exchange, symbol, d.year, d.month, d.day
                    )
                    if daily_content:
                        all_files_contents.extend(daily_content)
                    d += timedelta(days=1)

                if all_files_contents:
                    aggregated_df = aggregate_data(all_files_contents)
                    s3_path_info = upload_to_s3(aggregated_df, symbol, year, month)
                    if s3_path_info:
                        exported_files.append(s3_path_info)
                        total_files += 1
                        total_size += s3_path_info["size"]
            else:
                # Monthly data processing
                monthly_files_contents = download_and_extract_monthly(
                    exchange, symbol, year, month
                )
                if not monthly_files_contents:
                    log_safe(
                        f"Monthly data not found or download failed for {symbol} {year}-{month:02d}. Falling back to daily data."
                    )
                    # Fallback to daily data processing
                    monthly_files_contents = []
                    d = datetime(year, month, 1)
                    while d.month == month:
                        daily_content = download_and_extract(
                            exchange, symbol, d.year, d.month, d.day
                        )
                        if daily_content:
                            monthly_files_contents.extend(daily_content)
                        d += timedelta(days=1)

                if monthly_files_contents:
                    aggregated_df = aggregate_data(monthly_files_contents)
                    s3_path_info = upload_to_s3(aggregated_df, symbol, year, month)
                    if s3_path_info:
                        exported_files.append(s3_path_info)
                        total_files += 1
                        total_size += s3_path_info["size"]

    elapsed_time = time.time() - start_time
    log_safe(f"Completed processing for {exchange} {symbol}.")
    message = (
        f"*Crypto Processing Completed for {symbol}* :tada:\n"
        f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
        f"*Total Files Generated*: {total_files}\n"
        f"*Total Existing Files Skipped*: {existing_files_count}\n"
        f"*Total Size of Files*: {total_size / (1024 * 1024):.2f} MB\n"
        f"*S3 Folder*: s3://{BUCKET_NAME}/{s3_key}"
    )
    slack_manager.send_message(
        f"Completed processing for {exchange} {symbol}.", message
    )

    return exported_files, existing_files_count


def process_symbol_daily(exchange, symbol, start_date, end_date):
    log_safe(f"Processing daily update for {exchange} {symbol}...")

    exported_files = []
    year = start_date.year
    month = start_date.month
    day = start_date.day

    files_contents = download_and_extract(exchange, symbol, year, month, day)
    if files_contents:
        for content in files_contents:
            exported_files.append(content)

    log_safe(f"Completed daily update for {exchange} {symbol}.")
    return exported_files, symbol, start_date, end_date
