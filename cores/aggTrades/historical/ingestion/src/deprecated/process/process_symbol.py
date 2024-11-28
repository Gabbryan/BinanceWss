import os
import time
from datetime import datetime, timedelta
from google.cloud import storage
from config.slack_config import slack_manager
from data_processing import (
    download_and_extract,
    download_and_extract_monthly,
    aggregate_data,
)
from log.logging_config import log_safe
from utils.gcs_module import gcsModule
from config import BUCKET_NAME

gcs_module = gcsModule(bucket_name=BUCKET_NAME)

bucket = gcs_module.get_bucket(BUCKET_NAME)


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

            gcs_key = f"Raw/Binance/{symbol.replace('/', '')}/aggTrades_historical/{year}/{month:02d}/data.parquet"

            # Check if the file exists in GCS only for past months and years
            if not (year == current_year and month == current_month):
                if gcs_module.check_gcs_object_exists(bucket, gcs_key):
                    log_safe(
                        f"File {gcs_key} already exists in GCS. Skipping download and upload."
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
                    gcs_path_info = gcs_module.upload_to_gcs(
                        aggregated_df, symbol, year, month, bucket
                    )
                    if gcs_path_info:
                        exported_files.append(gcs_path_info)
                        total_files += 1
                        total_size += gcs_path_info["size"]
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
                    gcs_path_info = gcs_module.upload_to_gcs(
                        aggregated_df, symbol, year, month, bucket
                    )
                    if gcs_path_info:
                        exported_files.append(gcs_path_info)
                        total_files += 1
                        total_size += gcs_path_info["size"]

    end_time = time.time()
    log_safe(
        f"Processing for {exchange} {symbol} completed in {end_time - start_time:.2f} seconds."
    )
    log_safe(f"Total files exported: {total_files}")
    log_safe(f"Total size of exported files: {total_size / (1024 * 1024):.2f} MB")
    log_safe(f"Total existing files skipped: {existing_files_count}")

    return exported_files, existing_files_count


def process_symbol_daily(exchange, symbol, start_date, end_date):
    log_safe(f"Processing {exchange} {symbol} for daily update...")
    start_time = time.time()

    exported_files = []
    existing_files_count = 0
    total_files = 0
    total_size = 0

    current_year = datetime.now().year
    current_month = datetime.now().month

    d = start_date
    while d <= end_date:
        gcs_key = f"Raw/Binance/{symbol.replace('/', '')}/aggTrades_historical/{d.year}/{d.month:02d}/{d.day:02d}/data.parquet"

        if gcs_module.check_gcs_object_exists(bucket, gcs_key):
            log_safe(
                f"File {gcs_key} already exists in GCS. Skipping download and upload."
            )
            existing_files_count += 1
            d += timedelta(days=1)
            continue

        daily_content = download_and_extract(exchange, symbol, d.year, d.month, d.day)
        if daily_content:
            aggregated_df = aggregate_data(daily_content)
            gcs_path_info = gcs_module.upload_to_gcs(
                aggregated_df, symbol, d.year, d.month, bucket
            )
            if gcs_path_info:
                exported_files.append(gcs_path_info)
                total_files += 1
                total_size += gcs_path_info["size"]

        d += timedelta(days=1)

    end_time = time.time()
    log_safe(
        f"Processing for {exchange} {symbol} daily update completed in {end_time - start_time:.2f} seconds."
    )
    log_safe(f"Total files exported: {total_files}")
    log_safe(f"Total size of exported files: {total_size / (1024 * 1024):.2f} MB")
    log_safe(f"Total existing files skipped: {existing_files_count}")

    return exported_files, existing_files_count
