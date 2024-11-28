import time
from datetime import datetime, timedelta
from data_processing import download_and_extract, download_and_extract_monthly
from google.cloud import storage
from log.logging_config import log_safe


def get_bucket():
    bucket_name = "production-trustia-raw-data"
    storage_client = storage.Client()
    return storage_client.bucket(bucket_name)


bucket = get_bucket()


def check_gcs_object_exists(bucket, object_name):
    """Check if a GCS object exists."""
    blob = bucket.blob(object_name)
    return blob.exists()


def upload_to_gcs(file_content, filename):
    blob = bucket.blob(filename)
    blob.upload_from_string(file_content)
    return {"path": filename, "size": len(file_content)}


def process_symbol(exchange, symbol, start_date, end_date):
    log_safe(f"Processing {exchange} {symbol}...")
    start_time = time.time()

    start_year = start_date.year
    end_year = end_date.year
    exported_files = []
    existing_files_count = 0

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_date.year and month > end_date.month:
                break

            gcs_key = f"Raw/Binance/{symbol.replace('/', '')}/aggTrades/{year}/{month:02d}/data.zip"

            # Check if the file exists in GCS only for past months and years
            if check_gcs_object_exists(bucket, gcs_key):
                log_safe(
                    f"File {gcs_key} already exists in GCS. Skipping download and upload."
                )
                existing_files_count += 1
                continue

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
                for file_content in monthly_files_contents:
                    filename = f"Raw/Binance/{symbol.replace('/', '')}/aggTrades/{year}/{month:02d}/data_{d.year}-{d.month:02d}-{d.day:02d}.zip"
                    gcs_path_info = upload_to_gcs(file_content, filename)
                    if gcs_path_info:
                        exported_files.append(gcs_path_info)

    end_time = time.time()
    log_safe(
        f"Processing for {exchange} {symbol} completed in {end_time - start_time:.2f} seconds."
    )
    log_safe(f"Total files exported: {len(exported_files)}")
    log_safe(f"Total existing files skipped: {existing_files_count}")

    return exported_files, existing_files_count
