import logging
import time
from datetime import datetime, timedelta

from deprecated.aggTrades.historical.ingestion.config.bigTable_config import (
    bigtable_client,
    BIGTABLE_PROJECT_ID,
    BIGTABLE_INSTANCE_ID,
    BIGTABLE_TABLE_ID,
)
from config.slack_config import slack_manager
from data_processing import download_sync
from deprecated.aggTrades.historical.ingestion.utils.upload_to_bigTable import upload_to_bigtable
from log.logging_config import log_safe
from process.check_exists import check_bigtable_object_exists


def extract_and_process_custom_yield(content):
    logging.info(
        f"Starting extract_and_process_custom_yield with content size: {len(content)} bytes"
    )

    def process_csv(csv_content):
        for i, line in enumerate(csv_content.split("\n")[1:]):
            if i % 10000 == 0:
                logging.debug(f"Processing line {i}")
            if line:
                row = line.split(",")
                yield {
                    "agg_trade_id": int(row[0]),
                    "price": float(row[1]),
                    "quantity": float(row[2]),
                    "first_trade_id": int(row[3]),
                    "last_trade_id": int(row[4]),
                    "transact_time": int(row[5]),
                    "is_buyer_maker": row[6].lower() == "true",
                }

    return process_csv(content.decode("utf-8"))


def process_symbol_bigTable(exchange, symbol, start_date, end_date):
    log_safe(
        f"Starting process_symbol_bigTable for {exchange} {symbol} from {start_date} to {end_date}"
    )
    start_time = time.time()

    exported_files = []
    existing_files_count = 0
    total_files = 0
    total_size = 0

    current_year = datetime.now().year
    current_month = datetime.now().month

    for year in range(start_date.year, end_date.year + 1):
        for month in range(1, 13):
            if (year == start_date.year and month < start_date.month) or (
                year == end_date.year and month > end_date.month
            ):
                logging.debug(
                    f"Skipping {year}-{month:02d} as it's outside the date range"
                )
                continue

            bigtable_key_prefix = f"{symbol.replace('/', '')}#{year}-{month:02d}"
            logging.info(f"Processing {bigtable_key_prefix}")

            if not (year == current_year and month == current_month):
                logging.info(
                    f"Checking if data already exists in Bigtable for {bigtable_key_prefix}"
                )
                if check_bigtable_object_exists(
                    bigtable_client,
                    BIGTABLE_PROJECT_ID,
                    BIGTABLE_INSTANCE_ID,
                    BIGTABLE_TABLE_ID,
                    bigtable_key_prefix,
                ):
                    log_safe(
                        f"Data for {bigtable_key_prefix} already exists in Bigtable. Skipping download and upload."
                    )
                    existing_files_count += 1
                    continue

            if year == current_year and month == current_month:
                logging.info(
                    f"Processing current month data for {symbol} {year}-{month:02d}"
                )
                d = datetime(year, month, 1)
                while d.month == month and d.date() < datetime.now().date():
                    logging.info(
                        f"Downloading {symbol} data for {d.year}-{d.month:02d}-{d.day:02d}"
                    )
                    daily_content = download_sync(
                        exchange, symbol, d.year, d.month, d.day
                    )
                    if daily_content:
                        logging.debug(f"Downloaded {len(daily_content)} bytes for {d}")
                        processed_content = extract_and_process_custom_yield(
                            daily_content
                        )
                        bigtable_info = upload_to_bigtable(
                            processed_content, symbol, year, month
                        )
                        if bigtable_info:
                            exported_files.append(bigtable_info)
                            total_files += 1
                            total_size += bigtable_info["size"]
                        logging.info(
                            f"Uploaded data for {symbol} {d.year}-{d.month:02d}-{d.day:02d} to Bigtable"
                        )
                    else:
                        logging.warning(f"No data downloaded for {d}")
                    d += timedelta(days=1)
            else:
                logging.info(
                    f"Downloading monthly data for {symbol} {year}-{month:02d}"
                )
                monthly_content = download_sync(
                    exchange, symbol, year, month, is_monthly=True
                )
                if not monthly_content:
                    log_safe(
                        f"Monthly data not found or download failed for {symbol} {year}-{month:02d}. Falling back to daily data."
                    )
                    d = datetime(year, month, 1)
                    while d.month == month and d.date() <= end_date:
                        logging.info(
                            f"Downloading daily data for {symbol} {d.year}-{d.month:02d}-{d.day:02d}"
                        )
                        daily_content = download_sync(
                            exchange, symbol, d.year, d.month, d.day
                        )
                        if daily_content:
                            processed_content = extract_and_process_custom_yield(
                                daily_content
                            )
                            bigtable_info = upload_to_bigtable(
                                processed_content, symbol, year, month
                            )
                            if bigtable_info:
                                exported_files.append(bigtable_info)
                                total_files += 1
                                total_size += bigtable_info["size"]
                            logging.info(
                                f"Uploaded data for {symbol} {d.year}-{d.month:02d}-{d.day:02d} to Bigtable"
                            )
                        else:
                            logging.warning(f"No data downloaded for {d}")
                        d += timedelta(days=1)
                else:
                    processed_content = extract_and_process_custom_yield(
                        monthly_content
                    )
                    bigtable_info = upload_to_bigtable(
                        processed_content, symbol, year, month
                    )
                    if bigtable_info:
                        exported_files.append(bigtable_info)
                        total_files += 1
                        total_size += bigtable_info["size"]
                    logging.info(
                        f"Uploaded monthly data for {symbol} {year}-{month:02d} to Bigtable"
                    )

    elapsed_time = time.time() - start_time
    log_safe(f"Completed processing for {exchange} {symbol}")
    message = (
        f"*Crypto Processing Completed for {symbol}* :tada:\n"
        f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
        f"*Total Files Generated*: {total_files}\n"
        f"*Total Existing Files Skipped*: {existing_files_count}\n"
        f"*Total Size of Files*: {total_size / (1024 * 1024):.2f} MB\n"
        f"*Bigtable Row Key Prefix*: {bigtable_key_prefix}"
    )
    slack_manager.send_message(f"Completed processing for {exchange} {symbol}", message)

    logging.info(f"Process summary for {symbol}: {message}")
    return exported_files, existing_files_count
