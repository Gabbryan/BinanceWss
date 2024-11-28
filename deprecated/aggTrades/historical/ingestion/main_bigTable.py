import logging
import os
import sys
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv
from google.cloud import bigtable

from log.logging_config import log_safe
from notifications.slack_notifications import send_start_message, send_end_message
from deprecated.aggTrades.historical.ingestion.process.daily_update_bigTable import daily_update
from process.process_symbol_bigTable import process_symbol_bigTable
from utils.common_utils import validate_dates

# Load environment variables from .env file
load_dotenv()

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

# Google Cloud Bigtable Client Setup
client = bigtable.Client(project=os.getenv("BIGTABLE_PROJECT_ID"), admin=True)
instance = client.instance(os.getenv("BIGTABLE_INSTANCE_ID"))
table = instance.table(os.getenv("BIGTABLE_TABLE_ID"))


def main(start_date, end_date, symbols, exchanges):
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as e:
        log_safe(f"Invalid date format: {e}", logging.ERROR)
        return

    try:
        start_date, end_date = validate_dates(start_date, end_date)
    except ValueError as e:
        log_safe(e, logging.ERROR)
        return

    send_start_message(symbols, start_date, end_date)
    start_time = time.time()
    total_files = 0
    total_size = 0
    total_existing_files = 0

    for symbol in symbols:
        for ex in exchanges:
            exported_files, existing_files_count = process_symbol_bigTable(
                ex, symbol, start_date, end_date
            )
            total_files += len(exported_files)
            total_size += sum(file_info["size"] for file_info in exported_files)
            total_existing_files += existing_files_count

    elapsed_time = time.time() - start_time
    logging.info(f"Processing completed in {elapsed_time / 60:.2f} minutes.")
    send_end_message(start_time, total_files, total_existing_files, total_size)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    symbols = [
        "BTC/USDT",
    ]

    exchanges = ["binance-futures"]

    main("2024-04-01", datetime.now().strftime("%Y-%m-%d"), symbols, exchanges)

    schedule.every().day.at("10:00").do(
        daily_update, symbols=symbols, exchanges=exchanges
    )
    while True:
        schedule.run_pending()
        time.sleep(1)
