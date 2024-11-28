import logging
import time
from datetime import datetime
import sys
import os
import pytz
import schedule
from dotenv import load_dotenv

from log.logging_config import log_safe
from notifications.slack_notifications import send_start_message, send_end_message
from process.daily_update import daily_update
from process.process_symbol import process_symbol
from utils.common_utils import validate_dates

# Load environment variables from .env file
load_dotenv()

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)


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
            exported_files, existing_files_count = process_symbol(
                ex, symbol, start_date, end_date
            )
            total_files += len(exported_files)
            total_size += sum(file_info["size"] for file_info in exported_files)
            total_existing_files += existing_files_count

    send_end_message(start_time, total_files, total_existing_files, total_size)


if __name__ == "__main__":
    symbols = ["BTC/USDT", "ETH/USDT"]
    exchanges = ["binance-futures"]

    # Initial run of the main function
    main("2023-03-01", datetime.now().strftime("%Y-%m-%d"), symbols, exchanges)

    # Schedule the daily update to run every day at 10:00 AM
    schedule.every().day.at("10:00").do(
        daily_update, symbols=symbols, exchanges=exchanges
    )

    while True:
        schedule.run_pending()
        time.sleep(1)
