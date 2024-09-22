import time

from config.slack_config import slack_manager


def send_start_message(symbols, start_date, end_date):
    message = (
        f"*Crypto Processing Started* :rocket:\n"
        f"*Number of cryptocurrencies*: {len(symbols)}\n"
        f"*Date Range*: {start_date} to {end_date}\n"
        f"*Cryptocurrencies being processed*:\n"
    )
    for symbol in symbols:
        message += f"- {symbol}\n"
    message += "Processing has begun. Stay tuned for updates and completion details."
    slack_manager.send_message("Crypto Processing Started :rocket:", message, "#36a64f")


def send_end_message(start_time, total_files, total_existing_files, total_size):
    end_time = time.time()
    elapsed_time = end_time - start_time
    message = (
        f"*Crypto Processing Completed* :tada:\n"
        f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
        f"*Total Files Generated*: {total_files}\n"
        f"*Total Existing Files Skipped*: {total_existing_files}\n"
        f"*Total Size of Files*: {total_size / (1024 * 1024 * 1024):.2f} GB\n"
        f"Excellent work team! All files have been successfully processed and uploaded."
    )
    slack_manager.send_message("Crypto Processing Completed :tada:", message, "#36a64f")
