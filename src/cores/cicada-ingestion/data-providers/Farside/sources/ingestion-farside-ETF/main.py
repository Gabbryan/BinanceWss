import os
import time
from datetime import datetime, timedelta

import schedule
from farside_data_manager.ingestors.BTC_ETFIngestor import BitcoinETFIngestor
from farside_data_manager.ingestors.ETH_ETFIngestor import EthereumETFIngestor
from farside_data_manager.manager import FarsideDataManager
from slack_package import init_slack
from src.libs.google_cloud_bucket.gcs_module import gcsModule
from tqdm import tqdm

from config import BUCKET_NAME

# Initialize GCS module
gcs_module = gcsModule(bucket_name=BUCKET_NAME)

# Initialize Slack
webhook_url = os.getenv("WEBHOOK_URL")
slack_channel = init_slack(webhook_url)


def send_start_message():
    message = (
        ":hourglass_flowing_sand: *Daily Data Ingestion Initiated*\n\n"
        "🚀 *Task Overview:*\n"
        "- *Ingestors*: `BitcoinETFIngestor`, `EthereumETFIngestor`\n"
        "- *Start Time*: `00:00`\n\n"
        "The daily data ingestion process has started. "
        "The system is now processing the specified data sources. "
        "You will receive a notification once the process is complete."
    )
    slack_channel.send_message(
        ":hourglass_flowing_sand: Daily Data Ingestion Initiated", message, "#36a64f"
    )


def send_end_message(total_ingestors, total_uploaded):
    message = (
        ":white_check_mark: *Daily Data Ingestion Completed*\n\n"
        "🎯 *Task Summary:*\n"
        f"- *Total Ingestors Processed*: `{total_ingestors}`\n"
        f"- *Total Datasets Uploaded*: `{total_uploaded}`\n\n"
        "All data ingestors have been successfully processed and the data has been uploaded to the cloud storage. "
        "Great job team! :clap: The system is ready for the next cycle."
    )
    slack_channel.send_message(
        ":white_check_mark: Daily Data Ingestion Completed", message, "#36a64f"
    )


def run_daily_tasks():
    # Initialize data manager
    manager = FarsideDataManager()

    # Send start message
    send_start_message()

    # Register ingestors (add all ingestors you want to use)
    manager.register_ingestor(BitcoinETFIngestor())
    manager.register_ingestor(EthereumETFIngestor())
    # Add other ingestors here as needed

    # Run the data manager
    manager.run()

    total_uploaded = 0

    # Process and upload data for each ingestor
    for ingestor in tqdm(manager.ingestors, desc="Processing ingestors"):
        symbol = ingestor.__class__.__name__.replace("Ingestor", "")
        df = ingestor.data

        # Check if there's any data to process
        if df is not None and not df.empty:
            gcs_module.aggregate_and_upload(df, symbol)
            total_uploaded += 1

    # Send end message
    send_end_message(len(manager.ingestors), total_uploaded)


if __name__ == "__main__":
    # Schedule the task to run daily at midnight
    scheduled_time = "00:00"
    schedule.every().day.at(scheduled_time).do(run_daily_tasks)

    # Initialize the last run time to a time in the past
    last_run_time = datetime.now() - timedelta(days=1)

    while True:
        now = datetime.now()
        # Get the next scheduled time as a datetime object
        next_run_time = datetime.strptime(
            now.strftime("%Y-%m-%d") + " " + scheduled_time, "%Y-%m-%d %H:%M"
        )

        # If we've passed midnight (or the scheduled time) and haven't run the task today
        if now >= next_run_time and last_run_time < next_run_time:
            schedule.run_all()
            last_run_time = now

        # Check every hour
        time.sleep(3600)
