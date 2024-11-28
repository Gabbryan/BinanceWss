import logging
from datetime import datetime, timedelta
from io import BytesIO
import os
import sys
import pandas as pd

from config import BUCKET_NAME
from log.logging_config import log_safe
from process.process_symbol import process_symbol_daily
from utils.gcs_module import gcsModule
from config import BUCKET_NAME

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

# Initialize gcsModule
gcs_module = gcsModule(bucket_name=BUCKET_NAME)


def daily_update(symbols, exchanges):
    start_date = datetime.now().date() - timedelta(days=1)
    end_date = start_date
    all_exported_files = []

    for symbol in symbols:
        for ex in exchanges:
            exported_files, symbol, _, _ = process_symbol_daily(
                ex, symbol, start_date, end_date
            )
            if exported_files:
                symbol_url_part = symbol.replace("/", "")
                gcs_key = f"Raw/Binance/{symbol_url_part}/aggTrades_historical/{start_date.year}/{start_date.month:02d}/data.parquet"

                # Check if the file already exists in GCS
                existing_data = pd.DataFrame()
                if gcs_module.check_gcs_object_exists(gcs_module.bucket, gcs_key):
                    try:
                        existing_file = BytesIO()
                        blob = gcs_module.bucket.blob(gcs_key)
                        blob.download_to_file(existing_file)
                        existing_file.seek(0)
                        existing_data = pd.read_parquet(existing_file)
                    except Exception as e:
                        log_safe(
                            f"Failed to download existing file for {symbol}: {e}",
                            logging.ERROR,
                        )

                # Load the new data from exported files
                new_data_frames = []
                for content in exported_files:
                    new_file = BytesIO(content)
                    new_data = pd.read_parquet(new_file)
                    new_data_frames.append(new_data)

                new_data = pd.concat(new_data_frames, ignore_index=True)

                # Combine the existing data with the new data
                combined_data = pd.concat([existing_data, new_data], ignore_index=True)

                # Convert the combined DataFrame to Parquet format and upload to GCS
                gcs_module.upload_to_gcs(
                    combined_data,
                    symbol,
                    start_date.year,
                    start_date.month,
                    gcs_module.bucket,
                )
                log_safe(
                    f"Daily update completed for {symbol} {start_date.year}-{start_date.month:02d}-{start_date.day:02d}"
                )

                all_exported_files.append(
                    {
                        "url": f"gs://{BUCKET_NAME}/{gcs_key}",
                        "size": combined_data.memory_usage(deep=True).sum(),
                    }
                )

    log_safe("Daily update completed.")
    return all_exported_files, symbols, start_date, end_date
