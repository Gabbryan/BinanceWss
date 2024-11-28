import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd

from config import BUCKET_NAME, s3_client
from log.logging_config import log_safe
from .process_symbol import process_symbol_daily


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
                # TODO change s3 path
                s3_key = f"cicada-data/HistoricalTradeAggregator/binance-futures/{symbol_url_part}/{start_date.year}-{start_date.month}.parquet"

                # Download the existing file from S3
                existing_file = BytesIO()
                try:
                    s3_client.download_fileobj(BUCKET_NAME, s3_key, existing_file)
                    existing_file.seek(0)
                    existing_data = pd.read_parquet(existing_file)
                except Exception as e:
                    log_safe(
                        f"Failed to download existing file for {symbol}: {e}",
                        logging.ERROR,
                    )
                    existing_data = pd.DataFrame()

                # Load the new data from exported files
                new_data_frames = []
                for content in exported_files:
                    new_file = BytesIO(content)
                    new_data = pd.read_parquet(new_file)
                    new_data_frames.append(new_data)

                new_data = pd.concat(new_data_frames, ignore_index=True)

                # Combine the existing data with the new data
                combined_data = pd.concat([existing_data, new_data], ignore_index=True)

                # Convert the combined DataFrame to Parquet format and upload to S3
                parquet_buffer = BytesIO()
                combined_data.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                s3_client.upload_fileobj(parquet_buffer, BUCKET_NAME, s3_key)
                log_safe(
                    f"Daily update completed for {symbol} {start_date.year}-{start_date.month:02d}-{start_date.day:02d}"
                )

                all_exported_files.append(
                    {
                        "url": f"s3://{BUCKET_NAME}/{s3_key}",
                        "size": parquet_buffer.getbuffer().nbytes,
                    }
                )

    log_safe("Daily update completed.")
    return all_exported_files, symbols, start_date, end_date
