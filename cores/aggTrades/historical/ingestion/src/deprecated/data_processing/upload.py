# upload_gcs.py

import logging
from io import BytesIO
from google.cloud import storage
from log.logging_config import log_safe

# Assume storage_client is initialized elsewhere
from config import storage_client, BUCKET_NAME


def upload_to_gcs(aggregated_df, symbol, year, month):
    symbol_url_part = symbol.replace("/", "")
    gcs_blob_name = f"cicada-data/HistoricalTradeAggregator/binance_futures/{symbol_url_part}/{year}/{month:02d}.parquet"

    try:
        # Convert the DataFrame to Parquet format and upload to GCS
        parquet_buffer = BytesIO()
        aggregated_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        file_size = parquet_buffer.getbuffer().nbytes

        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
        log_safe(
            f"Uploaded aggregated data for {symbol} {year}-{month:02d} to GCS bucket {BUCKET_NAME}"
        )
        return {
            "url": f"gs://{BUCKET_NAME}/{gcs_blob_name}",
            "size": file_size,
        }
    except Exception as e:
        log_safe(
            f"Failed to upload aggregated data for {symbol} {year}-{month:02d} to GCS: {e}",
            logging.ERROR,
        )
        return None
