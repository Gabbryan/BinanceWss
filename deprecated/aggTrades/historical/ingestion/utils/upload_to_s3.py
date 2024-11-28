import logging
from io import BytesIO

from deprecated.aggTrades.historical.ingestion.config.aws_config import s3_client, BUCKET_NAME
from log.logging_config import log_safe


def upload_to_s3(aggregated_df, symbol, year, month):
    symbol_url_part = symbol.replace("/", "")
    s3_key = f"cicada-data/HistoricalTradeAggregator/binance-futures/{symbol_url_part}/{year}/{month:02d}.parquet"

    try:
        # Convert the DataFrame to Parquet format and upload to S3
        parquet_buffer = BytesIO()
        aggregated_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        file_size = parquet_buffer.getbuffer().nbytes

        s3_client.upload_fileobj(parquet_buffer, BUCKET_NAME, s3_key)
        log_safe(
            f"Uploaded aggregated data for {symbol} {year}-{month:02d} to S3 bucket {BUCKET_NAME}"
        )
        return {
            "url": f"s3://{BUCKET_NAME}/{s3_key}",
            "size": file_size,
        }
    except Exception as e:
        log_safe(
            f"Failed to upload aggregated data for {symbol} {year}-{month:02d} to S3: {e}",
            logging.ERROR,
        )
        return None
