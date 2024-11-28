import logging
import os
from time import sleep
from typing import List, Dict, Any

from dotenv import load_dotenv
from google.cloud import bigtable
from google.cloud.bigtable import row

# Load environment variables and set up Google Cloud credentials
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

# Google Cloud Bigtable Client Setup
BIGTABLE_PROJECT_ID = os.getenv("BIGTABLE_PROJECT_ID")
BIGTABLE_INSTANCE_ID = os.getenv("BIGTABLE_INSTANCE_ID")
BIGTABLE_TABLE_ID = os.getenv("BIGTABLE_TABLE_ID")

client = bigtable.Client(project=BIGTABLE_PROJECT_ID, admin=True)
instance = client.instance(BIGTABLE_INSTANCE_ID)
table = instance.table(BIGTABLE_TABLE_ID)

# Column names mapping
column_names = {
    "agg_trade_id": "agg_trade_id",
    "price": "price",
    "quantity": "quantity",
    "first_trade_id": "first_trade_id",
    "last_trade_id": "last_trade_id",
    "transact_time": "transact_time",
    "is_buyer_maker": "is_buyer_maker",
}


def retry_on_exception(func, max_attempts=3, delay=2):
    attempts = 0
    while attempts < max_attempts:
        try:
            return func()
        except Exception as e:
            attempts += 1
            if attempts == max_attempts:
                raise e
            logging.warning(
                f"Attempt {attempts} failed. Retrying in {delay} seconds..."
            )
            sleep(delay)


def upload_chunk(
    chunk: List[Dict[str, Any]], symbol: str, year: int, month: int, start_index: int
) -> int:
    total_size = 0
    rows = []
    logging.info(
        f"Preparing chunk starting at index {start_index} with size {len(chunk)}"
    )

    for index, row_data in enumerate(chunk):
        row_key = f"{symbol.replace('/', '')}#{year}-{month:02d}#{start_index + index}".encode()
        direct_row = row.DirectRow(row_key)
        for key, value in row_data.items():
            column_name = column_names.get(key, key)
            encoded_value = str(value).encode()
            direct_row.set_cell("cf1", column_name, encoded_value)
            total_size += len(encoded_value)
        rows.append(direct_row)

    logging.info(
        f"Uploading chunk starting at index {start_index} with size {len(rows)}"
    )

    def upload():
        table.mutate_rows(rows)

    retry_on_exception(upload)

    logging.info(
        f"Uploaded chunk starting at index {start_index} of size {len(chunk)}, total size {total_size} bytes"
    )

    return total_size


def upload_to_bigtable(
    data: List[Dict[str, Any]],
    symbol: str,
    year: int,
    month: int,
    chunk_size: int = 10000,
) -> Dict[str, int]:
    total_size = 0
    total_rows = len(data)
    chunks_count = (total_rows - 1) // chunk_size + 1

    logging.info(
        f"Starting upload to Bigtable for {symbol} {year}-{month:02d}. Total rows: {total_rows}"
    )

    for i in range(0, total_rows, chunk_size):
        chunk = data[i : i + chunk_size]
        chunk_size = upload_chunk(chunk, symbol, year, month, i)
        total_size += chunk_size
        logging.info(
            f"Uploaded chunk {i // chunk_size + 1}/{chunks_count} for {symbol} {year}-{month:02d}"
        )

    logging.info(
        f"Total uploaded size: {total_size} bytes for {symbol} {year}-{month:02d}"
    )
    return {"size": total_size, "rows": total_rows}
