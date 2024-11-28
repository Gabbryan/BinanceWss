import logging
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd

from deprecated.aggTrades.historical.ingestion.config.bigTable_config import (
    bigtable_client,
    BIGTABLE_PROJECT_ID,
    BIGTABLE_INSTANCE_ID,
    BIGTABLE_TABLE_ID,
)
from log.logging_config import log_safe
from process.process_symbol_bigTable import process_symbol_bigTable
from process.check_exists import check_bigtable_object_exists
from deprecated.aggTrades.historical.ingestion.utils.upload_to_bigTable import upload_to_bigtable


def daily_update(symbols, exchanges):
    start_date = datetime.now().date() - timedelta(days=1)
    end_date = start_date
    all_exported_files = []

    for symbol in symbols:
        for ex in exchanges:
            logging.info(f"Starting daily update for {ex} {symbol}.")
            exported_files, symbol, _, _ = process_symbol_bigTable(
                ex, symbol, start_date, end_date
            )
            if exported_files:
                symbol_url_part = symbol.replace("/", "")
                bigtable_key = f"{symbol_url_part}#{start_date.year}-{start_date.month}"

                # Check if the file exists in Bigtable
                if check_bigtable_object_exists(
                    bigtable_client,
                    BIGTABLE_PROJECT_ID,
                    BIGTABLE_INSTANCE_ID,
                    BIGTABLE_TABLE_ID,
                    bigtable_key,
                ):
                    log_safe(
                        f"Data for {bigtable_key} already exists in Bigtable. Merging new data."
                    )
                    # Download the existing file from Bigtable
                    existing_data = download_bigtable_data(bigtable_key)
                else:
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

                # Upload combined data to Bigtable
                bigtable_info = upload_to_bigtable(
                    combined_data, symbol, start_date.year, start_date.month
                )
                log_safe(
                    f"Daily update completed for {symbol} {start_date.year}-{start_date.month:02d}-{start_date.day:02d}"
                )

                all_exported_files.append(bigtable_info)

    log_safe("Daily update completed.")
    return all_exported_files, symbols, start_date, end_date


def download_bigtable_data(bigtable_key):
    # Function to download existing data from Bigtable
    instance = bigtable_client.instance(BIGTABLE_INSTANCE_ID)
    table = instance.table(BIGTABLE_TABLE_ID)
    rows = table.read_rows(prefix=bigtable_key.encode())
    data = []

    for row in rows:
        row_data = {}
        for cell in row.cells["cf1"]:
            row_data[cell.qualifier.decode()] = cell.value.decode()
        data.append(row_data)

    return pd.DataFrame(data)
