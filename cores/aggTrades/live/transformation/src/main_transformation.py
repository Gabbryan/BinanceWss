import datetime
from dotenv import load_dotenv
import logging
import os
import sys
import findspark
import pytz
import schedule
import time
from typing import List
import traceback
from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    IntegerType,
    MapType,
)
from pyspark.sql.functions import (
    col,
    when,
    to_json,
    struct,
    lit,
    udf,
    collect_list,
    round as round_,
    max as max_,
    min as min_,
    sum as sum_,
    first,
    last,
    from_unixtime,
)
from google.cloud import storage

from config.config import config
from data_processing import DataProcessor
from slack_package import SlackChannel, get_slack_decorators
from utils.gcs_module import gcsModule
from config import BUCKET_NAME
from sparkManager.spark_config import SparkManager

# Load the log file path from environment variable
log_file_path = os.getenv(
    "LOG_FILE_PATH",
    "/Trustia/Cicada-binance/cores/aggTrades/historical/transformation/src/app.log",
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()],
)

# Initialize findspark
findspark.init()

# Load environment variables from .env file
load_dotenv()

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

SLACK_WEBHOOK_URL = config.SLACK_WEBHOOK_URL
PROCESSED_FILES_LOG = config.PROCESSED_FILES_LOG

gcs_module = gcsModule(bucket_name=BUCKET_NAME)
slack_decorators = get_slack_decorators()
slack_channel = SlackChannel(SLACK_WEBHOOK_URL)
data_processor = DataProcessor()
spark_manager = SparkManager(app_name="AggTrades_Transformation")

# Initialize and start the Spark session
spark = spark_manager.get_spark_session()

# Define the schema for bid and ask data
json_schema_bid = StructType(
    [
        StructField("price_level", StringType(), True),
        StructField("bid_qty", StringType(), True),
        StructField("bid_trades", IntegerType(), True),
        StructField("bid_trades_aggr", IntegerType(), True),
    ]
)

json_schema_ask = StructType(
    [
        StructField("price_level", StringType(), True),
        StructField("ask_qty", StringType(), True),
        StructField("ask_trades", IntegerType(), True),
        StructField("ask_trades_aggr", IntegerType(), True),
    ]
)


def get_files_for_timeframe(symbol, date, timeframe):
    current_time = datetime.datetime.utcnow()
    if timeframe == "5m":
        file_count = 5
    elif timeframe == "15m":
        file_count = 15
    elif timeframe == "1h":
        file_count = 60
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    # Adjusted path based on your file structure
    prefix = f"Raw/Binance/{symbol}/aggTrades/{date[:4]}/{date[5:7]}/{date[8:10]}/"

    files = gcs_module.get_gcs_files(prefix=prefix)

    # Sort the files by timestamp
    files_sorted = sorted(files, key=lambda x: x.split("/")[-1][:20], reverse=True)

    # Select the latest `file_count` files
    relevant_files = files_sorted[:file_count]

    return relevant_files


def read_and_aggregate_files(files: List[str]) -> DataFrame:
    df_list = []

    # Trier les fichiers par ordre décroissant
    files_sorted = sorted(files, reverse=True)

    for file in files_sorted:
        try:
            df = spark.read.parquet(file)
            df_list.append(df)
        except Exception as e:
            logging.warning(
                f"File not found or cannot be read: {file}. Skipping. Error: {e}"
            )

    if not df_list:
        raise FileNotFoundError("No valid files to read and aggregate.")

    df = df_list[0]
    for temp_df in df_list[1:]:
        df = df.union(temp_df)

    return df


def process_foot_data(df, column_name, prefix, symbol):
    # Define the schema with StringType for all fields initially
    json_schema = StructType(
        [
            StructField("price_level", StringType(), True),
            StructField(f"{prefix}_qty", StringType(), True),
            StructField(f"{prefix}_trades", StringType(), True),
            StructField(f"{prefix}_trades_aggr", StringType(), True),
        ]
    )

    # Convert the array to a single JSON string
    df_with_foot = df.withColumn(
        f"{column_name}_str", F.concat_ws("", F.col(column_name))
    )

    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_str",
        F.regexp_replace(f"{column_name}_str", r"\}\s*\{", "},{"),
    )

    # Convert the concatenated string to an array of JSON strings
    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json_array", F.split(f"{column_name}_str", r",\{")
    )
    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json_array",
        F.expr(f"TRANSFORM({column_name}_json_array, x -> concat('{{', x))"),
    )

    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json", F.explode(f"{column_name}_json_array")
    )

    # Replace any occurrences of "{{" with "{" and "}}" with "}"
    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json",
        F.regexp_replace(F.col(f"{column_name}_json"), r"\{\{", "{"),
    )

    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json",
        F.regexp_replace(F.col(f"{column_name}_json"), r"\}\}", "}"),
    )

    # Parse JSON and select the necessary fields
    df = df_with_foot.select(
        F.col("time_rounded"),
        F.from_json(F.col(f"{column_name}_json"), json_schema).alias("data"),
    ).select(
        "time_rounded",
        F.col("data.price_level").cast("double").alias("price_level"),
        F.col(f"data.{prefix}_qty").cast("double").alias("qty"),
        F.col(f"data.{prefix}_trades").cast("integer").alias("trades"),
        F.col(f"data.{prefix}_trades_aggr").cast("integer").alias("trades_aggr"),
    )

    # Conditionally round price_level based on symbol
    if symbol == "BTCUSDT":
        df = df.withColumn("price_level", F.round(F.col("price_level")))
    elif symbol == "ETHUSDT":
        df = df.withColumn("price_level", F.round(F.col("price_level")))
    elif symbol == "SOLUSDT":
        df = df.withColumn("price_level", F.round(F.col("price_level"), 1))

    # Filter out null values
    df = df.filter(
        F.col("price_level").isNotNull()
        & F.col("qty").isNotNull()
        & F.col("trades").isNotNull()
        & F.col("trades_aggr").isNotNull()
    )

    # Aggregate data
    df_agg = df.groupBy("time_rounded", "price_level").agg(
        F.sum("qty").alias(f"total_{prefix}_qty"),
        F.sum("trades").alias(f"total_{prefix}_trades"),
    )

    # Sort the aggregated DataFrame by time_rounded and price_level
    df_agg_sorted = df_agg.orderBy("time_rounded", "price_level")

    # Aggregate total bid quantity by time_rounded
    df_qty = df_agg_sorted.groupBy("time_rounded").agg(
        F.sum(f"total_{prefix}_qty").alias(f"sum_total_{prefix}_qty")
    )

    return df_agg_sorted, df_qty


def transform_footprint(row):
    try:
        bid_data = row.aggregated_foot_bid
        ask_data = row.aggregated_foot_ask
        aggregated_data = {}

        if bid_data:
            for bid_item in bid_data:
                for price, bid_row in bid_item.items():
                    if price not in aggregated_data:
                        aggregated_data[price] = {
                            "bid_qty": 0,
                            "bid_trades": 0,
                            "ask_qty": 0,
                            "ask_trades": 0,
                        }
                    aggregated_data[price]["bid_qty"] += bid_row["bid_qty"]
                    aggregated_data[price]["bid_trades"] += bid_row["bid_trades"]

        if ask_data:
            for ask_item in ask_data:
                for price, ask_row in ask_item.items():
                    if price not in aggregated_data:
                        aggregated_data[price] = {
                            "bid_qty": 0,
                            "bid_trades": 0,
                            "ask_qty": 0,
                            "ask_trades": 0,
                        }
                    aggregated_data[price]["ask_qty"] += ask_row["ask_qty"]
                    aggregated_data[price]["ask_trades"] += ask_row["ask_trades"]

        logging.info(f"Transformed footprint for row: {row.time_rounded}")
        return aggregated_data

    except KeyError as e:
        logging.error(f"KeyError in row {row.time_rounded}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in row {row.time_rounded}: {e}")
        raise e


def process_timeframe_data(symbol, date, timeframe):
    try:
        date = datetime.datetime.utcnow()

        # Get files for the specified timeframe
        files = get_files_for_timeframe(symbol, date, timeframe)
        if not files:
            logging.warning(f"No files found for the specified timeframe: {timeframe}")
            return

        # Read and aggregate the files
        df = read_and_aggregate_files(files)

        # Convert timestamp from milliseconds to seconds and then to a readable format
        df = df.withColumn("readable_timestamp", from_unixtime(col("timestamp") / 1000))

        # Convert timestamp column to integer type for proper sorting
        df = df.withColumn("timestamp_int", col("timestamp").cast("long"))

        # Sort the DataFrame by timestamp
        df = df.orderBy(col("timestamp_int"))

        # Rename 'timestamp' to 'time'
        df = df.withColumnRenamed("timestamp", "time")

        # Apply the same process as in process_year_data
        df = data_processor.calc_df(df, True, timeframe)

        # Aggregate bid data
        df_bid_agg_sorted, df_bid_total_qty = process_foot_data(
            df, "foot_bid", "bid", symbol
        )

        # Aggregate ask data
        df_ask_agg_sorted, df_ask_total_qty = process_foot_data(
            df, "foot_ask", "ask", symbol
        )

        # Transform and aggregate bid footprint
        df_footprint_bids = data_processor.transform_and_aggregate_footprint(
            df_bid_agg_sorted, "bid"
        )

        # Transform and aggregate ask footprint
        df_footprint_asks = data_processor.transform_and_aggregate_footprint(
            df_ask_agg_sorted, "ask"
        )

        # Join bid and ask footprints
        df_joined_final = data_processor.join_bid_ask_footprints(
            df, df_bid_total_qty, df_ask_total_qty, df_footprint_bids, df_footprint_asks
        )

        # Define the UDF
        footprint_schema = MapType(
            StringType(),
            StructType(
                [
                    StructField("bid_qty", DoubleType(), True),
                    StructField("bid_trades", DoubleType(), True),
                    StructField("ask_qty", DoubleType(), True),
                    StructField("ask_trades", DoubleType(), True),
                ]
            ),
        )

        footprint_udf = F.udf(transform_footprint, footprint_schema)

        # Apply footprint transformation
        df_joined_final = df_joined_final.withColumn(
            "footprint",
            footprint_udf(
                F.struct(
                    df_joined_final.time_rounded,  # Ensure this column exists and is correctly named
                    df_joined_final.aggregated_foot_bid,
                    df_joined_final.aggregated_foot_ask,
                )
            ),
        )

        df_joined_final = df_joined_final.drop(
            "aggregated_foot_bid", "aggregated_foot_ask"
        )

        logging.info("Ordering by time")

        df_joined_final = df_joined_final.orderBy("time_rounded")

        # Save the final DataFrame to GCS
        ## TO DO : /ANNEE/MOIS/JOUR...

        output_path = f"gs://{BUCKET_NAME}/transformed/Binance/BTCUSDT/aggTrades/timeframe/{symbol}_{timeframe}_{date}.parquet"
        df_joined_final.write.mode("overwrite").parquet(output_path)

        logging.info(
            f"Data processing for {timeframe} timeframe completed successfully"
        )

    except Exception as e:
        logging.error(
            f"An exception occurred during data processing for {timeframe} timeframe: {str(e)}"
        )
        logging.error("Traceback details:", exc_info=True)
        raise


def process_files(symbol):
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    logging.info(f"Processing files for {symbol} on {today}")

    # Define the timeframes
    timeframes = ["1h"]

    for timeframe in timeframes:
        try:
            # Process and save the data for the timeframe
            process_timeframe_data(symbol, today, timeframe)

        except Exception as e:
            logging.error(f"Error processing files for {symbol} in {timeframe}: {e}")
            slack_channel.send_message(
                f"Error processing files for {symbol} in {timeframe}: {e}"
            )


def daily_process(symbols):
    for symbol in symbols:
        try:
            process_files(symbol)
        except Exception as e:
            logging.error(f"Error processing data for symbol {symbol}: {e}")
            slack_channel.send_message(
                f"Error processing data for symbol {symbol}: {e}"
            )


if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    daily_process(symbols)
