import datetime
import logging
import os
import time
import pytz
import schedule
import findspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import from_unixtime, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    MapType,
)
from google.cloud import storage

from config import config
from VolumeProfileCluster import DataProcessor
from VolumeProfileCluster.slack_package import SlackChannel, get_slack_decorators
from storage.gcs_module import gcsModule

# Initialize findspark
findspark.init()

# Set the path to your Google Cloud service account key file
service_account_path = "/root/Trustia/Cicada-binance/cores/aggTrades/historical/transformation/src/service-account-key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
BUCKET_NAME = "production-trustia-raw-data"

SLACK_WEBHOOK_URL = config.SLACK_WEBHOOK_URL
PROCESSED_FILES_LOG = config.PROCESSED_FILES_LOG


def get_bucket():
    bucket_name = "production-trustia-processed-data"
    storage_client = storage.Client()
    return storage_client.bucket(bucket_name)


bucket = get_bucket()


def get_spark_session(app_name="AggTrades_Transformation"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", "16g")
        .config("spark.executor.instances", "5")
        .config("spark.driver.memory", "8g")
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        )
        .getOrCreate()
    )

    return spark


# Initialize Spark session
spark = get_spark_session()

log_file_path = (
    "/root/Trustia/Cicada-binance/cores/aggTrades/historical/transformation/src/app.log"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()],
)

slack_decorators = get_slack_decorators()
slack_channel = SlackChannel(SLACK_WEBHOOK_URL)
data_processor = DataProcessor()
gcs_module = gcsModule(BUCKET_NAME)

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
        f"{column_name}_str", F.regexp_replace(f"{column_name}_str", r"\}\s*\{", "},{")
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
                    aggregated_data[price]["bid_qty"] += bid_row.bid_qty
                    aggregated_data[price]["bid_trades"] += bid_row.bid_trades

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
                    aggregated_data[price]["ask_qty"] += ask_row.ask_qty
                    aggregated_data[price]["ask_trades"] += ask_row.ask_trades

        logging.info(f"Transformed footprint for row: {row.time_rounded}")
        return aggregated_data

    except KeyError as e:
        logging.error(f"KeyError in row {row.time_rounded}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in row {row.time_rounded}: {e}")
        raise e


@slack_decorators.notify_with_link(
    header="A new Parquet file is available 🗂️",
    message="The pipeline has updated the daily agg Trade file",
    color="#6a0dad",
)
def process_year_data(
    df_year,
    data_processor,
    spark,
    gcs_paths_by_year,
    year,
    symbol,
    exchange,
    timeframe,
    start_month,
    end_month,
):
    try:
        logging.info(f"Processing year data for {year}")

        df = data_processor.calc_df(df_year, True, timeframe)

        # Aggregate bid data
        logging.info("aggregate bid data")
        (
            df_bid_agg_sorted,
            df_bid_total_qty,
        ) = process_foot_data(df, "foot_bid", "bid", symbol)
        logging.info("aggregate ask data")
        df_ask_agg_sorted, df_ask_total_qty = process_foot_data(
            df, "foot_ask", "ask", symbol
        )

        # Transform and aggregate bid footprint
        logging.info("transform_and_aggregate_bid_footprint")
        df_footprint_bids = data_processor.transform_and_aggregate_footprint(
            df_bid_agg_sorted, "bid"
        )
        logging.info("transform_and_aggregate_ask_footprint")
        df_footprint_asks = data_processor.transform_and_aggregate_footprint(
            df_ask_agg_sorted, "ask"
        )
        logging.info("df_footprint_asks")

        # Join bid and ask footprints
        logging.info("join_bid_ask_footprints")
        df_joined_final = data_processor.join_bid_ask_footprints(
            df,
            df_bid_total_qty,
            df_ask_total_qty,
            df_footprint_bids,
            df_footprint_asks,
        )

        # Define the schema for the UDF
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
                    df_joined_final.time_rounded,
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
        gcs_module.upload_to_gcs(df_joined_final, symbol, year, start_month, end_month)

        logging.info("Data pipeline execution completed successfully")
        return df_joined_final

    except Exception as e:
        logging.error(f"An exception occurred during data pipeline execution: {str(e)}")
        logging.error("Traceback details:", exc_info=True)
        raise


def daily_update():
    start_time = time.time()
    aggregate_trades = True
    start_year = "2023"
    start_month = 1
    end_year = "2024"
    end_month = 8
    timeframes = ["4h", "1d", "1w"]

    paris_tz = pytz.timezone("Europe/Paris")
    current_time = datetime.datetime.now(tz=paris_tz)
    cryptos = ["BTCUSDT"]

    # cryptos = gcs_module.get_cryptos(
    #     BUCKET_NAME, "cicada-data/HistoricalTradeAggregator/binance-futures/"
    # )

    total_files = 0
    total_size = 0

    for crypto in cryptos:
        for timeframe in timeframes:
            all_gcs_paths = []
            crypto_start_time = time.time()
            crypto_files = 0
            crypto_size = 0

            # Generate GCS paths for the entire range
            gcs_paths = gcs_module.generate_gcs_paths(
                crypto.replace("/", ""), start_year, end_year, start_month, end_month
            )
            all_gcs_paths.extend(gcs_paths)

            try:
                # Check if the entire data set is already processed
                success_path = f"transformed/Binance/{crypto.replace('/', '')}/aggTrades_historical/{timeframe}/{start_year}_{start_month:02d}_{end_year}_{end_month:02d}/data.parquet"
                success_files = gcs_module.list_gcs_success_files(success_path)

                if success_files:
                    logging.info(
                        f"Data already processed for {crypto} with {timeframe}. Skipping."
                    )
                    continue

                logging.info(f"Processing data for {crypto}")

                df_all = None
                for year, month, gcs_path in all_gcs_paths:
                    logging.info(f"Processing month: {month}, path: {gcs_path}")
                    try:
                        df_month = spark.read.parquet(gcs_path)
                        new_column_names = [
                            "agg_trades_id",
                            "price",
                            "quantity",
                            "first_trade_id",
                            "last_trade_id",
                            "time",
                            "is_buyer_maker",
                        ]
                        for old_name, new_name in zip(df_month.columns, new_column_names):
                            df_month = df_month.withColumnRenamed(old_name, new_name)

                        # Filter out header row if it exists
                        header_row = df_month.filter(col(new_column_names[0]) == "agg_trade_id")
                        if header_row.count() > 0:
                            df_month = df_month.filter(col(new_column_names[0]) != "agg_trade_id")

                        # Sort each df_month by time
                        df_month = df_month.orderBy(col("time").cast("long"))

                        if df_all is None:
                            df_all = df_month.withColumn(
                                "time_readable", from_unixtime(col("time") / 1000).cast("timestamp")
                            )
                        else:
                            df_month = df_month.withColumn(
                                "time_readable", from_unixtime(col("time") / 1000).cast("timestamp")
                            )
                            df_all = df_all.union(df_month)

                    except Exception as e:
                        logging.error(f"Failed to read data from {gcs_path}: {e}")

                if df_all is None:
                    logging.info(f"No data available for {crypto}. Skipping.")
                    continue

                df_all = df_all.orderBy(col("time").cast("long"))

                # Process the entire data frame
                logging.info(f"Processing the complete dataset for {crypto} and timeframe {timeframe}")
                process_year_data(
                    df_all,
                    data_processor,
                    spark,
                    gcs_paths_by_year,  # You can adjust this as needed, or pass all_gcs_paths
                    start_year,  # Adjust to use the entire period
                    crypto,
                    "binance-futures",
                    timeframe,
                    start_month,
                    end_month,
                )

                crypto_files += len(all_gcs_paths)

                total_files += crypto_files
                total_size += 0  # Add logic to calculate the total size if necessary

                crypto_duration = time.time() - crypto_start_time
                slack_channel.send_message(
                    f"✅ Crypto Transformation Complete: {crypto}",
                    f'Successfully processed *{crypto_files}* files for *{crypto}* in *{crypto_duration:.2f} seconds*. The data now occupies a total of *{crypto_size / (1024 ** 3):.2f} GB*.\n'
                    f"📈 *Files Processed*: {crypto_files}\n"
                    f"🕰️ *Processing Time*: {crypto_duration:.2f} seconds\n"
                    f"💾 *Total Data Size*: {crypto_size / (1024 ** 3):.2f} GB",
                )

            except Exception as e:
                logging.error(f"Failed to process data for crypto {crypto}: {e}")


    total_duration = time.time() - start_time
    slack_channel.send_message(
        "🎬 Daily Transformation Complete",
        f'_"In the end they knew."_ - Narrator\n\n'
        f"All cryptocurrencies have been processed. Here are the final stats for today's update:\n"
        f"📈 *Total Files Processed*: {total_files}\n"
        f"🕰️ *Total Processing Time*: {total_duration / 60:.2f} minutes\n"
        f"💾 *Total Data Size*: {total_size / (1024 ** 3):.2f} GB\n"
        f"Outstanding work, everyone! 🌟",
    )


schedule.every().day.at("10:00").do(daily_update)

schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)
