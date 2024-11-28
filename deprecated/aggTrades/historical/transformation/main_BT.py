import datetime
import logging
import os
import time

import findspark
import pytz
import schedule
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    MapType,
)

from VolumeProfileCluster import DataProcessor
from VolumeProfileCluster.slack_package import SlackChannel, get_slack_decorators
from config import config

# Initialize findspark
findspark.init()

# Load environment variables
BIGTABLE_PROJECT_ID = config.BIGTABLE_PROJECT_ID
BIGTABLE_INSTANCE_ID = config.BIGTABLE_INSTANCE_ID
BIGTABLE_TABLE_ID = config.BIGTABLE_TABLE_ID
BEARER_TOKEN = config.BEARER_TOKEN
SLACK_WEBHOOK_URL = config.SLACK_WEBHOOK_URL
PROCESSED_FILES_LOG = config.PROCESSED_FILES_LOG

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.GOOGLE_APPLICATION_CREDENTIALS


def get_spark_session(app_name="AggTrades_Transformation"):
    os.environ["SPARK_HOME"] = "/opt/spark"
    os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/envs/myenv/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/envs/myenv/bin/python"
    os.environ["SPARK_CONF_DIR"] = "/opt/spark/conf"

    spark = (
        SparkSession.builder.appName(app_name)
        .master("spark://84.247.143.179:7077")
        .config("spark.executor.memory", "16g")
        .config("spark.executor.instances", "5")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.host", "84.247.143.179")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.local.dir", "/tmp/spark-temp")
        .config("spark.executor.cores", "2")
        .config("spark.network.timeout", "600s")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.default.parallelism", "200")
        .config("spark.executor.heartbeatInterval", "100s")
        .getOrCreate()
    )

    return spark


# Initialize Spark session
spark = get_spark_session()

log_file_path = "./logs/app.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()],
)

slack_decorators = get_slack_decorators()
slack_channel = SlackChannel(SLACK_WEBHOOK_URL)
data_processor = DataProcessor()

# Bigtable Client
client = bigtable.Client(project=BIGTABLE_PROJECT_ID, admin=True)
instance = client.instance(BIGTABLE_INSTANCE_ID)
table = instance.table(BIGTABLE_TABLE_ID)


def read_from_bigtable(symbol, year, month):
    rows = []
    partial_rows = table.read_rows(filter_=row_filters.CellsColumnLimitFilter(1))
    for row in partial_rows:
        rows.append(row)
    return rows


def process_foot_data(df, column_name, prefix, symbol):
    json_schema = StructType(
        [
            StructField("price_level", StringType(), True),
            StructField(f"{prefix}_qty", StringType(), True),
            StructField(f"{prefix}_trades", StringType(), True),
            StructField(f"{prefix}_trades_aggr", StringType(), True),
        ]
    )

    df_with_foot = df.withColumn(
        f"{column_name}_str", F.concat_ws("", F.col(column_name))
    )

    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_str",
        F.regexp_replace(f"{column_name}_str", r"\}\s*\{", "},{"),
    )

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

    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json",
        F.regexp_replace(F.col(f"{column_name}_json"), r"\{\{", "{"),
    )

    df_with_foot = df_with_foot.withColumn(
        f"{column_name}_json",
        F.regexp_replace(F.col(f"{column_name}_json"), r"\}\}", "}"),
    )

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

    if symbol == "BTCUSDT":
        df = df.withColumn("price_level", F.round(F.col("price_level")))
    elif symbol == "ETHUSDT":
        df = df.withColumn("price_level", F.round(F.col("price_level")))
    elif symbol == "SOLUSDT":
        df = df.withColumn("price_level", F.round(F.col("price_level"), 1))

    df = df.filter(
        F.col("price_level").isNotNull()
        & F.col("qty").isNotNull()
        & F.col("trades").isNotNull()
        & F.col("trades_aggr").isNotNull()
    )

    df_agg = df.groupBy("time_rounded", "price_level").agg(
        F.sum("qty").alias(f"total_{prefix}_qty"),
        F.sum("trades").alias(f"total_{prefix}_trades"),
    )

    df_agg_sorted = df_agg.orderBy("time_rounded", "price_level")

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

        logging.info("Aggregate bid data")
        df_bid_agg_sorted, df_bid_total_qty = process_foot_data(
            df, "foot_bid", "bid", symbol
        )
        logging.info("Aggregate ask data")
        df_ask_agg_sorted, df_ask_total_qty = process_foot_data(
            df, "foot_ask", "ask", symbol
        )

        logging.info("Transform and aggregate bid footprint")
        df_footprint_bids = data_processor.transform_and_aggregate_footprint(
            df_bid_agg_sorted, "bid"
        )
        logging.info("Transform and aggregate ask footprint")
        df_footprint_asks = data_processor.transform_and_aggregate_footprint(
            df_ask_agg_sorted, "ask"
        )
        logging.info("Aggregated footprint data")

        logging.info("Join bid and ask footprints")
        df_joined_final = data_processor.join_bid_ask_footprints(
            df, df_bid_total_qty, df_ask_total_qty, df_footprint_bids, df_footprint_asks
        )

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

        data_processor.save_dataframe_to_bigtable(
            df_joined_final,
            symbol,
            exchange,
            timeframe,
            f"{year}-{start_month:02d}",
            f"{year}-{end_month:02d}",
        )

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
    start_month = "01"
    end_year = "2024"
    end_month = "01"
    timeframes = ["4h"]

    paris_tz = pytz.timezone("Europe/Paris")
    current_time = datetime.datetime.now(tz=paris_tz)
    cryptos = ["BTCUSDT"]

    total_files = 0
    total_size = 0

    for crypto in cryptos:
        for timeframe in timeframes:
            all_rows = []
            crypto_start_time = time.time()
            crypto_files = 0
            crypto_size = 0

            for year in range(int(start_year), int(end_year) + 1):
                for month in range(1, 13):
                    if (year == int(start_year) and month < int(start_month)) or (
                        year == int(end_year) and month > int(end_month)
                    ):
                        continue

                    logging.info(f"Processing data for {crypto} in {year}-{month:02d}")

                    rows = read_from_bigtable(crypto, year, month)
                    if not rows:
                        logging.info(
                            f"No data available for {year}-{month:02d}. Skipping."
                        )
                        continue

                    for row_data in rows:
                        # Process each row_data as needed to convert it to DataFrame rows
                        # This should match the schema defined in DataProcessor
                        all_rows.append(row_data)

            if not all_rows:
                logging.info(f"No data available for {crypto}. Skipping.")
                continue

            df_year = spark.createDataFrame(all_rows)

            year_start_month = int(start_month) if year == int(start_year) else 1
            year_end_month = int(end_month) if year == int(end_year) else 12

            slack_channel.send_message(
                "🚀 Daily Update Initiated",
                f"_\"You think I'm a parasite, don't you? But I just want to make money.\"_ - Jared Vennett\n\n"
                f"*Starting the daily transformation with* `{len(cryptos)}` *cryptocurrencies across* `{len(timeframes)}` *timeframes.*\n"
                f"🕰️ *Start Time*: {current_time}",
            )

            process_year_data(
                df_year,
                data_processor,
                spark,
                year,
                crypto,
                "binance-futures",
                timeframe,
                year_start_month,
                year_end_month,
            )

            crypto_files += len(all_rows)

            total_files += crypto_files
            total_size += 0

            crypto_duration = time.time() - crypto_start_time
            slack_channel.send_message(
                f"✅ Crypto Transformation Complete: {crypto}",
                f'_"This business kills the part of life that is essential, the part that has nothing to do with business."_ - Ben Rickert\n\n'
                f"Successfully processed *{crypto_files}* files for *{crypto}* in *{crypto_duration:.2f} seconds*. The data now occupies a total of *{crypto_size / (1024 ** 3):.2f} GB*.\n"
                f"📈 *Files Processed*: {crypto_files}\n"
                f"🕰️ *Processing Time*: {crypto_duration:.2f} seconds\n"
                f"💾 *Total Data Size*: {crypto_size / (1024 ** 3):.2f} GB",
            )

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
