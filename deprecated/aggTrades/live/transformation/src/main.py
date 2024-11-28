import datetime
import logging
import os
import time

import findspark
import pyarrow as pa
import pyarrow.parquet as pq
import pytz
import s3fs
import schedule
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    col,
    from_unixtime,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    MapType,
)

from VolumeProfileCluster import DataProcessor
from VolumeProfileCluster.slack_package import (
    SlackChannel,
    get_slack_decorators,
)
from config import config
from storage import s3Module

# Initialize findspark
findspark.init()

# Load environment variables
ACCESS_KEY = config.ACCESS_KEY
SECRET_KEY = config.SECRET_KEY
REGION_NAME = config.REGION_NAME
BUCKET_NAME = config.BUCKET_NAME
BEARER_TOKEN = config.BEARER_TOKEN
SLACK_WEBHOOK_URL = config.SLACK_WEBHOOK_URL
PROCESSED_FILES_LOG = config.PROCESSED_FILES_LOG

os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY


def get_spark_session(app_name="AggTrades_Transformation"):
    os.environ["SPARK_HOME"] = "/opt/spark"
    os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/envs/myenv/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/envs/myenv/bin/python"
    os.environ["SPARK_CONF_DIR"] = "/opt/spark/conf"

    spark = (
        SparkSession.builder.appName(app_name)
        .master("spark://84.247.143.179:7077")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.instances", "5")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.host", "84.247.143.179")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.local.dir", "/tmp/spark-temp")
        .config("spark.executor.cores", "2")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config(
            "spark.jars",
            "/root/spark_jars/hadoop-aws-3.2.2.jar,/root/spark_jars/aws-java-sdk-bundle-1.11.375.jar,/root/spark_jars/guava-31.0-jre.jar",
        )
        .config("spark.network.timeout", "600s")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.executor.heartbeatInterval", "100s")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000ms")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .getOrCreate()
    )

    return spark


# Initialize Spark session
spark = get_spark_session()

log_file_path = (
    "/root/EosData/cicada-ingestion-Binance/cores/aggTrades/historical/transformation/src/app.log"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()],
)

slack_decorators = get_slack_decorators()
slack_channel = SlackChannel(SLACK_WEBHOOK_URL)
data_processor = DataProcessor(ACCESS_KEY, SECRET_KEY, BUCKET_NAME)
s3_module = s3Module(ACCESS_KEY, SECRET_KEY, BUCKET_NAME, REGION_NAME)

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


def save_dataframe_to_s3(df, exchange, symbol, day, hour, minute):
    s3_path = f"s3a://{BUCKET_NAME}/cicada-data/aggTrades/live/transformation/{exchange}/{symbol}/{day}/{hour}/{minute}/data.parquet"
    fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)

    # Read existing data if the file exists
    if fs.exists(s3_path):
        existing_table = pq.read_table(s3_path, filesystem=fs)
        new_table = pa.Table.from_pandas(df)
        combined_table = pa.concat_tables([existing_table, new_table])
    else:
        combined_table = pa.Table.from_pandas(df)

    pq.write_table(combined_table, s3_path, filesystem=fs)
    print(f"DataFrame has been uploaded directly to S3 path: {s3_path}")
    return s3_path


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


@slack_decorators.notify_with_link(
    header="A new Parquet file is available 🗂️",
    message="The pipeline has updated the daily agg Trade file",
    color="#6a0dad",
)
def process_data(df, data_processor, spark, minute, symbol, exchange, timeframe):
    try:
        logging.info(f"Processing minute data for {minute}")

        df = data_processor.calc_df(df, True, timeframe)

        # Aggregate bid & ask data
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

        # Save the final DataFrame to S3
        save_dataframe_to_s3(
            df_joined_final,
            exchange,
            symbol,
            minute[:10],  # day
            minute[11:13],  # hour
            minute[14:16],  # minute
        )

        logging.info("Data pipeline execution completed successfully")
        return df_joined_final

    except Exception as e:
        logging.error(f"An exception occurred during data pipeline execution: {str(e)}")
        logging.error("Traceback details:", exc_info=True)
        raise


def update():
    start_time = time.time()
    aggregate_trades = True

    paris_tz = pytz.timezone("Europe/Paris")
    current_time = datetime.datetime.now(tz=paris_tz)
    cryptos = ["BTCUSDT"]

    total_files = 0
    total_size = 0

    for crypto in cryptos:
        all_s3_paths = []
        crypto_start_time = time.time()
        crypto_files = 0
        crypto_size = 0

        # Define the correct paths to fetch the files created by the WebSocket streaming code
        now = datetime.datetime.utcnow()
        previous_minute_time = now - datetime.timedelta(minutes=1)
        day = previous_minute_time.strftime("%Y-%m-%d")
        hour = previous_minute_time.strftime("%H")
        minute = previous_minute_time.strftime("%M")

        s3_path = f"s3a://{BUCKET_NAME}/cicada-data/aggTrades/live/ingestion/Binance/{crypto.lower()}/{day}/{hour}/{minute}/data.parquet"

        try:
            fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)
            if not fs.exists(s3_path):
                logging.info(
                    f"Previous minute's file {s3_path} does not exist yet. Skipping processing."
                )
                continue

            df = spark.read.parquet(s3_path)
            logging.info("Processing previous minute")
            logging.info(f"Day: {day}, Hour: {hour}, Minute: {minute}, Path: {s3_path}")
            try:
                # Sort each df by time
                df = df.orderBy(col("timestamp").cast("long"))
                logging.info("Initialization of minute dataframe")
                df = df.withColumn(
                    "time_readable",
                    from_unixtime(col("timestamp") / 1000).cast("timestamp"),
                )

            except Exception as e:
                logging.error(f"Failed to read data from {s3_path}: {e}")

            if df is None:
                logging.info(f"No data available for minute {minute}. Skipping.")
                continue

            slack_channel.send_message(
                "🚀 Update Initiated",
                f"_\"You think I'm a parasite, don't you? But I just want to make money.\"_ - Jared Vennett\n\n"
                f"*Starting the transformation with* `{len(cryptos)}` *cryptocurrencies.*\n"
                f"🕰️ *Start Time*: {current_time}",
            )
            logging.info(f"Process data for {crypto}")
            process_data(df, data_processor, spark, minute, crypto, "Binance", "1m")

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

        except Exception as e:
            logging.error(f"Failed to process data for crypto {crypto}: {e}")


schedule.every(20).seconds.do(update)

while True:
    schedule.run_pending()
    time.sleep(1)
