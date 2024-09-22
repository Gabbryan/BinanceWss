import logging
import os
import sys
import traceback

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    MapType,
)
from tqdm import tqdm

from pyspark_module.data_processing import DataProcessor
from pyspark_module.utils import gcsModule

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

gcs_module = gcsModule(bucket_name="production-trustia-raw-data")
data_processor = DataProcessor()

# Initialize and start the Spark session
spark = (
    SparkSession.builder.appName("DataProcessor")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.conf.set("spark.hadoop.fs.gs.buffer.size", "67108864")  # 64MB buffer size for GCS
spark.conf.set(
    "spark.hadoop.fs.gs.implementation",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
)
spark.conf.set(
    "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
)
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set(
    "spark.hadoop.fs.AbstractFileSystem.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
)  # Add this line

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


def process_year_data(
    df_year,
    data_processor,
    year,
    symbol,
    timeframe,
    start_month,
    end_month,
) -> DataFrame:
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
        logging.info("Data pipeline execution completed successfully")
        return df_joined_final

    except Exception as e:
        logging.error(f"An exception occurred during data pipeline execution: {str(e)}")
        logging.error("Traceback details:", exc_info=True)
        raise


def run(start_year, start_month, end_year, end_month, timeframe, crypto):
    all_gcs_paths = []
    crypto_files = 0
    # Generate GCS paths for the entire range
    gcs_paths = gcs_module.generate_gcs_paths(
        crypto.replace("/", ""), start_year, end_year, start_month, end_month
    )
    all_gcs_paths.extend(gcs_paths)
    try:
        #     # Check if the entire data set is already processed
        #     success_path = f"transformed/Binance/{crypto.replace('/', '')}/aggTrades_historical/{timeframe}/{start_year}_{start_month:02d}_{end_year}_{end_month:02d}/data.parquet"
        #     success_files = gcs_module.list_gcs_success_files()
        #
        #     if success_files:
        #         logging.info(
        #             f"Data already processed for {crypto} with {timeframe}. Skipping."
        #         )
        #         return 0

        logging.info(f"Processing data for {crypto}")

        df_all = None

        # Wrap the iterable with tqdm for progress tracking
        for year, month, day, gcs_path in tqdm(
            all_gcs_paths, desc="Processing GCS paths", unit="path"
        ):
            logging.info(
                f"Processing years: {year}, month: {month}, day: {day}, path: {gcs_path}"
            )
            try:
                df_month = spark.read.parquet(gcs_path).repartition(100)
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

                df_month.show()

                # # Filter out header row if it exists
                # header_row = df_month.filter(col(new_column_names[0]) == "agg_trade_id")
                # if header_row.count() > 0:
                #     df_month = df_month.filter(col(new_column_names[0]) != "agg_trade_id")

                # Sort each df_month by time
                df_month = df_month.orderBy(col("time").cast("long"))

                if df_all is None:
                    df_all = df_month.withColumn(
                        "time_readable",
                        from_unixtime(col("time") / 1000).cast("timestamp"),
                    )
                else:
                    df_month = df_month.withColumn(
                        "time_readable",
                        from_unixtime(col("time") / 1000).cast("timestamp"),
                    )
                    df_all = df_all.union(df_month)

            except Exception as e:
                logging.error(f"Failed to read data from {gcs_path}: {e}")

        if df_all is None:
            logging.info(f"No data available for {crypto}. Skipping.")
            return 1

        df_all = df_all.orderBy(col("time").cast("long"))
        df_all.show()
        # Process the entire data frame
        logging.info(
            f"Processing the complete dataset for {crypto} and timeframe {timeframe}"
        )
        df_joined_final = process_year_data(
            df_all,
            data_processor,
            start_year,
            crypto,
            timeframe,
            start_month,
            end_month,
        )
        df_joined_final.show()

        logging.info("Uploading to GCS")
        # Define the GCS path
        gcs_path = f"gs://production-trustia-raw-data/transformed/Binance/{crypto.replace('/', '')}/aggTrades_historical/{timeframe}/{year}_{start_month:02d}_{end_month:02d}/data"
        logging.info("Exporting DataFrame to Parquet format")
        df_joined_final.show()
        df_joined_final.write.option("compression", "snappy").format("parquet").mode(
            "overwrite"
        ).save(gcs_path)

        # Save DataFrame to GCS in Parquet format
        # df.write.format("parquet").mode("overwrite").save(gcs_path)
        logger.info(f"Successfully saved DataFrame to GCS path: {gcs_path}")
        crypto_files += len(all_gcs_paths)
    except Exception as e:
        logging.error(f"Failed to process data for crypto {crypto}: {e}")
        logging.error(traceback.format_exc())
        return 1


import argparse


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Run Data Processing Pipeline")
    parser.add_argument("--start_year", type=str, required=True, help="Start year")
    parser.add_argument("--start_month", type=int, required=True, help="Start month")
    parser.add_argument("--end_year", type=str, required=True, help="End year")
    parser.add_argument("--end_month", type=int, required=True, help="End month")
    parser.add_argument(
        "--timeframe", type=str, required=True, help="Timeframe for aggregation"
    )
    parser.add_argument(
        "--crypto", type=str, required=True, help="Cryptocurrency symbol"
    )

    args = parser.parse_args()

    # Call the run function with the parsed arguments
    run(
        args.start_year,
        args.start_month,
        args.end_year,
        args.end_month,
        args.timeframe,
        args.crypto,
    )


if __name__ == "__main__":
    main()
