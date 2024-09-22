import os
import logging
import pandas as pd
from io import BytesIO
from google.cloud import storage
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when, to_json, struct, lit, udf, collect_list
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
from datetime import datetime, timedelta

from slack_package import init_slack, get_slack_decorators
from sparkManager.spark_config import SparkManager

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Slack with the webhook URL
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
slack_manager = init_slack(WEBHOOK_URL)
slack_decorators = get_slack_decorators()

# Initialize SparkManager
spark_manager = SparkManager()


class DataProcessor:
    def __init__(self):
        self.spark = (
            spark_manager.get_spark_session()
        )  # Use SparkManager to get Spark session
        self.last_timestamp = 0

        # Initialize GCS client
        self.storage_client = storage.Client()
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.bucket = self.storage_client.bucket(self.bucket_name)

        # Define the schema for trade data
        self.json_schema_trade = StructType(
            [
                StructField("price", DoubleType(), True),
                StructField("quantity", DoubleType(), True),
                StructField("timestamp", LongType(), True),
                StructField("is_buyer_market", BooleanType(), True),
                StructField("quote_quantity", DoubleType(), True),
                StructField("is_best_match", BooleanType(), True),
                StructField("id", LongType(), True),
                StructField("oid", StringType(), True),
                StructField("date_insert", StringType(), True),
                StructField("date_update", StringType(), True),
                StructField("deleted", BooleanType(), True),
            ]
        )

    def round_down_to_interval(self, df, timeframe):
        return df.withColumn(
            "time_rounded",
            (F.col("time") / 1000).cast("timestamp").cast("long")
            - (F.col("time") / 1000).cast("long") % timeframe,
        ).withColumn(
            "time_rounded", F.from_unixtime(F.col("time_rounded")).cast("timestamp")
        )

    def calc_df(self, df, aggregate_trades, timeframe):
        intervals = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "12h": 60 * 60 * 12,
            "1d": 86400,
            "1w": 604800,
        }

        if timeframe not in intervals:
            raise ValueError("Unsupported timeframe provided.")

        interval_seconds = intervals[timeframe]
        df = self.round_down_to_interval(df, interval_seconds)

        foot_bid = when(
            col("is_buyer_maker") == "true",
            to_json(
                struct(
                    col("price").alias("price_level"),
                    col("quantity").alias("bid_qty"),
                    lit(1).alias("bid_trades"),
                    when(lit(aggregate_trades), lit(1))
                    .otherwise(lit(None))
                    .alias("bid_trades_aggr"),
                )
            ),
        )

        foot_ask = when(
            col("is_buyer_maker") == "false",
            to_json(
                struct(
                    col("price").alias("price_level"),
                    col("quantity").alias("ask_qty"),
                    lit(1).alias("ask_trades"),
                    when(lit(aggregate_trades), lit(1))
                    .otherwise(lit(None))
                    .alias("ask_trades_aggr"),
                )
            ),
        )

        df = df.withColumn("foot_bid", foot_bid).withColumn("foot_ask", foot_ask)

        df_agg = (
            df.groupBy(F.window("time_rounded", f"{interval_seconds} seconds"))
            .agg(
                F.first("price").alias("open"),
                F.last("price").alias("close"),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.sum("quantity").alias("qty"),
                F.collect_list("foot_bid").alias("foot_bid"),
                F.collect_list("foot_ask").alias("foot_ask"),
            )
            .select(
                col("window.start").alias("time_rounded"),
                "open",
                "close",
                "high",
                "low",
                "qty",
                "foot_bid",
                "foot_ask",
            )
            .orderBy("time_rounded")
        )

        return df_agg

    def process_foot_data(self, df, column_name, json_schema, prefix):
        try:
            logger.info(f"Processing column: {column_name}")

            df_with_foot = df.withColumn(
                f"{column_name}_str", F.concat_ws("", F.col(column_name))
            )
            logger.debug(f"Converted {column_name} to string")

            df_with_foot = df_with_foot.withColumn(
                f"{column_name}_str",
                F.regexp_replace(f"{column_name}_str", r"\}\s*\{", "},{"),
            )
            logger.debug(f"Replaced inner JSON object delimiters in {column_name}")

            df_with_foot = df_with_foot.withColumn(
                f"{column_name}_json_array", F.split(f"{column_name}_str", r",\{")
            )
            df_with_foot = df_with_foot.withColumn(
                f"{column_name}_json_array",
                F.expr(f"TRANSFORM({column_name}_json_array, x -> concat('{{', x))"),
            )
            logger.debug(
                f"Converted concatenated string to JSON array for {column_name}"
            )

            df_with_foot = df_with_foot.withColumn(
                f"{column_name}_json", F.explode(f"{column_name}_json_array")
            )
            logger.debug(f"Exploded JSON array for {column_name}")

            df_with_foot = df_with_foot.withColumn(
                f"{column_name}_json",
                F.regexp_replace(F.col(f"{column_name}_json"), r"\{\{", "{"),
            )

            df_with_foot = df_with_foot.withColumn(
                f"{column_name}_json",
                F.regexp_replace(F.col(f"{column_name}_json"), r"\}\}", "}"),
            )
            logger.debug(f"Replaced '{{' and '}}' with '{' and '}' in {column_name}")

            df = (
                df_with_foot.select(
                    F.col("time_rounded"),
                    F.from_json(F.col(f"{column_name}_json"), json_schema).alias(
                        "data"
                    ),
                )
                .select(
                    "time_rounded",
                    F.round(F.col("data.price_level")).alias("price_level"),
                    F.col(f"data.{column_name.split('_')[1]}_qty").alias("qty"),
                    F.col(f"data.{column_name.split('_')[1]}_trades").alias("trades"),
                )
                .filter(
                    F.col("price_level").isNotNull()
                    & F.col("qty").isNotNull()
                    & F.col("trades").isNotNull()
                )
            )
            logger.info(f"Parsed JSON and selected necessary fields for {column_name}")

            df_agg = df.groupBy("time_rounded", "price_level").agg(
                F.sum("qty").alias(f"total_{prefix}_qty"),
                F.sum("trades").alias(f"total_{prefix}_trades"),
            )
            logger.info(f"Aggregate data for {column_name}")

            df_agg_sorted = df_agg.orderBy("time_rounded", "price_level")
            logger.debug(f"Sorted aggregated DataFrame for {column_name}")

            df_total_qty = df_agg_sorted.groupBy("time_rounded").agg(
                F.sum(f"total_{prefix}_qty").alias(f"sum_{prefix}_total_qty")
            )
            logger.info(f"Compute total quantity for {column_name}")

            return df_agg_sorted, df_total_qty

        except Exception as e:
            logger.error(f"Error processing {column_name}: {e}")
            raise

    def transform_and_aggregate_footprint(self, df, column_prefix):
        try:
            return_schema = MapType(
                StringType(),
                StructType(
                    [
                        StructField(f"{column_prefix}_qty", DoubleType(), True),
                        StructField(f"{column_prefix}_trades", DoubleType(), True),
                    ]
                ),
            )

            def transform_row(price, qty, trades):
                return {
                    str(price): {
                        f"{column_prefix}_qty": qty,
                        f"{column_prefix}_trades": trades,
                    }
                }

            transform_row_udf = udf(transform_row, return_schema)

            df_transformed = df.withColumn(
                "transformed_aggregated_foot",
                transform_row_udf(
                    "price_level",
                    F.col(f"total_{column_prefix}_qty").cast(DoubleType()),
                    F.col(f"total_{column_prefix}_trades").cast(DoubleType()),
                ),
            )

            df_footprint = df_transformed.groupBy("time_rounded").agg(
                collect_list("transformed_aggregated_foot").alias(
                    f"aggregated_foot_{column_prefix}"
                )
            )

            df_footprint_sorted = df_footprint.orderBy("time_rounded")

            return df_footprint_sorted
        except Exception as e:
            logger.error(f"Error in transform_and_aggregate_footprint: {e}")
            raise

    def join_bid_ask_footprints(
        self, df, df_bid_qty, df_ask_qty, df_footprint_bids, df_footprint_asks
    ):
        try:
            # Drop unnecessary columns from df
            df = df.drop("foot_bid", "foot_ask")

            # Join the df built before
            df_joined = (
                df.join(df_bid_qty, on="time_rounded", how="outer")
                .join(df_ask_qty, on="time_rounded", how="outer")
                .join(df_footprint_bids, on="time_rounded", how="outer")
                .join(df_footprint_asks, on="time_rounded", how="outer")
                .orderBy("time_rounded")
            )
            return df_joined
        except Exception as e:
            logger.error(f"Error in join_bid_ask_footprints: {e}")
            raise

    def transform_footprint(self, row):
        try:
            bid_data = row["aggregated_foot_bid"]
            ask_data = row["aggregated_foot_ask"]
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
                        aggregated_data[price]["bid_qty"] += bid_row.get("bid_qty", 0)
                        aggregated_data[price]["bid_trades"] += bid_row.get(
                            "bid_trades", 0
                        )

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
                        aggregated_data[price]["ask_qty"] += ask_row.get("ask_qty", 0)
                        aggregated_data[price]["ask_trades"] += ask_row.get(
                            "ask_trades", 0
                        )

            logger.info(f"Transformed footprint for row: {row['time_rounded']}")
            return aggregated_data

        except KeyError as e:
            logger.error(f"KeyError in row {row['time_rounded']}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in row {row['time_rounded']}: {e}")
            raise

    @slack_decorators.notify_with_link(
        header="A new Parquet file is available 🗂️",
        message="The pipeline has updated the daily agg Trade file",
        color="#6a0dad",
    )
    def parse_timeframe(self, timeframe):
        unit = timeframe[-1]
        if unit == "m":
            minutes = int(timeframe[:-1])
            return minutes * 60
        return 60

    def data_pipeline_exec(
        self,
        df_trades,
        start_date,
        end_date,
        symbol,
        exchange,
        timeframe,
        aggregate_trades=1,
    ):
        json_schema_bid = StructType(
            [
                StructField("price_level", DoubleType(), True),
                StructField("bid_qty", DoubleType(), True),
                StructField("bid_trades", IntegerType(), True),
                StructField("bid_trades_aggr", IntegerType(), True),
            ]
        )

        json_schema_ask = StructType(
            [
                StructField("price_level", DoubleType(), True),
                StructField("ask_qty", DoubleType(), True),
                StructField("ask_trades", IntegerType(), True),
                StructField("ask_trades_aggr", IntegerType(), True),
            ]
        )
        logger.info("Fetching raw data")
        df = self.calc_df(df_trades, True, timeframe)

        logger.info("Process foot_bid and foot_ask columns")
        df_bid_agg_sorted, df_bid_total_qty = self.process_foot_data(
            df, "foot_bid", json_schema_bid, "bid"
        )

        df_ask_agg_sorted, df_ask_total_qty = self.process_foot_data(
            df, "foot_ask", json_schema_ask, "ask"
        )

        logger.info("Bid footprint transformation")
        df_footprint_bids = self.transform_and_aggregate_footprint(
            df_bid_agg_sorted, "bid"
        )

        logger.info("Ask footprint transformation")
        df_footprint_asks = self.transform_and_aggregate_footprint(
            df_ask_agg_sorted, "ask"
        )

        logger.info("Merge all DataFrames")
        df_joined_final = self.join_bid_ask_footprints(
            df, df_bid_total_qty, df_ask_total_qty, df_footprint_bids, df_footprint_asks
        )

        logger.info("Saving data to GCS")
        self.save_dataframe_to_gcs(
            df_joined_final, symbol, exchange, timeframe, start_date, end_date
        )

        return df_joined_final
