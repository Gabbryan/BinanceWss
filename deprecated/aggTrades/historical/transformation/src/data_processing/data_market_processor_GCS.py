import logging
from google.cloud import storage
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when, to_json, struct, lit, udf, collect_list
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    MapType,
)

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Initialize SparkManager


class DataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("DataProcessor").getOrCreate()
        # Use SparkManager to get Spark session
        self.last_timestamp = 0
        # Initialize GCS client
        self.storage_client = storage.Client()
        self.bucket_name = "production-trustia-raw-data"
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

    def _round_down_to_interval(self, df, timeframe):
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
        df = self._round_down_to_interval(df, interval_seconds)

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
