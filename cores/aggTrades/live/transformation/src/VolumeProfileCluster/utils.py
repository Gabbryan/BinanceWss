import datetime
import os

import pytz
from pyspark.sql import SparkSession


def setup_spark_session():
    return (
        SparkSession.builder.appName("DataAggregatorSpark")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.executor.memory", "20g")
        .config("spark.driver.memory", "20g")
        .config("spark.driver.maxResultSize", "20g")
        .config("spark.executor.memoryOverhead", "12g")
        .config("spark.driver.memoryOverhead", "12g")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config(
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        )
        .config(
            "spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        )
        .getOrCreate()
    )


def initialize_time():
    paris_tz = pytz.timezone("Europe/Paris")
    current_time = datetime.datetime.now(tz=paris_tz)
    print(f"Current Paris time: {current_time}")
    return paris_tz, current_time


def compute_sleep_time(current_time, interval_seconds):
    offset = (current_time.minute % (interval_seconds // 60)) * 60 + current_time.second
    return interval_seconds - offset if offset > 0 else 0
