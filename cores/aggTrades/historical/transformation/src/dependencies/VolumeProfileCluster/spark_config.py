import logging
from pyspark.sql import SparkSession
import os


def get_spark_session():
    spark = (
        SparkSession.builder.appName("DataAggregatorSpark")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.multipart.size", "104857600"
        )  # 100 MB, adjust as needed
        .config(
            "spark.hadoop.fs.s3a.multipart.threshold", "33554432"
        )  # 32 MB, adjust as needed
        .config("spark.hadoop.fs.s3a.max.connections", "100")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.paging.maximum", "1000")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
        .config("spark.executor.memory", "13g")
        .config("spark.executor.cores", "1")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.cores", "1")
        .config("spark.worker.cores", "1")
        .config("spark.worker.memory", "13g")
        .config("spark.driver.maxResultSize", "4g")
        .getOrCreate()
    )
    return spark


# Configure logging to log only errors
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)
