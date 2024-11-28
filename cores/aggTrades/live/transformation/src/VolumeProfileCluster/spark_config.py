import os
from pyspark.sql import SparkSession


def get_spark_session(app_name="DataAggregatorSpark"):
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.executor.memory", "45g")  # 75% of instance memory
        .config("spark.driver.memory", "8g")  # Allocate memory for the driver
        .config("spark.executor.memoryOverhead", "10g")  # Allocate memory overhead
        .config("spark.sql.shuffle.partitions", "200")  # Number of shuffle partitions
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config(
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .config("fs.s3a.committer.name", "magic")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        )
        .config(
            "spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        )
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "2")
        .config(
            "spark.dynamicAllocation.maxExecutors", "50"
        )  # Adjust based on your cluster
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.speculation", "true")
        .config(
            "spark.sql.autoBroadcastJoinThreshold", "-1"
        )  # Disable auto-broadcast join
        .config("spark.driver.maxResultSize", "8g")  # Set driver max result size
        .config("spark.executor.instances", "10")  # Set number of executor instances
        .config("spark.executor.cores", "4")  # Set number of cores per executor
        .getOrCreate()
    )
