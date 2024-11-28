import logging
from pyspark.sql import SparkSession
import os
from contextlib import contextmanager
from pyspark.accumulators import AccumulatorParam

class SparkManager:
    def __init__(self, app_name="DataAggregatorSpark", configs=None, cache_session=False):
        self.spark = None
        self.app_name = app_name
        self.configs = configs if configs else {}
        self.cache_session = cache_session

    def _create_spark_session(self):
        builder = SparkSession.builder.appName(self.app_name)
        default_configs = {
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.2",
            "spark.executor.memory": "8g",
            "spark.executor.instances": "4",
            "spark.driver.memory": "4g",
            "spark.driver.host": os.getenv("SPARK_DRIVER_HOST", "127.0.0.1"),
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": os.getenv("SPARK_DRIVER_PORT", "7078"),
            "spark.local.dir": os.getenv("SPARK_LOCAL_DIR", "/tmp/spark-temp"),
            "spark.executor.cores": "3",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            "spark.jars": "/root/spark_jars/hadoop-aws-3.2.2.jar,/root/spark_jars/aws-java-sdk-bundle-1.11.375.jar,/root/spark_jars/guava-31.0-jre.jar,/root/spark_jars/gcs-connector-hadoop3-latest.jar",
            "spark.network.timeout": "600s",
            "spark.sql.shuffle.partitions": "400",
            "spark.default.parallelism": "200",
            "spark.executor.heartbeatInterval": "100s",
            "spark.hadoop.fs.s3a.connection.maximum": "100",
            "spark.hadoop.fs.s3a.connection.timeout": "5000ms",
            "spark.hadoop.fs.s3a.attempts.maximum": "10"
        }
        default_configs.update(self.configs)

        for key, value in default_configs.items():
            if value is not None:
                builder = builder.config(key, value)

        # Set environment variables
        os.environ["SPARK_HOME"] = "/opt/spark"
        os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/envs/myenv/bin/python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/envs/myenv/bin/python"
        os.environ["SPARK_CONF_DIR"] = "/opt/spark/conf"

        return builder.getOrCreate()

    def start_session(self):
        if self.spark is None:
            try:
                self.spark = self._create_spark_session()
                logging.info("Spark session started.")
            except Exception as e:
                logging.error(f"Failed to start Spark session: {e}")
                raise
        else:
            logging.warning("Spark session is already active.")

    def stop_session(self):
        if self.spark is not None and not self.cache_session:
            try:
                self.spark.stop()
                self.spark = None
                logging.info("Spark session stopped.")
            except Exception as e:
                logging.error(f"Failed to stop Spark session: {e}")
                raise
        elif self.spark is None:
            logging.warning("Spark session is not active.")

    @contextmanager
    def session(self):
        self.start_session()
        try:
            yield self.spark
        finally:
            self.stop_session()

    def with_spark(self, func, *args, **kwargs):
        with self.session() as spark:
            try:
                return func(spark, *args, **kwargs)
            except Exception as e:
                logging.error(f"Error running function with Spark: {e}")
                raise

    def get_spark_session(self):
        self.start_session()
        return self.spark

    def repartition_df(self, df, num_partitions):
        try:
            return df.repartition(num_partitions)
        except Exception as e:
            logging.error(f"Error repartitioning DataFrame: {e}")
            raise

    def cache_df(self, df):
        try:
            return df.cache()
        except Exception as e:
            logging.error(f"Error caching DataFrame: {e}")
            raise

    def checkpoint_df(self, df, path, is_eager=True):
        try:
            self.spark.sparkContext.setCheckpointDir(path)
            if is_eager:
                return df.checkpoint(eager=True)
            else:
                return df.checkpoint()
        except Exception as e:
            logging.error(f"Error checkpointing DataFrame: {e}")
            raise

    def broadcast_variable(self, value):
        try:
            return self.spark.sparkContext.broadcast(value)
        except Exception as e:
            logging.error(f"Error broadcasting variable: {e}")
            raise

    def create_accumulator(self, value, accum_type):
        try:
            if accum_type == "int":
                return self.spark.sparkContext.accumulator(value)
            elif accum_type == "float":
                return self.spark.sparkContext.accumulator(value, AccumulatorParam())
            else:
                raise ValueError("Unsupported accumulator type.")
        except Exception as e:
            logging.error(f"Error creating accumulator: {e}")
            raise

    def read_data(self, path, format="parquet", **options):
        try:
            if format == "parquet":
                return self.spark.read.parquet(path, **options)
            elif format == "csv":
                return self.spark.read.csv(path, **options)
            elif format == "json":
                return self.spark.read.json(path, **options)
            else:
                raise ValueError("Unsupported data format.")
        except Exception as e:
            logging.error(f"Error reading data: {e}")
            raise

    def write_data(self, df, path, format="parquet", mode="overwrite", **options):
        try:
            if format == "parquet":
                df.write.mode(mode).parquet(path, **options)
            elif format == "csv":
                df.write.mode(mode).csv(path, **options)
            elif format == "json":
                df.write.mode(mode).json(path, **options)
            else:
                raise ValueError("Unsupported data format.")
        except Exception as e:
            logging.error(f"Error writing data: {e}")
            raise

# Configure logging with a dynamic logging level
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "ERROR").upper(),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
