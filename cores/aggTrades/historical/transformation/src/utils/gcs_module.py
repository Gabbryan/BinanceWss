import calendar
import logging
import os

from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from pyspark.sql import DataFrame, SparkSession

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("DataProcessor").getOrCreate()
spark.conf.set("spark.hadoop.fs.gs.buffer.size", "67108864")  # 64MB buffer size for GCS
spark.conf.set(
    "spark.hadoop.fs.gs.implementation",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
)
spark.conf.set(
    "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
)


class gcsModule:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.storage_client = None
        self.bucket = None
        self._initialize_gcs_client()

    def _initialize_gcs_client(self):
        try:
            key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if key_path and os.path.exists(key_path):
                self.storage_client = storage.Client.from_service_account_json(key_path)
            else:
                self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(self.bucket_name)
            logger.info("Initialized GCS client successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def _retry_with_backoff(self, func, *args, **kwargs):
        max_retries = 5
        backoff = 1
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except GoogleAPIError as e:
                logger.error(f"Google API error: {e}")
                raise
        logger.error(f"Failed after {max_retries} retries.")
        raise Exception("Max retries exceeded")

    def get_gcs_files(self, prefix):
        if not isinstance(prefix, str):
            logger.error("Prefix must be a string")
            raise TypeError("Prefix must be a string")

        try:
            blobs = self._retry_with_backoff(
                self.storage_client.list_blobs, self.bucket_name, prefix=prefix
            )
            files = [blob.name for blob in blobs]
            logger.info(
                f"Retrieved {len(files)} files from bucket '{self.bucket_name}' with prefix '{prefix}'"
            )
            return files
        except Exception as e:
            logger.error(f"Failed to retrieve files from GCS: {e}")
            raise

    def get_cryptos(self, base_prefix):
        if not isinstance(base_prefix, str):
            logger.error("Base prefix must be a string")
            raise TypeError("Base prefix must be a string")

        try:
            cryptos = set()
            files = self.get_gcs_files(base_prefix)
            for file in files:
                parts = file.split("/")
                if len(parts) > 4:
                    cryptos.add(parts[3])
            logger.info(
                f"Retrieved {len(cryptos)} unique cryptos from bucket '{self.bucket_name}' with base prefix '{base_prefix}'"
            )
            return list(cryptos)
        except Exception as e:
            logger.error(f"Failed to retrieve cryptos: {e}")
            raise


    def generate_gcs_paths(
        self,
        symbol,
        start_year,
        end_year,
        start_month,
        end_month,
        start_day=1,
        end_day=31,
    ):
        try:
            gcs_paths = []
            for year in range(int(start_year), int(end_year) + 1):
                month_start = int(start_month) if year == int(start_year) else 1
                month_end = int(end_month) if year == int(end_year) else 12
                for month in range(month_start, month_end + 1):
                    day_start = (
                        int(start_day)
                        if year == int(start_year) and month == int(start_month)
                        else 1
                    )
                    day_end = (
                        int(end_day)
                        if year == int(end_year) and month == int(end_month)
                        else 31
                    )
                    for day in range(day_start, day_end + 1):
                        gcs_path = f"gs://{self.bucket_name}/Raw/binance-data-vision/historical/{symbol}/futures/aggTrades/{year}/{month:02d}/{day:02d}/data.parquet"
                        gcs_paths.append((year, month, day, gcs_path))
            logger.info(
                f"Generated {len(gcs_paths)} GCS paths for symbol '{symbol}' from {start_year}-{start_month}-{start_day} to {end_year}-{end_month}-{end_day}"
            )
            return gcs_paths
        except Exception as e:
            logger.error(f"Failed to generate GCS paths: {e}")
            raise

    def list_gcs_success_files(self, prefix):
        try:
            blobs = self._retry_with_backoff(
                self.storage_client.list_blobs, self.bucket_name, prefix=prefix
            )
            success_files = [
                blob.name for blob in blobs if blob.name.endswith("_SUCCESS")
            ]
            return success_files
        except Exception as e:
            logger.error(f"Failed to list _SUCCESS files in GCS: {e}")
            raise
