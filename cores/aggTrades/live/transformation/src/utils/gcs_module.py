import os
import logging
from google.cloud import storage
from pyspark.sql import DataFrame
import gcsfs

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class gcsModule:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.storage_client = None
        self.bucket = None

        self._initialize_gcs_client()

    def _initialize_gcs_client(self):
        try:
            # Initialize GCS client with the service account key file
            key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if key_path and os.path.exists(key_path):
                self.storage_client = storage.Client.from_service_account_json(key_path)
            else:
                self.storage_client = storage.Client()
            self.bucket = self.storage_client.get_bucket(self.bucket_name)
            logger.info("Initialized GCS client successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def get_gcs_files(self, prefix):
        if not isinstance(prefix, str):
            logger.error("Prefix must be a string")
            raise TypeError("Prefix must be a string")

        try:
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
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

    def generate_gcs_paths(self, symbol, start_year, end_year, start_month, end_month):
        try:
            gcs_paths = []
            for year in range(int(start_year), int(end_year) + 1):
                month_start = int(start_month) if year == int(start_year) else 1
                month_end = int(end_month) if year == int(end_year) else 12
                for month in range(month_start, month_end + 1):
                    gcs_path = f"gs://{self.bucket_name}/Raw/Binance/{symbol}/aggTrades_historical/{year}/{month:02d}/data.parquet"
                    gcs_paths.append((year, month, gcs_path))
            logger.info(
                f"Generated {len(gcs_paths)} GCS paths for symbol '{symbol}' from {start_year}-{start_month} to {end_year}-{end_month}"
            )
            return gcs_paths
        except Exception as e:
            logger.error(f"Failed to generate GCS paths: {e}")
            raise

    def list_gcs_success_files(self, prefix):
        try:
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
            success_files = [
                blob.name for blob in blobs if blob.name.endswith("_SUCCESS")
            ]
            return success_files
        except Exception as e:
            logger.error(f"Failed to list _SUCCESS files in GCS: {e}")
            raise

    def upload_to_gcs(self, df: DataFrame, symbol, year, start_month, end_month):
        try:
            # Define the GCS path
            filename = f"transformed/Binance/{symbol.replace('/', '')}/aggTrades_historical/{year}_{start_month:02d}_{end_month:02d}/data.parquet"
            gcs_path = f"gs://{self.bucket_name}/{filename}"

            # Write DataFrame directly to GCS
            df.write.mode("overwrite").parquet(gcs_path)

            # Create GCS filesystem interface
            fs = gcsfs.GCSFileSystem(project=self.storage_client.project)

            # Get the size of the uploaded file
            file_info = fs.info(gcs_path)
            file_size = file_info["size"]

            logger.info(f"Uploaded DataFrame to {gcs_path} with size {file_size} bytes")
            return {"path": gcs_path, "size": file_size}
        except Exception as e:
            logger.error(f"Failed to upload DataFrame to GCS: {e}")
            raise
