import logging
import os
from google.cloud import storage
from datetime import datetime, timedelta


class gcsModule:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        logging.info("Initialized GCS client successfully")

    @staticmethod
    def get_bucket(bucket_name):
        storage_client = storage.Client()
        return storage_client.bucket(bucket_name)

    def get_gcs_files(self, prefix):
        try:
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
            files = [blob.name for blob in blobs]
            logging.info(
                f"Retrieved {len(files)} files from bucket '{self.bucket_name}' with prefix '{prefix}'"
            )
            return files
        except Exception as e:
            logging.error(f"Failed to retrieve files from GCS: {e}")
            raise

    def get_cryptos(self, base_prefix):
        try:
            cryptos = set()
            files = self.get_gcs_files(base_prefix)
            for file in files:
                parts = file.split("/")
                if len(parts) > 4:
                    cryptos.add(parts[2])
            logging.info(
                f"Retrieved {len(cryptos)} unique cryptos from bucket '{self.bucket_name}' with base prefix '{base_prefix}'"
            )
            return list(cryptos)
        except Exception as e:
            logging.error(f"Failed to retrieve cryptos: {e}")
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
            logging.info(
                f"Generated {len(gcs_paths)} GCS paths for symbol '{symbol}' from {start_year}-{start_month} to {end_year}-{end_month}"
            )
            return gcs_paths
        except Exception as e:
            logging.error(f"Failed to generate GCS paths: {e}")
            raise

    def list_gcs_success_files(self, prefix):
        try:
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
            success_files = [
                blob.name for blob in blobs if blob.name.endswith("_SUCCESS")
            ]
            return success_files
        except Exception as e:
            logging.error(f"Failed to list GCS success files: {e}")
            raise

    def upload_to_gcs(self, df, symbol, year, month, bucket):
        dt = datetime(year, month, 1)
        filename = f"Raw/Binance/{symbol.replace('/', '')}/aggTrades_historical/{year}/{month:02d}/data.parquet"
        temp_file_path = f"/tmp/{symbol.replace('/', '')}_{year}_{month:02d}.parquet"

        df.to_parquet(temp_file_path, index=False)

        blob = bucket.blob(filename)
        blob.upload_from_filename(temp_file_path)

        return {"path": filename, "size": os.path.getsize(temp_file_path)}

    def check_gcs_object_exists(self, bucket, object_name):
        """Check if a GCS object exists."""
        blob = bucket.blob(object_name)
        return blob.exists()
