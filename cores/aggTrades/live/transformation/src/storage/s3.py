import boto3
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.getenv("AWS_REGION_NAME")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")


class s3Module:

    def __init__(self, access_key, secret_key, bucket_name, region_name="eu-north-1"):
        self.last_timestamp = 0
        self.aws_access_key_id = access_key
        self.aws_secret_access_key = secret_key
        self.bucket_name = bucket_name

        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
            )
            logger.info("Initialized S3 client successfully")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def get_s3_files(self, bucket_name, prefix):
        if not isinstance(bucket_name, str) or not isinstance(prefix, str):
            logger.error("Bucket name and prefix must be strings")
            raise TypeError("Bucket name and prefix must be strings")

        try:
            s3 = boto3.client("s3")
            result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = [item["Key"] for item in result.get("Contents", [])]
            logger.info(
                f"Retrieved {len(files)} files from bucket '{bucket_name}' with prefix '{prefix}'"
            )
            return files
        except Exception as e:
            logger.error(f"Failed to retrieve files from S3: {e}")
            raise

    def get_cryptos(self, bucket_name, base_prefix):
        if not isinstance(bucket_name, str) or not isinstance(base_prefix, str):
            logger.error("Bucket name and base prefix must be strings")
            raise TypeError("Bucket name and base prefix must be strings")

        try:
            cryptos = set()
            files = self.get_s3_files(bucket_name, base_prefix)
            for file in files:
                parts = file.split("/")
                if len(parts) > 4:
                    cryptos.add(parts[3])
            logger.info(
                f"Retrieved {len(cryptos)} unique cryptos from bucket '{bucket_name}' with base prefix '{base_prefix}'"
            )
            return list(cryptos)
        except Exception as e:
            logger.error(f"Failed to retrieve cryptos: {e}")
            raise

    def generate_s3_paths(self, symbol, start_year, end_year, start_month, end_month):
        try:
            s3_paths = []
            for year in range(int(start_year), int(end_year) + 1):
                month_start = int(start_month) if year == int(start_year) else 1
                month_end = int(end_month) if year == int(end_year) else 12
                for month in range(month_start, month_end + 1):
                    s3_path = f"s3a://{self.bucket_name}/cicada-data/HistoricalTradeAggregator/binance-futures/{symbol}/{year}/{month:02d}.parquet"
                    s3_paths.append((year, month, s3_path))
            logger.info(
                f"Generated {len(s3_paths)} S3 paths for symbol '{symbol}' from {start_year}-{start_month} to {end_year}-{end_month}"
            )
            return s3_paths
        except Exception as e:
            logger.error(f"Failed to generate S3 paths: {e}")
            raise

    def list_s3_success_DataTransformation_files(self, bucket_name, prefix):
        s3 = boto3.client("s3")

        # Initialiser une liste pour stocker les chemins des fichiers _SUCCESS
        success_files = []

        # Pagination pour gérer les gros volumes de fichiers
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith("_SUCCESS"):
                        success_files.append(obj["Key"])

        return success_files
