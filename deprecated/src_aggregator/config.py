# Basic configuration for logging
import logging
import os
from logging.handlers import RotatingFileHandler
from services.S3BucketManager import S3BucketManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("logs/app.log", maxBytes=5000000, backupCount=5),
    ],
)

logger = logging.getLogger(__name__)
# Access keys are obtained from environment variables for security.

# Fallback to hardcoded values is not recommended.
access_key_id = os.getenv("ACCESSKEYID")
secret_access_key = os.getenv("SECRETACCESSKEY")
region_name = "eu-north-1"
bucket_name = "xtrus31"

# Instantiate an object to interact with S3, encapsulating the credentials and bucket info
s3_manager = S3BucketManager(access_key_id, secret_access_key, region_name, bucket_name)
