import os
import sys
import logging
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure the project root directory is in the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Get the path to your Google Cloud service account key file from the environment variable
service_account_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Ensure the service account key file path is valid
if not service_account_key_path or not os.path.exists(service_account_key_path):
    logging.error(f"Service account key file not found: {service_account_key_path}")
    sys.exit(1)

# Initialize the storage client
try:
    storage_client = storage.Client.from_service_account_json(service_account_key_path)
    logging.info(
        f"Initialized Google Cloud Storage client with key file {service_account_key_path}"
    )
except Exception as e:
    logging.error(f"Failed to initialize Google Cloud Storage client: {e}")
    sys.exit(1)

# Define the bucket name
BUCKET_NAME = os.getenv("BUCKET_NAME", "production-trustia-raw-data")

# Access the bucket and list its contents
try:
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    if blobs:
        logging.info(f"Bucket '{BUCKET_NAME}' is accessible. Listing contents:")
        for blob in blobs:
            logging.info(f"- {blob.name}")
    else:
        logging.info(f"Bucket '{BUCKET_NAME}' is accessible but empty.")
except Exception as e:
    logging.error(f"Error accessing bucket '{BUCKET_NAME}': {e}")


# Configuration class to hold the environment variables
class Config:
    # Google Cloud Credentials
    GOOGLE_APPLICATION_CREDENTIALS = service_account_key_path

    # Spark Configuration
    SPARK_HOME = os.getenv("SPARK_HOME")
    PYSPARK_PYTHON = os.getenv("PYSPARK_PYTHON")
    PYSPARK_DRIVER_PYTHON = os.getenv("PYSPARK_DRIVER_PYTHON")
    SPARK_CONF_DIR = os.getenv("SPARK_CONF_DIR")
    SPARK_MASTER = os.getenv("SPARK_MASTER")
    SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
    SPARK_DRIVER_PORT = os.getenv("SPARK_DRIVER_PORT")
    SPARK_LOCAL_DIR = os.getenv("SPARK_LOCAL_DIR")
    SPARK_DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS")

    # Slack Configuration
    SLACK_WEBHOOK_URL = os.getenv("WEBHOOK_URL")

    # Other Configurations
    PROCESSED_FILES_LOG = os.getenv("PROCESSED_FILES_LOG", "processed_files.json")
    SPARK_CONFIG = os.getenv("SPARK_CONFIG", "spark_config")
    BEARER_TOKEN = os.getenv("BEARER_TOKEN")


# Create a config instance
config = Config()
