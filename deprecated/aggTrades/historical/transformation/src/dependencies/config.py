import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION_NAME = os.getenv("AWS_REGION_NAME")
    BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    BEARER_TOKEN = os.getenv("BEARER_TOKEN")
    SLACK_WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    PROCESSED_FILES_LOG = os.getenv("PROCESSED_FILES_LOG", "processed_files.json")
    SPARK_CONFIG = os.getenv("SPARK_CONFIG", "spark_config")


config = Config()
