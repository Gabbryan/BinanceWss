import os
from dotenv import load_dotenv
import boto3

load_dotenv()

# AWS Configuration
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.getenv("AWS_REGION_NAME")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# S3 Client
s3_client = boto3.client(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)
