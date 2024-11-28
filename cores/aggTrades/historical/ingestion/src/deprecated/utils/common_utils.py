# utils/common_utils.py

import logging
from datetime import datetime, date


def validate_dates(start_date, end_date):
    # Convert to datetime.date if they're datetime objects
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    # Ensure they're date objects
    if not isinstance(start_date, date) or not isinstance(end_date, date):
        raise ValueError("Both start_date and end_date must be date objects")

    # Perform your date validation logic here
    if start_date > end_date:
        raise ValueError("Start date cannot be after end date")

    # You can add more validation as needed

    return start_date, end_date


def check_gcs_object_exists(storage_client, bucket_name, object_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking if GCS object exists: {e}")
        return False
