import json
import os
import tempfile
from datetime import datetime

from .data_check import data_exists
from config import logger


def save_data_to_s3(s3_manager, data, task_name):
    date_str = datetime.now().strftime("%Y-%m-%d")
    object_name = f"{task_name}_{date_str}.json"

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_file_path = os.path.join(tmpdir, object_name)

        files = s3_manager.list_files(object_name)
        if object_name in files:
            s3_manager.download_file(object_name, temp_file_path)
            with open(temp_file_path, "r") as f:
                existing_data = json.load(f)

            if not data_exists(existing_data, data):
                existing_data.append(data)
                with open(temp_file_path, "w") as f:
                    json.dump(existing_data, f, indent=4)
                s3_manager.upload_file(temp_file_path, object_name)
                logger.info(f"Successfully saved data for {task_name} to S3.")
            else:
                logger.info(
                    f"Data for {task_name} already exists. Skipping save to S3."
                )
        else:
            with open(temp_file_path, "w") as f:
                json.dump([data], f, indent=4)
            s3_manager.upload_file(temp_file_path, object_name)
            logger.info(f"Successfully saved data for {task_name} to S3.")
