import glob
import logging
import os
import shutil
import time


def check_disk_space(threshold=10):
    total, used, free = shutil.disk_usage("/")
    free_gb = free // (2**30)
    if free_gb < threshold:
        logging.warning(f"Low disk space: {free_gb} GB remaining. Clearing old files.")
        clear_old_files()


def clear_old_files(retention_days=30):
    current_time = time.time()
    for filepath in glob.glob("/path/to/logs/*"):
        file_creation_time = os.path.getctime(filepath)
        if (current_time - file_creation_time) // (24 * 3600) >= retention_days:
            os.remove(filepath)
            logging.info(f"Deleted old file: {filepath}")


def clear_temp_files(temp_dir="/path/to/temp"):
    for temp_file in glob.glob(f"{temp_dir}/*"):
        try:
            os.remove(temp_file)
            logging.info(f"Deleted temporary file: {temp_file}")
        except Exception as e:
            logging.error(f"Failed to delete temporary file {temp_file}: {e}")
