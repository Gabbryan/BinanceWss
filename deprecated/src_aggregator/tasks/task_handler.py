# Wrapper function that executes a task, saves the response, and handles exceptions
import threading

from config import logger, s3_manager
from utils.s3_upload import save_data_to_s3


def task_wrapper(api, method, endpoint, data, task_name):
    try:
        logger.info(f"Executing task: {task_name}")
        response = api.send_request(method, endpoint, data)
        save_data_to_s3(s3_manager, response, task_name)
    except Exception as e:
        logger.error(f"An error occurred during task execution: {e}", exc_info=True)


def run_threaded(job_func, *args):
    """
    Helper function to run a job function in a separate thread
    """
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()
