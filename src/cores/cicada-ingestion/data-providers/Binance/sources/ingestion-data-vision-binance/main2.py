#from src.libs.utils.sys.scheduler.scheduler_controller import SchedulerController
# from src.commons.logs.logging_controller import LoggingController
# from controller_binance_api import BinanceAPIClient
from src.commons.env_manager.env_controller import EnvController
from src.libs.utils.sys.threading.controller_threading import ThreadController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from controller_binance_data_vision import BinanceDataVision
from src.commons.syn.server.server import Server

# TODO: A INTERFACER ??
from datetime import datetime, timedelta
import itertools
import zipfile
from urllib3.util.retry import Retry
import zipfile

# Initialize the controller for the 'development' environment
env_controller = EnvController()

if __name__ == "__main__":
    initial_start_date = datetime(2024, 9, 28)
    initial_end_date = datetime.today()
    binance_data_vision = BinanceDataVision()
    total_files, total_existing_files, total_size = BinanceDataVision.download_and_process_data(2024-9-25)
    print(total_files, total_existing_files)