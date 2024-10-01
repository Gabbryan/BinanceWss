#from src.libs.utils.sys.scheduler.scheduler_controller import SchedulerController
# from src.commons.logs.logging_controller import LoggingController
# from controller_binance_api import BinanceAPIClient
from src.commons.env_manager.env_controller import EnvController
from src.libs.utils.sys.threading.controller_threading import ThreadController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from controller_binance_data_vision import BinanceDataVision

# TODO: A INTERFACER ??
from datetime import datetime, timedelta
import itertools
import zipfile
from urllib3.util.retry import Retry
import zipfile

# Initialize the controller for the 'development' environment
env_controller = EnvController()

if __name__ == "__main__":
    symbols = env_controller.get_yaml_config('symbols')
    initial_start_date = datetime(2024, 9, 28)
    initial_end_date = datetime.today()
    for symbol in symbols : 
        binance_data_vision = BinanceDataVision(symbol, env_controller.get_yaml_config('timeframes'))
        print(binance_data_vision)