import itertools
from datetime import datetime, timedelta
from time import time

import pandas as pd
import requests

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_slack import NotificationsSlackController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.utils.sys.threading.controller_threading import ThreadController


class BinanceDataVision:
    def __init__(self, request_limit=5, request_interval=1):
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.EnvController = EnvController()
        self.logger = LoggingController()
        self.zip_controller = ZipController()
        self.base_url = self.EnvController.get_yaml_config('Binance-data-vision', 'base_url')
        self.bucket_name = self.EnvController.get_yaml_config('Binance-data-vision', 'bucket')
        self.GCSController = GCSController(self.bucket_name)
        self.symbols = self.EnvController.get_yaml_config('Binance-data-vision', 'symbols')
        self.stream = self.EnvController.get_yaml_config('Binance-data-vision',
                                                         'stream') if self.EnvController.env == 'development' else self.EnvController.get_env(
            'STREAM')
        self.notifications_slack_controller = NotificationsSlackController(f"Binance Data Vision - {self.stream}")
        self.timeframes = self.EnvController.get_yaml_config('Binance-data-vision', 'timeframes')
        self.fetch_urls = []
        self.thread_controller = ThreadController()

    def _define_urls_to_fetch(self):
        """
        Define URLs to fetch based on the stream and symbols.
        """
        for symbol in self.symbols:
            if self.stream == "aggTrades" or self.stream == "trades":
                self.fetch_urls = [
                    f"{self.base_url}spot/daily/aggTrades/{symbol}",
                    f"{self.base_url}futures/um/daily/aggTrades/{symbol}",
                ]
            elif self.stream == "klines":
                self.fetch_urls = [
                    f"{self.base_url}futures/um/daily/klines/{symbol}",
                    f"{self.base_url}spot/um/daily/klines/{symbol}",
                ]
            elif self.stream == "bookDepth":
                self.fetch_urls = [
                    f"{self.base_url}futures/um/daily/bookDepth/{symbol}",
                ]

    def download_and_process_data(self, start_date=None, end_date=None):
        """
        Download and process Binance data between the specified start and end dates.
        """
        symbols = self.symbols
        stream = self.stream

        # Define URLs based on the selected stream
        self._define_urls_to_fetch()

        # Default to yesterday if dates are not provided
        if start_date is None:
            start_date = datetime.today() - timedelta(days=1)
        if end_date is None:
            end_date = datetime.today() - timedelta(days=1)

        # Generate a list of dates to process
        dates = pd.date_range(start_date, end_date, freq="D")
        self.notifications_slack_controller.send_process_start_message()
        start_time = time()  # Start timer

        # Organize tasks by year
        tasks_by_year = {}
        for date in dates:
            year = date.year
            if year not in tasks_by_year:
                tasks_by_year[year] = []
            if stream == "klines":
                tasks_by_year[year].extend(
                    itertools.product(self.fetch_urls, symbols, self.timeframes, [date])
                )
            else:
                tasks_by_year[year].extend(itertools.product(self.fetch_urls, symbols, [date]))

        self.logger.log_info(f"{sum(len(tasks) for tasks in tasks_by_year.values())} tasks generated.")

        # Process tasks using threads
        for year, tasks in tasks_by_year.items():
            self.process_tasks_with_threads(tasks)
            self.logger.log_info(f"Year {year} processed successfully.")

        self.notifications_slack_controller.send_process_end_message()

    def process_tasks_with_threads(self, tasks):
        """
        Process tasks using threading to speed up the process.
        """
        for task in tasks:
            # Add tasks as threads to the ThreadController
            self.thread_controller.add_thread(self, "process_task", task)

        # Start all threads
        self.thread_controller.start_all()

        # Join threads to ensure proper completion
        self.thread_controller.stop_all()

    def process_task(self, task):
        """
        Process an individual task (can be run in a thread).
        """
        if len(task) == 4:
            base_url, symbol, timeframe, date = task
        elif len(task) == 3:
            base_url, symbol, date = task
            timeframe = None  # No timeframe for this task
        else:
            self.logger.log_error(f"Invalid task format: {task}")
            return

        formatted_date = date.strftime("%Y-%m-%d")
        self.logger.log_info(f"Processing {symbol} for {formatted_date}.")
        market = self._get_market(base_url)

        # Prepare GCS paths before downloading any data
        params = {'symbol': symbol, 'market': market, 'year': date.year, 'month': date.month, 'day': date.day}
        template = "Raw/binance-data-vision/historical/{symbol}/{market}/{year}/{month:02d}/{day:02d}/data.parquet"
        gcs_paths = self.GCSController.generate_gcs_paths(params, template)

        # Check if the file already exists in GCS for all paths
        for gcs_path in gcs_paths:
            if self.GCSController.check_file_exists(gcs_path):
                self.logger.log_info(f"File {gcs_path} already exists. Skipping download and upload.")
                return  # Skip download and upload if the file exists

        # Download and extract data if the file doesn't exist
        df = self.download_and_extract(base_url, symbol, timeframe, date)
        if df is not None:
            # Upload the dataframe to GCS
            for gcs_path in gcs_paths:
                self.GCSController.upload_dataframe_to_gcs(df, gcs_path)

    def download_and_extract(self, base_url, symbol, timeframe, date):
        """
        Download and extract data from a ZIP file.
        """
        formatted_date = date.strftime("%Y-%d-%m")
        try:
            # Construct the URL
            if timeframe:
                url = f"{base_url}/{symbol.upper()}-{self.stream}-{timeframe}-{formatted_date}.zip"
            else:
                url = f"{base_url}/{symbol.upper()}-{self.stream}-{formatted_date}.zip"

            # Log the download process
            self.logger.log_info(f"Downloading {url}")
            response = requests.get(url, allow_redirects=True)
            response.raise_for_status()  # Raise an error for failed requests

            # Extract data from the ZIP file
            df = self.zip_controller.extract_zip_to_dataframe(response.content)
            return df

        except requests.exceptions.RequestException as e:
            self.logger.log_error(f"Request error for {url}: {e}")
        except ValueError as e:
            self.logger.log_error(f"Data extraction error for {url}: {e}")
        return None

    def _get_market(self, base_url):
        """
        Determine the market (futures or spot) based on the base URL.
        """
        if "futures" in base_url:
            market = "futures"
        elif "spot" in base_url:
            market = "spot"
        else:
            market = "unknown"
        return market
