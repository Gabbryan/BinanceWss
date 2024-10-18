import itertools
from datetime import datetime, timedelta

import pandas as pd
import requests

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_controller import NotificationsController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.utils.sys.threading.controller_threading import ThreadController


class BinanceDataVision:
    def __init__(self, request_limit=5, request_interval=1):
        """
        Initialize the BinanceDataVision class with request limits, environment settings, logging,
        and necessary controllers.

        :param request_limit: Number of allowed requests within the limit.
        :param request_interval: Time interval between requests in seconds.
        """
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.EnvController = EnvController()
        self.logger = LoggingController("BinanceDataVision")
        self.zip_controller = ZipController()
        self.base_url = self.EnvController.get_yaml_config('Binance-data-vision', 'base_url')
        self.bucket_name = self.EnvController.get_yaml_config('Binance-data-vision', 'bucket')
        self.GCSController = GCSController(self.bucket_name)
        self.symbols = self.EnvController.get_yaml_config('Binance-data-vision', 'symbols')
        self.stream = self.EnvController.get_env('STREAM')
        self.notifications_controller = NotificationsController(f"Binance Data Vision - {self.stream} ({self.EnvController.env})")
        self.timeframes = self.EnvController.get_yaml_config('Binance-data-vision', 'timeframes')
        self.fetch_urls = []
        self.thread_controller = ThreadController()

    def _define_urls_to_fetch(self):
        """
        Define the URLs to fetch based on the selected stream and available symbols.
        """
        self.fetch_urls = []  # Initialiser une liste vide pour accumuler toutes les URLs
        for symbol in self.symbols:
            if self.stream == "aggTrades" or self.stream == "trades":
                self.fetch_urls.append(f"{self.base_url}spot/daily/aggTrades/{symbol}")
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/aggTrades/{symbol}")
            elif self.stream == "klines":
                for timeframe in self.timeframes:
                    self.fetch_urls.append(f"{self.base_url}futures/um/daily/klines/{symbol}/{timeframe}")
                    self.fetch_urls.append(f"{self.base_url}spot/daily/klines/{symbol}/{timeframe}")
            elif self.stream == "bookDepth":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")


    def download_and_process_data(self, start_date=None, end_date=None):
        """
        Download and process Binance data between specified start and end dates.

        :param start_date: The start date for data download.
        :param end_date: The end date for data download.
        """
        self.notifications_controller.send_process_start_message()

        # Définir les URL à partir des symboles et des timeframes
        self._define_urls_to_fetch()

        if start_date is None:
            start_date = datetime.today() - timedelta(days=1)
        if end_date is None:
            end_date = datetime.today() - timedelta(days=1)

        dates = pd.date_range(start_date, end_date, freq="D")

        tasks_by_year = {}
        for date in dates:
            year = date.year
            if year not in tasks_by_year:
                tasks_by_year[year] = []

            # Créer des tâches spécifiques aux symboles et aux timeframes
            if self.stream == "klines":
                for symbol in self.symbols:
                    for timeframe in self.timeframes:
                        for base_url in self.fetch_urls:
                            if symbol in base_url:  # S'assurer que l'URL contient le bon symbole
                                tasks_by_year[year].append((base_url, symbol, timeframe, date))
            else:
                for symbol in self.symbols:
                    for base_url in self.fetch_urls:
                        if symbol in base_url:  # Vérifier que l'URL correspond bien au symbole
                            tasks_by_year[year].append((base_url, symbol, date))

        self.logger.log_info(f"{sum(len(tasks) for tasks in tasks_by_year.values())} tasks generated.")

        for year, tasks in tasks_by_year.items():
            self.process_tasks_with_threads(tasks)
            self.logger.log_info(f"Year {year} processed successfully.")

        self.notifications_controller.send_process_end_message()


    def process_tasks_with_threads(self, tasks):
        """
        Process tasks using threading to speed up the download and processing.

        :param tasks: A list of tasks to be processed.
        """
        for task in tasks:
            self.thread_controller.add_thread(self, "process_task", task)

        self.thread_controller.start_all()
        self.thread_controller.stop_all()

    def process_task(self, task):
        """
        Process an individual task (can be run in a thread).

        :param task: A single task containing the base URL, symbol, timeframe (optional), and date.
        """
        if len(task) == 4:
            base_url, symbol, timeframe, date = task
        elif len(task) == 3:
            base_url, symbol, date = task
            timeframe = None
        else:
            self.logger.log_error(f"Invalid task format: {task}")
            return

        formatted_date = date.strftime("%Y-%m-%d")
        self.logger.log_info(f"Processing {symbol} for {formatted_date}.")
        market = self._get_market(base_url)

        # Prepare GCS paths before downloading any data, including timeframe if applicable
        params = {
            'symbol': symbol,
            'market': market,
            'year': date.year,
            'month': date.month,
            'day': date.day,
            'stream': self.stream
        }
        if timeframe:
            params['timeframe'] = timeframe
            params['bar'] = 'klines'
            template = "Raw/binance-data-vision/historical/{symbol}/{market}/{stream}/{timeframe}/{year}/{month:02d}/{day:02d}/data.parquet"

        else:
            template = "Raw/binance-data-vision/historical/{symbol}/{market}/{stream}/{year}/{month:02d}/{day:02d}/data.parquet"

        gcs_paths = self.GCSController.generate_gcs_paths(params, template)

        for gcs_path in gcs_paths:
            if self.GCSController.check_file_exists(gcs_path):
                self.logger.log_info(f"File {gcs_path} already exists. Skipping download and upload.")
                return

        # Download and extract data if the file doesn't exist
        df = self.download_and_extract(base_url, symbol, date, timeframe)
        if df is not None:
            for gcs_path in gcs_paths:
                self.GCSController.upload_dataframe_to_gcs(df, gcs_path)

    def download_and_extract(self, base_url, symbol, date, timeframe):
        """
        Download and extract data from a ZIP file.

        :param base_url: The base URL for data download.
        :param symbol: The symbol for which data is being downloaded.
        :param timeframe: The timeframe for klines data (optional).
        :param date: The date for which data is being downloaded.

        :return: A pandas DataFrame containing the extracted data, or None if an error occurs.
        """
        formatted_date = date.strftime("%Y-%m-%d")
        try:
            # Correct URL format to match the expected Binance data format
            if "klines" in base_url and timeframe:
                print(base_url)
                # URL for klines data with the correct format, without repeating symbol/timeframe
                url = f"{base_url}/{symbol.upper()}-{timeframe}-{date.strftime('%Y-%m-%d')}.zip"
            else:
                # URL for other streams (e.g., aggTrades, trades, etc.)
                url = f"{base_url}/{symbol.upper()}/{self.stream}/{symbol.upper()}-{self.stream}-{formatted_date}.zip"

            self.logger.log_info(f"Downloading {url}")
            response = requests.get(url, allow_redirects=True)
            response.raise_for_status()
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

        :param base_url: The base URL for data download.

        :return: A string representing the market type ('futures' or 'spot').
        """
        if "futures" in base_url:
            market = "futures"
        elif "spot" in base_url:
            market = "spot"
        else:
            market = "unknown"
        return market
