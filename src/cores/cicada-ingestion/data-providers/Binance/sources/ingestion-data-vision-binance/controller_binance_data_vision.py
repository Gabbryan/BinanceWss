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
        """
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.EnvController = EnvController()
        self.logger = LoggingController("BinanceDataVision")
        self.logger.log_info("Initializing BinanceDataVision", context={'mod': 'BinanceDataVision', 'action': 'Init'})
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
        self.fetch_urls = []
        for symbol in self.symbols:
            if self.stream == "trades":
                self.fetch_urls.append(f"{self.base_url}spot/daily/trades/{symbol}")
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/trades/{symbol}")
            elif self.stream == "aggTrades":
                self.fetch_urls.append(f"{self.base_url}spot/daily/aggTrades/{symbol}")
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/aggTrades/{symbol}")
            elif self.stream == "klines":
                for timeframe in self.timeframes:
                    self.fetch_urls.append(f"{self.base_url}spot/daily/klines/{symbol}/{timeframe}")
                    self.fetch_urls.append(f"{self.base_url}futures/um/daily/klines/{symbol}/{timeframe}")
            elif self.stream == "bookDepth":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "bookTicker":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "indexPriceKlines":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "liquidationSnapshot":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "markPriceKlines":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "metrics":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "premiumIndexKlines":
                self.fetch_urls.append(f"{self.base_url}futures/um/daily/bookDepth/{symbol}")
            elif self.stream == "BVOLIndex":
                bvol_symbol = f"{symbol[:-4]}BVOL{symbol[-4:]}"
                self.fetch_urls.append(f"{self.base_url}option/daily/BVOLIndex/{bvol_symbol}")
            elif self.stream == "EOHSummary":
                self.fetch_urls.append(f"{self.base_url}option/daily/EOHSummary/{symbol}")

        self.logger.log_info(f"URLs defined for fetching: {len(self.fetch_urls)}", context={'mod': 'BinanceDataVision', 'action': 'DefineURLs'})

    def download_and_process_data(self, start_date=None, end_date=None):
        """
        Download and process Binance data between specified start and end dates.
        """
        self.notifications_controller.send_process_start_message()

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

            if self.stream == "klines":
                for symbol in self.symbols:
                    for timeframe in self.timeframes:
                        for fetch_url in self.fetch_urls:
                            if symbol in fetch_url: 
                                url_timeframe = fetch_url.split('/')[-1]
                                if url_timeframe == timeframe: 
                                    tasks_by_year[year].append((fetch_url, symbol, timeframe, date))
            else:
                for symbol in self.symbols:
                    for fetch_url in self.fetch_urls:
                        if self._symbol_in_url(symbol, fetch_url):
                            tasks_by_year[year].append((fetch_url, symbol, date))
   

        self.logger.log_info(f"{sum(len(tasks) for tasks in tasks_by_year.values())} tasks generated.", context={'mod': 'BinanceDataVision', 'action': 'GenerateTasks'})

        for year, tasks in tasks_by_year.items():
            self.process_tasks_with_threads(tasks)
            self.logger.log_info(f"Year {year} processed successfully.", context={'mod': 'BinanceDataVision', 'action': 'ProcessYear'})

        self.notifications_controller.send_process_end_message()

    def process_tasks_with_threads(self, tasks):
        """
        Process tasks using threading to speed up the download and processing.
        """
        for task in tasks:
            self.thread_controller.add_thread(self, "process_task", task)

        self.thread_controller.start_all()
        self.thread_controller.stop_all()

    def process_task(self, task):
        """
        Process an individual task (can be run in a thread).
        """
        if len(task) == 4:
            url, symbol, timeframe, date = task
        elif len(task) == 3:
            url, symbol, date = task
            timeframe = None
        else:
            self.logger.log_error(f"Invalid task format: {task}", context={'mod': 'BinanceDataVision', 'action': 'ProcessTask'})
            return

        formatted_date = date.strftime("%Y-%m-%d")

        self.logger.log_info(f"Processing {symbol} for {formatted_date}.", context={'mod': 'BinanceDataVision', 'action': 'ProcessTask'})
        market = self._get_market(base_url)

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
                self.logger.log_info(f"File {gcs_path} already exists. Skipping download and upload.", context={'mod': 'BinanceDataVision', 'action': 'CheckFileExists'})
                return

        df = self.download_and_extract(url, symbol, date, timeframe)

            for gcs_path in gcs_paths:
                self.GCSController.upload_dataframe_to_gcs(df, gcs_path)
                self.logger.log_info(f"Data uploaded to {gcs_path}", context={'mod': 'BinanceDataVision', 'action': 'UploadData'})

    def download_and_extract(self, base_url, symbol, date, timeframe=None):

        formatted_date = date.strftime("%Y-%m-%d")
        try:
            # Modify the symbol format using _symbol_in_url method if necessary
            formatted_symbol = self._get_formatted_symbol(symbol, base_url)

            # Construct the URL based on the type of data
            if "klines" in base_url and timeframe:
                url = f"{base_url}/{formatted_symbol}-{timeframe}-{formatted_date}.zip"
            elif "BVOLIndex" in base_url:
                url = f"{base_url}/{formatted_symbol}-BVOLIndex-{formatted_date}.zip"
            elif "EOHSummary" in base_url:
                url = f"{base_url}/{formatted_symbol}-EOHSummary-{formatted_date}.zip"
            else:
                url = f"{base_url}/{formatted_symbol}-{self.stream}-{formatted_date}.zip"


            self.logger.log_info(f"Downloading {url}", context={'mod': 'BinanceDataVision', 'action': 'DownloadData'})
            response = requests.get(url, allow_redirects=True)
            response.raise_for_status()
            
            df = self.zip_controller.extract_zip_to_dataframe(response.content)
            return df

        except requests.exceptions.RequestException as e:
            self.logger.log_error(f"Request error for {url}: {e}", context={'mod': 'BinanceDataVision', 'action': 'DownloadError'})
        except ValueError as e:
            self.logger.log_error(f"Data extraction error for {url}: {e}", context={'mod': 'BinanceDataVision', 'action': 'ExtractionError'})
        return None

    def _get_market(self, url):

        """
        Determine the market (futures or spot) based on the base URL.
        """
        if "futures" in url:
            market = "futures"
        elif "spot" in url:
            market = "spot"
        elif "option" in url:
            market = "option"
        else:
            market = "unknown"
        self.logger.log_info(f"Market determined: {market}", context={'mod': 'BinanceDataVision', 'action': 'DetermineMarket'})
        return market

    def _symbol_in_url(self, symbol, url):
        """
        Check if a given symbol or its modified format is in the URL.
        For BVOLIndex, convert BTCUSDT to BTCBVOLUSDT and check.
        """
        if self.stream == "BVOLIndex":
            # Convert symbol to the BVOLIndex format, e.g., BTCUSDT -> BTCBVOLUSDT
            bvol_symbol = f"{symbol[:-4]}BVOL{symbol[-4:]}"
            return bvol_symbol in url
        else:
            return symbol in url
        
    def _get_formatted_symbol(self, symbol, base_url):
        """
        Helper method to format the symbol based on the base URL or stream type.
        """
        if "BVOLIndex" in base_url:
            # Format symbol for BVOLIndex, e.g., BTCUSDT -> BTCBVOLUSDT
            return f"{symbol[:-4]}BVOL{symbol[-4:]}"
        else:
            # Return the symbol as is for other cases
            return symbol