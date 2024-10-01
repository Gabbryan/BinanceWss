from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.utils.sys.threading.controller_threading import ThreadController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.commons.syn.server.server import Server
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.third_services.slack.controller_slack import SlackMessageController

# TODO : INTERFACER ?
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
from io import BytesIO
from datetime import datetime, timedelta
from tqdm import tqdm 
import pandas as pd
from time import time
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed


class BinanceDataVision:
    def __init__(self, request_limit=5, request_interval=1):
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.session = self._configure_session()
        self.EnvController = EnvController()
        self.logger = LoggingController()
        self.server = Server()
        self.zipcontroller = ZipController()
        self.slack_controller = SlackMessageController(self.EnvController.get_env("WEBHOOK_URL"))
        self.server.urlServer = "https://data.binance.vision/data/" 
        self.bucket_name = self.EnvController.get_yaml_config('buckets', 'binance_data_vision', default_value='2')
        print(self.bucket_name)
        self.GCSController = GCSController(self.bucket_name)
        self.symbols = self.EnvController.get_yaml_config('symbols')
        self.stream = self.EnvController.get_yaml_config('stream')
        self.timeframes = self.EnvController.get_yaml_config('timeframes')
    
    def _configure_session(self):
        # Configure session with retries and increased connection pool size
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        session.mount("https://", adapter)
        return session
    
    def _send_start_message(self, symbols, start_date, end_date, data_name):
        message = (
            f"*{data_name.capitalize()} Processing Started* :rocket:\n"
            f"*Number of cryptocurrencies*: {len(symbols)}\n"
            f"*Date Range*: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n"
            f"*Cryptocurrencies being processed*:\n"
        )
        for symbol in symbols:
            message += f"- {symbol}\n"
        message += "Processing has begun. Stay tuned for updates and completion details."
        self.slack_controller.send_message("Crypto Processing Started :rocket:", message, "#36a64f")
        
    def _send_end_message(self, start_time, stream):
        end_time = time.time()
        elapsed_time = end_time - start_time
        message = (
            f"*{data_name} Processing Completed* :tada:\n"
            f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
            "Excellent work team! All files have been successfully processed and uploaded."
        )
        self.slack_controller.send_message("Crypto Processing Completed :tada:", message, "#36a64f")
        
    def download_and_process_data(self, start_date=None, end_date=None):
        symbols = self.symbols
        stream = self.stream
        
        # Définir les URL de base selon le stream sélectionné
        if stream == "aggTrades" or stream == "trades":
            base_urls = [
                "https://data.binance.vision/data/spot/daily/aggTrades",
                "https://data.binance.vision/data/futures/um/daily/aggTrades",
            ]
        elif stream == "klines":
            base_urls = [
                "https://data.binance.vision/data/futures/um/daily/klines",
                "https://data.binance.vision/data/spot/um/daily/klines",
            ]
        elif stream == "bookDepth":
            base_urls = [
                "https://data.binance.vision/?prefix=data/futures/um/daily/bookDepth/",
            ]

        # Définir les dates par défaut si elles ne sont pas spécifiées
        if start_date is None:
            start_date = datetime.today() - timedelta(days=1)
        if end_date is None:
            end_date = datetime.today() - timedelta(days=1)

        # Générer une liste des dates à traiter
        dates = pd.date_range(start_date, end_date, freq="D")
        self._send_start_message(symbols, start_date, end_date, stream)
        start_time = time.time()  # Démarrer le chrono

        # Organiser les tâches par année
        tasks_by_year = {}
        for date in dates:
            year = date.year
            if year not in tasks_by_year:
                tasks_by_year[year] = []
            if stream == "klines":
                tasks_by_year[year].extend(
                    itertools.product(base_urls, symbols, self.timeframes, [date])
                )
            else:
                tasks_by_year[year].extend(itertools.product(base_urls, symbols, [date]))

        self.logger.info(
            f"{sum(len(tasks) for tasks in tasks_by_year.values())} tasks have been generated."
        )

        # Processus séquentiel sans multi-threading
        for year, tasks in tasks_by_year.items():
            try:
                self.logger.info(f"Year {year} processed successfully.")
            except Exception as e:
                self.logger.error(f"Error processing data for year {year}: {e}")

        # Envoyer un message de fin avec les statistiques
        self._send_end_message(
            start_time, stream
        )

        return

    def process_tasks(self, tasks, stream):
        stream = self.stream
        bucket_name = self.bucket_name
        existing_files_count = 0
        total_size = 0
        file_info_list = []

        for task in tasks:
            if stream == "klines":
                base_url, symbol, timeframe, date = task
            else:
                base_url, symbol, date = task
                timeframe = ""  # No timeframe needed for non-klines data

            formatted_date = date.strftime("%Y-%m-%d")
            year = date.year
            month = date.month
            day = date.day

            base_type = "futures" if "futures" in base_url else "spot"
            gcs_key = (
                f"Raw/Binance-data-vision/historical/{symbol.replace('/', '')}/"
                f"{base_type}/{stream}/{timeframe}/{year}/{month:02d}/{day:02d}/data.parquet"
            )

            # Check if the file already exists in GCS
            if self.GCSController.check_gcs_object_exists(bucket_name, gcs_key):
                self.logger.info(
                    f"File {gcs_key} already exists in GCS. Skipping download and upload."
                )
                existing_files_count += 1
                continue  # Skip to the next date

            if stream == "klines":
                url = f"{base_url}/{symbol.upper()}/{timeframe}/{symbol.upper()}-{timeframe}-{formatted_date}.zip"
            else:
                url = f"{base_url}/{stream}/{symbol.upper()}/{symbol.upper()}-{stream}-{formatted_date}.zip"

            self.logger.info(
                f"Downloading {symbol} {stream} for {formatted_date} with {timeframe}: {url}"
            )

            df = self.download_and_extract(url, formatted_date)
            if df is not None:
                self.GCScontroller.upload_dataframe_to_gcs(df, gcs_key)
                    
        return
    
    def download_and_extract(self, url, date):
        """
        Download and extract a ZIP file from the given URL.
        Returns a DataFrame containing the data.
        """
        try:
            response = self.server.get("spot/daily/aggTrades/1000SATSFDUSD/1000SATSFDUSD-aggTrades-2024-09-30.zip", withAuth=False, allow_redirects=True)
            response.raise_for_status()  # Vérifier que la requête a réussi

            # Utilisation de ZipController pour extraire les données du fichier ZIP
            df = ZipController.extract_zip_to_dataframe(response.content)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erreur lors du téléchargement de {url}: {e}")
            return None

        except ValueError as e:
            self.logger.error(f"Erreur dans les données : {e}")
            return None

        except Exception as e:
            self.logger.error(f"Erreur inattendue : {e}")
            return None
    
    # TODO : Keep only aggregate and put upload method in gcs controller
    # def aggregate_and_upload(self, df, symbol):
    #     """
    #     Aggregate the DataFrame data and upload it to GCS in monthly chunks.

    #     :param df: The pandas DataFrame containing the data.
    #     :param symbol: The symbol representing the data.
    #     """
    #     if df is not None and not df.empty:
    #         logger.log_info(f"Starting aggregation and upload for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'StartAggregateUpload'})

    #         # Ensure 'Date' column is in datetime format
    #         df["Date"] = pd.to_datetime(df["Date"], format="%d %b %Y", errors="coerce")
    #         df = df.dropna(subset=["Date"])

    #         # Get min and max dates
    #         min_date = df["Date"].min()
    #         max_date = df["Date"].max()

    #         # Determine start and end year/month
    #         start_year = min_date.year
    #         start_month = min_date.month
    #         end_year = max_date.year
    #         end_month = max_date.month

    #         # Generate paths
    #         paths = self.generate_gcs_paths(symbol, start_year, end_year, start_month, end_month)

    #         # Group by year and month
    #         df["Year"] = df["Date"].dt.year
    #         df["Month"] = df["Date"].dt.month

    #         for year, month, gcs_path in paths:
    #             month_df = df[(df["Year"] == year) & (df["Month"] == month)].drop(columns=["Year", "Month"])
    #             if not month_df.empty:
    #                 try:
    #                     # Generate temp local file
    #                     temp_local_file = f"/tmp/{symbol.replace('/', '')}_{year}_{month:02d}.parquet"

    #                     # Save to parquet
    #                     month_df.to_parquet(temp_local_file, index=False)
    #                     logger.log_info(f"Saved DataFrame for {symbol} {year}-{month:02d} to {temp_local_file}",
    #                                     context={'mod': 'GCSController', 'action': 'SaveToParquet'})

    #                     # Upload to GCS
    #                     self.gcs_client.upload_file(temp_local_file, gcs_path)
    #                     logger.log_info(f"Uploaded {temp_local_file} to GCS path {gcs_path}", context={'mod': 'GCSController', 'action': 'UploadToGCS'})

    #                     # Remove temp file
    #                     os.remove(temp_local_file)
    #                     logger.log_info(f"Removed temporary file {temp_local_file}", context={'mod': 'GCSController', 'action': 'RemoveTempFile'})

    #                 except Exception as e:
    #                     logger.log_error(f"Error processing {symbol} {year}-{month:02d}: {e}", context={'mod': 'GCSController', 'action': 'ErrorInUpload'})
    #     else:
    #         logger.log_warning(f"No data to process for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'NoDataToProcess'})
    #         return  # No data to process