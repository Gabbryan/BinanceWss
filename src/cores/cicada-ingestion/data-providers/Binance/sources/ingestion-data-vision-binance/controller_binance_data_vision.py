from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.utils.sys.threading.controller_threading import ThreadController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.commons.syn.server.server import Server
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.third_services.slack.controller_slack import SlackMessageController
    
message = "XXXXX"
slack_controller.send_slack_message(
        ":hourglass_flowing_sand: Daily Data Ingestion Initiated", message, "#36a64f"
    )

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
    def __init__(self, bucket_name, timeframes, request_limit=5, request_interval=1):
        self.timeframes = timeframes
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.session = self._configure_session()
        self.EnvController = EnvController()
        self.logger = LoggingController()
        self.server = Server()
        self.zipcontroller = ZipController()
        self.slack_controller = SlackMessageController(self.EnvController.get_env("WEBHOOK_URL"))
        self.server.urlServer = "https://data.binance.vision/data/" 
        self.GCSController = GCSController(bucket_name=self.EnvController.get_yaml_config('DBV_bucket_name'))
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
        
    def _send_end_message(self, start_time, total_files, total_existing_files, total_size, data_name):
        end_time = time.time()
        elapsed_time = end_time - start_time
        message = (
            f"*{data_name} Processing Completed* :tada:\n"
            f"*Total Time Taken*: {elapsed_time / 60:.2f} minutes\n"
            f"*Total Files Generated*: {total_files}\n"
            f"*Total Existing Files Skipped*: {total_existing_files}\n"
            f"*Total Size of Files*: {total_size / (1024 * 1024 * 1024):.2f} GB\n"
            "Excellent work team! All files have been successfully processed and uploaded."
        )
        self.slack_controller.send_message("Crypto Processing Completed :tada:", message, "#36a64f")
    
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
        else:
            base_urls = [
                "https://data.binance.vision/data/futures/um/daily",
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

        # Initialiser les variables de suivi des fichiers et de la taille
        total_files = 0
        total_existing_files = 0
        total_size = 0

        # Processus séquentiel sans multi-threading
        for year, tasks in tasks_by_year.items():
            try:
                # Appel de la méthode `process_tasks` pour chaque année
                file_info_list, existing_files_count, year_total_size = process_tasks(tasks, data_name, expected_columns)
                
                # Mise à jour des statistiques globales
                total_files += len(file_info_list)
                total_existing_files += existing_files_count
                total_size += year_total_size

                logging.info(f"Year {year} processed successfully.")
            except Exception as e:
                logging.error(f"Error processing data for year {year}: {e}")

        # Envoyer un message de fin avec les statistiques
        send_end_message(
            start_time, total_files, total_existing_files, total_size, data_name
        )

        return total_files, total_existing_files, total_size

        
    def process_tasks(self,tasks, data_name):
        existing_files_count = 0
        total_size = 0
        file_info_list = []

        for task in tqdm(tasks):
            try:
                if data_name == "klines":
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
                    f"{base_type}/{data_name}/{timeframe}/{year}/{month:02d}/{day:02d}/data.parquet"
                )

                # Check if the file already exists in GCS
                if gcs_module.check_gcs_object_exists(gcs_module.bucket, gcs_key):
                    logging.info(
                        f"File {gcs_key} already exists in GCS. Skipping download and upload."
                    )
                    existing_files_count += 1
                    continue  # Skip to the next date

                if data_name == "klines":
                    url = f"{base_url}/{symbol.upper()}/{timeframe}/{symbol.upper()}-{timeframe}-{formatted_date}.zip"
                else:
                    url = f"{base_url}/{data_name}/{symbol.upper()}/{symbol.upper()}-{data_name}-{formatted_date}.zip"

                logging.info(
                    f"Downloading {symbol} {data_name} for {formatted_date} with {timeframe}: {url}"
                )

                df = download_and_extract(url, formatted_date, expected_columns)
                if df is not None:
                    daily_dir_path = f"{year}/{month:02d}/{day:02d}"
                    if not os.path.exists(daily_dir_path):
                        os.makedirs(daily_dir_path)
                    daily_parquet_file_path = os.path.join(daily_dir_path, "data.parquet")
                    df.to_parquet(daily_parquet_file_path)

                    # Verify file creation
                    if os.path.exists(daily_parquet_file_path):
                        logging.info(f"Parquet file created at {daily_parquet_file_path}.")
                        try:
                            file_size = os.path.getsize(daily_parquet_file_path)
                            total_size += file_size
                        except FileNotFoundError:
                            logging.error(
                                f"File {daily_parquet_file_path} not found. Skipping size calculation."
                            )
                            continue
                    else:
                        logging.error(
                            f"Failed to create parquet file at {daily_parquet_file_path}."
                        )
                        continue

                    gcs_module.upload_to_gcs(
                        pd.read_parquet(daily_parquet_file_path),
                        symbol,
                        year,
                        month,
                        gcs_module.bucket,
                        gcs_key,
                    )
                    logging.info(
                        f"Uploaded {symbol} {data_name} for {formatted_date} with {timeframe} directly to GCS at {gcs_key}."
                    )

                    file_info_list.append({"url": gcs_key, "size": file_size})

                time.sleep(REQUEST_INTERVAL / REQUEST_LIMIT)

            except Exception as e:
                logging.error(f"Error processing {symbol} on {formatted_date}: {e}")
                continue  # Skip to the next task

        return file_info_list, existing_files_count, total_size
    
    # def download_and_extract(self, url, date):
    #     """Download and extract a ZIP file from the given URL."""
    #     try:
    #         response = self.session.get(url, stream=True, timeout=60)
    #         response.raise_for_status()
    #         with zipfile.ZipFile(BytesIO(response.content)) as z:
    #             with z.open(z.namelist()[0]) as file:
    #                 df = pd.read_csv(file)
    #         return df
    #     except Exception as e:
    #         self.logger.error(f"Error downloading or extracting {url}: {e}")
    #         return None

    # def process_tasks(self, tasks):
    #     existing_files_count = 0
    #     total_size = 0
    #     file_info_list = []

    #     for task in tqdm(tasks):
    #         base_url, symbol, timeframe, date = task
    #         gcs_key = self._generate_gcs_key(symbol, timeframe, date)
            
    #         if self.gcs_module.check_gcs_object_exists(self.gcs_module.bucket, gcs_key):
    #             self.logger.info(f"File {gcs_key} exists in GCS. Skipping.")
    #             existing_files_count += 1
    #             continue

    #         df = self.download_and_extract(base_url, date)
    #         if df is not None:
    #             local_file_path = self._save_to_parquet(df, date)
    #             if os.path.exists(local_file_path):
    #                 file_size = os.path.getsize(local_file_path)
    #                 total_size += file_size
    #                 self.gcs_module.upload_to_gcs(pd.read_parquet(local_file_path), symbol, date.year, date.month, self.gcs_module.bucket, gcs_key)
    #                 file_info_list.append({"url": gcs_key, "size": file_size})
    #         time.sleep(self.request_interval / self.request_limit)
    #     return file_info_list, existing_files_count, total_size

    # def _generate_gcs_key(self, symbol, timeframe, date):
    #     formatted_date = date.strftime("%Y-%m-%d")
    #     base_type = "futures" if "futures" in symbol else "spot"
    #     gcs_key = f"Raw/Binance-data-vision/historical/{symbol.replace('/', '')}/{base_type}/klines/{timeframe}/{date.year}/{date.month:02d}/{date.day:02d}/data.parquet"
    #     return gcs_key

    # def _save_to_parquet(self, df, date):
    #     local_dir = f"{date.year}/{date.month:02d}/{date.day:02d}"
    #     if not os.path.exists(local_dir):
    #         os.makedirs(local_dir)
    #     local_file_path = os.path.join(local_dir, "data.parquet")
    #     df.to_parquet(local_file_path)
    #     return local_file_path

    # def download_and_process_data(self, start_date=None, end_date=None):
    #     if start_date is None:
    #         start_date = datetime.today() - timedelta(days=1)
    #     if end_date is None:
    #         end_date = datetime.today() - timedelta(days=1)

    #     dates = pd.date_range(start_date, end_date, freq="D")
    #     tasks = list(itertools.product(self.symbols, self.timeframes, dates))
        
    #     with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    #         futures = {executor.submit(self.process_tasks, tasks): "task_batch"}
    #         for future in as_completed(futures):
    #             try:
    #                 file_info_list, existing_files_count, total_size = future.result()
    #                 self.logger.info(f"Processed batch with {len(file_info_list)} new files.")
    #             except Exception as e:
    #                 self.logger.error(f"Error in processing tasks: {e}")

    # def schedule_job(self, job_time="10:00"):
    #     schedule.every().day.at(job_time).do(self.download_and_process_data)
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(60)
    
    # TODO : Keep only aggregate and put upload method in gcs controller
    def aggregate_and_upload(self, df, symbol):
        """
        Aggregate the DataFrame data and upload it to GCS in monthly chunks.

        :param df: The pandas DataFrame containing the data.
        :param symbol: The symbol representing the data.
        """
        if df is not None and not df.empty:
            logger.log_info(f"Starting aggregation and upload for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'StartAggregateUpload'})

            # Ensure 'Date' column is in datetime format
            df["Date"] = pd.to_datetime(df["Date"], format="%d %b %Y", errors="coerce")
            df = df.dropna(subset=["Date"])

            # Get min and max dates
            min_date = df["Date"].min()
            max_date = df["Date"].max()

            # Determine start and end year/month
            start_year = min_date.year
            start_month = min_date.month
            end_year = max_date.year
            end_month = max_date.month

            # Generate paths
            paths = self.generate_gcs_paths(symbol, start_year, end_year, start_month, end_month)

            # Group by year and month
            df["Year"] = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month

            for year, month, gcs_path in paths:
                month_df = df[(df["Year"] == year) & (df["Month"] == month)].drop(columns=["Year", "Month"])
                if not month_df.empty:
                    try:
                        # Generate temp local file
                        temp_local_file = f"/tmp/{symbol.replace('/', '')}_{year}_{month:02d}.parquet"

                        # Save to parquet
                        month_df.to_parquet(temp_local_file, index=False)
                        logger.log_info(f"Saved DataFrame for {symbol} {year}-{month:02d} to {temp_local_file}",
                                        context={'mod': 'GCSController', 'action': 'SaveToParquet'})

                        # Upload to GCS
                        self.gcs_client.upload_file(temp_local_file, gcs_path)
                        logger.log_info(f"Uploaded {temp_local_file} to GCS path {gcs_path}", context={'mod': 'GCSController', 'action': 'UploadToGCS'})

                        # Remove temp file
                        os.remove(temp_local_file)
                        logger.log_info(f"Removed temporary file {temp_local_file}", context={'mod': 'GCSController', 'action': 'RemoveTempFile'})

                    except Exception as e:
                        logger.log_error(f"Error processing {symbol} {year}-{month:02d}: {e}", context={'mod': 'GCSController', 'action': 'ErrorInUpload'})
        else:
            logger.log_warning(f"No data to process for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'NoDataToProcess'})
            return  # No data to process