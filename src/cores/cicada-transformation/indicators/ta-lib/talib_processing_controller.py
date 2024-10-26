import re
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.financial.calculation.ta_lib.talib_controller import TAIndicatorController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.utils.sys.threading.controller_threading import ThreadController
import threading



class TransformationTaLib:

    def __init__(self):
        self.EnvController = EnvController()
        self.logger = LoggingController("Ta-lib controller")
        self.zip_controller = ZipController()
        self.bucket_name = self.EnvController.get_yaml_config('Ta-lib', 'bucket')
        self.bucket_manager = GCSController(self.bucket_name)
        self.custom_indicators = self.EnvController.get_yaml_config('Ta-lib', 'custom_indicators')
        self.thread_controller = ThreadController()

    def _extract_metadata_from_path(self, gcs_path):
        """
        Extract symbol, market, klines, and timeframe from the GCS file path.
        """
        try:
            match = re.search(
                r'historical/(?P<symbol>[^/]+)/(?P<market>[^/]+)/(?P<klines>[^/]+)/(?P<timeframe>[^/]+)/', gcs_path)
            if match:
                return match.group('symbol'), match.group('market'), match.group('timeframe')
        except Exception as e:
            self.logger.log_error(f"Erreur lors de l'extraction des métadonnées à partir du chemin {gcs_path}: {e}")
        return None, None, None


    def extract_metadata_from_file(self, file_path):
        try:
            match = re.search(r'data_(\d{4})_(\d{2})_(\d{2})_(\d{4})_(\d{2})_(\d{2})\.parquet', file_path)
            if match:
                start_year, start_month, start_day, end_year, end_month, end_day = map(int, match.groups())
                end_date = datetime(end_year, end_month, end_day)
                return end_date

            match = re.search(r'(\d{4})/(\d{2})/(\d{2})/data\.parquet', file_path)
            if match:
                year, month, day = map(int, match.groups())
                end_date = datetime(year, month, day)
                return end_date

            self.logger.log_warning(f"Impossible d'extraire la date de fin du fichier {file_path}. Format attendu: data_YYYY_MM_DD_YYYY_MM_DD.parquet ou chemin contenant /YYYY/MM/DD/data.parquet")
            return None
        except Exception as e:
            self.logger.log_error(f"Erreur lors de l'extraction des métadonnées à partir du chemin {file_path}: {e}")
            return None


    def read_parquet_with_pyarrow(self, file_path):
        try:
            table = pq.read_table(file_path)
            df = table.to_pandas()
            df.columns = [
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'count', 'taker_buy_volume',
                'taker_buy_quote_volume', 'ignore'
            ]
            return df
        except Exception as e:
            self.logger.log_error(f"Error reading parquet file with pyarrow: {e}")
            return pd.DataFrame()


    def download_and_aggregate_klines(self, gcs_paths):
        df_combined = pd.DataFrame()

        def process_file(gcs_file):
            try:
                file_path = f"gs://{self.bucket_name}/{gcs_file}"

                df_part = self.read_parquet_with_pyarrow(file_path)

                if df_part.empty:
                    self.logger.log_error(f"Le fichier {gcs_file} est vide ou contient des colonnes incorrectes.")
                    return pd.DataFrame()

                if 'open_time' in df_part.columns:
                    df_part['timestamp'] = pd.to_datetime(df_part['open_time'], unit='ms')
                elif 'timestamp' in df_part.columns:
                    df_part['timestamp'] = pd.to_datetime(df_part['timestamp'], unit='ms')
                else:
                    self.logger.log_error(f"Le fichier {gcs_file} ne contient pas de colonne 'open_time' ou 'timestamp'.")
                    return pd.DataFrame()

                columns_to_keep = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df_part = df_part[[col for col in columns_to_keep if col in df_part.columns]]

                return df_part
            except Exception as e:
                self.logger.log_error(f"Erreur lors du traitement du fichier {gcs_file}: {e}")
                return pd.DataFrame()

        with ThreadPoolExecutor(max_workers=8) as executor:
            dfs = list(executor.map(process_file, gcs_paths))

        if dfs:
            df_combined = pd.concat(dfs, ignore_index=True)

        return df_combined


    def _create_indicator_tasks_by_group(self):
        """
        Create tasks for each (symbol, market, timeframe) group.
        This function groups GCS files by (symbol, market, timeframe) and prepares them for processing.
        """
        root_path = "Raw/binance-data-vision/historical/"
        gcs_files = self.bucket_manager.list_files(root_path, folder_name="klines")

        if not gcs_files:
            self.logger.log_warning("Aucun fichier trouvé dans le bucket GCS.")
            return {}

        tasks_by_group = set()
        for gcs_file in gcs_files:
            symbol, market, timeframe = self._extract_metadata_from_path(gcs_file)
            if symbol and market and timeframe:
                tasks_by_group.add((symbol, market, timeframe))

        return tasks_by_group

    def compute_and_upload_indicators(self):
        self.logger.log_info("Starting the transformation of Ta-lib indicators")

        tasks_by_group = self._create_indicator_tasks_by_group()

        self._process_tasks_with_threads(tasks_by_group)

        self.logger.log_info("All tasks completed successfully.")


    def _process_tasks_with_threads(self, tasks_by_group):
        for task in tasks_by_group:
            self.thread_controller.add_thread(self, "process_group_task", *task)

        self.thread_controller.start_all()

        self.thread_controller.stop_all()


    def process_group_task(self, symbol, market, timeframe):
    
        try:
            thread_name = threading.current_thread().name
            self.logger.log_info(f"Thread {thread_name} started processing for {symbol} / {market} / {timeframe}...")

            root_path = f"Raw/binance-data-vision/historical/{symbol}/{market}/klines/{timeframe}/"
            file_list = self.bucket_manager.list_files(root_path)

            if not file_list:
                self.logger.log_warning(f"File list is empty for {symbol} / {market} / {timeframe}. Skipping this task.")
                return

            output_path_prefix = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/"
            existing_files = self.bucket_manager.list_files(output_path_prefix)

            latest_end_date = None
            df_existing = pd.DataFrame()

            if existing_files:
                self.logger.log_info(f"Existing files found for {symbol} / {market} / {timeframe}.")
                last_file = sorted(existing_files)[-1]
                latest_end_date = self.extract_metadata_from_file(last_file)

                df_existing = self.bucket_manager.load_gcs_file_to_dataframe(last_file, file_format='parquet')

                if latest_end_date and latest_end_date.date() == datetime.now().date():
                    self.logger.log_info(f"File for {symbol} / {market} / {timeframe} is up to date. Checking for missing indicators.")
                    missing_indicators = [ind['name'] for ind in self.custom_indicators if ind['name'] not in df_existing.columns]

                    if not missing_indicators:
                        self.logger.log_info(f"All indicators already present for {symbol}/{market}/{timeframe}.")
                        return
                    else:
                        self.logger.log_info(f"Missing indicators for {symbol}/{market}/{timeframe}: {missing_indicators}")
                        self._compute_and_upload_missing_indicators(symbol, market, timeframe, df_existing, missing_indicators, last_file)
                        return

                elif latest_end_date:
                    self._process_incomplete_data(symbol, market, timeframe, file_list, df_existing, latest_end_date)

            else:
                self._process_complete_data(symbol, market, timeframe, file_list, df_existing, latest_end_date)

            self.logger.log_info(f"Thread {thread_name} finished processing for {symbol} / {market} / {timeframe}.")
        except Exception as e:
            self.logger.log_error(f"Error in thread {thread_name} for {symbol}/{market}/{timeframe}: {e}")


    def _compute_and_upload_missing_indicators(self, symbol, market, timeframe, df_existing, missing_indicators, last_file):
        try:
            ta_indicator_controller = TAIndicatorController(df_existing)
            df_with_missing_indicators = ta_indicator_controller.compute_custom_indicators(
                [{'name': ind_name} for ind_name in missing_indicators]
            )

            df_with_missing_indicators = df_with_missing_indicators.loc[:, ~df_with_missing_indicators.columns.duplicated()]

            self.bucket_manager.upload_dataframe_to_gcs(df_with_missing_indicators, last_file, file_format='parquet')
            self.logger.log_info(f"Uploaded DataFrame with missing indicators for {symbol}/{market}/{timeframe} to GCS.")
        except Exception as e:
            self.logger.log_error(f"Error calculating/uploading missing indicators for {symbol} / {market} / {timeframe}: {e}")


    def _process_incomplete_data(self, symbol, market, timeframe, file_list, df_existing, latest_end_date):
        try:
            start_date = latest_end_date + pd.Timedelta(days=1) if latest_end_date else None

            df_new = self.download_and_aggregate_klines(file_list)
            if df_new.empty:
                self.logger.log_warning(f"No new data available for {symbol}, {market}, {timeframe}.")
                return

            if not df_existing.empty:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            indicators_to_remove = [ind['name'] for ind in self.custom_indicators if ind['name'] in df_combined.columns]
            if indicators_to_remove:
                self.logger.log_info(f"Removing existing indicator columns: {indicators_to_remove}")
                df_combined = df_combined.drop(columns=indicators_to_remove, errors='ignore')

            ta_indicator_controller = TAIndicatorController(df_combined)
            df_with_indicators = ta_indicator_controller.compute_custom_indicators(self.custom_indicators)

            df_with_indicators = df_with_indicators.loc[:, ~df_with_indicators.columns.duplicated()]

            self._upload_new_dataframe(symbol, market, timeframe, df_with_indicators)

        except Exception as e:
            self.logger.log_error(f"Error processing incomplete data for {symbol} / {market} / {timeframe}: {e}")


    def _process_complete_data(self, symbol, market, timeframe, file_list, df_existing, latest_end_date):

        try:
            start_date = latest_end_date + pd.Timedelta(days=1) if latest_end_date else None

            files_to_process = [file for file in file_list if self._extract_metadata_from_path(file)
                                and self._extract_metadata_from_path(file)[0] == symbol  # Verify symbol
                                and self._extract_metadata_from_path(file)[1] == market  # Verify market
                                and self._extract_metadata_from_path(file)[2] == timeframe  # Verify timeframe
                                and (self.extract_metadata_from_file(file) >= start_date if start_date else True)]

            df_new = self.download_and_aggregate_klines(files_to_process)

            if df_new.empty:
                self.logger.log_warning(f"No new data available for {symbol}, {market}, {timeframe}.")
                return

            if not df_existing.empty:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            indicators_to_remove = [ind['name'] for ind in self.custom_indicators if ind['name'] in df_combined.columns]
            if indicators_to_remove:
                self.logger.log_info(f"Removing existing indicator columns: {indicators_to_remove}")
                df_combined = df_combined.drop(columns=indicators_to_remove, errors='ignore')

            ta_indicator_controller = TAIndicatorController(df_combined)
            df_with_indicators = ta_indicator_controller.compute_custom_indicators(self.custom_indicators)

            df_with_indicators = df_with_indicators.loc[:, ~df_with_indicators.columns.duplicated()]

            self._upload_new_dataframe(symbol, market, timeframe, df_with_indicators)

        except Exception as e:
            self.logger.log_error(f"Error processing incomplete data for {symbol} / {market} / {timeframe}: {e}")


    def _upload_new_dataframe(self, symbol, market, timeframe, df_with_indicators):
        try:
            start_date = df_with_indicators['timestamp'].min()
            end_date = df_with_indicators['timestamp'].max()
            output_gcs_path = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/data_{start_date:%Y_%m_%d}_{end_date:%Y_%m_%d}.parquet"

            if self.bucket_manager.check_file_exists(output_gcs_path):
                self.logger.log_info(f"Existing file found: {output_gcs_path}. Deleting it before rewriting.")
                self.bucket_manager.delete_file(output_gcs_path)

            self.bucket_manager.upload_dataframe_to_gcs(df_with_indicators, output_gcs_path, file_format='parquet')
            self.logger.log_info(f"Uploaded DataFrame with complete indicators for {symbol} / {market} / {timeframe} to GCS.")

        except Exception as e:
            self.logger.log_error(f"Error uploading new DataFrame for {symbol} / {market} / {timeframe}: {e}")
