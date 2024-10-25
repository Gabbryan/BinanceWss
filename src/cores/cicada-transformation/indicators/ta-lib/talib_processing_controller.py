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
            self.logger.log_error(f"Error extracting metadata from path {gcs_path}: {e}", context={'mod': 'TransformationTaLib', 'action': 'ExtractMetadata', 'path': gcs_path})
        return None, None, None

    def extract_metadata_from_file(self, file_path):
        """
        Extracts the end date from the file path.
        """
        try:
            match = re.search(r'data_(\d{4})_(\d{2})_(\d{2})_(\d{4})_(\d{2})_(\d{2})\.parquet', file_path)
            if match:
                end_date = datetime(*map(int, match.groups()[3:]))
                return end_date

            match = re.search(r'(\d{4})/(\d{2})/(\d{2})/data\.parquet', file_path)
            if match:
                end_date = datetime(*map(int, match.groups()))
                return end_date

            self.logger.log_warning(f"Unable to extract end date from file {file_path}.", context={'mod': 'TransformationTaLib', 'action': 'ExtractEndDate', 'path': file_path})
            return None
        except Exception as e:
            self.logger.log_error(f"Error extracting metadata from file path {file_path}: {e}", context={'mod': 'TransformationTaLib', 'action': 'ExtractMetadataError', 'path': file_path})
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
            self.logger.log_error(f"Error reading parquet file {file_path}: {e}", context={'mod': 'TransformationTaLib', 'action': 'ReadParquet', 'path': file_path})
            return pd.DataFrame()

    def download_and_aggregate_klines(self, gcs_paths):
        df_combined = pd.DataFrame()

        def process_file(gcs_file):
            try:
                file_path = f"gs://{self.bucket_name}/{gcs_file}"
                df_part = self.read_parquet_with_pyarrow(file_path)
                if df_part.empty:
                    self.logger.log_error(f"File {gcs_file} is empty or has incorrect columns.", context={'mod': 'TransformationTaLib', 'action': 'ProcessFileError', 'file': gcs_file})
                    return pd.DataFrame()

                if 'open_time' in df_part.columns:
                    df_part['timestamp'] = pd.to_datetime(df_part['open_time'], unit='ms')
                else:
                    self.logger.log_error(f"File {gcs_file} missing 'open_time' or 'timestamp' column.", context={'mod': 'TransformationTaLib', 'action': 'MissingTimestamp', 'file': gcs_file})
                    return pd.DataFrame()

                columns_to_keep = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                return df_part[[col for col in columns_to_keep if col in df_part.columns]]
            except Exception as e:
                self.logger.log_error(f"Error processing file {gcs_file}: {e}", context={'mod': 'TransformationTaLib', 'action': 'ProcessFile', 'file': gcs_file})
                return pd.DataFrame()

        with ThreadPoolExecutor(max_workers=8) as executor:
            dfs = list(executor.map(process_file, gcs_paths))

        if dfs:
            df_combined = pd.concat(dfs, ignore_index=True)

        return df_combined

    def _create_indicator_tasks_by_group(self):
        root_path = "Raw/binance-data-vision/historical/"
        gcs_files = self.bucket_manager.list_files(root_path, folder_name="klines")

        if not gcs_files:
            self.logger.log_warning("No files found in GCS bucket.", context={'mod': 'TransformationTaLib', 'action': 'CreateTasks'})
            return {}

        tasks_by_group = set()
        for gcs_file in gcs_files:
            symbol, market, timeframe = self._extract_metadata_from_path(gcs_file)
            if symbol and market and timeframe:
                tasks_by_group.add((symbol, market, timeframe))

        return tasks_by_group

    def compute_and_upload_indicators(self):
        self.logger.log_info("Starting Ta-lib indicators transformation", context={'mod': 'TransformationTaLib', 'action': 'StartTransformation'})

        tasks_by_group = self._create_indicator_tasks_by_group()
        self._process_tasks_with_threads(tasks_by_group)

        self.logger.log_info("All tasks completed successfully.", context={'mod': 'TransformationTaLib', 'action': 'CompleteTransformation'})

    def _process_tasks_with_threads(self, tasks_by_group):
        for task in tasks_by_group:
            self.thread_controller.add_thread(self, "process_group_task", *task)

        self.thread_controller.start_all()
        self.thread_controller.stop_all()

    def process_group_task(self, symbol, market, timeframe):
        try:
            thread_name = threading.current_thread().name
            self.logger.log_info(f"Thread {thread_name} processing {symbol}/{market}/{timeframe}.", context={'mod': 'TransformationTaLib', 'action': 'ProcessTask', 'symbol': symbol, 'market': market, 'timeframe': timeframe})

            root_path = f"Raw/binance-data-vision/historical/{symbol}/{market}/klines/{timeframe}/"
            file_list = self.bucket_manager.list_files(root_path)
            if not file_list:
                self.logger.log_warning(f"No files for {symbol}/{market}/{timeframe}.", context={'mod': 'TransformationTaLib', 'action': 'EmptyFileList', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
                return

            output_path_prefix = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/"
            existing_files = self.bucket_manager.list_files(output_path_prefix)

            latest_end_date = None
            df_existing = pd.DataFrame()

            if existing_files:
                self.logger.log_info(f"Found existing files for {symbol}/{market}/{timeframe}.", context={'mod': 'TransformationTaLib', 'action': 'ExistingFilesFound', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
                last_file = sorted(existing_files)[-1]
                latest_end_date = self.extract_metadata_from_file(last_file)
                df_existing = self.bucket_manager.load_gcs_file_to_dataframe(self.bucket_name, last_file, file_format='parquet')

            if latest_end_date and latest_end_date.date() == datetime.now().date():
                missing_indicators = [ind['name'] for ind in self.custom_indicators if ind['name'] not in df_existing.columns]
                if missing_indicators:
                    self._compute_and_upload_missing_indicators(symbol, market, timeframe, df_existing, missing_indicators, last_file)
                return

            self._process_incomplete_data(symbol, market, timeframe, file_list, df_existing, latest_end_date)

            self.logger.log_info(f"Thread {thread_name} completed for {symbol}/{market}/{timeframe}.", context={'mod': 'TransformationTaLib', 'action': 'ThreadComplete', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
        except Exception as e:
            self.logger.log_error(f"Error in thread {thread_name} for {symbol}/{market}/{timeframe}: {e}", context={'mod': 'TransformationTaLib', 'action': 'ThreadError', 'symbol': symbol, 'market': market, 'timeframe': timeframe})

    def _compute_and_upload_missing_indicators(self, symbol, market, timeframe, df_existing, missing_indicators, last_file):
        try:
            ta_indicator_controller = TAIndicatorController(df_existing)
            df_with_missing_indicators = ta_indicator_controller.compute_custom_indicators(
                [{'name': ind_name} for ind_name in missing_indicators]
            )
            df_with_missing_indicators = df_with_missing_indicators.loc[:, ~df_with_missing_indicators.columns.duplicated()]
            self.bucket_manager.upload_dataframe_to_gcs(df_with_missing_indicators, last_file, file_format='parquet')
            self.logger.log_info(f"Uploaded DataFrame with missing indicators for {symbol}/{market}/{timeframe}.", context={'mod': 'TransformationTaLib', 'action': 'UploadMissingIndicators', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
        except Exception as e:
            self.logger.log_error(f"Error calculating/uploading missing indicators for {symbol}/{market}/{timeframe}: {e}", context={'mod': 'TransformationTaLib', 'action': 'UploadMissingIndicatorsError', 'symbol': symbol, 'market': market, 'timeframe': timeframe})

    def _process_incomplete_data(self, symbol, market, timeframe, file_list, df_existing, latest_end_date):
        try:
            start_date = latest_end_date + pd.Timedelta(days=1) if latest_end_date else None
            df_new = self.download_and_aggregate_klines(file_list)

            if not df_existing.empty:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            indicators_to_remove = [ind['name'] for ind in self.custom_indicators if ind['name'] in df_combined.columns]
            if indicators_to_remove:
                self.logger.log_info(f"Removing existing indicator columns: {indicators_to_remove}", context={'mod': 'TransformationTaLib', 'action': 'RemoveExistingIndicators', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
                df_combined = df_combined.drop(columns=indicators_to_remove, errors='ignore')

            ta_indicator_controller = TAIndicatorController(df_combined)
            df_with_indicators = ta_indicator_controller.compute_custom_indicators(self.custom_indicators)
            df_with_indicators = df_with_indicators.loc[:, ~df_with_indicators.columns.duplicated()]

            self._upload_new_dataframe(symbol, market, timeframe, df_with_indicators)
        except Exception as e:
            self.logger.log_error(f"Error processing incomplete data for {symbol}/{market}/{timeframe}: {e}", context={'mod': 'TransformationTaLib', 'action': 'ProcessIncompleteDataError', 'symbol': symbol, 'market': market, 'timeframe': timeframe})

    def _upload_new_dataframe(self, symbol, market, timeframe, df_with_indicators):
        try:
            start_date = df_with_indicators['timestamp'].min()
            end_date = df_with_indicators['timestamp'].max()
            output_gcs_path = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/data_{start_date:%Y_%m_%d}_{end_date:%Y_%m_%d}.parquet"

            if self.bucket_manager.check_file_exists(output_gcs_path):
                self.logger.log_info(f"Deleting existing file: {output_gcs_path}", context={'mod': 'TransformationTaLib', 'action': 'DeleteExistingFile', 'path': output_gcs_path})
                self.bucket_manager.delete_file(output_gcs_path)

            self.bucket_manager.upload_dataframe_to_gcs(df_with_indicators, output_gcs_path, file_format='parquet')
            self.logger.log_info(f"Uploaded new DataFrame for {symbol}/{market}/{timeframe}.", context={'mod': 'TransformationTaLib', 'action': 'UploadNewDataFrame', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
        except Exception as e:
            self.logger.log_error(f"Error uploading new DataFrame for {symbol}/{market}/{timeframe}: {e}", context={'mod': 'TransformationTaLib', 'action': 'UploadNewDataFrameError', 'symbol': symbol, 'market': market, 'timeframe': timeframe})
