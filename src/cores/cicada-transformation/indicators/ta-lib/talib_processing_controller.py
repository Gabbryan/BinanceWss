import re
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.financial.calculation.ta_lib.talib_controller import TAIndicatorController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController


class TransformationTaLib:

    def __init__(self):
        self.EnvController = EnvController()
        self.logger = LoggingController("Ta-lib controller")
        self.zip_controller = ZipController()
        self.bucket_name = self.EnvController.get_yaml_config('Ta-lib', 'bucket')
        self.bucket_manager = GCSController(self.bucket_name)
        self.custom_indicators = self.EnvController.get_yaml_config('Ta-lib', 'custom_indicators')

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
        for gcs_file in gcs_paths:
            try:
                file_path = f"gs://{self.bucket_name}/{gcs_file}"
                df_part = self.read_parquet_with_pyarrow(file_path)
                if 'open_time' in df_part.columns:
                    df_part['timestamp'] = pd.to_datetime(df_part['open_time'], unit='ms')
                elif 'timestamp' in df_part.columns:
                    df_part['timestamp'] = pd.to_datetime(df_part['timestamp'], unit='ms')
                else:
                    self.logger.log_error(f"Le fichier {gcs_file} ne contient pas de colonne 'open_time' ou 'timestamp'.")
                    continue

                columns_to_keep = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df_part = df_part[[col for col in columns_to_keep if col in df_part.columns]]
                df_combined = pd.concat([df_combined, df_part], ignore_index=True)

            except Exception as e:
                self.logger.log_error(f"Erreur lors du traitement du fichier {gcs_file}: {e}")
        return df_combined

    def compute_and_upload_indicators(self):
        root_path = "Raw/binance-data-vision/historical/"
        gcs_files = self.bucket_manager.list_files(root_path, folder_name="klines")

        if not gcs_files:
            self.logger.log_warning("Aucun fichier trouvé dans le bucket GCS.")
            return

        files_by_group = {}
        for gcs_file in gcs_files:
            symbol, market, timeframe = self._extract_metadata_from_path(gcs_file)
            if symbol and market and timeframe:
                key = (symbol, market, timeframe)
                if key not in files_by_group:
                    files_by_group[key] = []
                files_by_group[key].append(gcs_file)

        # Process each (symbol, market, timeframe) group separately
        for (symbol, market, timeframe), file_list in files_by_group.items():
            self.logger.log_info(f"Traitement des données pour {symbol} / {market} / {timeframe}...")
            output_path_template = "Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/data_{start_year:02d}_{start_month:02d}_{start_day:02d}_{end_year:02d}_{end_month:02d}_{end_day:02d}.parquet"
            existing_files = self.bucket_manager.list_files(f"Transformed/{symbol}/{market}/bars/time-bars/{timeframe}/")
            if existing_files:
                #   Validate the date process
                last_file = sorted(existing_files)[-1]
                match = re.search(r'data_(\d{4})_(\d{2})_(\d{2})_(\d{4})_(\d{2})_(\d{2})\.parquet', last_file)
                if match:
                    start_year, start_month, start_day, end_year, end_month, end_day = map(int, match.groups())
                    end_date = datetime(end_year, end_month, end_day)
                    self.logger.log_info(f"Latest existing file: {last_file}, processing from {end_date + pd.Timedelta(days=1)} onwards.")
            else:
                self.logger.log_info(f"No existing file found for {symbol}, {market}, {timeframe}. Processing all data.")

            # Download and aggregate klines for the new date range
            df = self.download_and_aggregate_klines(file_list)
            if df.empty:
                self.logger.log_warning(f"Aucune donnée disponible pour {symbol}, {market}, {timeframe}.")
                continue

            # Compute custom technical indicators
            ta_indicator_controller = TAIndicatorController(df)
            custom_indicators = self.custom_indicators

            try:
                df_with_indicators = ta_indicator_controller.compute_custom_indicators(custom_indicators)
            except Exception as e:
                self.logger.log_error(f"Erreur lors du calcul des indicateurs pour {symbol} / {market} / {timeframe}: {e}")
                continue

            # Generate output path based on the new date range
            start_date = df['timestamp'].min()
            end_date = df['timestamp'].max()
            output_gcs_path = output_path_template.format(
                symbol=symbol, market=market, timeframe=timeframe,
                start_year=start_date.year, start_month=start_date.month, start_day=start_date.day,
                end_year=end_date.year, end_month=end_date.month, end_day=end_date.day
            )

            try:
                self.bucket_manager.upload_dataframe_to_gcs(df_with_indicators, output_gcs_path, file_format='parquet')
                self.logger.log_info(f"Uploaded DataFrame for {symbol} / {market} / {timeframe} to GCS.")
            except Exception as e:
                self.logger.log_error(f"Erreur lors de l'upload du DataFrame vers GCS pour {symbol} / {market} / {timeframe}: {e}")
