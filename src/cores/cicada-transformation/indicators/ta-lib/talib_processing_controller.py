import re

import pandas as pd

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_controller import NotificationsController
from src.libs.financial.calculation.ta_lib.talib_controller import TAIndicatorController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController


class TransformationTaLib:

    def __init__(self):
        self.EnvController = EnvController()
        self.logger = LoggingController("Ta-lib")
        self.zip_controller = ZipController()
        self.bucket_name = self.EnvController.get_yaml_config('Ta-lib', 'bucket')
        self.bucket_manager = GCSController(self.bucket_name)
        self.custom_indicators = self.EnvController.get_yaml_config('Ta-lib', 'custom_indicators')
        self.notifications_slack_controller = NotificationsController(f"Ta libs controller")

    def _extract_metadata_from_path(self, gcs_path):
        """
        Extract symbol, market, klines, and timeframe from the GCS file path.

        Expected format: production-trustia-raw-data/Raw/binance-data-vision/historical/{symbol}/{market}/{klines}/{timeframe}/...
        """
        try:
            # Regex to extract symbol, market, klines type, and timeframe from the GCS path
            match = re.search(
                r'historical/(?P<symbol>[^/]+)/(?P<market>[^/]+)/(?P<klines>[^/]+)/(?P<timeframe>[^/]+)/', gcs_path)
            if match:
                return match.group('symbol'), match.group('market'), match.group('timeframe')
        except Exception as e:
            self.logger.log_error(
                f"Erreur lors de l'extraction des métadonnées à partir du chemin {gcs_path}: {e}")
        return None, None, None

    def download_and_aggregate_klines(self, gcs_paths):
        """
        Aggregate parquet files from the given GCS paths into one DataFrame.

        :param gcs_paths: List of GCS paths to parquet files.
        :return: Combined pandas DataFrame.
        """
        df_combined = pd.DataFrame()

        for gcs_file in gcs_paths:
            try:
                file_path = f"gs://{self.bucket_name}/{gcs_file}"
                df_part = pd.read_parquet(file_path)
                df_combined = pd.concat([df_combined, df_part], ignore_index=True)
            except Exception as e:
                self.logger.log_error(
                    f"Erreur lors du traitement du fichier {gcs_file}: {e}")

        return df_combined

    def compute_and_upload_indicators(self):
        """
        Download and aggregate klines data for all symbols, markets, and timeframes, compute custom technical indicators,
        and upload the enriched data to GCS.
        :return: None
        """
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

        for (symbol, market, timeframe), file_list in files_by_group.items():
            self.logger.log_info(f"Traitement des données pour {symbol} / {market} / {timeframe}...")

            df = self.download_and_aggregate_klines(file_list)

            if df.empty:
                self.logger.log_warning(f"Aucune donnée disponible pour {symbol}, {market}, {timeframe}.")
                continue

            # Extract start_date and end_date from the DataFrame
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                start_date = df['timestamp'].min()
                end_date = df['timestamp'].max()
            else:
                self.logger.log_error(f"Timestamp column not found for {symbol} / {market} / {timeframe}")
                continue

            # Compute custom technical indicators
            self.ta_indicator_controller = TAIndicatorController(df)
            custom_indicators = self.custom_indicators

            try:
                df_with_indicators = self.ta_indicator_controller.compute_custom_indicators(
                    custom_indicators)
            except Exception as e:
                self.logger.log_error(
                    f"Erreur lors du calcul des indicateurs pour {symbol} / {market} / {timeframe}: {e}")
                continue

            # Define the output GCS path
            path_template = "/Transformed/{symbol}/{market}/bars/time-bars/{timeframe}/data_{start_year:02d}_{start_month:02d}_{start_day:02d}_{end_year:02d}_{end_month:02d}_{end_day:02d}.parquet"
            params = {
                'symbol': symbol,
                'market': market,
                'timeframe': timeframe,
                'start_year': start_date.year,
                'start_month': start_date.month,
                'start_day': start_date.day,
                'end_year': end_date.year,
                'end_month': end_date.month,
                'end_day': end_date.day
            }

            output_gcs_path = path_template.format(**params)

            try:
                self.bucket_manager.upload_dataframe_to_gcs(
                    df_with_indicators, output_gcs_path, file_format='parquet')
            except Exception as e:
                self.logger.log_error(
                    f"Erreur lors de l'upload du DataFrame vers GCS pour {symbol} / {market} / {timeframe}: {e}")
                continue

            self.logger.log_info(
                f"Calcul des indicateurs terminé avec succès pour {symbol} / {market} / {timeframe}. DataFrame uploadé sur GCS.")
