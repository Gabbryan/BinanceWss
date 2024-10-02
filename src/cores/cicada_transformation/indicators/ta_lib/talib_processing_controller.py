import itertools
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import os
import sys
import tempfile
import findspark
import pytz
import schedule
import time
import traceback
from google.cloud import storage
import pandas as pd
import requests
import gcsfs

from src.libs.utils.spark.spark_controller import SparkController
from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_slack import NotificationsSlackController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.utils.sys.threading.controller_threading import ThreadController
from src.libs.financial.calculation.ta_lib.talib_controller import TAIndicatorController

class TransformationTalib:
    
    def __init__(self, request_limit=5, request_interval=1):
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.EnvController = EnvController()
        self.logger = LoggingController("Ta-lib")
        self.zip_controller = ZipController()
        self.base_url = self.EnvController.get_yaml_config('Ta-lib', 'base_url')
        self.bucket_name = self.EnvController.get_yaml_config('Ta-lib', 'bucket')
        self.GCSController = GCSController(self.bucket_name)
        self.symbol = self.EnvController.get_yaml_config('Ta-lib', 'symbol')
        self.assets = self.EnvController.get_yaml_config('Ta-lib', 'assets')
        self.broker = self.EnvController.get_yaml_config('Ta-lib', 'broker')
        self.bars = self.EnvController.get_yaml_config('Ta-lib', 'bars')
        self.market = self.EnvController.get_yaml_config('Ta-lib', 'market')
        self.custom_indicators = self.EnvController.get_yaml_config('Ta-lib', 'custom_indicators')
        self.stream = self.EnvController.get_yaml_config('Ta-lib',
                                                         'stream') if self.EnvController.env == 'development' else self.EnvController.get_env('STREAM')
        self.notifications_slack_controller = NotificationsSlackController(f"Binance Data Vision - {self.stream}")
        self.timeframe = self.EnvController.get_yaml_config('Ta-lib', 'timeframe')
        self.fetch_urls = []
        self.thread_controller = ThreadController()
        self.ta_indicator_controller = TAIndicatorController()
        self.fs = gcsfs.GCSFileSystem()

    def downwload_and_aggregate_klines(self, start_date, end_date, klines=True):
        formatted_start_date = start_date.strftime("%Y-%m-%d")
        formatted_end_date = end_date.strftime("%Y-%m-%d")
        market = self.market

        params = {
            'symbol': self.symbol,
            'market': self.market,
            'timeframe': self.timeframe,
            'start_year': start_date.year,
            'start_month': start_date.month,
            'start_day': start_date.day,
            'end_year': end_date.year,
            'end_month': end_date.month,
            'end_day': end_date.day,
            'stream': self.stream,
            'bars': self.bars,
            'timeframe': self.timeframe
        }

        year_range = range(start_date.year, end_date.year + 1)
        month_range = range(1, 13)
        day_range = range(1,32)
        
        params['year_range'] = year_range
        params['month_range'] = month_range
        params['day_range'] = day_range

        df = None

        if klines==True:
            path = "production-trustia-raw-data/Raw/binance-data-vision/historical/{symbol}/{market}/{stream}/{timeframe}/{year}/{month:02d}/{day:02d}/data.parquet"
        else:
            path = "production-trustia-raw-data/Raw/binance-data-vision/historical/{symbol}/{market}/{stream}/{year}/{month:02d}/{day:02d}/data.parquet"

        gcs_paths = self.GCSController.generate_gcs_paths(params, path)
        print(gcs_paths)
        for gcs_path in gcs_paths:
            try:
                
                with self.fs.open(gcs_path) as f:
                    df0 = pd.read_parquet(f)                    
                
                if df0 is not None:
                    if params['stream'] == 'klines':
                        new_column_names = ["open_time", "Open", "High", "Low", "Close", "Volume", "close_time", "quote_volume", "trades_nb", "taker_buy_volume", "taker_buy_quote_volume", "unused"]
                    elif params['stream'] == 'aggTrades':
                        new_column_names = ["agg_trades_id", "price", "quantity", "first_trade_id", "last_trade_id", "time", "is_buyer_maker"]

                    df0.rename(columns=dict(zip(df0.columns, new_column_names)), inplace=True)

                    df0.sort_values(by="open_time", inplace=True)

                    if df is None:
                        df = df0
                    else:
                        df = pd.concat([df, df0], ignore_index=True)
            except Exception as e:
                self.logger.log_error(f"Erreur lors de la lecture du fichier {gcs_path}: {e}")

        return df
    
    def compute_and_upload_indicators(self, start_date, end_date):
        """
        Télécharger et agréger les données de klines, calculer les indicateurs techniques personnalisés, 
        puis uploader les données enrichies sur GCS.
        """
        # Étape 1: Télécharger et agréger les données de klines
        df = self.downwload_and_aggregate_klines(start_date=start_date, end_date=end_date, klines=True)
        
        if df is None or df.empty:
            self.logger.log_warning("Aucune donnée disponible pour les dates spécifiées.")
            return None
        
        # Étape 2: Initialiser le contrôleur d'indicateurs techniques avec le DataFrame téléchargé
        self.ta_indicator_controller = TAIndicatorController(df)

        # Étape 3: Calculer les indicateurs personnalisés
        custom_indicators = self.custom_indicators
        
        try:
            df_with_indicators = self.ta_indicator_controller.compute_custom_indicators(custom_indicators)
        except Exception as e:
            self.logger.log_error(f"Erreur lors du calcul des indicateurs: {e}")
            return None

        # Étape 4: Générer le chemin de sortie pour GCS
        path_template = "Transformed/{assets}/{broker}/{bars}/{symbol}/{timeframe}/indicators/data_{start_year:02d}_{start_month:02d}_{start_day:02d}_{end_year:02d}_{end_month:02d}_{end_day:02d}"
        params = {
            'assets': self.assets,
            'broker': self.broker,
            'bars': self.bars,
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'start_year': start_date.year,
            'start_month': start_date.month,
            'start_day': start_date.day,
            'end_year': end_date.year,
            'end_month': end_date.month,
            'end_day': end_date.day
        }

        # Utilisation de generate_gcs_paths pour créer le chemin de sortie unique
        gcs_output_paths = self.GCSController.generate_gcs_paths(params, path_template)
        
        if len(gcs_output_paths) == 0:
            self.logger.log_error("Erreur lors de la génération du chemin GCS.")
            return None

        # Nous prenons le premier chemin généré (celui qui correspond à l'intervalle spécifié)
        output_gcs_path = gcs_output_paths[0]

        # Étape 5: Uploader le DataFrame avec les indicateurs sur GCS
        try:
            self.GCSController.upload_dataframe_to_gcs(df_with_indicators, output_gcs_path, file_format='parquet')
        except Exception as e:
            self.logger.log_error(f"Erreur lors de l'upload du DataFrame vers GCS: {e}")
            return None

        # Étape 6: Retourner le DataFrame enrichi d'indicateurs
        self.logger.log_info("Calcul des indicateurs terminé avec succès et DataFrame uploadé sur GCS.")
        return df_with_indicators
