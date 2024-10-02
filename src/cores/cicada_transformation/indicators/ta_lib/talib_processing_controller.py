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

    def process_task(self, start_date, end_date, klines=True):
        formatted_start_date = start_date.strftime("%Y-%m-%d")
        formatted_end_date = end_date.strftime("%Y-%m-%d")
        market = self.market

        # Préparation des paramètres pour générer les chemins GCS
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

        # Range pour les années et mois à inclure
        year_range = range(start_date.year, end_date.year + 1)
        month_range = range(1, 13)  # Optionnel : ajustez selon le mois de début/fin si nécessaire
        
        params['year_range'] = year_range
        params['month_range'] = month_range

        # Initialisation du DataFrame vide
        df = None

        # Générez les chemins GCS
        for date in pd.date_range(start_date, end_date, freq="D"):
            params['year'] = date.year
            params['month'] = date.month
            params['day'] = date.day

            if klines==True:
                path = "production-trustia-raw-data/Raw/binance-data-vision/historical/{symbol}/{market}/{stream}/{timeframe}/{year}/{month:02d}/{day:02d}/data.parquet"
            else:
                path = "production-trustia-raw-data/Raw/binance-data-vision/historical/{symbol}/{market}/{stream}/{year}/{month:02d}/{day:02d}/data.parquet"

            gcs_paths = self.GCSController.generate_gcs_paths(params, path)
            print(gcs_paths)
            
            for gcs_path in gcs_paths:
                try:
                    # Lecture du fichier Parquet à partir de GCS avec pandas et gcsfs
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
                    
        output_path = "/root/Trustia/Cicada-binance/src/cores/cicada_transformation/indicators/ta_lib/data_output.parquet"

        if not df.empty:
            try:
                df.to_parquet(output_path, index=False)
                self.logger.log_info(f"DataFrame exporté avec succès à l'emplacement: {output_path}")
            except Exception as e:
                self.logger.log_error(f"Erreur lors de l'exportation du DataFrame en Parquet: {e}")
        else:
            self.logger.log_warning("Le DataFrame est vide, aucune donnée à exporter.")
            
        return df
