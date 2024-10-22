import re
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.financial.calculation.ta_lib.talib_controller import TAIndicatorController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.archives.zip_controller import ZipController
from src.libs.utils.sys.threading.controller_threading import ThreadController


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
        """
        Extracts the end date from the file path.
        Supports two formats:
        1. data_YYYY_MM_DD_YYYY_MM_DD.parquet (where the date is in the filename)
        2. Raw/binance-data-vision/historical/ADAUSDT/futures/klines/1h/YYYY/MM/DD/data.parquet
        """
        try:
            # Essayer de capturer la date avec le format complet data_YYYY_MM_DD_YYYY_MM_DD.parquet
            match = re.search(r'data_(\d{4})_(\d{2})_(\d{2})_(\d{4})_(\d{2})_(\d{2})\.parquet', file_path)
            if match:
                # Extraire la date de fin (les trois dernières valeurs du match)
                start_year, start_month, start_day, end_year, end_month, end_day = map(int, match.groups())
                end_date = datetime(end_year, end_month, end_day)
                return end_date

            # Si le premier format échoue, essayer d'extraire la date du chemin du fichier
            match = re.search(r'(\d{4})/(\d{2})/(\d{2})/data\.parquet', file_path)
            if match:
                # Extraire la date à partir du chemin (ex: /2020/04/26/)
                year, month, day = map(int, match.groups())
                end_date = datetime(year, month, day)
                return end_date

            # Si aucun format ne correspond, logguer un avertissement
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
        """
        Compute and upload indicators for all symbol/market/timeframe groups in parallel.
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

        # Process each (symbol, market, timeframe) group in parallel using threads
        for (symbol, market, timeframe), file_list in files_by_group.items():
            # Add each task to the thread controller
            self.thread_controller.add_thread(self, "process_group_task", symbol, market, timeframe, file_list)

        # Start and manage all threads for parallel execution
        self.thread_controller.start_all()
        self.thread_controller.stop_all()


    def process_group_task(self, symbol, market, timeframe, file_list):
        self.logger.log_info(f"Traitement des données pour {symbol} / {market} / {timeframe}...")
        # Partie 1 : Gérer les chemins de sortie (output paths)
        output_path_prefix = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/"
        existing_files = self.bucket_manager.list_files(output_path_prefix)
        latest_end_date = None
        df_existing = pd.DataFrame()

        if existing_files:
            self.logger.log_info(f"File already exists for {symbol} / {market} / {timeframe}...")
            # Extraire la date de fin du dernier fichier de sortie (output)
            last_file = sorted(existing_files)[-1]
            latest_end_date = self.extract_metadata_from_file(last_file)
            
            # Charger les données existantes dans un DataFrame
            df_existing = self.bucket_manager.load_gcs_file_to_dataframe(last_file, file_format='parquet')
            
            # Si la date de fin est aujourd'hui, vérifier les indicateurs
            if latest_end_date and latest_end_date.date() == datetime.now().date():
                self.logger.log_info(f"File for {symbol} / {market} / {timeframe}... is complete, checking for indicators")
                missing_indicators = [ind['name'] for ind in self.custom_indicators if ind['name'] not in df_existing.columns]
                
                if not missing_indicators:
                    self.logger.log_info(f"Tous les indicateurs sont déjà présents dans {last_file} pour {symbol}/{market}/{timeframe}.")
                    return
                else:
                    self.logger.log_info(f"Indicateurs manquants pour {symbol}/{market}/{timeframe}: {missing_indicators}")

                    # Calculer les indicateurs manquants
                    ta_indicator_controller = TAIndicatorController(df_existing)

                    try:
                        df_with_missing_indicators = ta_indicator_controller.compute_custom_indicators(
                            [{'name': ind_name} for ind_name in missing_indicators]
                        )
                    except Exception as e:
                        self.logger.log_error(f"Erreur lors du calcul des indicateurs manquants pour {symbol} / {market} / {timeframe}: {e}")
                        return

                    # Supprimer les colonnes en double avant de sauvegarder
                    df_with_missing_indicators = df_with_missing_indicators.loc[:, ~df_with_missing_indicators.columns.duplicated()]

                    # Sauvegarder la DataFrame mise à jour avec les indicateurs manquants
                    output_gcs_path = last_file  # On réécrit sur le fichier existant
                    try:
                        self.bucket_manager.upload_dataframe_to_gcs(df_with_missing_indicators, output_gcs_path, file_format='parquet')
                        self.logger.log_info(f"Uploaded DataFrame with missing indicators for {symbol} / {market} / {timeframe} to GCS.")
                    except Exception as e:
                        self.logger.log_error(f"Erreur lors de l'upload des indicateurs manquants vers GCS pour {symbol} / {market} / {timeframe}: {e}")
                    return  # On passe au prochain groupe
            
            elif latest_end_date:
                self.logger.log_info(f"File for {symbol} / {market} / {timeframe}... is incomplete")
                start_date = latest_end_date + pd.Timedelta(days=1)
                # Filtrer les fichiers en fonction du timeframe et symbol correct
                files_to_process = [file for file in file_list if self._extract_metadata_from_path(file) 
                            and self._extract_metadata_from_path(file)[0] == symbol  # Vérification du symbol
                            and self._extract_metadata_from_path(file)[2] == timeframe  # Vérification du timeframe
                            and self.extract_metadata_from_file(file) >= start_date]

                df_new = self.download_and_aggregate_klines(files_to_process)
                if df_new.empty:
                    self.logger.log_warning(f"Aucune donnée disponible pour {symbol}, {market}, {timeframe}.")
                    return
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                
                # Suppression des colonnes d'indicateurs avant recalcul
                indicators_to_remove = [ind['name'] for ind in self.custom_indicators if ind['name'] in df_combined.columns]
                if indicators_to_remove:
                    self.logger.log_info(f"Removing existing indicator columns: {indicators_to_remove}")
                    df_combined = df_combined.drop(columns=indicators_to_remove, errors='ignore')
                    
                # Calculer les indicateurs techniques personnalisés pour toutes les données
                ta_indicator_controller = TAIndicatorController(df_combined)

                try:
                    df_with_indicators = ta_indicator_controller.compute_custom_indicators(self.custom_indicators)
                except Exception as e:
                    self.logger.log_error(f"ERREUR lors du calcul des indicateurs pour {symbol} / {market} / {timeframe}: {e}")
                    return
                
                # Supprimer les colonnes en double après le recalcul
                df_with_indicators = df_with_indicators.loc[:, ~df_with_indicators.columns.duplicated()]
                
                # 1. Calculer la nouvelle plage de dates
                start_date = df_combined['timestamp'].min()
                end_date = df_combined['timestamp'].max()
                
                # 2. Générer un nouveau chemin basé sur la nouvelle plage de dates, en gardant le même format
                output_gcs_path = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/data_{start_date:%Y_%m_%d}_{end_date:%Y_%m_%d}.parquet"

                # 3. Supprimer le fichier existant avant de sauvegarder le nouveau fichier
                try:
                    # Vérifier si le fichier existe déjà dans GCS
                    if self.bucket_manager.check_file_exists(output_gcs_path):
                        self.logger.log_info(f"Fichier existant trouvé : {last_file}. Suppression du fichier avant de réécrire.")
                        self.bucket_manager.delete_file(last_file)
                    
                    # Réécriture du fichier avec le nouveau nom basé sur les dates
                    self.bucket_manager.upload_dataframe_to_gcs(df_with_indicators, output_gcs_path, file_format='parquet')
                    self.logger.log_info(f"Uploaded DataFrame with complete indicators for {symbol} / {market} / {timeframe} to GCS at {output_gcs_path}.")
                except Exception as e:
                    self.logger.log_error(f"Erreur lors de l'upload du DataFrame vers GCS pour {symbol} / {market} / {timeframe}: {e}")
                return  
        
        else:
            self.logger.log_info(f"No existing file for {symbol} / {market} / {timeframe}...")
            df = self.download_and_aggregate_klines(file_list)
            ta_indicator_controller = TAIndicatorController(df)

            try:
                df_with_indicators = ta_indicator_controller.compute_custom_indicators(self.custom_indicators)
            except Exception as e:
                self.logger.log_error(f"ERREUR lors du calcul des indicateurs pour {symbol} / {market} / {timeframe}: {e}")
                return

            # Supprimer les colonnes en double après calcul
            df_with_indicators = df_with_indicators.loc[:, ~df_with_indicators.columns.duplicated()]

            # Générer le chemin de sortie basé sur la nouvelle plage de dates
            start_date = df_combined['timestamp'].min()
            end_date = df_combined['timestamp'].max()
            output_gcs_path = f"Transformed/cryptos/{symbol}/{market}/bars/time-bars/{timeframe}/data_{start_date:%Y_%m_%d}_{end_date:%Y_%m_%d}.parquet"

            try:
                self.bucket_manager.upload_dataframe_to_gcs(df_with_indicators, output_gcs_path, file_format='parquet')
                self.logger.log_info(f"Uploaded DataFrame for {symbol} / {market} / {timeframe} to GCS.")
            except Exception as e:
                self.logger.log_error(f"Erreur lors de l'upload du DataFrame vers GCS pour {symbol} / {market} / {timeframe}: {e}")
