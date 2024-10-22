import os
import tempfile
from google.cloud import storage

import pandas as pd

from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.google_cloud_bucket.server_gcs import GCSClient

# Initialize the logging controller
logger = LoggingController("GCSController")


class GCSController:
    def __init__(self, bucket_name):
        """
        Initialize the GCS controller to manage high-level operations.

        :param bucket_name: The name of the GCS bucket.
        """
        self.gcs_client = GCSClient(bucket_name)
        self.bucket_name = bucket_name
        logger.log_info(f"GCS Controller initialized with bucket: {bucket_name}", context={'mod': 'GCSController', 'action': 'Init'})
        
    def load_gcs_file_to_dataframe(self, file_path, file_format='parquet'):
        """
        Télécharge un fichier à partir de Google Cloud Storage et le charge dans un DataFrame.

        :param file_path: Le chemin du fichier dans le bucket.
        :param file_format: Le format du fichier ('parquet', 'csv', etc.).
        :return: DataFrame contenant les données du fichier.
        """
        try:
            # Créer un client GCS
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            
            # Récupérer le blob (fichier) depuis le bucket
            blob = bucket.blob(file_path)
            
            # Créer un fichier temporaire local
            temp_file_path = '/tmp/temp_file.' + file_format
            
            # Télécharger le fichier depuis GCS
            blob.download_to_filename(temp_file_path)
            logger.log_info(f"Fichier téléchargé depuis GCS : {temp_file_path}", context={'mod': 'GCSController', 'action': 'DownloadFile'})
            
            # Charger le fichier dans un DataFrame
            if file_format == 'parquet':
                df = pd.read_parquet(temp_file_path)
            elif file_format == 'csv':
                df = pd.read_csv(temp_file_path)
            else:
                raise ValueError(f"Format de fichier non supporté : {file_format}")
            
            # Retourner le DataFrame
            return df
        
        except Exception as e:
            logger.log_error(f"Erreur lors du téléchargement ou du chargement du fichier : {e}", context={'mod': 'GCSController', 'action': 'LoadFileError'})
            return None

    def list_files(self, prefix, folder_name=None):
        """
        List all files in the GCS bucket recursively under a given prefix (folder).

        :param prefix: The root path in the GCS bucket.
        :param folder_name: (Optional) The folder name to include in the file paths (e.g., 'klines').
        :return: A list of file paths that include the specified folder.
        """
        try:
            logger.log_info(f"Listing files in GCS bucket {self.bucket_name} under prefix: {prefix}",
                            context={'mod': 'GCSClient', 'action': 'ListFiles'})

            blobs_iterator = self.gcs_client.bucket.list_blobs(prefix=prefix)

            blobs = list(blobs_iterator)

            # Filter files that contain the folder_name (if provided)
            if folder_name:
                file_list = [blob.name for blob in blobs if f"/{folder_name}/" in blob.name and not blob.name.endswith('/')]
            else:
                file_list = [blob.name for blob in blobs if not blob.name.endswith('/')]

            logger.log_info(f"Found {len(file_list)} files under {prefix} that contain folder: {folder_name}",
                            context={'mod': 'GCSClient', 'action': 'FilesListed'})
            return file_list
        except Exception as e:
            logger.log_error(f"Error listing files in GCS: {e}", context={'mod': 'GCSClient', 'action': 'ListFilesError'})
            return []

    def _generate_temp_file(self, file_format='parquet'):
        """
        Generate a secure temporary file with the specified file format.

        :param file_format: The file extension (e.g., 'parquet', 'csv')
        :return: The path to the temporary file.
        """
        suffix = f'.{file_format}'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            return temp_file.name

    def _save_data_to_temp(self, data, file_format='parquet'):
        """
        Save data to a temporary file in the specified format.

        :param data: The data to be saved (pandas DataFrame, etc.).
        :param file_format: The file format to save (default is 'parquet').
        :return: The path to the saved temporary file.
        """
        temp_file = self._generate_temp_file(file_format)

        # Handle saving based on file format
        if file_format == 'parquet':
            data.to_parquet(temp_file, index=False)
        elif file_format == 'csv':
            data.to_csv(temp_file, index=False)
        elif file_format == 'xlsx':
            data.to_excel(temp_file, index=False)
        elif file_format == 'json':
            data.to_json(temp_file, orient='records', lines=True)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        logger.log_info(f"Data saved to temporary file: {temp_file}", context={'mod': 'GCSController', 'action': 'SaveData'})
        return temp_file

    def _upload_to_gcs(self, local_file, gcs_path):
        """
        Upload a file to GCS and handle cleanup.

        :param local_file: The local file path.
        :param gcs_path: The GCS destination path.
        """
        try:
            self.gcs_client.upload_file(local_file, gcs_path)
            logger.log_info(f"Uploaded {local_file} to GCS path {gcs_path}", context={'mod': 'GCSController', 'action': 'UploadToGCS'})
        except Exception as e:
            logger.log_error(f"Error uploading file to GCS: {e}", context={'mod': 'GCSController', 'action': 'UploadError'})
            raise

    def _remove_temp_file(self, temp_file):
        """
        Remove the temporary file after it has been uploaded to GCS.

        :param temp_file: The path to the temporary file to remove.
        """
        try:
            os.remove(temp_file)
            logger.log_info(f"Removed temporary file: {temp_file}", context={'mod': 'GCSController', 'action': 'RemoveTempFile'})
        except Exception as e:
            logger.log_error(f"Error removing temporary file: {e}", context={'mod': 'GCSController', 'action': 'RemoveTempFileError'})

    def check_file_exists(self, gcs_path):
        """
        Check if a file exists in the GCS bucket.

        :param gcs_path: The GCS path to check for file existence.
        :return: True if the file exists, False otherwise.
        """
        try:
            blobs = list(self.gcs_client.bucket.list_blobs(prefix=gcs_path))
            file_exists = any(blob.name == gcs_path for blob in blobs)

            if file_exists:
                logger.log_info(f"File {gcs_path} exists in GCS.", context={'mod': 'GCSController', 'action': 'CheckFileExists'})
            else:
                logger.log_info(f"File {gcs_path} does not exist in GCS.", context={'mod': 'GCSController', 'action': 'CheckFileDoesNotExist'})

            return file_exists
        except Exception as e:
            logger.log_error(f"Error checking if file exists in GCS: {e}", context={'mod': 'GCSController', 'action': 'CheckFileExistsError'})
            return False

    def upload_dataframe_to_gcs(self, df, gcs_path, file_format='parquet', is_exist_verification=True):
        """
        Save a pandas DataFrame as a Parquet file and upload it to GCS, ensuring the columns (header) are included.

        :param is_exist_verification: A boolean indicating whether to verify if the file already exists in GCS before uploading.
        :param df: The pandas DataFrame to be saved and uploaded.
        :param gcs_path: The destination path in the GCS bucket.
        :param file_format: The file format to save the DataFrame (default is 'parquet').
        """
        logger.log_info(f"Starting DataFrame upload to GCS as {file_format} format, ensuring columns (header) are saved.",
                        context={'mod': 'GCSController', 'action': 'StartDFUpload'})

        temp_file = None
        try:
            if is_exist_verification:
                if self.check_file_exists(gcs_path):
                    logger.log_info(f"File {gcs_path} already exists. Skipping upload.", context={'mod': 'GCSController', 'action': 'FileExists'})
                    return

            # Ensure the DataFrame is saved with its header (column names)
            temp_file = self._save_data_to_temp(df, file_format)

            # Upload the file to GCS
            self._upload_to_gcs(temp_file, gcs_path)
            logger.log_info(f"Uploaded DataFrame to {gcs_path} in GCS, including columns.", context={'mod': 'GCSController', 'action': 'DFUploadSuccess'})

        except Exception as e:
            logger.log_error(f"Error uploading DataFrame to GCS: {e}", context={'mod': 'GCSController', 'action': 'DFUploadError'})
            raise

        finally:
            if temp_file is not None:
                self._remove_temp_file(temp_file)

    def generate_gcs_paths(self, parameters, template, file_format="parquet"):
        """
        Generate GCS paths based on a flexible template.

        :param parameters: A dictionary containing the values to populate the template (e.g., {'symbol': 'BTC', 'year_range': range(2023, 2024)}).
        :param template: A string template for generating the GCS path. Use placeholders like {symbol}, {year}, {month}, etc.
        :param file_format: The file format to be used in the path (default is 'parquet').
        :return: A list of GCS paths with placeholders replaced by the actual parameter values.
        """
        logger.log_info(f"Generating GCS paths with template: {template} and parameters: {parameters}",
                        context={'mod': 'GCSController', 'action': 'GeneratePaths'})

        gcs_paths = []

        year_range = parameters.get('year_range', [])
        month_range = parameters.get('month_range', [])
        day_range = parameters.get('day_range', [])
        parameters['file_format'] = file_format

        end_year = parameters.get('end_year', None) or parameters.get('year', None)
        end_month = parameters.get('end_month', None) or parameters.get('month', None)
        end_day = parameters.get('end_day', None) or parameters.get('day', None)

        end_date = pd.Timestamp(year=end_year, month=end_month, day=end_day)

        if year_range and month_range:
            for year in year_range:
                for month in month_range:
                    for day in day_range:
                        try:
                            # Créer une date avec les valeurs actuelles de year, month, day
                            current_date = pd.Timestamp(year=year, month=month, day=day)

                            # Si la date dépasse la end_date, arrêter la génération de chemins
                            if current_date > end_date:
                                break

                            parameters['year'] = int(year)
                            parameters['month'] = int(month)
                            parameters['day'] = int(day)

                            # Générer le chemin GCS
                            path = template.format(**parameters)
                            gcs_paths.append(path)
                        except ValueError:
                            # Ignorer les dates non valides (comme le 31 février)
                            continue
                        except KeyError as e:
                            logger.log_error(f"Missing key in parameters for path generation: {e}")
        else:
            try:
                path = template.format(**parameters)
                gcs_paths.append(path)
            except KeyError as e:
                logger.log_error(f"Missing key in parameters for path generation: {e}")

        logger.log_info(f"Generated {len(gcs_paths)} GCS paths.", context={'mod': 'GCSController', 'action': 'GeneratePathsComplete'})
        return gcs_paths

    def download_parquet_as_dataframe(self, gcs_path):
        """
        Download a Parquet file from GCS and convert it into a Pandas DataFrame.

        :param gcs_path: The path to the Parquet file in GCS.
        :return: The DataFrame containing the data from the Parquet file.
        """
        try:
            bucket = self.gcs_client.bucket
            blob = bucket.blob(gcs_path)
            temp_file = self._generate_temp_file('parquet')

            # Download the Parquet file from GCS to a temporary file
            blob.download_to_filename(temp_file)
            logger.log_info(f"Downloaded Parquet file from GCS: {gcs_path}")

            # Read the temporary file into a DataFrame
            df = pd.read_parquet(temp_file)

            # Remove the temporary file after reading
            self._remove_temp_file(temp_file)

            return df
        except Exception as e:
            logger.log_error(f"Error downloading Parquet file from GCS: {e}")
            raise
